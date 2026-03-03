#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import importlib
import json
import os
import platform
import re
import shutil
import subprocess
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Dict, List

import requests

from tender_radar import (
    account_filings,
    build_shortlist,
    create_ch_session,
    detect_currency_and_unit,
    document_pdf_url,
    download_pdf,
    extract_audit_fee,
    extract_external_auditor,
    load_api_key_from_file,
    load_dotenv_file,
    make_row,
    parse_year,
    search_companies,
    write_csv,
)

CH_BULK_INDEX_URL = "https://download.companieshouse.gov.uk/en_output.html"


def _normalize_company_number(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    return s.zfill(8) if s.isdigit() and len(s) < 8 else s


def _first_present(d: Dict[str, str], keys: List[str]) -> str:
    for k in keys:
        v = d.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def load_active_companies_from_csv(csv_path: Path, max_companies: int) -> List[Dict[str, str]]:
    """
    Load companies from CH bulk/basic CSV and keep active entries only.
    Expected columns can vary, so we probe common names.
    """
    out: List[Dict[str, str]] = []
    seen = set()
    with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            norm_row = {str(k).strip().lower(): (v if v is not None else "") for k, v in row.items()}

            status = _first_present(
                norm_row,
                [
                    "company_status",
                    "companystatus",
                    "status",
                ],
            ).lower()
            if status != "active":
                continue

            number = _normalize_company_number(
                _first_present(
                    norm_row,
                    [
                        "company_number",
                        "companynumber",
                        "companynumber",
                        "company number",
                    ],
                )
            )
            if not number or number in seen:
                continue
            seen.add(number)

            title = _first_present(
                norm_row,
                [
                    "company_name",
                    "companyname",
                    "title",
                    "company name",
                ],
            )
            out.append({"company_number": number, "title": title or number, "company_status": "active"})
            if max_companies > 0 and len(out) >= max_companies:
                break
    return out


def is_target_accounts_filing(filing: Dict[str, str]) -> bool:
    """
    Keep filings that likely contain annual report/accounts or statutory audit sections.
    """
    desc = str(filing.get("description", "")).lower()
    typ = str(filing.get("type", "")).lower()
    if filing.get("category") != "accounts":
        return False
    keywords = [
        "statutory audit",
        "annual report",
        "annual accounts",
        "full accounts",
        "audited",
        "group accounts",
    ]
    if any(k in desc for k in keywords):
        return True
    # Common full-accounts filing types at Companies House.
    return typ in {"aa", "aa01", "aa02", "aa03", "aa04", "aa06", "aa07"}


def _pick_bulk_zip_name(index_html: str) -> str:
    one_file = re.findall(r'href="(BasicCompanyDataAsOneFile-[^"]+\.zip)"', index_html, flags=re.IGNORECASE)
    if one_file:
        return sorted(one_file)[-1]
    split_files = re.findall(r'href="(BasicCompanyData-[^"]+\.zip)"', index_html, flags=re.IGNORECASE)
    if split_files:
        return sorted(split_files)[-1]
    return ""


def ensure_companies_csv_from_companies_house(cache_dir: Path) -> Path:
    """
    Download latest CH bulk BasicCompanyData zip and extract CSV.
    Returns extracted CSV path.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    resp = requests.get(CH_BULK_INDEX_URL, timeout=60)
    resp.raise_for_status()
    zip_name = _pick_bulk_zip_name(resp.text)
    if not zip_name:
        raise RuntimeError("Could not find BasicCompanyData zip on Companies House bulk download page.")

    zip_url = f"https://download.companieshouse.gov.uk/{zip_name}"
    zip_path = cache_dir / zip_name
    if not zip_path.exists():
        with requests.get(zip_url, stream=True, timeout=300) as r:
            r.raise_for_status()
            with zip_path.open("wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

    extract_dir = cache_dir / zip_path.stem
    extract_dir.mkdir(parents=True, exist_ok=True)
    csv_candidates = list(extract_dir.rglob("*.csv"))
    if not csv_candidates:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)
        csv_candidates = list(extract_dir.rglob("*.csv"))
    if not csv_candidates:
        raise RuntimeError(f"No CSV found after extracting {zip_path.name}")
    return sorted(csv_candidates, key=lambda p: p.stat().st_size if p.exists() else 0, reverse=True)[0]


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parent
    load_dotenv_file(root / ".env")
    default_key_file = Path(os.getenv("CH_API_KEY_FILE", str(root / "ch_api_key.txt")))
    p = argparse.ArgumentParser(description="Tender radar using MinerU for PDF extraction.")
    p.add_argument("--api-key", default=os.getenv("CH_API_KEY"), help="Companies House API key")
    p.add_argument("--api-key-file", default=str(default_key_file), help="API key file path")
    p.add_argument("--company-query", default="plc", help="Search query for companies")
    p.add_argument(
        "--companies-csv",
        default=os.getenv("TENDER_COMPANIES_CSV", ""),
        help="Optional full company CSV path (active companies loaded from file, bypass search query)",
    )
    p.add_argument(
        "--company-source",
        default=os.getenv("TENDER_COMPANY_SOURCE", "search"),
        choices=["search", "csv", "auto-all"],
        help="Company input source: search API / local CSV / auto-download CH full company data",
    )
    p.add_argument(
        "--companies-cache-dir",
        default=os.getenv("TENDER_COMPANIES_CACHE_DIR", str(Path(__file__).resolve().parent / "companies_house_cache")),
        help="Cache directory used by auto-all mode",
    )
    p.add_argument("--max-companies", type=int, default=100, help="Max active companies to process (<=0 means all)")
    p.add_argument("--max-filings-per-company", type=int, default=5, help="Recent filings per company")
    p.add_argument("--sleep-seconds", type=float, default=0.25, help="API throttle")
    p.add_argument("--include-all-accounts", action="store_true", help="Include every accounts filing type")
    p.add_argument(
        "--download-dir",
        default=os.getenv("TENDER_DOWNLOAD_DIR", str(root / "uk_accounts_pdfs")),
        help="Local PDF folder",
    )
    p.add_argument(
        "--history-csv",
        default=os.getenv("TENDER_HISTORY_CSV", str(root / "tender_history.csv")),
        help="Detailed output CSV",
    )
    p.add_argument(
        "--shortlist-csv",
        default=os.getenv("TENDER_SHORTLIST_CSV", str(root / "tender_shortlist.csv")),
        help="Priority shortlist CSV",
    )
    p.add_argument(
        "--mineru-output-dir",
        default=os.getenv("TENDER_MINERU_OUTPUT_DIR", str(root / "mineru_outputs")),
        help="MinerU output root directory",
    )
    p.add_argument(
        "--mineru-backend",
        default=os.getenv("TENDER_MINERU_BACKEND", "pipeline"),
        choices=[
            "pipeline",
            "hybrid-auto-engine",
            "hybrid-http-client",
            "vlm-auto-engine",
            "vlm-http-client",
        ],
        help="MinerU backend",
    )
    p.add_argument(
        "--mineru-method",
        default=os.getenv("TENDER_MINERU_METHOD", "auto"),
        choices=["auto", "txt", "ocr"],
        help="MinerU method",
    )
    p.add_argument("--mineru-lang", default=os.getenv("TENDER_MINERU_LANG", ""), help="MinerU language hint")
    p.add_argument(
        "--mineru-device",
        default=os.getenv("TENDER_MINERU_DEVICE", ""),
        help="MinerU device (e.g. mps/cpu/cuda). Empty means auto-detect.",
    )
    p.add_argument(
        "--mineru-formula",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("TENDER_MINERU_FORMULA", "true").lower() != "false",
        help="Enable formula parsing in MinerU",
    )
    p.add_argument(
        "--mineru-table",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("TENDER_MINERU_TABLE", "true").lower() != "false",
        help="Enable table parsing in MinerU",
    )
    p.add_argument(
        "--mineru-source",
        default=os.getenv("TENDER_MINERU_SOURCE", ""),
        help="MinerU model source (huggingface/modelscope/local)",
    )
    p.add_argument(
        "--mineru-force-refresh",
        action="store_true",
        help="Always rerun MinerU even if outputs already exist",
    )
    args, _unknown = p.parse_known_args()
    return args


def ensure_mineru_cli() -> bool:
    return shutil.which("mineru") is not None


def check_mineru_runtime_deps() -> List[str]:
    missing = []
    for pkg in ("ftfy",):
        try:
            importlib.import_module(pkg)
        except Exception:
            missing.append(pkg)
    return missing


def detect_default_device() -> str:
    try:
        if platform.system() == "Darwin":
            machine = platform.machine().lower()
            if "arm64" in machine or "aarch64" in machine:
                return "mps"
    except Exception:
        pass
    return "cpu"


def build_mineru_cmd(
    pdf_path: Path,
    output_dir: Path,
    *,
    backend: str,
    method: str,
    lang: str,
    device: str,
    formula: bool,
    table: bool,
    source: str,
) -> List[str]:
    cmd: List[str] = [
        "mineru",
        "-p",
        str(pdf_path),
        "-o",
        str(output_dir),
        "-b",
        backend,
        "-m",
        method,
    ]
    if lang:
        cmd.extend(["-l", lang])
    if device:
        cmd.extend(["-d", device])
    cmd.extend(["-f", str(bool(formula))])
    cmd.extend(["-t", str(bool(table))])
    if source:
        cmd.extend(["--source", source])
    return cmd


def _save_mineru_logs(output_dir: Path, stdout: str, stderr: str) -> None:
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "mineru_stdout.log").write_text(stdout or "", encoding="utf-8")
        (output_dir / "mineru_stderr.log").write_text(stderr or "", encoding="utf-8")
    except Exception:
        return


def needs_reserialize(stdout: str, stderr: str) -> bool:
    combined = f"{stdout}\n{stderr}".lower()
    return "unknown file suffix" in combined


def reserialize_pdf(pdf_path: Path) -> Path | None:
    if shutil.which("qpdf") is None:
        return None
    fd, temp_name = tempfile.mkstemp(prefix="mineru_reserialize_", suffix=".pdf")
    os.close(fd)
    out = Path(temp_name)
    proc = subprocess.run(["qpdf", str(pdf_path), str(out)], capture_output=True, text=True)
    if proc.returncode != 0:
        out.unlink(missing_ok=True)
        return None
    return out


def _load_markdown_text(output_dir: Path) -> str:
    md_files = sorted(output_dir.rglob("*.md"), key=lambda p: p.stat().st_size if p.exists() else 0, reverse=True)
    for md in md_files:
        try:
            text = md.read_text(encoding="utf-8", errors="ignore").strip()
            if text:
                return text
        except Exception:
            continue
    return ""


def _load_content_list_text(output_dir: Path) -> str:
    json_files = sorted(output_dir.rglob("*content_list.json"))
    for fp in json_files:
        try:
            data = json.loads(fp.read_text(encoding="utf-8", errors="ignore"))
        except Exception:
            continue
        if not isinstance(data, list):
            continue
        chunks: List[str] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            if str(item.get("type", "")).lower() != "text":
                continue
            text = str(item.get("text", "")).strip()
            if text:
                chunks.append(text)
        merged = "\n".join(chunks).strip()
        if merged:
            return merged
    return ""


def _load_plain_text(output_dir: Path) -> str:
    txt_files = sorted(output_dir.rglob("*.txt"), key=lambda p: p.stat().st_size if p.exists() else 0, reverse=True)
    for fp in txt_files:
        try:
            text = fp.read_text(encoding="utf-8", errors="ignore").strip()
            if text:
                return text
        except Exception:
            continue
    return ""


def run_mineru_extract(
    pdf_path: Path,
    output_dir: Path,
    backend: str,
    method: str,
    lang: str,
    force_refresh: bool,
    device: str = "",
    formula: bool = True,
    table: bool = True,
    source: str = "",
) -> str:
    output_dir.mkdir(parents=True, exist_ok=True)
    if force_refresh:
        for item in output_dir.iterdir():
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
            else:
                item.unlink(missing_ok=True)

    cached = _load_markdown_text(output_dir) or _load_content_list_text(output_dir) or _load_plain_text(output_dir)
    if cached and not force_refresh:
        return cached

    resolved_device = device or detect_default_device()

    def _run_once(path_to_pdf: Path, run_method: str, run_formula: bool, run_table: bool) -> tuple[int, str, str]:
        cmd = build_mineru_cmd(
            path_to_pdf,
            output_dir,
            backend=backend,
            method=run_method,
            lang=lang,
            device=resolved_device,
            formula=run_formula,
            table=run_table,
            source=source,
        )
        proc = subprocess.run(cmd, capture_output=True, text=True)
        return proc.returncode, proc.stdout or "", proc.stderr or ""

    rc, out, err = _run_once(pdf_path, method, formula, table)
    text = _load_markdown_text(output_dir) or _load_content_list_text(output_dir) or _load_plain_text(output_dir)
    if rc == 0 and text:
        return text

    # Retry once with reserialized PDF if MinerU mis-detected file suffix/format.
    retry_pdf = None
    if needs_reserialize(out, err):
        retry_pdf = reserialize_pdf(pdf_path)
        if retry_pdf:
            rc, out, err = _run_once(retry_pdf, method, formula, table)
            text = _load_markdown_text(output_dir) or _load_content_list_text(output_dir) or _load_plain_text(output_dir)
            if rc == 0 and text:
                retry_pdf.unlink(missing_ok=True)
                return text

    # Fallback: disable formula model to avoid MFR crashes on malformed image crops.
    err_low = err.lower()
    if formula and ("'nonetype' object has no attribute 'shape'" in err_low or "mfr predict" in err_low):
        rc, out2, err2 = _run_once(pdf_path, method, False, table)
        out = f"{out}\n\n[FORMULA OFF RETRY]\n{out2}"
        err = f"{err}\n\n[FORMULA OFF RETRY]\n{err2}"
        text = _load_markdown_text(output_dir) or _load_content_list_text(output_dir) or _load_plain_text(output_dir)
        if rc == 0 and text:
            if retry_pdf:
                retry_pdf.unlink(missing_ok=True)
            return text

    # Fallback: if method auto failed/empty, force OCR once.
    if method == "auto":
        rc, out2, err2 = _run_once(pdf_path, "ocr", False, table)
        out = f"{out}\n\n[OCR RETRY]\n{out2}"
        err = f"{err}\n\n[OCR RETRY]\n{err2}"
        text = _load_markdown_text(output_dir) or _load_content_list_text(output_dir) or _load_plain_text(output_dir)
        if rc == 0 and text:
            if retry_pdf:
                retry_pdf.unlink(missing_ok=True)
            return text

    # Last fallback: plain text mode with formula/table both off.
    rc, out2, err2 = _run_once(pdf_path, "txt", False, False)
    out = f"{out}\n\n[TXT RETRY]\n{out2}"
    err = f"{err}\n\n[TXT RETRY]\n{err2}"
    text = _load_markdown_text(output_dir) or _load_content_list_text(output_dir) or _load_plain_text(output_dir)
    if rc == 0 and text:
        if retry_pdf:
            retry_pdf.unlink(missing_ok=True)
        return text

    if retry_pdf:
        retry_pdf.unlink(missing_ok=True)
    _save_mineru_logs(output_dir, out, err)
    return ""


def run_cli() -> int:
    start_ts = time.time()
    args = parse_args()

    if not ensure_mineru_cli():
        print('MinerU CLI not found. Install first, e.g. `uv pip install -U "mineru[all]"`.')
        return 1
    missing = check_mineru_runtime_deps()
    if missing:
        print(f"Missing MinerU runtime deps: {', '.join(missing)}")
        print("Install with: pip install " + " ".join(missing))
        return 1

    api_key = args.api_key or load_api_key_from_file(Path(args.api_key_file))
    if not api_key:
        print("Missing API key. Provide --api-key, set CH_API_KEY, or create ch_api_key.txt in project root.")
        return 1

    session, headers = create_ch_session(api_key)
    if args.company_source == "csv":
        if not args.companies_csv:
            raise RuntimeError("company-source=csv requires --companies-csv")
        companies = load_active_companies_from_csv(Path(args.companies_csv), max_companies=args.max_companies)
        print(f"Loaded active companies from CSV: {len(companies)}")
    elif args.company_source == "auto-all":
        csv_path = ensure_companies_csv_from_companies_house(Path(args.companies_cache_dir))
        print(f"Using Companies House bulk CSV: {csv_path}")
        companies = load_active_companies_from_csv(csv_path, max_companies=args.max_companies)
        print(f"Loaded active companies from CH bulk CSV: {len(companies)}")
    else:
        companies = search_companies(
            session=session,
            headers=headers,
            sleep_seconds=args.sleep_seconds,
            query=args.company_query,
            limit=args.max_companies,
        )
    if not companies:
        print("No active companies found.")
        return 0

    history_rows: List[Dict[str, str]] = []
    download_dir = Path(args.download_dir)
    mineru_output_root = Path(args.mineru_output_dir)

    for c in companies:
        company_number = str(c.get("company_number") or "")
        if not company_number:
            continue
        company_name = str(c.get("title") or company_number)

        filings = account_filings(
            session=session,
            headers=headers,
            sleep_seconds=args.sleep_seconds,
            company_number=company_number,
            limit=args.max_filings_per_company,
            include_all_accounts=True,
        )
        for filing in filings:
            if not is_target_accounts_filing(filing):
                continue
            filing_date = str(filing.get("date") or "")
            meta_url = filing.get("links", {}).get("document_metadata")
            if not meta_url:
                continue

            pdf_url = document_pdf_url(session=session, headers=headers, document_metadata_url=str(meta_url))
            if not pdf_url:
                continue

            pdf_path = download_dir / f"{company_number}_{filing_date}.pdf"
            if not pdf_path.exists() and not download_pdf(session=session, headers=headers, pdf_url=pdf_url, output_path=pdf_path):
                continue

            per_pdf_output = mineru_output_root / f"{company_number}_{filing_date}"
            text = run_mineru_extract(
                pdf_path=pdf_path,
                output_dir=per_pdf_output,
                backend=args.mineru_backend,
                method=args.mineru_method,
                lang=args.mineru_lang,
                force_refresh=args.mineru_force_refresh,
                device=args.mineru_device,
                formula=args.mineru_formula,
                table=args.mineru_table,
                source=args.mineru_source,
            )
            if not text:
                continue

            auditor, confidence = extract_external_auditor(text)
            audit_fee, _ = extract_audit_fee(text)
            currency, fee_unit = detect_currency_and_unit(text)
            year = parse_year(filing_date, text)

            history_rows.append(
                make_row(
                    company_number=company_number,
                    company=company_name,
                    year=year,
                    external_auditor=auditor,
                    audit_fee=audit_fee,
                    fee_unit=fee_unit,
                    currency=currency,
                    filing_date=filing_date,
                    confidence=confidence,
                    pdf_path=str(pdf_path),
                )
            )

    history_rows.sort(key=lambda r: (r.get("company_number", ""), r.get("year", "")), reverse=True)
    shortlist_rows = build_shortlist(history_rows)

    write_csv(
        Path(args.history_csv),
        history_rows,
        [
            "company_number",
            "company",
            "year",
            "external_auditor",
            "audit_fee",
            "fee_unit",
            "currency",
            "filing_date",
            "confidence",
            "pdf_path",
        ],
    )
    write_csv(
        Path(args.shortlist_csv),
        shortlist_rows,
        [
            "company_number",
            "company",
            "current_external_auditor",
            "continuous_tenure_years",
            "latest_audit_fee_gbp",
            "priority_score",
            "tender_status",
        ],
    )

    print(f"[DONE] history CSV: {args.history_csv}")
    print(f"[DONE] shortlist CSV: {args.shortlist_csv}")
    print(f"[DONE] rows: history={len(history_rows)} shortlist={len(shortlist_rows)}")
    elapsed = time.time() - start_ts
    print(f"[DONE] runtime_seconds: {elapsed:.2f}")
    print(f"[DONE] runtime_minutes: {elapsed / 60:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_cli())
