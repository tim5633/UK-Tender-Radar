#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import csv
import math
import os
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

COMPANIES_HOUSE_API = "https://api.company-information.service.gov.uk"
DOCUMENT_API_HOST = "https://document-api.company-information.service.gov.uk"

AUDITOR_NORMALIZATION = {
    "pricewaterhousecoopers": "PwC",
    "pwc": "PwC",
    "ernst & young": "EY",
    "ernst and young": "EY",
    "ey": "EY",
    "kpmg": "KPMG",
    "deloitte": "Deloitte",
    "bdo": "BDO",
    "grant thornton": "Grant Thornton",
    "mazars": "Mazars",
    "rsm": "RSM",
}


@dataclass
class ExtractedRow:
    company_number: str
    company: str
    year: str
    external_auditor: str
    audit_fee: str
    fee_unit: str
    currency: str
    filing_date: str
    confidence: str
    pdf_path: str

    def as_dict(self) -> Dict[str, str]:
        return {
            "company_number": self.company_number,
            "company": self.company,
            "year": self.year,
            "external_auditor": self.external_auditor,
            "audit_fee": self.audit_fee,
            "fee_unit": self.fee_unit,
            "currency": self.currency,
            "filing_date": self.filing_date,
            "confidence": self.confidence,
            "pdf_path": self.pdf_path,
        }


class CHClient:
    def __init__(self, api_key: str, sleep_seconds: float = 0.25) -> None:
        self.sleep_seconds = sleep_seconds
        self.session = requests.Session()
        auth = base64.b64encode(f"{api_key}:".encode("utf-8")).decode("utf-8")
        self.base_headers = {"Authorization": f"Basic {auth}"}

    def _json(self, url: str, params: Optional[dict] = None, retries: int = 3) -> Optional[dict]:
        for attempt in range(1, retries + 1):
            try:
                r = self.session.get(url, headers=self.base_headers, params=params, timeout=60)
                if r.status_code == 200:
                    return r.json()
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(1.2 * attempt)
                    continue
                return None
            except requests.RequestException:
                if attempt == retries:
                    return None
                time.sleep(1.2 * attempt)
        return None

    def search_companies(self, query: str, limit: int) -> List[dict]:
        items: List[dict] = []
        start_index = 0
        page_size = 100
        while len(items) < limit:
            data = self._json(
                f"{COMPANIES_HOUSE_API}/search/companies",
                params={"q": query, "start_index": start_index, "items_per_page": page_size},
            )
            if not data:
                break
            page_items = data.get("items", [])
            if not page_items:
                break
            for item in page_items:
                if item.get("company_status") == "active":
                    items.append(item)
                    if len(items) >= limit:
                        break
            start_index += page_size
            if start_index >= int(data.get("total_results", 0)):
                break
            time.sleep(self.sleep_seconds)
        return items[:limit]

    def account_filings(self, company_number: str, limit: int, include_all_accounts: bool) -> List[dict]:
        start_index = 0
        page_size = 100
        out: List[dict] = []
        seen_keys = set()
        while len(out) < limit:
            data = self._json(
                f"{COMPANIES_HOUSE_API}/company/{company_number}/filing-history",
                params={"start_index": start_index, "items_per_page": page_size},
            )
            if not data:
                break
            items = data.get("items", [])
            if not items:
                break

            for filing in items:
                if filing.get("category") != "accounts":
                    continue
                if not include_all_accounts and not is_probably_full_audited_accounts(filing):
                    continue
                key = (
                    str(filing.get("date", "")),
                    str(filing.get("type", "")),
                    str(filing.get("links", {}).get("document_metadata", "")),
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                out.append(filing)
                if len(out) >= limit:
                    break
            start_index += page_size
            if start_index >= int(data.get("total_count", 0)):
                break
            time.sleep(self.sleep_seconds)
        return out

    def document_pdf_url(self, document_metadata_url: str) -> Optional[str]:
        meta = self._json(document_metadata_url)
        if not meta:
            return None
        link = meta.get("links", {}).get("document")
        if not link:
            return None
        return link if str(link).startswith("http") else f"{DOCUMENT_API_HOST}{link}"

    def download_pdf(self, pdf_url: str, output_path: Path) -> bool:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        headers = dict(self.base_headers)
        headers["Accept"] = "application/pdf"
        for attempt in range(1, 4):
            try:
                r = self.session.get(pdf_url, headers=headers, timeout=120)
                if r.status_code == 200 and r.content:
                    output_path.write_bytes(r.content)
                    return True
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(1.2 * attempt)
                    continue
                return False
            except requests.RequestException:
                if attempt == 3:
                    return False
                time.sleep(1.2 * attempt)
        return False


def load_dotenv_file(path: Path) -> None:
    """
    Load KEY=VALUE pairs from .env into process env without overriding existing vars.
    """
    if not path.exists():
        return
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            k = key.strip()
            if not k:
                continue
            v = value.strip().strip("'").strip('"')
            os.environ.setdefault(k, v)
    except Exception:
        return


def is_probably_full_audited_accounts(filing: dict) -> bool:
    desc = str(filing.get("description", "")).lower()
    typ = str(filing.get("type", "")).lower()
    hints = ("audited", "full accounts", "group of companies accounts")
    return any(h in desc for h in hints) or typ in {"aa", "aa01", "aa02", "aa03", "aa04", "aa06", "aa07"}


def normalize_auditor_name(name: str) -> str:
    cleaned = " ".join(name.replace("\u00a0", " ").split()).strip(" ,.;:-")
    low = cleaned.lower()
    for raw, normalized in AUDITOR_NORMALIZATION.items():
        if raw in low:
            return normalized
    return cleaned


def _is_plausible_audit_firm(name: str) -> bool:
    low = name.lower().strip()
    if not low:
        return False
    if len(low) > 80:
        return False
    bad_hints = [
        "consolidated financial statements",
        "climate-related",
        "note ",
        "contents",
        "directors",
        "statement",
    ]
    if any(b in low for b in bad_hints):
        return False
    if any(k in low for k in AUDITOR_NORMALIZATION.keys()):
        return True
    legal_suffix = (" llp", " ltd", " limited", " plc")
    return low.endswith(legal_suffix)


def extract_external_auditor(text: str) -> Tuple[str, str]:
    compact = text.replace("\u00a0", " ")
    lower = compact.lower()
    patterns = [
        r"(?is)independent auditor(?:s)?(?:'|’) report[^\n]{0,120}\n([^\n]{2,120})",
        r"(?is)(?:signed for and on behalf of|for and on behalf of)\s*\n?\s*([A-Z][A-Za-z&,\.\- '\(\)]{2,120})",
        r"(?im)^\s*auditor(?:s)?\s*[:\-]\s*([A-Z][A-Za-z&,\.\- '\(\)]{2,120})\s*$",
    ]
    for pat in patterns:
        m = re.search(pat, compact)
        if m:
            candidate = normalize_auditor_name(m.group(1))
            if _is_plausible_audit_firm(candidate):
                return (candidate, "high")
    for raw, normalized in AUDITOR_NORMALIZATION.items():
        idx = lower.find(raw)
        if idx != -1:
            near = lower[max(0, idx - 140): idx + 140]
            if "auditor" in near or "audit report" in near or "independent" in near:
                return (normalized, "medium")
    return ("", "low")


def detect_currency_and_unit(text: str) -> Tuple[str, str]:
    low = text.lower()
    currency = ""
    if re.search(r"\bgbp\b|£|pounds sterling", low):
        currency = "GBP"
    elif re.search(r"\busd\b|\$", low):
        currency = "USD"
    elif re.search(r"\beur\b|€", low):
        currency = "EUR"

    unit = ""
    unit_patterns = [
        (r"(?i)(£\s*['’]?\s*000|000s|in thousands|thousand)", "thousand"),
        (r"(?i)(£m|us\$m|€m|in millions|million)", "million"),
        (r"(?i)(billion|bn)", "billion"),
    ]
    for pat, val in unit_patterns:
        if re.search(pat, text):
            unit = val
            break
    return (currency, unit)


def _extract_number_tokens(s: str) -> List[str]:
    vals = []
    for m in re.finditer(r"\(?\d[\d,]*(?:\.\d+)?\)?", s):
        token = m.group(0).replace(",", "").replace("(", "-").replace(")", "").strip()
        if token:
            vals.append(token)
    return vals


def _is_fee_row(line: str) -> bool:
    low = line.lower()
    primary = (
        "audit of the",
        "audit of group accounts",
        "audit of the company",
        "audit of financial statements",
        "audit of the annual accounts",
        "fees payable to the company",
        "fees payable to the group's auditor",
        "statutory audit",
    )
    exclude = ("other services", "other assurance", "tax", "non-audit", "subsidiaries", "pension", "total")
    return any(p in low for p in primary) and not any(e in low for e in exclude)


def extract_audit_fee(text: str) -> Tuple[str, str]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    header_idx = [
        i for i, ln in enumerate(lines)
        if "auditor" in ln.lower() and ("remuneration" in ln.lower() or "fees payable" in ln.lower())
    ]
    for idx in header_idx:
        window = lines[idx: idx + 40]
        for ln in window:
            if _is_fee_row(ln):
                nums = _extract_number_tokens(ln)
                if nums:
                    return (nums[0], "table")
        for i in range(0, max(0, len(window) - 1)):
            merged = f"{window[i]} {window[i + 1]}"
            if _is_fee_row(merged):
                nums = _extract_number_tokens(merged)
                if nums:
                    return (nums[0], "table")

    for ln in lines:
        if _is_fee_row(ln):
            nums = _extract_number_tokens(ln)
            if nums:
                return (nums[0], "table")

    sentence_patterns = [
        r"(?i)fees?\s+payable\s+to\s+the\s+(?:group'?s\s+)?(?:external\s+)?auditor[^\n\r]{0,120}?audit[^\n\r]{0,120}?accounts?[^\n\r]{0,60}?([£$€]?\s*\(?\d[\d,]*(?:\.\d+)?\)?)",
        r"(?i)audit(?:or)?(?:s)?\s+(?:fee|fees|remuneration)[^\n\r]{0,120}?([£$€]?\s*\(?\d[\d,]*(?:\.\d+)?\)?)",
        r"(?i)statutory\s+audit[^\n\r]{0,100}?([£$€]?\s*\(?\d[\d,]*(?:\.\d+)?\)?)",
    ]
    for pat in sentence_patterns:
        m = re.search(pat, text)
        if m:
            token = m.group(1).replace(",", "").replace("(", "-").replace(")", "").strip()
            token = re.sub(r"^[£$€]\s*", "", token)
            num = re.search(r"-?\d+(?:\.\d+)?", token)
            if num:
                return (num.group(0), "text")
    return ("", "none")


def parse_year(filing_date: str, text: str) -> str:
    m = re.search(r"(?i)for the year ended[^\n\r]{0,40}\b(20\d{2})\b", text)
    if m:
        return m.group(1)
    m = re.search(r"(?i)year ended[^\n\r]{0,40}\b(20\d{2})\b", text)
    if m:
        return m.group(1)
    m = re.search(r"\b(20\d{2})\b", filing_date or "")
    return m.group(1) if m else ""


def extract_pdf_text_sampled(pdf_path: Path, front_pages: int = 25, tail_pages: int = 80, tail_stride: int = 3) -> str:
    """
    Fast path: extract all front pages + sampled tail pages.
    This is significantly faster than parsing every page.
    """
    try:
        import fitz  # type: ignore
    except Exception:
        return ""

    try:
        doc = fitz.open(str(pdf_path))
    except Exception:
        return ""

    page_count = doc.page_count
    pages: List[int] = list(range(0, min(front_pages, page_count)))
    tail_start = max(0, page_count - tail_pages)
    for p in range(tail_start, page_count, max(1, tail_stride)):
        if p not in pages:
            pages.append(p)

    chunks: List[str] = []
    for p in pages:
        try:
            txt = (doc[p].get_text("text") or "").strip()
            if txt:
                chunks.append(txt)
        except Exception:
            continue
    return "\n\n".join(chunks)


def _ocr_page_text(doc, page_index: int, zoom: float = 2.2) -> str:
    try:
        from PIL import Image  # type: ignore
        import pytesseract  # type: ignore
    except Exception:
        return ""
    # Make OCR robust across conda/VS Code environments.
    tesseract_candidates = [
        os.getenv("TESSERACT_CMD", ""),
        "/opt/miniconda3/bin/tesseract",
        "/usr/local/bin/tesseract",
        "/opt/homebrew/bin/tesseract",
    ]
    for candidate in tesseract_candidates:
        if candidate and Path(candidate).exists():
            pytesseract.pytesseract.tesseract_cmd = candidate
            break
    try:
        page = doc[page_index]
        pix = page.get_pixmap(matrix=__import__("fitz").Matrix(zoom, zoom), alpha=False)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        return (pytesseract.image_to_string(img, lang="eng") or "").strip()
    except Exception:
        return ""


def ocr_targeted_text(pdf_path: Path, max_pages: int = 80) -> str:
    """
    OCR fallback for scanned PDFs:
    1) sparse scan to detect likely auditor/remuneration pages
    2) dense scan around hits (+ front pages for auditor signature)
    """
    try:
        import fitz  # type: ignore
    except Exception:
        return ""
    try:
        doc = fitz.open(str(pdf_path))
    except Exception:
        return ""

    page_count = doc.page_count
    if page_count <= 0:
        return ""

    sparse = set(range(0, min(20, page_count), 3))
    step = 8 if page_count > 120 else 5
    sparse.update(range(0, page_count, step))

    hits = set()
    for p in sorted(sparse):
        t = _ocr_page_text(doc, p, zoom=1.6)
        if not t:
            continue
        low = t.lower()
        if (
            "independent auditor" in low
            or ("auditor" in low and "report" in low)
            or ("auditor" in low and ("remuneration" in low or "fees payable" in low))
            or "audit fee" in low
        ):
            hits.add(p)

    ordered: List[int] = []
    seen = set()

    def push(p: int) -> None:
        if 0 <= p < page_count and p not in seen:
            seen.add(p)
            ordered.append(p)

    # Prioritize front pages for auditor signature sections.
    for p in range(0, min(14, page_count)):
        push(p)

    # Then prioritize neighborhoods around remuneration/report hits.
    for h in sorted(hits):
        for q in range(max(0, h - 2), min(page_count - 1, h + 2) + 1):
            push(q)

    pages = ordered[:max_pages]
    chunks: List[str] = []
    for p in pages:
        t = _ocr_page_text(doc, p, zoom=2.3)
        if t:
            chunks.append(t)
    return "\n\n".join(chunks)


def fee_to_gbp_numeric(value: str, unit: str, currency: str) -> Optional[float]:
    if not value:
        return None
    try:
        v = float(value)
    except ValueError:
        return None
    if currency and currency != "GBP":
        return None
    mul = 1.0
    if unit == "thousand":
        mul = 1_000.0
    elif unit == "million":
        mul = 1_000_000.0
    elif unit == "billion":
        mul = 1_000_000_000.0
    return v * mul


def compute_continuous_tenure(rows: List[Dict[str, str]]) -> Tuple[str, int]:
    if not rows:
        return ("", 0)
    ordered = sorted(rows, key=lambda r: r.get("year", ""), reverse=True)
    auditor = ordered[0].get("external_auditor", "")
    if not auditor:
        return ("", 0)
    n = 0
    for r in ordered:
        if r.get("external_auditor", "") == auditor:
            n += 1
        else:
            break
    return (auditor, n)


def build_shortlist(history_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in history_rows:
        grouped[row["company_number"]].append(row)

    out: List[Dict[str, str]] = []
    for company_number, rows in grouped.items():
        company = rows[0].get("company", "")
        auditor, tenure_years = compute_continuous_tenure(rows)
        fee_values = []
        for r in rows:
            gbp = fee_to_gbp_numeric(r.get("audit_fee", ""), r.get("fee_unit", ""), r.get("currency", ""))
            if gbp is not None and gbp > 0:
                fee_values.append(gbp)
        latest_fee = max(fee_values) if fee_values else None

        tenure_score = min(100.0, tenure_years * 10.0)
        fee_score = 0.0 if latest_fee is None else min(100.0, math.log1p(latest_fee) * 5.0)
        priority_score = round(0.65 * tenure_score + 0.35 * fee_score, 2)

        status = "monitor"
        if tenure_years >= 8 and (latest_fee or 0) >= 1_000_000:
            status = "hot"
        elif tenure_years >= 6:
            status = "watch"

        out.append(
            {
                "company_number": company_number,
                "company": company,
                "current_external_auditor": auditor,
                "continuous_tenure_years": str(tenure_years),
                "latest_audit_fee_gbp": f"{latest_fee:.0f}" if latest_fee is not None else "",
                "priority_score": str(priority_score),
                "tender_status": status,
            }
        )
    out.sort(key=lambda r: float(r["priority_score"]), reverse=True)
    return out


def write_csv(path: Path, rows: Iterable[Dict[str, str]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def load_api_key_from_file(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            return s
    except Exception:
        return None
    return None


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parent
    load_dotenv_file(root / ".env")
    default_key_file = Path(os.getenv("CH_API_KEY_FILE", str(root / "ch_api_key.txt")))
    p = argparse.ArgumentParser(description="Build external-auditor tender radar from UK filings.")
    p.add_argument("--api-key", default=os.getenv("CH_API_KEY"), help="Companies House API key")
    p.add_argument(
        "--api-key-file",
        default=str(default_key_file),
        help="File path containing API key (first non-empty line)",
    )
    p.add_argument("--company-query", default="plc", help="Search query for companies")
    p.add_argument("--max-companies", type=int, default=100, help="Max active companies to process")
    p.add_argument("--max-filings-per-company", type=int, default=5, help="Recent accounts filings per company")
    p.add_argument("--sleep-seconds", type=float, default=0.25, help="API throttle")
    p.add_argument("--include-all-accounts", action="store_true", help="Include every accounts filing type")
    p.add_argument(
        "--download-dir",
        default=os.getenv("TENDER_DOWNLOAD_DIR", str(root / "uk_accounts_pdfs")),
        help="Local PDF folder",
    )
    p.add_argument(
        "--enable-ocr-fallback",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run targeted OCR when fields are missing",
    )
    p.add_argument("--ocr-max-pages", type=int, default=80, help="Max pages for OCR fallback")
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
    # Jupyter/VS Code notebook kernels inject extra args (e.g. --f=...).
    args, _unknown = p.parse_known_args()
    return args


def main() -> int:
    start_ts = time.time()
    args = parse_args()
    api_key = args.api_key or load_api_key_from_file(Path(args.api_key_file))
    if not api_key:
        print(
            "Missing API key. Provide --api-key, set CH_API_KEY, "
            "or create ch_api_key.txt in project root."
        )
        return 1

    client = CHClient(api_key=api_key, sleep_seconds=args.sleep_seconds)
    companies = client.search_companies(query=args.company_query, limit=args.max_companies)
    if not companies:
        print("No active companies found.")
        return 0

    history_rows: List[Dict[str, str]] = []
    download_dir = Path(args.download_dir)

    for c in companies:
        company_number = str(c.get("company_number") or "")
        if not company_number:
            continue
        company_name = str(c.get("title") or company_number)

        filings = client.account_filings(
            company_number=company_number,
            limit=args.max_filings_per_company,
            include_all_accounts=args.include_all_accounts,
        )
        for filing in filings:
            filing_date = str(filing.get("date") or "")
            meta_url = filing.get("links", {}).get("document_metadata")
            if not meta_url:
                continue

            pdf_url = client.document_pdf_url(str(meta_url))
            if not pdf_url:
                continue

            pdf_path = download_dir / f"{company_number}_{filing_date}.pdf"
            if not pdf_path.exists() and not client.download_pdf(pdf_url=pdf_url, output_path=pdf_path):
                continue

            text = extract_pdf_text_sampled(pdf_path)
            auditor, confidence = extract_external_auditor(text)
            audit_fee, _ = extract_audit_fee(text)
            currency, fee_unit = detect_currency_and_unit(text)
            year = parse_year(filing_date, text)

            # Accuracy fallback for scanned / low-text PDFs.
            need_ocr = args.enable_ocr_fallback and (
                not auditor or not audit_fee or not currency or not fee_unit
            )
            if need_ocr:
                ocr_text = ocr_targeted_text(pdf_path, max_pages=args.ocr_max_pages)
                if ocr_text:
                    ocr_auditor, ocr_conf = extract_external_auditor(ocr_text)
                    ocr_fee, _ = extract_audit_fee(ocr_text)
                    ocr_currency, ocr_unit = detect_currency_and_unit(ocr_text)
                    ocr_year = parse_year(filing_date, ocr_text)

                    if ocr_auditor and not auditor:
                        auditor = ocr_auditor
                        confidence = ocr_conf
                    if ocr_fee and not audit_fee:
                        audit_fee = ocr_fee
                    if ocr_currency and not currency:
                        currency = ocr_currency
                    if ocr_unit and not fee_unit:
                        fee_unit = ocr_unit
                    if ocr_year and not year:
                        year = ocr_year

            row = ExtractedRow(
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
            history_rows.append(row.as_dict())

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
    exit_code = main()
    if "ipykernel" not in sys.modules:
        raise SystemExit(exit_code)
