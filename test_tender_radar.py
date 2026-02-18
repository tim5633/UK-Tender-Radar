#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

from tender_radar import (
    CHClient,
    ExtractedRow,
    build_shortlist,
    detect_currency_and_unit,
    extract_audit_fee,
    extract_external_auditor,
    extract_pdf_text_sampled,
    load_dotenv_file,
    load_api_key_from_file,
    ocr_targeted_text,
    parse_year,
    write_csv,
)

TARGET_COMPANIES = [
    "Howden Joinery Group",
    "British Airways",
    "Lloyds Banking Group",
    "BT Group",
]


def pick_target_companies(client: CHClient) -> List[Dict[str, str]]:
    """
    Pull only the target companies and keep one best active match per target query.
    """
    selected: List[Dict[str, str]] = []
    seen_numbers = set()

    for query in TARGET_COMPANIES:
        candidates = client.search_companies(query=query, limit=20)
        if not candidates:
            continue

        query_low = query.lower()
        exact = []
        partial = []
        for c in candidates:
            title = str(c.get("title") or "")
            title_low = title.lower()
            if title_low == query_low or query_low in title_low:
                exact.append(c)
            else:
                partial.append(c)

        chosen = (exact or partial)[0]
        number = str(chosen.get("company_number") or "")
        if not number or number in seen_numbers:
            continue
        seen_numbers.add(number)
        selected.append(chosen)

    return selected


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parent
    load_dotenv_file(root / ".env")
    default_key_file = Path(os.getenv("CH_API_KEY_FILE", str(root / "ch_api_key.txt")))
    p = argparse.ArgumentParser(
        description="Test runner for tender_radar using fixed companies: Howdens, British Airways, Lloyds, BT."
    )
    p.add_argument("--api-key", default=os.getenv("CH_API_KEY"), help="Companies House API key")
    p.add_argument(
        "--api-key-file",
        default=str(default_key_file),
        help="File containing API key (first non-empty line)",
    )
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
        default=os.getenv("TENDER_TEST_HISTORY_CSV", str(root / "test_tender_history.csv")),
        help="Detailed output CSV",
    )
    p.add_argument(
        "--shortlist-csv",
        default=os.getenv("TENDER_TEST_SHORTLIST_CSV", str(root / "test_tender_shortlist.csv")),
        help="Priority shortlist CSV",
    )
    # Jupyter/VS Code notebook kernels inject extra args (e.g. --f=...).
    args, _unknown = p.parse_known_args()
    return args


def main() -> int:
    start_time = time.perf_counter()
    args = parse_args()
    api_key = args.api_key or load_api_key_from_file(Path(args.api_key_file))
    if not api_key:
        print(
            "Missing API key. Provide --api-key, set CH_API_KEY, "
            "or create ch_api_key.txt in project root."
        )
        return 1

    client = CHClient(api_key=api_key, sleep_seconds=args.sleep_seconds)
    companies = pick_target_companies(client)
    if not companies:
        print("No target companies found.")
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
    elapsed = time.perf_counter() - start_time
    print(f"[DONE] runtime_seconds: {elapsed:.2f}")
    print(f"[DONE] runtime_minutes: {elapsed / 60:.2f}")
    return 0


if __name__ == "__main__":
    exit_code = main()
    if "ipykernel" not in sys.modules:
        raise SystemExit(exit_code)
