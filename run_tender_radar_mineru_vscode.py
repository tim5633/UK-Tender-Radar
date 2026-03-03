# %%
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd

from run_tender_radar_mineru import (
    check_mineru_runtime_deps,
    ensure_mineru_cli,
    ensure_companies_csv_from_companies_house,
    is_target_accounts_filing,
    load_active_companies_from_csv,
    run_mineru_extract,
)
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


def get_pdf_page_count(pdf_path: Path) -> int:
    try:
        import fitz  # type: ignore
    except Exception:
        return 0
    try:
        doc = fitz.open(str(pdf_path))
        return int(doc.page_count or 0)
    except Exception:
        return 0


# %% 1) Config: edit values here
# Works in both .py and .ipynb: __file__ is not defined in notebooks.
ROOT = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
load_dotenv_file(ROOT / ".env")

COMPANY_SOURCE = "auto-all"
COMPANIES_CSV = ""
MAX_COMPANIES = 0

COMPANY_QUERY = "plc"
# COMPANY_SOURCE = os.getenv("TENDER_COMPANY_SOURCE", "search")  # search | csv | auto-all
COMPANIES_CSV = os.getenv("TENDER_COMPANIES_CSV", "")  # e.g. /path/to/BasicCompanyData-....csv
COMPANIES_CACHE_DIR = Path(
    os.getenv("TENDER_COMPANIES_CACHE_DIR", str(ROOT / "companies_house_cache"))
)
# MAX_COMPANIES = 0  # <=0 means all active companies from selected source
TARGET_COMPANY_KEYWORDS = ["howden joinery group plc"]  # [] means no keyword filter
MAX_FILINGS_PER_COMPANY = 20
SLEEP_SECONDS = 0.25
INCLUDE_ALL_ACCOUNTS = False
PREVIEW_COMPANY_INDEX = 0  # e.g. 3 means companies[3]
RUN_ONLY_PREVIEW_COMPANY = True  # False = process all filtered companies in full-run cell
SAMPLE_ONLY_COMPANY_NUMBER = "02128710"  # Howden Joinery Group PLC

DOWNLOAD_DIR = Path(os.getenv("TENDER_DOWNLOAD_DIR", str(ROOT / "uk_accounts_pdfs")))
PREVIEW_DOWNLOAD_DIR = Path(os.getenv("TENDER_PREVIEW_DOWNLOAD_DIR", str(ROOT / "preview_downloads")))
HISTORY_CSV = Path(os.getenv("TENDER_HISTORY_CSV", str(ROOT / "tender_history.csv")))
SHORTLIST_CSV = Path(os.getenv("TENDER_SHORTLIST_CSV", str(ROOT / "tender_shortlist.csv")))

MINERU_OUTPUT_DIR = Path(os.getenv("TENDER_MINERU_OUTPUT_DIR", str(ROOT / "mineru_outputs")))
MINERU_BACKEND = os.getenv("TENDER_MINERU_BACKEND", "pipeline")
MINERU_METHOD = os.getenv("TENDER_MINERU_METHOD", "auto")
MINERU_LANG = os.getenv("TENDER_MINERU_LANG", "")
MINERU_DEVICE = os.getenv("TENDER_MINERU_DEVICE", "")
MINERU_FORMULA = os.getenv("TENDER_MINERU_FORMULA", "false").lower() != "false"
MINERU_TABLE = os.getenv("TENDER_MINERU_TABLE", "true").lower() != "false"
MINERU_SOURCE = os.getenv("TENDER_MINERU_SOURCE", "")
MINERU_FORCE_REFRESH = False

API_KEY_FILE = Path(os.getenv("CH_API_KEY_FILE", str(ROOT / "ch_api_key.txt")))
API_KEY = os.getenv("CH_API_KEY") or load_api_key_from_file(API_KEY_FILE)

print(f"ROOT={ROOT}")
print(f"COMPANY_SOURCE={COMPANY_SOURCE}")
print(f"COMPANIES_CSV={COMPANIES_CSV or '(not set)'}")
print(f"COMPANIES_CACHE_DIR={COMPANIES_CACHE_DIR}")
print(f"TARGET_COMPANY_KEYWORDS={TARGET_COMPANY_KEYWORDS}")
print(f"DOWNLOAD_DIR={DOWNLOAD_DIR}")
print(f"PREVIEW_DOWNLOAD_DIR={PREVIEW_DOWNLOAD_DIR}")
print(f"HISTORY_CSV={HISTORY_CSV}")
print(f"SHORTLIST_CSV={SHORTLIST_CSV}")
print(f"RUN_ONLY_PREVIEW_COMPANY={RUN_ONLY_PREVIEW_COMPANY}")
print(f"SAMPLE_ONLY_COMPANY_NUMBER={SAMPLE_ONLY_COMPANY_NUMBER}")
print(f"MINERU_BACKEND={MINERU_BACKEND}, METHOD={MINERU_METHOD}, DEVICE={MINERU_DEVICE or 'auto'}")


# %% 2) Pre-flight checks
if not ensure_mineru_cli():
    raise RuntimeError('MinerU CLI not found. Install: uv pip install -U "mineru[all]"')
missing = check_mineru_runtime_deps()
if missing:
    raise RuntimeError(f"Missing MinerU runtime deps: {missing}. Install in current env first.")

if not API_KEY:
    raise RuntimeError("Missing API key. Set CH_API_KEY in .env or provide ch_api_key.txt")

print("Pre-flight checks passed.")


# %% 3) Create API session and fetch companies
session, headers = create_ch_session(API_KEY)
if COMPANY_SOURCE == "csv":
    if not COMPANIES_CSV:
        raise RuntimeError("COMPANY_SOURCE='csv' requires COMPANIES_CSV to be set.")
    print(f"Company source: csv ({COMPANIES_CSV})")
    companies = load_active_companies_from_csv(Path(COMPANIES_CSV), max_companies=MAX_COMPANIES)
elif COMPANY_SOURCE == "auto-all":
    print("Company source: auto-all (download latest Companies House BasicCompanyData)")
    csv_path = ensure_companies_csv_from_companies_house(COMPANIES_CACHE_DIR)
    print(f"Using Companies House bulk CSV: {csv_path}")
    companies = load_active_companies_from_csv(csv_path, max_companies=MAX_COMPANIES)
else:
    print(f"Company source: search (query={COMPANY_QUERY})")
    companies = search_companies(
        session=session,
        headers=headers,
        sleep_seconds=SLEEP_SECONDS,
        query=COMPANY_QUERY,
        limit=MAX_COMPANIES,
    )

print(f"Found active companies: {len(companies)}")
# for i, c in enumerate(companies, start=1):
#     print(f"{i:02d}. {c.get('company_number')} | {c.get('title')}")


# %% 4) Optional keyword filter (e.g. Howden Joinery)
if TARGET_COMPANY_KEYWORDS:
    keys = [k.strip().lower() for k in TARGET_COMPANY_KEYWORDS if k.strip()]
    filtered_companies = []
    for c in companies:
        title = str(c.get("title") or "").lower()
        if any(k in title for k in keys):
            filtered_companies.append(c)
    companies = filtered_companies
    print(f"After keyword filter: {len(companies)}")
    for c in companies[:20]:
        print(f"- {c.get('company_number')} | {c.get('title')}")


# %% 5) Inspect filings for all filtered companies (and pick one preview company)
if not companies:
    raise RuntimeError("No companies found. Try another COMPANY_QUERY.")

if PREVIEW_COMPANY_INDEX < 0 or PREVIEW_COMPANY_INDEX >= len(companies):
    raise RuntimeError(f"PREVIEW_COMPANY_INDEX out of range: {PREVIEW_COMPANY_INDEX}")

company_filings_by_number: Dict[str, List[Dict[str, str]]] = {}
for c in companies:
    company_number = str(c.get("company_number") or "")
    if not company_number:
        continue
    filings = account_filings(
        session=session,
        headers=headers,
        sleep_seconds=SLEEP_SECONDS,
        company_number=company_number,
        limit=MAX_FILINGS_PER_COMPANY,
        include_all_accounts=True,
    )
    filings = [f for f in filings if is_target_accounts_filing(f)]
    company_filings_by_number[company_number] = filings

print(f"Companies to inspect: {len(companies)}")
company_filing_summary_rows: List[Dict[str, str]] = []
company_filing_detail_rows: List[Dict[str, str]] = []
for c in companies:
    n = str(c.get("company_number") or "")
    name = str(c.get("title") or n)
    filings = company_filings_by_number.get(n, [])
    company_filing_summary_rows.append(
        {
            "company_number": n,
            "company": name,
            "filings_count": len(filings),
        }
    )
    print(f"{n} | {name} | filings={len(filings)}")
    for f in filings:
        company_filing_detail_rows.append(
            {
                "company_number": n,
                "company": name,
                "filing_date": str(f.get("date") or ""),
                "filing_type": str(f.get("type") or ""),
                "description": str(f.get("description") or ""),
            }
        )
        print(f"- {f.get('date')} | {f.get('type')} | {f.get('description')}")
    print("")

company_filing_summary_df = pd.DataFrame(company_filing_summary_rows)
company_filing_details_df = pd.DataFrame(company_filing_detail_rows)
print("company_filing_summary_df:")
display(company_filing_summary_df)
print("company_filing_details_df (head 100):")
display(company_filing_details_df.head(100))

first_company = companies[PREVIEW_COMPANY_INDEX]
first_number = str(first_company.get("company_number") or "")
first_name = str(first_company.get("title") or first_number)
first_filings = company_filings_by_number.get(first_number, [])
print(f"Preview company (index {PREVIEW_COMPANY_INDEX}): {first_number} | {first_name}")


# %% 6) Download filings PDFs for all filtered companies
PREVIEW_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
downloaded_rows: List[Dict[str, str]] = []
for c in companies:
    company_number = str(c.get("company_number") or "")
    company_name = str(c.get("title") or company_number)
    filings = company_filings_by_number.get(company_number, [])
    for filing in filings:
        filing_date = str(filing.get("date") or "")
        meta_url = filing.get("links", {}).get("document_metadata")
        if not meta_url:
            continue
        pdf_url = document_pdf_url(session=session, headers=headers, document_metadata_url=str(meta_url))
        if not pdf_url:
            continue

        preview_pdf_path = PREVIEW_DOWNLOAD_DIR / f"{company_number}_{filing_date}.pdf"
        downloaded = True
        if not preview_pdf_path.exists():
            downloaded = download_pdf(session=session, headers=headers, pdf_url=pdf_url, output_path=preview_pdf_path)
        if downloaded:
            downloaded_rows.append(
                {
                    "company_number": company_number,
                    "company": company_name,
                    "filing_date": filing_date,
                    "pdf_path": str(preview_pdf_path),
                }
            )

print(f"Preview folder: {PREVIEW_DOWNLOAD_DIR}")
print(f"Downloaded/ready files: {len(downloaded_rows)}")
downloaded_files_df = pd.DataFrame(downloaded_rows)
display(downloaded_files_df.head(200))


# %% 7) Run one sample filing per company (quick validation across all companies)
sample_batch_start_ts = time.time()
sample_extraction_rows: List[Dict[str, str]] = []
sample_output_dirs: Dict[str, Path] = {}

sample_companies = [
    c for c in companies if str(c.get("company_number") or "") == SAMPLE_ONLY_COMPANY_NUMBER
]
if not sample_companies:
    raise RuntimeError(f"SAMPLE_ONLY_COMPANY_NUMBER not found in current companies: {SAMPLE_ONLY_COMPANY_NUMBER}")

for c in sample_companies:
    company_start_ts = time.time()
    company_number = str(c.get("company_number") or "")
    company_name = str(c.get("title") or company_number)
    filings = company_filings_by_number.get(company_number, [])
    if not filings:
        continue

    # Choose the smallest-page filing as sample to make this step faster.
    candidates = []
    for filing in filings:
        filing_date = str(filing.get("date") or "")
        meta_url = filing.get("links", {}).get("document_metadata")
        if not meta_url:
            continue
        pdf_url = document_pdf_url(session=session, headers=headers, document_metadata_url=str(meta_url))
        if not pdf_url:
            continue

        sample_pdf_path = DOWNLOAD_DIR / f"{company_number}_{filing_date}.pdf"
        if not sample_pdf_path.exists():
            ok = download_pdf(session=session, headers=headers, pdf_url=pdf_url, output_path=sample_pdf_path)
            if not ok:
                continue
        page_count = get_pdf_page_count(sample_pdf_path)
        rank_pages = page_count if page_count > 0 else 10_000
        candidates.append((rank_pages, filing, sample_pdf_path))

    if not candidates:
        continue

    candidates.sort(key=lambda x: x[0])
    chosen_pages, sample_filing, sample_pdf_path = candidates[0]
    sample_date = str(sample_filing.get("date") or "")

    sample_output_dir = MINERU_OUTPUT_DIR / f"{company_number}_{sample_date}"
    sample_output_dirs[company_number] = sample_output_dir
    sample_text = run_mineru_extract(
        pdf_path=sample_pdf_path,
        output_dir=sample_output_dir,
        backend=MINERU_BACKEND,
        method=MINERU_METHOD,
        lang=MINERU_LANG,
        force_refresh=MINERU_FORCE_REFRESH,
        device=MINERU_DEVICE,
        formula=MINERU_FORMULA,
        table=MINERU_TABLE,
        source=MINERU_SOURCE,
    )
    if not sample_text:
        sample_extraction_rows.append(
            {
                "company_number": company_number,
                "company": company_name,
                "filing_date": sample_date,
                "sample_pages": chosen_pages if chosen_pages < 10_000 else "",
                "status": "failed",
                "log_path": str(sample_output_dir / "mineru_stderr.log"),
                "runtime_seconds": round(time.time() - company_start_ts, 2),
            }
        )
        continue

    sample_auditor, sample_conf = extract_external_auditor(sample_text)
    sample_fee, _ = extract_audit_fee(sample_text)
    sample_currency, sample_unit = detect_currency_and_unit(sample_text)
    sample_year = parse_year(sample_date, sample_text)
    sample_extraction_rows.append(
        {
            "company_number": company_number,
            "company": company_name,
            "filing_date": sample_date,
            "sample_pages": chosen_pages if chosen_pages < 10_000 else "",
            "year": sample_year,
            "external_auditor": sample_auditor,
            "audit_fee": sample_fee,
            "fee_unit": sample_unit,
            "currency": sample_currency,
            "confidence": sample_conf,
            "status": "ok",
            "pdf_path": str(sample_pdf_path),
            "runtime_seconds": round(time.time() - company_start_ts, 2),
        }
    )

sample_extraction_df = pd.DataFrame(sample_extraction_rows)
display(sample_extraction_df)
sample_batch_elapsed = time.time() - sample_batch_start_ts
print(f"[SAMPLE] companies_processed={len(sample_extraction_rows)} runtime_seconds={sample_batch_elapsed:.2f}")
print(f"[SAMPLE] runtime_minutes={sample_batch_elapsed / 60:.2f}")


# # %% 7) Debug MinerU logs for failed sample rows
# if "sample_extraction_df" not in globals() or sample_extraction_df.empty:
#     print("No sample_extraction_df found.")
# else:
#     failed = sample_extraction_df[sample_extraction_df["status"] != "ok"]
#     if failed.empty:
#         print("No failed sample rows.")
#     else:
#         display(failed)
#         for _, row in failed.iterrows():
#             company_number = str(row["company_number"])
#             filing_date = str(row["filing_date"])
#             out_dir = MINERU_OUTPUT_DIR / f"{company_number}_{filing_date}"
#             sample_stdout_log = out_dir / "mineru_stdout.log"
#             sample_stderr_log = out_dir / "mineru_stderr.log"
#             print(f"\n=== {company_number} {filing_date} ===")
#             print(f"stdout log: {sample_stdout_log}")
#             print(f"stderr log: {sample_stderr_log}")
#             if sample_stderr_log.exists():
#                 err_lines = sample_stderr_log.read_text(encoding='utf-8', errors='ignore').splitlines()
#                 print(f"[stderr] lines={len(err_lines)}")
#                 for ln in err_lines[-80:]:
#                     print(ln)


# %% 8) Full run and save CSV
start_ts = time.time()
history_rows: List[Dict[str, str]] = []

run_companies = [first_company] if RUN_ONLY_PREVIEW_COMPANY else companies
for c in run_companies:
    company_number = str(c.get("company_number") or "")
    if not company_number:
        continue
    company_name = str(c.get("title") or company_number)

    filings = account_filings(
        session=session,
        headers=headers,
        sleep_seconds=SLEEP_SECONDS,
        company_number=company_number,
        limit=MAX_FILINGS_PER_COMPANY,
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

        pdf_path = DOWNLOAD_DIR / f"{company_number}_{filing_date}.pdf"
        if not pdf_path.exists():
            ok = download_pdf(session=session, headers=headers, pdf_url=pdf_url, output_path=pdf_path)
            if not ok:
                continue

        per_pdf_output = MINERU_OUTPUT_DIR / f"{company_number}_{filing_date}"
        text = run_mineru_extract(
            pdf_path=pdf_path,
            output_dir=per_pdf_output,
            backend=MINERU_BACKEND,
            method=MINERU_METHOD,
            lang=MINERU_LANG,
            force_refresh=MINERU_FORCE_REFRESH,
            device=MINERU_DEVICE,
            formula=MINERU_FORMULA,
            table=MINERU_TABLE,
            source=MINERU_SOURCE,
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
history_df = pd.DataFrame(history_rows)
shortlist_df = pd.DataFrame(shortlist_rows)

write_csv(
    HISTORY_CSV,
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
    SHORTLIST_CSV,
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

elapsed = time.time() - start_ts
print(f"[DONE] history CSV: {HISTORY_CSV}")
print(f"[DONE] shortlist CSV: {SHORTLIST_CSV}")
print(f"[DONE] rows: history={len(history_rows)} shortlist={len(shortlist_rows)}")
print(f"[DONE] runtime_seconds: {elapsed:.2f}")
print(f"[DONE] runtime_minutes: {elapsed / 60:.2f}")
display(history_df.head(200))
display(shortlist_df)

# %%
