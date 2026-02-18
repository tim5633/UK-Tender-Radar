# UK Tender Radar

## Executive Summary

This project builds a practical tender radar for UK companies by extracting key audit signals from Companies House filings:
- `company`
- `external_auditor`
- `audit_fee`
- `currency`
- `year`

Business goal:
1. Identify companies with **high audit fees**.
2. Identify companies with **long auditor tenure** (possible upcoming tender window).
3. Prioritize outreach/coverage using a shortlist score.

How it works:
1. Pull company filings from Companies House.
2. Download accounts PDFs.
3. Extract text (fast sampled mode + OCR fallback for scanned files).
4. Parse auditor/fee/unit/currency/year.
5. Generate:
   - Full historical dataset (`history` CSV)
   - Action shortlist (`shortlist` CSV)

Why this is useful:
1. Converts unstructured annual-report text into analyzable rows.
2. Supports monitoring auditor change risk and tender timing.
3. Provides a repeatable pipeline for large-scale UK coverage.

Current caveats:
1. Disclosure formats differ by company and year.
2. Scanned PDF quality can affect extraction accuracy.
3. Some fee lines are ambiguous and require stricter business rules.

Single-file Python pipeline to extract:
- `external_auditor`
- `audit_fee`
- `currency`
- `year`

From UK Companies House accounts filings, then rank companies by tender priority.

## Files
- `tender_radar.py`: main script
- `requirements.txt`: dependencies
- `.gitignore`: repo hygiene

## Setup
```bash
conda activate audit_fee
pip install -r requirements.txt
cp .env.example .env
```

## Run
Set API key in project-root `.env`:
```bash
CH_API_KEY=YOUR_COMPANIES_HOUSE_API_KEY
```

## First Time Notes
- This repo is ready to run inside your GitHub project folder.
- Keep secrets in `.env` (do not commit API keys to GitHub).
- Downloaded PDFs are stored in `uk_accounts_pdfs/` and are ignored by git by default.

Then run:
```bash
python tender_radar.py \
  --company-query "plc" \
  --max-companies 200 \
  --max-filings-per-company 5 \
  --enable-ocr-fallback \
  --history-csv ./tender_history.csv \
  --shortlist-csv ./tender_shortlist.csv
```

You can still override with `--api-key` / `--api-key-file` if needed.

## Outputs
- `tender_history.csv`:
  `company_number, company, year, external_auditor, audit_fee, fee_unit, currency, filing_date, confidence, pdf_path`
- `tender_shortlist.csv`:
  `company_number, company, current_external_auditor, continuous_tenure_years, latest_audit_fee_gbp, priority_score, tender_status`

## Notes
- The script uses a fast sampled text strategy (front pages + sampled tail pages) for speed.
- Add `--enable-ocr-fallback` for better recall on scanned PDFs.
- Some scanned PDFs may still have missing fields; these show low confidence and can be reviewed separately.

## Architecture & Logic

### 1) System architecture
1. **Data source layer**: Companies House APIs (`search`, `filing-history`, `document metadata`, `PDF download`).
2. **Ingestion layer**: Download filings PDF to local folder (`uk_accounts_pdfs`).
3. **Text extraction layer**:
   - Fast path: sampled PDF text extraction.
   - Fallback path: targeted OCR for scanned/low-text PDFs.
4. **Information extraction layer**:
   - `external_auditor`
   - `audit_fee`
   - `fee_unit` (thousand/million/billion)
   - `currency`
   - `year`
5. **Normalization & scoring layer**:
   - Auditor name normalization (PwC/EY/KPMG/Deloitte, etc.)
   - Fee normalization to GBP-equivalent numeric field for ranking
   - Tenure estimation and tender priority score.
6. **Output layer**:
   - Detailed history CSV
   - Shortlist CSV for tender radar.

### 2) End-to-end processing logic
1. Find companies by query (or fixed company list in `test_tender_radar.py`).
2. Fetch recent account filings per company.
3. Download filing PDF if local copy does not exist.
4. Extract sampled text; parse auditor/fee/unit/currency/year.
5. If key fields are missing, run OCR fallback and merge better values.
6. Append one row per filing to `history` output.
7. Build shortlist by latest auditor continuity + fee magnitude + scoring formula.
8. Write CSV outputs and print runtime.

### 3) Accuracy design
1. Multi-pattern auditor extraction:
   - Independent auditor report signature lines
   - Explicit `Auditor:` lines
   - Big-firm keyword with local context check.
2. Remuneration table-first fee extraction:
   - Prioritize rows around `auditor remuneration` / `fees payable`.
   - Fallback to narrative fee mentions.
3. Unit/currency detection:
   - Detect `GBP/USD/EUR` + symbols (`£`, `$`, `€`)
   - Detect unit hints (`000`, `thousand`, `million`, `bn`).
4. Confidence levels:
   - `high`: direct section/table match.
   - `medium`: contextual match.
   - `low`: weak/partial evidence.

### 4) Performance design
1. Sampled text extraction to avoid full-page OCR by default.
2. OCR only when needed (`--enable-ocr-fallback`), with page cap (`--ocr-max-pages`).
3. Local PDF caching (skip re-download if file exists).
4. API throttling via `--sleep-seconds` to reduce rate-limit risk.
5. Runtime metrics printed at end:
   - `runtime_seconds`
   - `runtime_minutes`.
  
## Test Results (`test_tender_history.csv`)

| company_number | company | year | external_auditor | audit_fee | fee_unit | currency | filing_date | confidence | pdf_path |
|---|---|---:|---|---:|---|---|---|---|---|
| SC095000 | LLOYDS BANKING GROUP PLC | 2025 |  | 2.0 | million | GBP | 2025-04-01 | low | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/SC095000_2025-04-01.pdf |
| SC095000 | LLOYDS BANKING GROUP PLC | 2024 | Deloitte |  | million | GBP | 2024-04-15 | medium | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/SC095000_2024-04-15.pdf |
| SC095000 | LLOYDS BANKING GROUP PLC | 2023 | Deloitte | 12 | million | GBP | 2023-06-02 | high | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/SC095000_2023-06-02.pdf |
| SC095000 | LLOYDS BANKING GROUP PLC | 2022 | PwC | 2021 | million | GBP | 2022-04-05 | medium | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/SC095000_2022-04-05.pdf |
| SC095000 | LLOYDS BANKING GROUP PLC | 2021 | PwC |  | million | GBP | 2021-04-28 | medium | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/SC095000_2021-04-28.pdf |
| 04190816 | BT GROUP PLC | 2025 | KPMG |  | million | GBP | 2025-08-08 | high | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/04190816_2025-08-08.pdf |
| 04190816 | BT GROUP PLC | 2024 | KPMG | 2070 | million | GBP | 2024-10-04 | medium | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/04190816_2024-10-04.pdf |
| 04190816 | BT GROUP PLC | 2023 | KPMG |  | million | GBP | 2023-09-18 | medium | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/04190816_2023-09-18.pdf |
| 04190816 | BT GROUP PLC | 2022 |  |  | million | GBP | 2022-10-05 | low | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/04190816_2022-10-05.pdf |
| 04190816 | BT GROUP PLC | 2021 | KPMG | 31 | million | GBP | 2021-08-19 | high | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/04190816_2021-08-19.pdf |
| 02128710 | HOWDEN JOINERY GROUP PLC | 2025 | KPMG | -1.1 | million | GBP | 2025-05-19 | medium | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/02128710_2025-05-19.pdf |
| 02128710 | HOWDEN JOINERY GROUP PLC | 2024 |  | -2022 | million | GBP | 2024-05-19 | low | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/02128710_2024-05-19.pdf |
| 02128710 | HOWDEN JOINERY GROUP PLC | 2023 | KPMG | 41 | million | GBP | 2023-05-19 | medium | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/02128710_2023-05-19.pdf |
| 02128710 | HOWDEN JOINERY GROUP PLC | 2022 | PwC | -0.5 | million | GBP | 2022-05-24 | medium | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/02128710_2022-05-24.pdf |
| 02128710 | HOWDEN JOINERY GROUP PLC | 2021 | PwC | 10 | million | GBP | 2021-06-03 | medium | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/02128710_2021-06-03.pdf |
| 01777777 | BRITISH AIRWAYS PLC | 2025 | KPMG |  | thousand | GBP | 2025-05-16 | high | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/01777777_2025-05-16.pdf |
| 01777777 | BRITISH AIRWAYS PLC | 2024 |  |  | million | GBP | 2024-12-16 | low | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/01777777_2024-12-16.pdf |
| 01777777 | BRITISH AIRWAYS PLC | 2024 | KPMG |  | thousand | GBP | 2024-03-28 | high | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/01777777_2024-03-28.pdf |
| 01777777 | BRITISH AIRWAYS PLC | 2023 | KPMG |  | million | GBP | 2023-04-25 | high | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/01777777_2023-04-25.pdf |
| 01777777 | BRITISH AIRWAYS PLC | 2022 | KPMG |  | million | GBP | 2022-05-24 | medium | /Users/timliu/Documents/GitHub/UK-Tender-Radar/uk_accounts_pdfs/01777777_2022-05-24.pdf |

## Test Results (`test_tender_shortlist.csv`)

| company_number | company | current_external_auditor | continuous_tenure_years | latest_audit_fee_gbp | priority_score | tender_status |
|---|---|---|---:|---:|---:|---|
| 04190816 | BT GROUP PLC | KPMG | 3 | 2070000000 | 54.5 | monitor |
| 02128710 | HOWDEN JOINERY GROUP PLC | KPMG | 1 | 41000000 | 37.18 | monitor |
| SC095000 | LLOYDS BANKING GROUP PLC |  | 0 | 2021000000 | 35.0 | monitor |
| 01777777 | BRITISH AIRWAYS PLC | KPMG | 1 |  | 6.5 | monitor |

### 5) Known limitations
1. Company disclosures are heterogeneous; wording and table layouts vary significantly.
2. Scanned PDFs can still degrade extraction quality.
3. Some filings contain multiple fee lines (group/statutory/subsidiary/pension) requiring stricter business rules.
4. Year duplication can occur when multiple filings exist in one period.

