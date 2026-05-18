# Companies House API & Existing Scraper Research

Date: 2025-04-15

## 1. atchai/companies_house_scraper ("SIC Scanner")

### What It Does
Python CLI tool that scans Companies House by SIC code, downloads all active companies in a given sector, and extracts structured financial data from their annual accounts filings. Goal: sector-level financial intelligence.

### Companies House APIs Used
- **Advanced Search API** (`/advanced-search/companies`) — finds companies by SIC code, with date-range chunking to overcome the 5,000-result cap
- **Company Profile API** (`/company/{number}`) — full company profile
- **Filing History API** (`/company/{number}/filing-history?category=accounts`) — accounts filings list
- **Document API** (`document-api.company-information.service.gov.uk`) — 3-step download: metadata → content redirect (302) → S3 fetch (unauthenticated)

Does NOT use officers, charges, PSC, or insolvency APIs.

### Data Extracted from iXBRL Accounts
Revenue, gross profit, operating profit, net profit, total assets, net assets, cash, total debt, debtors, creditors, employee count, prior year revenue, accounts year-end dates. Uses XBRL concept mapping with segment/dimension-aware extraction.

### Architecture
- Python 3.12+, async (asyncio + httpx)
- SQLAlchemy 2.0 ORM with SQLite
- Typer CLI: `search`, `enrich`, `parse-accounts`, `run`, `stats`
- ixbrlparse for iXBRL, PyMuPDF + OpenAI GPT for PDF fallback
- 3-phase pipeline: Search → Enrich → Parse Accounts

### Rate Limiting
- Sliding-window: 580 requests per 300 seconds (under CH's 600/5min)
- 429 retry: up to 3 retries with 30s/60s/90s backoff
- Concurrency: asyncio.Semaphore (default 5)
- API key: `.env` via python-dotenv, HTTP Basic Auth

### Key Patterns
- **5,000-result cap workaround**: chunks by incorporation year
- **3-step document download**: metadata → 302 redirect → S3 fetch
- **XBRL segment-aware extraction**: creditors segmented by maturity
- **LLM fallback**: GPT vision for paper-filed PDFs (~1-2% of filings)

---

## 2. Companies House API — Full Endpoint Catalog

### Authentication
Register at developer.company-information.service.gov.uk, create application, get API key. Keys passed via HTTP Basic Auth (key as username, blank password). OAuth 2.0 for write APIs.

### Rate Limits
- **600 requests per 5-minute window**
- HTTP 429 on exceed, blocked until window resets
- Higher limits available on request

### REST API Endpoints

| API Area | Key Endpoints | Data Returned | Enrichment Value |
|---|---|---|---|
| **Company Profile** | `GET /company/{number}` | Name, status, type, SIC codes, registered address, incorporation date, accounting dates | **Critical** |
| **Search** | 7 endpoints: company, officer, dissolved, alphabetical, advanced, search-all | Matching companies/officers by name/keyword | **High** |
| **Officers** | `GET /company/{number}/officers` | Name, role, appointed/resigned dates, nationality, occupation, DOB (month/year) | **High** |
| **Officer Appointments** | `GET /officers/{id}/appointments` | All appointments across companies | **High** (network mapping) |
| **Filing History** | `GET /company/{number}/filing-history` | Form type, description, dates, document links | **Medium** |
| **Charges (Mortgages)** | `GET /company/{number}/charges` | Charge holder, dates, secured amounts, status | **High** (financial risk) |
| **PSCs** | 11 endpoints covering individuals, corporate entities, legal persons | Name, nature of control, share %, notified date | **Critical** (beneficial ownership) |
| **Insolvency** | `GET /company/{number}/insolvency` | Cases, practitioner details, case type, dates | **High** (risk assessment) |
| **Registers** | `GET /company/{number}/registers` | Register locations | **Low** |
| **Exemptions** | `GET /company/{number}/exemptions` | Filing exemptions | **Low** |
| **Disqualified Officers** | `GET /disqualified-officers/natural/{id}` | Disqualification details, dates, reasons | **High** (due diligence) |
| **UK Establishments** | `GET /company/{number}/uk-establishments` | UK establishments of overseas companies | **Low** |
| **Document API** | Separate API for filing documents | PDF/TIFF documents from filing history | **Medium** |

### Streaming API (Real-Time)

| Stream | Endpoint |
|---|---|
| Companies | `stream.companieshouse.gov.uk/companies` |
| Filings | `stream.companieshouse.gov.uk/filings` |
| PSCs | `stream.companieshouse.gov.uk/persons-with-significant-control` |
| Officers | `stream.companieshouse.gov.uk/officers` |
| Disqualified Officers | `stream.companieshouse.gov.uk/disqualified-officers` |
| Charges | `stream.companieshouse.gov.uk/charges` |

### Key Gaps
- **No structured financial data API** — only PDF accounts via Document API
- **No bulk download API** — bulk data is free monthly CSV snapshots only
