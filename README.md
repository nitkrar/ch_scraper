# ch-bulk

Download and query UK Companies House bulk data by SIC code.

Uses [DuckDB](https://duckdb.org/) for fast CSV ingestion and analytical queries. Downloads the free monthly bulk data snapshots from Companies House (~470 MB) and builds a local database you can query by SIC code.

## Install

**Prerequisite:** Python 3.12 or newer. Check with `python3 --version`.

### Option A: pip install (simplest)

```bash
pip install git+https://github.com/nitkrar/ch_scraper.git
```

This installs `ch-bulk` and the `ChBulk` Python API. No cloning needed.

### Option B: Clone and setup script

```bash
git clone https://github.com/nitkrar/ch_scraper.git
cd ch_scraper
./setup.sh
source .venv/bin/activate
```

The setup script creates an isolated virtual environment, installs everything, and verifies the installation.

## Quick Start

### Option 1: Python API (no CLI knowledge needed)

```python
from ch_bulk import ChBulk

# Create an instance (uses ./data for downloads, ch_bulk.duckdb for the database)
ch = ChBulk()

# Download Companies House data and build the database (one command)
ch.sync()

# Find all active software development companies
companies = ch.query("62012")
print(f"Found {len(companies)} companies")

# Look at the first result
print(companies[0])
# {'company_number': '12345678', 'company_name': 'ACME SOFTWARE LTD', ...}

# Search multiple SIC codes at once
companies = ch.query("62012,69201")

# Include dissolved/inactive companies too
companies = ch.query("62012", status=None)

# Export results to a CSV file
ch.query("62012", output_csv="software_companies.csv")

# Export the whole database to SQLite format
ch.export_sqlite("ch_bulk.sqlite")

# See what's in the database
stats = ch.info()
print(f"Total companies: {stats['total_companies']}")
```

### Option 2: Command Line

```bash
# Download and build database in one step
ch-bulk sync

# Query by SIC code
ch-bulk query 62012

# Search multiple codes, export to CSV
ch-bulk query 62012,69201 --output-csv results.csv

# Only show first 20 results
ch-bulk query 62012 --limit 20

# Include all company statuses (not just Active)
ch-bulk query 62012 --status ""

# Export database to SQLite
ch-bulk export-sqlite --output ch_bulk.sqlite

# Show database statistics
ch-bulk info
```

### Step by Step (if you prefer more control)

```bash
# Step 1: Download bulk data files (7 parts, ~70 MB each)
ch-bulk download

# Step 2: Process CSVs into DuckDB database
ch-bulk process

# Step 3: Query
ch-bulk query 62012
```

## Setup On A New Machine

1. Clone the repo and install the package:

```bash
git clone https://github.com/nitkrar/ch_scraper.git
cd ch_scraper
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

2. Create your local settings file and fill in the API keys:

```bash
cp data/settings.example.json data/settings.json
```

3. Load the bulk source data first. In the current CLI surface `ch-bulk sync` refreshes the Companies House bulk data. After that, run the CQC and HSCA bulk steps from your local workflow before importing or rebuilding derived tables.

4. Pick one rebuild path:

- Full rebuild: run the match and enrichment/classification pipeline locally. Expect roughly 6-8 hours end-to-end when the CH/CQC API enrichers and website classification all need to be regenerated.
- Option B, migration bundle: run the bulk sync workflow first (`ch-bulk sync` for Companies House, plus the CQC and HSCA bulk steps you already use locally), then import a Parquet bundle with `ch-bulk migration import --bundle /path/to/portable-bundle`. The bundle only contains the portable derived tables, so importing before bulk sync is invalid. After import, run `ch-bulk match --mode all` to refresh deterministic matches from local bulk plus imported API data.

5. Start the local LLM before website classification runs. A typical `llama.cpp` flow is:

```bash
# Download Qwen2.5-14B-Instruct-Q4_K_M.gguf from a GGUF source such as Hugging Face.
llama-server \
  -m /absolute/path/Qwen2.5-14B-Instruct-Q4_K_M.gguf \
  --ctx-size 16384 \
  --host 127.0.0.1 \
  --port 9741
```

6. Optional browser fallback for JS-heavy sites:

```bash
pip install -e '.[browser]'
playwright install chromium
```

7. Useful commands after setup:

```bash
ch-bulk match
ch-bulk cqc-enrich providers --mode incremental
ch-bulk cqc-enrich locations --mode incremental
ch-bulk ch-enrich directors
ch-bulk ch-enrich revenue
ch-bulk classify --mode incremental
ch-bulk migration export --bundle data/migration/portable-bundle
ch-bulk migration import --bundle data/migration/portable-bundle
```

## What Data Is Available?

Each company record includes:

| Field | Example |
|---|---|
| `company_number` | `"12345678"` |
| `company_name` | `"ACME SOFTWARE LTD"` |
| `company_status` | `"Active"`, `"Dissolved"`, `"Liquidation"` |
| `company_type` | `"Private Limited Company"`, `"PLC"`, `"LLP"` |
| `sic_code_1` to `sic_code_4` | `"62012"` (up to 4 codes per company) |
| `sic_text_1` to `sic_text_4` | `"62012 - Business and domestic software development"` |
| `registered_address` | `"123 High Street, London, England"` |
| `postcode` | `"EC1A 1BB"` |
| `incorporation_date` | `2015-03-20` |
| `country_of_origin` | `"United Kingdom"` |

History tracking columns (added by the upsert pipeline):

| Field | Meaning |
|---|---|
| `is_active` | `TRUE` if the company appeared in the most recent scrape |
| `first_scrape_date` | Date of the first scrape this row appeared in |
| `last_scrape_date` | Date of the most recent scrape this row appeared in |
| `marked_inactive_scrape_date` | Date the row first stopped appearing (NULL if still active) |
| `last_enriched_at` | TIMESTAMP set by enrichment runs (NULL until enriched) |

## Upsert model — preserving history across scrapes

Each `ch-bulk process` (and the GUI's Process button) does an
**inverted-model upsert**: build a fresh `companies` table from the
new scrape, then carry forward rows from the previous table that
weren't in the new scrape (marking them inactive). The result is
a running historical record — dissolved/struck-off companies stay
in the DB with `is_active = FALSE` instead of being deleted.

Two sanity checks gate the upsert (5% threshold each, force-overridable
via the GUI prompt or `--force` CLI flag):

- **Row-count delta** — aborts if the new scrape differs by >5% from
  the existing table. Catches missing CSV parts or truncated downloads.
- **Inactive churn** — aborts if the new scrape would mark >5% of
  currently-active rows as inactive.

A third check is **strict zero** and cannot be overridden:

- **Duplicate company_numbers in source** — aborts immediately if the
  staging table has duplicate keys. This indicates a real data problem
  in the source CSV.

Multi-month replay: drop multiple months' CSVs into `data/input/ch/`
and `ch.process()` will run them in chronological order — first month
bootstraps, each subsequent month upserts. Useful for backfilling
history. Production usage typically has just the latest month.

## Why DuckDB (not SQLite or another DB)?

CH bulk data is analytical and read-heavy: large CSV ingest, columnar
aggregations (SIC code group-bys, status filters), and SCD-style
upserts. DuckDB ships native `read_csv_auto()` (no separate CSV
loader), columnar storage (~2-3× smaller files than row-oriented),
MERGE INTO, session variables, and `COPY FROM DATABASE` for
single-statement compaction. SQLite would work but requires writing
the CSV ingest yourself, and aggregations would be slower. We do
support exporting to SQLite (`ChBulk.export_sqlite()`) for downstream
tools that need it.

## Data Source

Companies House publishes free monthly snapshots of all live UK companies:
http://download.companieshouse.gov.uk/en_output.html

Updated within 5 working days of each month end. No API key required.

## Common SIC Codes

| Code | Description |
|---|---|
| `62012` | Business and domestic software development |
| `62020` | IT consultancy activities |
| `68320` | Management of real estate |
| `69201` | Accounting and auditing activities |
| `86210` | General medical practice activities |
| `88100` | Social work without accommodation |

Full list: https://resources.companieshouse.gov.uk/sic/

## Troubleshooting

**"Python 3.12+ is required but not found"** — Install from https://www.python.org/downloads/

**"command not found: ch-bulk"** — Run `source .venv/bin/activate` first

**Download is slow** — Companies House servers can be slow. Downloads resume automatically if interrupted — just run `ch-bulk download` again.

**"No BasicCompanyData CSV files found"** — Run `ch-bulk download` before `ch-bulk process`, or use `ch-bulk sync` to do both.

## License

MIT
