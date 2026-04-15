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
