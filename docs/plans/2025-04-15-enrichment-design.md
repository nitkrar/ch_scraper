# Companies House Enrichment — Design Document

Date: 2025-04-15
Status: Revised (v3 — parallel fetching + DB design notes)
Reviews: [enrichment-design-review-claude.md](enrichment-design-review-claude.md), [enrichment-design-review-2.md](enrichment-design-review-2.md)

## Overview

Extend ch_bulk to enrich companies from the local DuckDB database with structured JSON data from the Companies House REST API. Users select companies from bulk search results, trigger enrichment (single or batch), and view/export the enriched data. Supports GUI, CLI, and Python API.

## Changes from v1

- **Dropped asyncio** — entire stack stays synchronous (httpx.Client + threading), matching existing codebase patterns
- **DuckDB write safety** — shared connection with threading.Lock for writes, read_only connections for reads
- **Re-enrichment strategy** — DELETE + INSERT in a transaction, with 'partial' status for partial failures
- **Primary keys** on all enrichment tables
- **Removed `company_profile_enriched` table** — bulk data already has most fields, store only API-exclusive fields in enrichment_log
- **Fixed Treeview interaction** — text indicators with click detection instead of impossible icon widgets
- **Added popup window** (Toplevel) as second viewing option alongside right panel
- **Rate limiter** — standalone thread-safe class using threading.Lock + time.monotonic()
- **API key** — os.environ instead of python-dotenv (project already has .envrc)
- **Filing history capped** at 100 items by default
- **Company number auto-padding** for CLI input
- **Batch enrichment confirmation** prompt for large sets
- **Raw JSON storage** for debugging and future-proofing
- **Defined EnrichmentResult** dataclass
- **v3: ThreadPoolExecutor** for parallel endpoint fetching within a single company (6 endpoints at once)
- **v3: DB design rationale** — why DuckDB tables over single JSON blob, normalization trade-offs

## Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                            GUI (Tkinter)                                   │
│  ┌──────────────────────────────┐  ┌────────────────────────────────────┐ │
│  │   Main Area (existing)       │  │   Right Panel (PanedWindow, new)   │ │
│  │   - Search/filter controls   │  │   - Company header                 │ │
│  │   - Results table            │  │   - Officers section               │ │
│  │     + checkbox column        │  │   - PSCs section                   │ │
│  │     + action text column     │  │   - Charges section                │ │
│  │       [E] enrich             │  │   - Filing History section         │ │
│  │       [V] view (enriched)    │  │   - Insolvency section             │ │
│  │       [P] popup (enriched)   │  │   [Close X]                        │ │
│  │   - Pagination               │  └────────────────────────────────────┘ │
│  │   - "Enrich Selected" btn    │                                         │
│  └──────────────────────────────┘  ┌────────────────────────────────────┐ │
│                                     │   Popup Window (Toplevel, new)     │ │
│                                     │   - Same content as right panel    │ │
│                                     │   - Independent window             │ │
│                                     │   - Multiple can be open           │ │
│                                     │   - Full-width, no table squeeze   │ │
│                                     └────────────────────────────────────┘ │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │   Status/Progress bar (existing, reused for enrichment)             │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────┘
         │                                      │
         ▼                                      ▼
┌─────────────────┐                   ┌──────────────────┐
│   CLI (Typer)   │                   │  Python API      │
│   ch-bulk       │                   │  ChBulk class    │
│   enrich ...    │                   │  .enrich()       │
│   officers ...  │                   │  .get_officers() │
└────────┬────────┘                   └────────┬─────────┘
         │                                      │
         ▼                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Enrichment Module (new)                               │
│    enrichment.py — calls CH API, normalizes JSON, stores to DB          │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                  ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐
│  CH API Client   │  │  DuckDB Storage  │  │  Environment │
│  ch_client.py    │  │  (new tables)    │  │  CH_API_KEY  │
│  - rate limiter  │  │  officers, pscs  │  │  via .envrc  │
│  - retry logic   │  │  charges, etc.   │  │  or env var  │
│  - auth          │  │                  │  └──────────────┘
│  - SYNCHRONOUS   │  │  Write lock      │
└──────────────────┘  └──────────────────┘
```

## Layer 1: CH API Client — `ch_bulk/ch_client.py`

New module. Fully synchronous. Handles all HTTP communication with the Companies House REST API.

### Authentication
- API key from `os.environ["CH_API_KEY"]` with clear error message if missing
- No python-dotenv dependency — users set via `.envrc`, shell profile, or `export`
- Passed as HTTP Basic Auth username with blank password (standard CH pattern)

### Rate Limiting — Standalone `RateLimiter` Class
```python
class RateLimiter:
    """Thread-safe sliding-window rate limiter."""
    def __init__(self, max_requests: int = 580, window_seconds: float = 300.0):
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()
        self._max = max_requests
        self._window = window_seconds

    def acquire(self) -> None:
        """Block until a request slot is available."""
        while True:
            with self._lock:
                now = time.monotonic()
                # Prune expired timestamps
                while self._timestamps and self._timestamps[0] <= now - self._window:
                    self._timestamps.popleft()
                if len(self._timestamps) < self._max:
                    self._timestamps.append(now)
                    return
                # Calculate sleep time
                sleep_until = self._timestamps[0] + self._window
            time.sleep(max(0, sleep_until - time.monotonic()) + 0.1)
```

- Thread-safe via `threading.Lock`
- Uses `time.monotonic()` (immune to NTP clock jumps)
- 580 requests per 300 seconds (safety margin under CH's 600/5min hard limit)
- Shared across all threads — single instance owned by CHClient

### Retry Logic
- On HTTP 429: retry up to 3 times with escalating backoff (30s, 60s, 90s)
- On HTTP 5xx: retry up to 2 times with 5s backoff
- On HTTP 404: return None (company not found, not an error)
- All other HTTP errors: raise with context

### Company Number Normalization
```python
def normalize_company_number(number: str) -> str:
    """Pad numeric-only company numbers to 8 digits.
    Leave alphanumeric prefixed numbers (SC, NI, OC, etc.) as-is."""
    number = number.strip()
    if number.isdigit():
        return number.zfill(8)
    return number.upper()
```

### Interface
```python
class CHClient:
    """Synchronous Companies House API client."""

    def __init__(self, api_key: str | None = None):
        self._api_key = api_key or os.environ["CH_API_KEY"]
        self._client = httpx.Client(
            base_url="https://api.company-information.service.gov.uk",
            auth=(self._api_key, ""),
            timeout=30.0,
        )
        self._rate_limiter = RateLimiter()

    def get_company_profile(self, company_number: str) -> dict | None: ...
    def get_officers(self, company_number: str, max_items: int = 200) -> list[dict]: ...
    def get_pscs(self, company_number: str, max_items: int = 200) -> list[dict]: ...
    def get_charges(self, company_number: str, max_items: int = 200) -> list[dict]: ...
    def get_filing_history(self, company_number: str, max_items: int = 100) -> list[dict]: ...
    def get_insolvency(self, company_number: str) -> list[dict]: ...
```

All methods are synchronous. Pagination handled internally with configurable `max_items` cap. Filing history defaults to 100 items (old companies can have thousands).

## Layer 2: Enrichment Module — `ch_bulk/enrichment.py`

Orchestrates enrichment: calls CH API client, normalizes responses, stores to DuckDB.

### EnrichmentResult Dataclass
```python
@dataclass
class EnrichmentResult:
    company_number: str
    status: str  # 'success', 'partial', 'error', 'not_found'
    error_message: str | None = None
    endpoints: dict[str, str] = field(default_factory=dict)
    # e.g. {"officers": "ok", "pscs": "ok", "charges": "error: 500"}
```

### Single Company Enrichment — Parallel Endpoint Fetching
```python
def enrich_company(
    client: CHClient,
    db_path: Path,
    company_number: str,
    db_lock: threading.Lock,
    progress_callback: Callable | None = None,
) -> EnrichmentResult:
    """Fetch all endpoint data for one company in parallel, store to DB.

    Uses ThreadPoolExecutor to fire all 6 endpoints concurrently.
    Each endpoint call goes through the shared RateLimiter.acquire()
    which blocks until a slot is available — so parallelism is
    naturally bounded by the rate limit.

    If some endpoints fail, the successful ones are still stored
    and status is set to 'partial'.
    """
```

#### How parallel fetching works

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def enrich_company(client, db_path, company_number, db_lock, progress_callback=None):
    endpoints = {
        "profile": client.get_company_profile,
        "officers": client.get_officers,
        "pscs": client.get_pscs,
        "charges": client.get_charges,
        "filing_history": client.get_filing_history,
        "insolvency": client.get_insolvency,
    }

    results = {}
    endpoint_status = {}

    # Fire all 6 endpoints in parallel — rate limiter throttles naturally
    with ThreadPoolExecutor(max_workers=6) as pool:
        future_to_name = {
            pool.submit(fn, company_number): name
            for name, fn in endpoints.items()
        }
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                results[name] = future.result()
                endpoint_status[name] = "ok"
            except Exception as exc:
                endpoint_status[name] = f"error: {exc}"

    # Store successful results to DB (serialized via db_lock)
    with db_lock:
        con = duckdb.connect(str(db_path))
        try:
            for name, data in results.items():
                if data is not None:
                    _store_endpoint_data(con, company_number, name, data)
            _write_enrichment_log(con, company_number, endpoint_status)
        finally:
            con.close()

    return EnrichmentResult(...)
```

**Why this works:**
- 6 threads each call `rate_limiter.acquire()` before making HTTP requests
- The rate limiter blocks threads that can't get a slot — prevents overload
- For a single company, all 6 endpoints typically complete in ~3-4 seconds (vs ~6-7 sequential) since they compete for rate limit slots
- For batch enrichment of N companies, the rate limiter is still the bottleneck — but within each company we get parallelism

### Re-enrichment Strategy
For each company being enriched:
1. Call all API endpoints in parallel, collect results
2. Acquire `db_lock`, then for each successful endpoint within a single connection:
   ```sql
   BEGIN TRANSACTION;
   DELETE FROM officers WHERE company_number = ?;
   INSERT INTO officers VALUES (...);
   DELETE FROM pscs WHERE company_number = ?;
   INSERT INTO pscs VALUES (...);
   -- ... all tables for this company ...
   COMMIT;
   ```
   All tables for one company are updated in a single transaction — so re-enrichment is atomic. Either all new data replaces old data, or none does.
3. Write to `enrichment_log` with appropriate status:
   - All endpoints succeeded → `'success'`
   - Some endpoints failed → `'partial'` (with details in `endpoints_json`)
   - All endpoints failed → `'error'`
   - Company not found (404 on profile) → `'not_found'`

### Batch Enrichment
```python
def enrich_batch(
    client: CHClient,
    db_path: Path,
    company_numbers: list[str],
    db_lock: threading.Lock,
    progress_callback: Callable | None = None,
) -> list[EnrichmentResult]:
    """Enrich multiple companies with rate limiting.

    Each company's 6 endpoints are fetched in parallel via
    ThreadPoolExecutor(max_workers=6) inside enrich_company().
    Companies are processed sequentially — the rate limiter is
    the real bottleneck at ~2 req/sec, so parallelizing across
    companies would not improve throughput.

    Always continues on per-company errors — never stops the batch.
    """
```

#### Throughput estimate
- Rate limit: 580 requests per 300 seconds ≈ 1.93 req/sec
- Per company: 6 endpoints (profile + officers + PSCs + charges + filings + insolvency)
- Some endpoints may need pagination (officers with many pages = more requests)
- **Best case**: ~32 companies per 5-minute window (580 / 6 endpoints × ~3 pages avg)
- **Typical**: ~20-25 companies per 5-minute window (with pagination overhead)
- **Batch of 100 companies**: ~20-25 minutes

### JSON Normalization
Each endpoint's JSON response gets normalized to flat dicts for DuckDB insertion:
- Nested objects (e.g., officer addresses) → flattened or dropped (address not stored)
- Lists (e.g., `nature_of_control` in PSCs) → comma-separated strings
- IDs extracted from URL paths (e.g., `/officers/ABC123/appointments` → `ABC123`)
- Raw JSON stored alongside normalized data for debugging

## Layer 3: DuckDB Schema — New Tables

### DB Design Rationale

**Why separate normalized tables (not a single JSON blob)?**

Option A (what we're doing): Separate `officers`, `pscs`, `charges`, `filing_history`, `insolvency` tables with flat columns.

Option B (alternative): Single `enrichment_data` table with `company_number`, `endpoint`, `raw_json` columns.

We chose Option A because:
1. **Queryable** — "find all companies where a director named X is appointed" is a simple SQL WHERE, not JSON path extraction. DuckDB supports JSON functions, but they're slower and harder to write.
2. **Exportable** — CSV/Excel export with LEFT JOINs gives flat rows. A JSON blob column isn't useful in a spreadsheet.
3. **Aggregatable** — "count of companies with outstanding charges" is `SELECT COUNT(DISTINCT company_number) FROM charges WHERE satisfied_on IS NULL`. With JSON this would require parsing every blob.
4. **CLI output** — `ch-bulk officers 12345678` can query the table directly and format as a Rich table. No JSON parsing needed.

**Trade-off**: We store `raw_json` as a VARCHAR column on every table anyway, so we get the best of both — structured columns for querying + raw JSON for debugging and future-proofing. If the CH API adds new fields, the raw JSON has them even before we add columns.

**Normalization level**: Deliberately flat (no separate address tables, no officer-to-appointment junction tables). One table per API endpoint. This keeps the schema simple and matches the 1:1 relationship between "what we fetch" and "where we store it." DuckDB is analytical, not transactional — over-normalizing hurts more than it helps.

### Write Safety
- All write operations use a shared `threading.Lock` (passed through as `db_lock`)
- Read operations use `duckdb.connect(db_path, read_only=True)` — safe for concurrent access
- Existing export functions (`export_query_csv`, `export_filtered_csv`) must be fixed to use `read_only=True` connections
- Only one write operation can run at a time, enforced by the GUI's `_run_task()` (already prevents concurrent background tasks) and the `db_lock` for the API/CLI path

### `enrichment_log`
| Column | Type | Constraint | Description |
|---|---|---|---|
| company_number | VARCHAR | PRIMARY KEY | FK to companies |
| enriched_at | TIMESTAMP | NOT NULL | When enrichment last ran |
| status | VARCHAR | NOT NULL | 'success', 'partial', 'error', 'not_found' |
| error_message | VARCHAR | | Error details if failed |
| endpoints_json | VARCHAR | | JSON: per-endpoint status |
| can_file | BOOLEAN | | API-only field (not in bulk data) |
| has_insolvency_history | BOOLEAN | | API-only field (not in bulk data) |

Note: `company_profile_enriched` table from v1 removed. Most profile fields already exist in the bulk `companies` table. Only API-exclusive fields (`can_file`, `has_insolvency_history`) stored here.

### `officers`
| Column | Type | Constraint |
|---|---|---|
| company_number | VARCHAR | PK (composite) |
| officer_id | VARCHAR | PK (composite) |
| name | VARCHAR | |
| officer_role | VARCHAR | |
| appointed_on | DATE | |
| resigned_on | DATE | |
| nationality | VARCHAR | |
| occupation | VARCHAR | |
| dob_month | INTEGER | |
| dob_year | INTEGER | |
| country_of_residence | VARCHAR | |
| raw_json | VARCHAR | |

### `pscs`
| Column | Type | Constraint |
|---|---|---|
| company_number | VARCHAR | PK (composite) |
| psc_id | VARCHAR | PK (composite) |
| name | VARCHAR | |
| kind | VARCHAR | |
| nature_of_control | VARCHAR | |
| notified_on | DATE | |
| ceased_on | DATE | |
| nationality | VARCHAR | |
| country_of_residence | VARCHAR | |
| raw_json | VARCHAR | |

### `charges`
| Column | Type | Constraint |
|---|---|---|
| company_number | VARCHAR | PK (composite) |
| charge_code | VARCHAR | PK (composite) |
| charge_number | INTEGER | |
| status | VARCHAR | |
| created_on | DATE | |
| delivered_on | DATE | |
| satisfied_on | DATE | |
| charge_holder | VARCHAR | |
| short_particulars | VARCHAR | |
| raw_json | VARCHAR | |

### `filing_history`
| Column | Type | Constraint |
|---|---|---|
| company_number | VARCHAR | PK (composite) |
| transaction_id | VARCHAR | PK (composite) |
| category | VARCHAR | |
| type | VARCHAR | |
| description | VARCHAR | |
| date | DATE | |
| barcode | VARCHAR | |
| raw_json | VARCHAR | |

### `insolvency`
| Column | Type | Constraint |
|---|---|---|
| company_number | VARCHAR | PK (composite) |
| case_number | INTEGER | PK (composite) |
| case_type | VARCHAR | |
| date_of_order | DATE | |
| practitioner_name | VARCHAR | |
| practitioner_firm | VARCHAR | |
| practitioner_role | VARCHAR | |
| raw_json | VARCHAR | |

## Layer 4: Python API — `ChBulk` Extensions

New methods on the existing `ChBulk` class:

```python
class ChBulk:
    def __init__(self, data_dir, db_path):
        ...
        self._db_lock = threading.Lock()  # NEW: shared write lock

    # ── Enrichment ──────────────────────────────────────────────
    def enrich(
        self,
        company_numbers: str | list[str],
        progress_callback: Callable | None = None,
    ) -> list[EnrichmentResult]:
        """Enrich one or many companies. Stores results to DB.
        Creates CHClient internally (reads CH_API_KEY from env).
        Handles single string or list. Returns per-company results."""

    # ── Querying enrichment data ────────────────────────────────
    def get_officers(self, company_number: str) -> list[dict]:
    def get_pscs(self, company_number: str) -> list[dict]:
    def get_charges(self, company_number: str) -> list[dict]:
    def get_filing_history(self, company_number: str) -> list[dict]:
    def get_insolvency(self, company_number: str) -> list[dict]:

    # ── Enrichment status ───────────────────────────────────────
    def is_enriched(self, company_number: str) -> bool:
    def get_enrichment_status(self, company_number: str) -> dict | None:
        """Returns {enriched_at, status, endpoints} or None."""
```

## Layer 5: CLI Commands

```bash
# Enrich single company (auto-pads to 8 digits)
ch-bulk enrich 12345678

# Enrich multiple (space-separated)
ch-bulk enrich 12345678 87654321 SC123456

# Enrich from file (one company number per line)
ch-bulk enrich --file companies.txt

# Enrich all companies matching a SIC code in the DB
# Shows count and prompts for confirmation before proceeding
ch-bulk enrich --sic 62012
# > Found 14,329 companies with SIC 62012. Enrich all? [y/N]

# Skip confirmation
ch-bulk enrich --sic 62012 --yes

# Limit batch size
ch-bulk enrich --sic 62012 --limit 100

# View enrichment data
ch-bulk officers 12345678
ch-bulk pscs 12345678
ch-bulk charges 12345678
ch-bulk filings 12345678

# Machine-readable output
ch-bulk officers 12345678 --json
```

Output format: Rich tables in terminal by default, `--json` flag for machine-readable output.

## Layer 6: GUI Changes

### Results Table — Action Column

Since `ttk.Treeview` cannot embed clickable widgets in cells, the actions column uses **text indicators with click detection**:

- **Action column** (last column) displays text tags:
  - `[E]` — always visible, triggers enrichment for that company
  - `[V]` — visible only for enriched companies, opens right panel
  - `[P]` — visible only for enriched companies, opens popup window
  - Not enriched: `[E]`
  - Enriched: `[E] [V] [P]`

- **Click detection** via Treeview event binding:
  ```python
  self.tree.bind("<ButtonRelease-1>", self._on_tree_click)

  def _on_tree_click(self, event):
      region = self.tree.identify_region(event.x, event.y)
      column = self.tree.identify_column(event.x)
      if region == "cell" and column == "#N":  # action column
          item = self.tree.identify_row(event.y)
          # Parse click position to determine which tag was clicked
          # Dispatch to _enrich_single, _show_panel, or _open_popup
  ```

- **Visual styling** via Treeview tags:
  - Enriched rows get a tag that changes background color (subtle green tint)
  - `self.tree.tag_configure("enriched", background="#e8f5e9")`

### Checkbox Column for Multi-Select

- First column `#0` (tree column) used for checkboxes via text: `"☑"` / `"☐"`
- Click on `#0` column toggles the check state
- **"Enrich Selected"** button in toolbar processes all checked rows
- **Select All / Deselect All** toggle in toolbar

### Right Panel (PanedWindow)

The results area uses `ttk.PanedWindow(orient="horizontal")`:
- **Left pane**: existing results table + pagination
- **Right pane**: detail viewer panel (hidden by default)

Opening the panel:
- Click `[V]` on an enriched row → right pane appears via `PanedWindow.add()`
- Table compresses. Default window width increased to `1200x700` with `minsize(900, 500)` to accommodate

Closing the panel:
- Click `[X]` button → `PanedWindow.remove()` restores full table width

Panel sections use the existing `_toggle_details` pattern (pack/pack_forget with toggle buttons):

```
┌──────────────────────────────────┐
│ [X]  ACME SOFTWARE LTD          │
│ Company No: 12345678             │
│ Status: Active                   │
│ Enriched: 2025-04-15 14:30       │
│ [Re-enrich]                      │
├──────────────────────────────────┤
│ ▼ Officers (3)                   │
│   Name         Role     Appointed│
│   J. Smith     Director 2020-01  │
│   A. Jones     Director 2018-06  │
│   B. Brown     Secretary 2019-03 │
├──────────────────────────────────┤
│ ▼ PSCs (2)                       │
│   Name         Control           │
│   J. Smith     75%+ shares       │
│   XYZ Holdings 25-50% shares     │
├──────────────────────────────────┤
│ ▶ Charges (1)                    │
├──────────────────────────────────┤
│ ▶ Filing History (12)            │
├──────────────────────────────────┤
│   No insolvency records          │
└──────────────────────────────────┘
```

### Popup Window (Toplevel)

Independent window opened via `[P]` action on enriched rows:
- `tk.Toplevel` — separate window, does not affect main window layout
- Same content/sections as the right panel (shared widget-building code)
- Multiple popup windows can be open simultaneously (for comparing companies)
- Each popup has its own `[Re-enrich]` button
- Window title: `"Company Details — ACME SOFTWARE LTD (12345678)"`
- Default size: `600x700`

The right panel and popup window share a common `_build_detail_view(parent_frame, company_number)` method that constructs the section widgets into any parent frame. This avoids duplicating the layout code.

### Enrichment Progress
Reuses existing progress bar and status label. During batch enrichment:
- Progress bar switches to determinate mode (percentage)
- Status label shows "Enriching 12/47 companies... (3 errors)"
- All buttons disabled during enrichment
- GUI's existing `_run_task` / `_poll_task` pattern reused — enrichment runs in a `threading.Thread`

## Layer 7: Export

### Current behavior preserved
- "Export CSV" continues to export the filtered company list as before

### Enrichment columns appended
When exporting, if any companies in the result set have been enriched, append summary columns via LEFT JOINs to enrichment summary subqueries:
- `officer_count` — number of active officers (resigned_on IS NULL)
- `psc_count` — number of active PSCs (ceased_on IS NULL)
- `has_charges` — yes/no (any outstanding charges)
- `has_insolvency` — yes/no
- `latest_filing_date` — most recent filing date
- `enriched_at` — when enrichment was performed

### Future: Multi-sheet Excel
Separate sheets for companies, officers, PSCs, charges. Each sheet includes company_number for joining. Not in initial scope.

## Dependencies

### Existing (no changes)
- `httpx>=0.27` — used synchronously (httpx.Client), already a dependency
- `duckdb>=1.0` — already a dependency
- `typer>=0.12` — already a dependency
- `rich>=13.0` — already a dependency (used for CLI table output)

### No new dependencies required
- API key via `os.environ` (no python-dotenv needed, project has .envrc)
- httpx.Client for HTTP (already installed)

## Out of Scope

- Streaming API / real-time updates
- PDF document downloads or iXBRL/accounts parsing
- Re-enrichment staleness warnings (user triggers manually via [E] or Re-enrich button)
- OAuth / filing API (write access)
- Financial data extraction from accounts
- Multi-sheet Excel export (future)

## Implementation Phases

**Phase 1 — Foundation**
- `RateLimiter` class (standalone, thread-safe, testable)
- `CHClient` class (sync httpx.Client, auth, retry, pagination, company number normalization)
- DuckDB schema creation (all new tables with PKs)
- `enrichment.py` module (enrich_company, enrich_batch, JSON normalization, DELETE+INSERT transactions)
- `ChBulk` API methods (enrich, get_officers, get_pscs, etc.)
- CLI: `ch-bulk enrich` command with single/batch/file/sic modes

**Phase 2 — CLI Viewing**
- `ch-bulk officers`, `ch-bulk pscs`, `ch-bulk charges`, `ch-bulk filings` commands
- Rich table output + `--json` flag

**Phase 3 — GUI**
- Action text column with click detection ([E], [V], [P])
- Checkbox column for multi-select + "Enrich Selected" button
- Enrichment progress (determinate progress bar)
- Right panel via PanedWindow
- Popup window via Toplevel
- Shared `_build_detail_view()` for both viewing modes
- Row styling for enriched companies
- Export with enrichment summary columns
