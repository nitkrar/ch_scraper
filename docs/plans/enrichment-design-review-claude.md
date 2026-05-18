VERDICT: NEEDS_REVISION

## Summary Assessment

The design is well-structured and the layered architecture (client / enrichment / storage / API / CLI / GUI) is sound. However, there are critical issues around the asyncio-in-Tkinter concurrency model, DuckDB concurrent write safety, missing UPSERT/re-enrichment semantics, and a rate limiter design that will not work correctly across threads.

## Critical Issues (must fix)

### 1. asyncio.run() cannot be called from a background thread alongside Tkinter

The design says `enrich()` in the Python API wraps async code via `asyncio.run()`. The GUI already runs long operations in `threading.Thread` (see `gui.py` lines 371, 472, 496). The call chain would be: Tkinter main thread -> spawns `threading.Thread` -> calls `ChBulk.enrich()` -> calls `asyncio.run()`.

This *technically* works (each thread can have its own event loop), but it introduces unnecessary complexity for no real gain. The CH API rate limit is 600 requests per 5 minutes (~2 req/sec). At that rate, the concurrency ceiling is so low that `asyncio.Semaphore(5)` with true async I/O provides negligible throughput improvement over a simple synchronous `httpx.Client` with a `ThreadPoolExecutor(max_workers=5)` or even sequential requests with a rate-limiter sleep.

**Recommendation:** Drop asyncio entirely. Use synchronous `httpx.Client` (already a project dependency used in `downloader.py`), with a simple `time.sleep()` rate limiter. This aligns with the existing codebase pattern (the downloader is fully synchronous with `httpx.Client`), avoids the threading+asyncio debugging nightmare, and simplifies the entire stack. If you later need higher concurrency, you can swap to `concurrent.futures.ThreadPoolExecutor` -- still no asyncio needed.

### 2. DuckDB concurrent access is unsafe for writes

The codebase opens fresh `duckdb.connect()` calls per operation (see every function in `query.py` and `processor.py`). DuckDB allows only **one writer process at a time** to a database file. The design has:
- GUI search queries running in background threads (`read_only=True` -- fine)
- Enrichment running in a background thread writing to the same DB

If a user triggers enrichment while an export is running (which opens a *write* connection in `export_query_csv` and `export_filtered_csv` -- they use `duckdb.connect(str(db_path))` without `read_only=True`), the writes will conflict and one will fail with a lock error.

**Recommendation:** 
1. Fix existing export functions to use `read_only=True` connections (they use temp tables unnecessarily -- can use subqueries or CTEs for COPY).
2. For enrichment writes, either use a single long-lived connection with a mutex, or serialize all write operations through a queue. Document that only one write operation can run at a time and enforce it in `_run_task()`.

### 3. No UPSERT semantics -- re-enrichment will create duplicates

The design says enrichment "inserts into DuckDB tables" but never mentions what happens when a company is enriched a second time. The `enrichment_log` table has no UNIQUE constraint, and neither do `officers`, `pscs`, `charges`, etc. Re-enriching the same company will duplicate all rows.

**Recommendation:** Before inserting enrichment data for a company, `DELETE FROM officers WHERE company_number = ?` (and same for all tables), then insert fresh. Wrap the delete+insert in a transaction. Alternatively, use DuckDB's `INSERT OR REPLACE` with a composite primary key, but the delete-then-insert pattern is simpler and makes the "replace all data" intent explicit.

### 4. Sliding-window rate limiter will not be thread-safe as described

The design says "track timestamps of last N requests" with a sliding window. If the CHClient is async (Semaphore-based), this is fine within a single event loop. But if you drop to sync-with-threads (per recommendation #1), a naive `list` of timestamps accessed from multiple threads without a lock will race.

Even in the async model: if `asyncio.run()` is called from a background thread, and progress callbacks update Tkinter via `root.after()`, the rate limiter state lives in the async event loop's thread -- that part is safe. But the design does not make this threading boundary explicit.

**Recommendation:** If staying async, document the threading model clearly. If going sync, use `threading.Lock` around the timestamp deque, or use a simpler approach: a `threading.Semaphore` combined with `time.sleep()` to enforce the rate.

### 5. Missing primary keys / unique constraints on enrichment tables

None of the enrichment tables define a PRIMARY KEY. The `officers` table has `officer_id` but no UNIQUE constraint. The `charges` table uses `charge_number` (INTEGER) which may not be globally unique (it is per-company). The `filing_history` table has `transaction_id` but no constraint. The `insolvency` table has `case_number` but again no uniqueness guarantee.

Without keys, there is no way to detect or prevent duplicates, no efficient lookups by ID, and no referential integrity.

**Recommendation:** Add composite primary keys: `(company_number, officer_id)` for officers, `(company_number, charge_code)` for charges, `(company_number, transaction_id)` for filing_history, `(company_number, case_number)` for insolvency, `(company_number, psc_id)` for PSCs.

### 6. `company_profile_enriched` table duplicates data already in `companies`

The bulk CSV data already contains `has_charges` (via `num_mort_charges`), account dates (`accounts_last_made_up`, `accounts_next_due`), and confirmation statement dates (`conf_stmt_last_made_up`, `conf_stmt_next_due`). The `company_profile_enriched` table replicates most of these fields.

**Recommendation:** Either (a) skip the `company_profile_enriched` table entirely and just use the existing `companies` data plus the `enrichment_log`, or (b) rename it to something like `company_api_profile` and store only fields that are *not* in the bulk data (e.g., `can_file`, `has_insolvency_history` which is more current than bulk data). Make the purpose clear.

## Suggestions (nice to have)

### 7. Filing history pagination could be expensive

The CH filing history endpoint can return hundreds of filings for long-lived companies. The design says "handles pagination automatically" but does not address limits. A company incorporated in 1900 could have thousands of filings.

**Suggestion:** Add a `max_items` parameter (default 100) to `get_filing_history()` to cap how many pages are fetched. Let the user override it if they want the full history.

### 8. Consider storing raw JSON alongside normalized data

Flattening complex nested JSON (officer addresses, PSC control details) loses information. If the API response format changes, re-enrichment would be needed.

**Suggestion:** Add a `raw_json` TEXT column to each enrichment table (or a single `enrichment_raw` table keyed by `(company_number, endpoint)`). This allows debugging, auditing, and future re-processing without re-fetching.

### 9. GUI right panel will be cramped on 1050x700 minimum window

The current window geometry is `1050x700` with `minsize(800, 500)`. The results table already uses most of the horizontal space with 8 columns. Adding a right panel will either compress the table to an unusable width or require a significantly larger minimum window.

**Suggestion:** Consider a bottom panel (below the results table) or a pop-up/dialog window instead of a right panel. Alternatively, increase the minimum width to at least 1400px when the panel is open, and make the panel togglable so it does not permanently consume space. A `PanedWindow` widget would let the user drag the split point.

### 10. python-dotenv is a new dependency -- consider alternatives

The project currently has only 4 dependencies. `python-dotenv` adds a 5th just for reading one env var. You could use `os.environ.get("CH_API_KEY")` and tell users to `export CH_API_KEY=...` or set it in `.envrc` (which the repo already has).

**Suggestion:** Use `os.environ["CH_API_KEY"]` with a clear error message when missing. Skip `python-dotenv`. If you do want `.env` support, `httpx` does not provide it, but you could use a 3-line manual parser instead of a full dependency.

### 11. Error reporting for partial batch failures needs design

The design mentions `EnrichmentResult` but does not define it. In a batch of 500 companies, if 3 fail with HTTP 500 and 1 returns 404:
- Should the batch continue or stop?
- How does the GUI report partial success?
- Is the enrichment_log entry per-company or per-batch?

**Suggestion:** Define `EnrichmentResult` explicitly (dataclass with company_number, status, error_message, endpoints_fetched). Always continue on per-company errors. Write to `enrichment_log` per company. The GUI progress should show "Enriched 496/500 (4 errors)" and allow viewing the error log.

### 12. CLI `enrich --sic 62012` could accidentally trigger thousands of API calls

If a SIC code matches 50,000 companies, `ch-bulk enrich --sic 62012` would attempt to enrich all of them. At 2 requests/second across 6 endpoints, that is ~42 hours of API calls.

**Suggestion:** Add a confirmation prompt showing the count before proceeding, and a `--limit` flag. Consider a `--dry-run` flag too.

### 13. The `officer_id` in the CH API is a URL path segment, not a simple ID

The CH officers endpoint returns items with a `links.officer.appointments` field like `/officers/ABC123DEF/appointments`. The actual "officer_id" must be extracted from this URL. The design does not mention this extraction.

**Suggestion:** Document the ID extraction logic. Same applies to PSC IDs and charge codes.

## Verified Claims (things confirmed as correct)

1. **httpx is already a dependency** -- confirmed in `pyproject.toml` line 21: `"httpx>=0.27"`. No new HTTP dependency needed. The downloader already uses `httpx.Client` synchronously.

2. **The existing codebase uses threading, not asyncio** -- confirmed. `gui.py` uses `threading.Thread` for all background work. Zero asyncio usage anywhere in the codebase.

3. **Rate limit of 600/5min is correct** -- confirmed in the CH API research document. The 580/300s safety margin is a reasonable approach borrowed from the `atchai/companies_house_scraper` reference.

4. **DuckDB is the only database** -- confirmed. No SQLAlchemy, no SQLite (except as an export target). All queries go directly through `duckdb.connect()`.

5. **The codebase pattern of db_path being passed through layers is consistent** -- confirmed. `ChBulk.__init__` stores `self.db_path`, passes it to `query.py` functions. The enrichment design correctly follows this pattern.

6. **Typer + Rich are the CLI framework** -- confirmed. New CLI commands (`enrich`, `officers`, etc.) fit naturally as additional `@app.command()` decorators.

7. **The progress_callback pattern is established** -- confirmed in `downloader.py` and `gui.py`. Using the same callback pattern for enrichment progress is the right approach.

8. **DuckDB allows read-only concurrent connections** -- confirmed. `query.py` uses `read_only=True` for all SELECT queries, which DuckDB supports from multiple threads/connections.
