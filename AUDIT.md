# Audit

Date: 2026-11-14. Scope: the `ch_bulk` package, `scripts/`, packaging, and
CI in this repository. The prior review (previous session) is the source of
the findings below; each item was checked against source code or a runtime
probe before being listed as verified.

Status values: `Open`, `In progress`, `Fixed`, `Partial`, `Deferred`.
Update the status and the verification note in place when an item is fixed.
Do not renumber items; add new findings at the end.

| ID | Severity | Status | Finding |
| --- | --- | --- | --- |
| AUDIT-01 | High | Fixed | Wheel omits SQL files; default data dir sits under the install prefix |
| AUDIT-02 | High | Fixed | Empty SIC codes produce invalid SQL (`IN ()`) |
| AUDIT-03 | Medium | Fixed | Negative `limit` / `page` / `page_size` reach DuckDB and raise binder errors |
| AUDIT-04 | Medium | Fixed | Empty company ID normalises to `00000000` in financials fetch |
| AUDIT-05 | High | Fixed | HTTP retry loops do not retry transport errors; `Retry-After` parsing is integer-only and uncapped |
| AUDIT-06 | Medium | Fixed | Officers and filing-history lists fetch one page only |
| AUDIT-07 | Medium | Fixed | Read queries open DuckDB read-write; compaction recovery and missing-table handling are inconsistent |
| AUDIT-08 | High | Partial | API keys stored unencrypted and shown in plaintext in the GUI |
| AUDIT-09 | Low | Open | Website search checks reachability for every result before scoring or denylisting |
| AUDIT-10 | Low | Open | Classifier fetches seven pages per site and shares one HTTP/LLM client across threads |
| AUDIT-11 | Medium | Open | Bulk download is sequential with duplicated GUI/CLI loops; size mismatch is a warning only |
| AUDIT-12 | Low | Open | Validation, progress, and API-client helpers are duplicated across five modules |
| AUDIT-13 | Low | Open | GUI browse panes duplicate query, display, sort, paging, and export logic |
| AUDIT-14 | Low | Open | `process()` re-reads and re-upserts every month on each run; no processed-month checkpoint |
| AUDIT-15 | Low | Open | Minor correctness issues: duplicated regex extraction, silent CSV error drops, LLM JSON parsing cost, `or`-defaults |
| AUDIT-16 | Medium | Open | `query_by_sic` loads all rows into memory (`SELECT *` + `fetchall`) |
| AUDIT-17 | Feature | Open | No stale-enrichment view or selective re-enrichment |
| AUDIT-18 | Feature | Open | No shared HTTP retry/throttle layer (follows from AUDIT-05) |
| AUDIT-19 | Feature | Open | Website search has a single DuckDuckGo HTML backend with no retry |
| AUDIT-20 | Feature | Open | Export scripts in `scripts/` duplicate pipeline logic instead of using the CLI/GUI |
| AUDIT-21 | Low | Open | No CI test gate; version repeated in three places; spec lists stale modules; README describes only the SIC use case |

## Verified bugs

### AUDIT-01 — Wheel omits SQL files; default data dir sits under the install prefix

Evidence: `pyproject.toml` declares no package data and there is no
`MANIFEST.in`; a wheel built with `uv build` contained 56 files and no
`.sql` files. `ch_bulk/core/paths.py` derives `SQL_DIR` and
`DEFAULT_DATA_DIR` from the package location, so an unpacked wheel has
`SQL_DIR` missing and `DEFAULT_DATA_DIR` under the install prefix.
In a source checkout both resolve correctly, which is why this only
breaks installed use. Fix: declare the SQL files as package data and
include them in the sdist, and choose a user-owned default data
directory for installed use while preserving checkout behaviour.
Verify with a fresh wheel build and an unpacked-wheel import.

Fixed in this working copy (uncommitted): SQL files are declared in
`pyproject.toml` package data and `MANIFEST.in` (16 `.sql` files in a
rebuilt wheel, verified), the PyInstaller spec collects `ch_bulk`
data files, and the default data directory falls back to
`~/.ch_bulk/data` for installed packages while keeping the checkout
and frozen-app locations (verified by an isolated unpacked-wheel
import and `tests/test_settings.py`).

Fixed in this working copy (uncommitted): the bands CSV was moved
into the package as `ch_bulk/data/revenue_bands.csv` (byte-identical
to the tracked original, which was removed). A data-directory file
under `<data_dir>/reference/` takes precedence; otherwise the
packaged defaults are loaded via `revenue_model.resolve_revenue_bands`.
The Settings pane has General and Revenue Bands tabs; the bands tab
shows the effective bands (custom or packaged), edits them, saves
the custom file, and can restore the packaged defaults. The CSV is
declared in package data and `MANIFEST.in` and is present in both
the rebuilt wheel and sdist (verified). Pinned by
`tests/test_revenue_bands.py`.

### AUDIT-02 — Empty SIC codes produce invalid SQL

Evidence: `_build_sic_where([])` returns
`(sic_code_1 IN () OR ...)`. Probes confirmed `query_by_sic(db, "")`,
`query_by_sic(db, [])`, and `query_companies(..., sic_codes="")` raise
DuckDB parser errors. Decision: `None` means "no SIC filter"; an
explicitly empty value raises `ValueError`. The GUI only passes
`sic_codes` when non-empty, so this decision does not change GUI
behaviour. Fix in the shared SIC builder so every caller benefits.

Fixed in this working copy (uncommitted): `_build_sic_where`
rejects empty input, `_normalise_sic_codes` drops blank list entries,
and `tests/test_query_validation.py` pins the behaviour.

### AUDIT-03 — Negative `limit` / `page` / `page_size` reach DuckDB

Evidence: probes confirmed `limit=-1`, `page=0`, `page=-1`, and
`page_size=-5` raise DuckDB binder errors (`LIMIT/OFFSET cannot be
negative`) from interpolated SQL in the CH, CQC, and tiered-targets
query helpers. Fix: validate in one shared helper — `page >= 1`,
`page_size >= 1`, `limit >= 0` (`limit=0` returns no rows) — and raise
`ValueError` with a clear message in every paginated query path.

Fixed in this working copy (uncommitted): new
`ch_bulk/core/validation.py` (`validate_limit`,
`validate_pagination`) is used by the CH, CQC, and tiered-targets
query helpers, pinned by `tests/test_query_validation.py`.

### AUDIT-04 — Empty company ID normalises to `00000000`

Evidence: in `financials_fetch._normalize_company_ids`, an empty string
is padded before the emptiness check, so `["", "  ", "123"]` becomes
`["00000000", "00000123"]` (probe-confirmed). The other normalisers in
the codebase guard emptiness first. Fix: skip blank values before
padding, in the financials normaliser and any other `zfill` site that
lacks the guard.

Fixed in this working copy (uncommitted): blank values are skipped
before padding in `financials_fetch._normalize_company_ids`, pinned
by `tests/test_query_validation.py`.

### AUDIT-05 — Transport errors are not retried; `Retry-After` is integer-only and uncapped

Evidence: the `_get` retry loops in the CQC client, the Companies House
officers client, and the Companies House financials client continue
only on HTTP 429/502/503/504. A timeout or connection error escapes
immediately. `Retry-After` is parsed with `int()`, so an HTTP-date
value falls back to 60 seconds, and a large numeric value is slept
without a cap. Fix: retry transport errors with exponential backoff
without retrying non-retryable HTTP status errors, parse HTTP-date
`Retry-After` values, and cap the pause. Keep cancellation working
during the backoff sleep.

Fixed in this working copy (uncommitted): new
`ch_bulk/core/http_retry.py` parses delay-seconds and HTTP-date
`Retry-After` values with a 300-second cap; all three clients retry
transport errors, and the document-download path also retries
retryable HTTP statuses. Pinned by `tests/test_http_retry.py`.

### AUDIT-06 — Officers and filing-history lists fetch one page only

Evidence: `CompaniesHouseClient.get_officers` and
`CompaniesHouseFinancialsClient.get_filing_history` request
`items_per_page=100` and never advance `start_index`. The Companies
House public-data API documents `items_per_page`, `start_index`, and
`total_results` for both endpoints, and an earlier design review in
`docs/plans/` already flagged filing-history pagination. Fix:
paginate officers fully, and page filing history until the latest
annual-accounts filing is found or the result set is exhausted, using
`total_results` when present and stopping after a single page when it
is absent (preserves existing mocked-client behaviour).

Fixed in this working copy (uncommitted): `get_officers` pages to
`total_results` (100-page cap) and the filing-history fetch pages
until the latest annual filing is found (20-page cap), pinned by
`tests/test_http_retry.py`.

Correction (review, 2026-11-14): the filing-history loop first read
`total_results`, but the live filing-history endpoint returns
`total_count` (officers do return `total_results`; both checked with
one live call), so it always stopped after the first page and its
test passed only because the mock used the wrong field. It now reads
`total_count`, and the test mocks the real field.

### AUDIT-07 — Read queries open DuckDB read-write; handling is inconsistent

Evidence: query helpers in `companies_house/query.py`, `cqc/query.py`,
and `matching/query.py` open connections without `read_only=True`,
while only enrichment/staging paths use the lock-retry helper.
`query_companies` omits the interrupted-compaction recovery that its
directors/financials siblings perform, and omits their
`CatalogException -> ([], 0)` fallback for missing enrichment tables.
Fix: open pure-read paths read-only (temp tables work in read-only
mode, probe-confirmed), add the recovery call to `query_companies`,
and align the missing-table behaviour; route lock retries through the
existing helper where reads can collide with a writer.

Fixed in this working copy (uncommitted): there was no blocker —
all 17 read-query/export paths in the CH, CQC, and matching modules
are idempotent reads, so they now route through
`with_duckdb_connection` with `read_only=True`, which retries lock
conflicts and runs the compaction recovery. The one remaining direct
read, the CQC pane status counts, also routes through the helper but
with short-bounded retries (4 attempts, 0.05s delay) because it runs
on the GUI main thread. Pinned by `tests/test_query_lock_retry.py`.

Correction (review, 2026-11-14): `read_only=True` was reverted on
these paths. DuckDB refuses a read-only connection to a file the same
process already has open read-write, and the reverse, with "different
configuration" (not matched as a lock conflict, so not retried).
Enrichment and uploads write from a worker thread inside the GUI
process, so a search or status refresh overlapping a job's write made
one of them fail immediately (reproduced). The reads still go through
`with_duckdb_connection`, so lock retry, compaction recovery and the
missing-table fallback are kept. Pinned by
`ReadsAlongsideInProcessWriterTests`. The pre-existing read-only
batch-status read in `financials_staging.load_financials_staging` had
the same exposure and was also switched, pinned by
`LoadAlongsideOpenConnectionTests`.

### AUDIT-08 — API keys stored unencrypted and shown in plaintext

Evidence: `core/settings.py` writes `settings.json` with no file
permission restriction, the Settings pane admits the storage is
unencrypted, and its entries show key contents in full. The local
`data/settings.json` is git-ignored and is not tracked, but it holds
live-looking keys. Fix: mask the GUI entries, restrict the settings
file to owner-only permissions on write, and rotate any keys that
have been stored in plaintext. Environment-variable or OS-keychain
storage is a follow-up, not part of the minimal fix.

Partially fixed in this working copy (uncommitted): the GUI key
entries are masked (`show="*"`), and `save_settings` restricts new
settings files to owner-only permissions on POSIX (pinned by
`tests/test_settings.py`). Remaining: the existing local
`data/settings.json` keeps its current permissions until it is
re-saved, and the keys stored in it have not been rotated — both
need the user to act.

## Inefficiencies and duplication

### AUDIT-09 — Reachability is checked before scoring or denylisting

Evidence: `web/search.py` runs HEAD/GET reachability checks for all
results, including denylisted and low-scoring ones, with a fresh
`requests` call per check instead of a shared session. Fix: score
and denylist first, then check reachability only for survivors, using
a shared session.

### AUDIT-10 — Classifier fetches seven pages per site and shares clients across threads

Evidence: `web/classifier_content.py` fetches seven paths for every
site even when the homepage already has enough text, and
`web/classifier_pipeline.py` shares one `requests.Session` and one
`httpx.Client` across worker threads. Fix: fetch the homepage first
and stop early, and use per-thread sessions or a client documented as
thread-safe.

### AUDIT-11 — Bulk download is sequential with duplicated loops

Evidence: `companies_house/downloader.py` downloads the seven parts
sequentially and repeats the whole loop for the GUI and CLI paths.
The resume `HEAD` request is not retried, and a final size mismatch
is logged as a warning only. Fix: share one download loop, retry the
`HEAD`, parallelise parts, and fail on a size mismatch.

### AUDIT-12 — Validation, progress, and API-client helpers are duplicated

Evidence: `_validated_mode`, `_progress_text`, and `_duration_text`
each appear in five modules, the three API clients repeat retry and
throttle handling, and callers repeat the
`if cancel_event is None: ... else: ...(cancel_event=...)` pattern
even though the callee defaults accept `None`. Fix: centralise the
helpers in `core` and pass `cancel_event` through directly.

### AUDIT-13 — GUI browse panes duplicate logic

Evidence: the CH, CQC, and Targets panes in `gui.py` each
reimplement query, display, sort, paging, and export. Fix: extract
one shared browse-pane base class; the per-pane differences are
filters, columns, and sort keys.

### AUDIT-14 — `process()` re-reads every month on each run

Evidence: `ChBulk.process()` discovers and upserts every CSV in the
input directory each time. Fix: record processed scrape dates and
skip months already ingested, while keeping an explicit
reprocess/force path.

### AUDIT-15 — Minor correctness issues

Evidence: each SIC `CASE` in `companies_house/processor.py` calls
`regexp_extract` twice; `read_csv_auto(..., ignore_errors=True)`
silently drops malformed rows with no rejected-row count;
`_extract_json_object` in `web/classifier_llm.py` retries parsing
from every `{` in the LLM output; and `float(... or 0.1)` /
`int(... or 400)` turn an explicit `temperature=0` or
`max_tokens=0` into the default. Fix each in place; the CSV change
needs a rejected-row count or report, not silent acceptance.

### AUDIT-16 — `query_by_sic` loads all rows into memory

Evidence: `query_by_sic` uses `SELECT *` plus `fetchall()`, while
the export path already streams with `COPY`. Fix: add an
iterator/chunked query API and use it for large result sets.

## Features

### AUDIT-17 — No stale-enrichment view

The data already carries `filing_age_months`, `revenue_source`, and
filing status. Add a view listing companies whose financials or
ratings are stale, with selective re-enrichment for those rows only.

### AUDIT-18 — No shared HTTP retry/throttle layer

Follows from AUDIT-05. Once transport retries and `Retry-After`
handling are fixed per client, extract one shared layer so the three
clients cannot drift apart again.

### AUDIT-19 — Single DuckDuckGo HTML search backend

Website search depends on one scraped HTML backend with fixed CSS
selectors and no retry. Abstract the backend so a second provider or
an official search API can be added without touching scoring.

### AUDIT-20 — Export scripts duplicate pipeline logic

The one-off exporters in `scripts/` duplicate pipeline behaviour.
Promote the maintained ones behind the CLI/GUI entry points and
retire or clearly freeze the rest.

### AUDIT-21 — No CI test gate; version and docs drift

Evidence: the only workflow builds a DMG on tags and runs no tests,
lint, or type check. The version is repeated in `pyproject.toml`,
`ch_bulk.spec`, and `build_app.sh`; the spec lists stale top-level
modules (`ch_bulk.query`, `ch_bulk.downloader`,
`ch_bulk.processor`); and the README still describes the project
mainly as a SIC bulk downloader. Fix: add a CI test gate, use one
version source, correct the spec, and update the README to cover
the CQC/HSCA/financials/classification pipeline.

## Verification (uncommitted working copy, 2026-11-14)

- New regression tests were run before the fixes and failed as
  expected; after the fixes, the full suite passes: 386 passed,
  1 skipped, 308 subtests (previously 356 passed, 1 skipped).
- A wheel rebuilt with `uv build` contains 16 `.sql` files
  (previously 0); an isolated unpacked-wheel import confirms the
  SQL directory exists and the default data directory falls back
  to `~/.ch_bulk/data`.
- After AUDIT-01 and AUDIT-07 were finished, the full suite passes:
  400 passed, 1 skipped, 313 subtests. A rebuilt wheel and sdist
  each contain `ch_bulk/data/revenue_bands.csv` and 16 `.sql` files.
- After the review corrections to AUDIT-06 and AUDIT-07: 404 passed,
  1 skipped, 313 subtests.
- No commits were made; all changes are in the working copy.
