# ch_bulk refactor — folder structure + code cleanup

**Date:** 2026-05-26
**Status:** Phase 1 design v3 (after pool-codex-1 rounds 1-2 in req-0121 + ad-hoc codex independent proposal in req-0122)
**Scope:** Reorganize the `ch_bulk/` Python package and root-level adhoc scripts. No behavioral changes except two small mechanical ones (centralized repo-root paths, updated lazy-export targets in `__init__.py`).

## Changes from v2

1. **Primary organizing axis flipped from "by pipeline stage" to "by data source".** v2 split `ingest/{ch,cqc}/` vs `enrich/{ch,cqc}/`; v3 unifies into `companies_house/` and `cqc/` folders. Reason: ad-hoc codex's independent proposal converged on by-source; user picked it. Concrete win: the cross-folder coupling `cqc/processor.py` → `companies_house/processor.py` is now an honest by-source dependency rather than a violated "ingest subsystems are independent" claim.
2. **`core/ingest_helpers.py` extraction dropped.** Was needed in v2 to honor the "ingest/ch and ingest/cqc are independent" claim. Now that both processors sit under their source folders, the existing CQC→CH helper import is fine in place. Smaller diff, less code motion.
3. **`core/` split into `core/` + `db/`.** Codex round-1 and the independent proposal both flagged this. `core/` holds 4 files that don't touch DuckDB (logging, settings, rate_limit, paths); `db/` holds 4 DuckDB-layer files (bootstrap, staging, sync_batches, migration).
4. **`migration.py` → `db/migration.py`** (was top-level in v2). Consistent with the core/+db/ decision.
5. **`query.py` and `cqc_query.py` move inside their source folders** as `companies_house/query.py` and `cqc/query.py` (v2 had a separate `query/` subpackage). Consistent with by-source axis.
6. **`matcher.py` → `matching/ch_cqc.py`** (codex's naming; clearer than just `matcher.py` in a `matching/` folder).
7. **Public-API contract narrowed.** v2 said "public Python API unchanged" while removing old module paths — contradiction. v3 narrows the contract to: CLI commands, GUI, and top-level `from ch_bulk import {ChBulk, SanityCheckError, SanityCheckResult, ensure_pipeline_schema}`. Grep confirmed no external consumer (uk-homecare-deals or otherwise) imports `ch_bulk` submodules.
8. **Step 1 query-shadowing bug fixed.** v2's Step 1 created `ch_bulk/query/__init__.py` before Step 6 moved `query.py`, which shadows the module and breaks `pytest` immediately. v3 has no `query/` subpackage so the bug is moot; the moved files live in `companies_house/query.py` and `cqc/query.py`.
9. **`__file__` audit broadened.** v2 only grepped the exact `Path(__file__).resolve().parent.parent` string. v3 audits all `__file__` usage inside `ch_bulk/` to catch any drift to other relative-path expressions.
10. **`ch-bulk info` baseline captured in Step 0** so Step 15's behavior diff has something to compare against.
11. **`ensure_pipeline_schema` added to final smoke** — it's one of the four top-level exports, must verify it imports cleanly.

## Problem (unchanged from v1/v2)

`ch_bulk/` has grown to 31 files in a flat layout. Concerns are mixed: bulk-CSV ingest, REST-API enrichment, web-discovery enrichment, matching, shared infra, CLI, GUI, migration tooling, and one operational salvage script all live as siblings. Several files are very large (`classifier.py` 66 KB, `financials_enricher.py` 94 KB, `gui.py` 53 KB). The project root has accumulated 5 one-off adhoc scripts plus a 2 GB stale DuckDB backup.

## Non-goals (Phase 1)

- **No behavioral changes** except two small mechanical extractions: `core/paths.py` centralizes `__file__` resolution; `ch_bulk/__init__.py` `__getattr__` targets are updated to new module paths. CLI surface, GUI behavior, top-level Python imports unchanged.
- **No splitting of large files.** `classifier.py`, `financials_enricher.py`, `gui.py` stay single files this phase.
- **No new abstractions.** No base classes, no shared interfaces, no DI containers.
- **`data/staging/` is not touched.** Nothing under `data/` (staging, input, logs, output) moves, renames, or deletes.
- **`sql/`, `tests/`, `docs/` layouts unchanged.** Only test files have import paths updated.
- **Single commit.** All 16 execution steps land in one git commit at Step 16. Intermediate steps exist only for sequencing and verification, not separate commits.

## Public API contract

The following stay stable across Phase 1:

- **CLI surface**: `ch-bulk download`, `ch-bulk process`, `ch-bulk query`, `ch-bulk sync`, `ch-bulk match`, `ch-bulk info`, `ch-bulk ui`, `ch-bulk load-staging`, `ch-bulk classify`, `ch-bulk find-websites`, `ch-bulk cqc-enrich {providers,locations}`, `ch-bulk ch-enrich {directors,revenue}`, `ch-bulk enrich-financials`, `ch-bulk migration {export,import}`, `ch-bulk export-sqlite`. All flags identical.
- **GUI**: launches via `ch-bulk ui`, identical panes and behavior.
- **Top-level Python imports**: `from ch_bulk import ChBulk`, `from ch_bulk import SanityCheckError`, `from ch_bulk import SanityCheckResult`, `from ch_bulk import ensure_pipeline_schema`.

Direct submodule imports (`from ch_bulk.processor import ...`, `from ch_bulk.financials_enricher import ...`, etc.) are NOT part of the contract. Confirmed by grep: no external consumer uses them. In-repo consumers (2 scripts in `scripts/`, 11 test files, the 5 adhoc scripts that don't import `ch_bulk`) are updated in lockstep.

## Target structure

```
ch_bulk/
├── __init__.py                  # updated lazy __getattr__ targets
├── __main__.py
├── api.py                       # ChBulk facade
├── cli.py                       # Typer commands
├── gui.py                       # Tkinter app
│
├── core/                        # cross-cutting runtime utils (no DuckDB)
│   ├── __init__.py
│   ├── logging.py               # was _logging.py
│   ├── rate_limit.py
│   ├── settings.py
│   └── paths.py                 # NEW — REPO_ROOT, SQL_DIR, DATA_REFERENCE_DIR
│
├── db/                          # DuckDB layer
│   ├── __init__.py
│   ├── bootstrap.py             # schema init
│   ├── staging.py               # JSONL stager + DuckDB loader
│   ├── sync_batches.py          # batch tracking rows
│   └── migration.py             # parquet export/import
│
├── companies_house/
│   ├── __init__.py
│   ├── downloader.py            # bulk CSV downloader
│   ├── processor.py             # bulk CSV → DuckDB ingest
│   ├── query.py                 # was ch_bulk/query.py
│   ├── ch_enricher.py           # REST API director enrichment + revenue model
│   ├── financials_enricher.py   # REST API filings → iXBRL/OCR (split target Phase 2)
│   └── revenue_model.py
│
├── cqc/
│   ├── __init__.py
│   ├── downloader.py            # was cqc_downloader.py
│   ├── processor.py             # was cqc_processor.py
│   ├── query.py                 # was cqc_query.py
│   ├── api_client.py            # was cqc_api_client.py
│   └── api_enricher.py          # was cqc_api_enricher.py
│
├── web/
│   ├── __init__.py
│   ├── browser.py
│   ├── search.py                # was web_search.py
│   ├── website_finder.py
│   └── classifier.py            # (split target Phase 2)
│
└── matching/
    ├── __init__.py
    └── ch_cqc.py                # was matcher.py

scripts/
├── adhoc/                       # NEW
│   ├── batch_commit.py                       # was /adhoc_batch_commit.py
│   ├── batch_prep.py                         # was /adhoc_batch_prep.py
│   ├── fetch_batch.py                        # was /adhoc_fetch_batch.py
│   ├── fetch_shard_0.py                      # was /fetch_shard_0.py
│   ├── classify_shard.py                     # was /classify_shard_adhoc.py
│   └── salvage_classification_parse_errors.py # was ch_bulk/salvage_classification_parse_errors.py
├── (existing scripts/* unchanged)
└── README.md                    # NEW — script index

DELETE: ch_bulk.duckdb.bak-pre-trackA-201255  (2 GB, stale)
```

## Rationale per grouping

1. **`core/`** — 4 cross-cutting runtime utils (`logging`, `rate_limit`, `settings`, `paths`) that do NOT touch DuckDB. Imported broadly across the package.
2. **`db/`** — 4 modules that all touch the DuckDB connection (`bootstrap`, `staging`, `sync_batches`, `migration`). Grouping makes the database boundary explicit.
3. **`companies_house/`** — every module that deals with CH data (bulk downloader, bulk processor, query, REST enrichers, revenue model). One source, one folder.
4. **`cqc/`** — same pattern for CQC. Note `cqc/processor.py` still imports a few helpers from `companies_house/processor.py` (`SanityCheckError`, `SanityCheckResult`, `_escape_path`, `compact_database`, `timed_phase`, `SQL_DIR`). Honest cross-source dependency — left as-is; Phase 2 may extract.
5. **`web/`** — `browser`, `search`, `website_finder`, `classifier` form the "discover and classify a company's website" subsystem. Logically a third source-like vertical (the website is the data).
6. **`matching/`** — `ch_cqc.py` is the only genuine cross-source workflow. Its own folder leaves room for future matching variants without crowding `companies_house/` or `cqc/`.
7. **Top level** — `__init__.py`, `__main__.py`, `api.py`, `cli.py`, `gui.py`. Public/operational surface; keeps the entrypoints discoverable.

## `core/paths.py`

```python
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]  # ch_bulk/core/paths.py → repo root
SQL_DIR = REPO_ROOT / "sql"
DATA_REFERENCE_DIR = REPO_ROOT / "data" / "reference"
```

Callers that currently compute paths from `__file__`:

| Current file | Current line | Change |
|---|---|---|
| `ch_bulk/bootstrap.py:19` | `SQL_DIR = Path(__file__).resolve().parent.parent / "sql"` | `from ch_bulk.core.paths import SQL_DIR` |
| `ch_bulk/processor.py:69` | `SQL_DIR = Path(__file__).resolve().parent.parent / "sql" / "ch"` | `from ch_bulk.core.paths import SQL_DIR as _ROOT_SQL_DIR; SQL_DIR = _ROOT_SQL_DIR / "ch"` |
| `ch_bulk/migration.py:67-68` | `_repo_root()` body | `from ch_bulk.core.paths import REPO_ROOT` (replace body) |
| `ch_bulk/ch_enricher.py:966` | `bands_path = Path(__file__).resolve().parent.parent / "data" / "reference" / "revenue_bands.csv"` | `from ch_bulk.core.paths import DATA_REFERENCE_DIR; bands_path = DATA_REFERENCE_DIR / "revenue_bands.csv"` |

Note: `cqc/processor.py` derives `SQL_DIR = _CH_SQL_DIR.parent / "cqc"` from its `companies_house.processor` import. After the move, that derivation still works because `companies_house/processor.py` exposes its `SQL_DIR` from `core.paths`. Cleaner to switch it to its own `from ch_bulk.core.paths import SQL_DIR as _ROOT_SQL_DIR; SQL_DIR = _ROOT_SQL_DIR / "cqc"` — done in Step 5.

## `__init__.py` updates

Current `__init__.py` lazy-exports:

```python
def __getattr__(name: str):
    if name == "ChBulk":
        from ch_bulk.api import ChBulk
        return ChBulk
    if name in {"SanityCheckError", "SanityCheckResult"}:
        from ch_bulk.processor import SanityCheckError, SanityCheckResult
        ...
```

Plus eager: `from ch_bulk.bootstrap import ensure_pipeline_schema`.

After Phase 1:

```python
from ch_bulk.db.bootstrap import ensure_pipeline_schema  # eager

def __getattr__(name: str):
    if name == "ChBulk":
        from ch_bulk.api import ChBulk
        return ChBulk
    if name in {"SanityCheckError", "SanityCheckResult"}:
        from ch_bulk.companies_house.processor import SanityCheckError, SanityCheckResult
        ...
```

Top-level `from ch_bulk import {ChBulk, SanityCheckError, SanityCheckResult, ensure_pipeline_schema}` continues to work.

## Execution plan — verifiable checklist

Every step has a **Do** (the action) and a **Verify** (how to confirm it worked). Steps are sequencing/checkpoints only — the entire phase ships as ONE git commit at Step 16.

If a Verify step fails: stop, `git restore` the failing step's changes, fix, re-attempt. Do not commit partial state.

### Step 0 — Baseline capture

- [ ] **Do:** save current `pytest -q` output to `/tmp/ch_bulk_baseline_tests.txt`.
- [ ] **Do:** save current `ch-bulk --help` output to `/tmp/ch_bulk_baseline_cli_help.txt`.
- [ ] **Do:** save current `ch-bulk info --db-path ch_bulk.duckdb` output to `/tmp/ch_bulk_baseline_info.txt`.
- [ ] **Do:** save current `python -c "from ch_bulk import ChBulk, SanityCheckError, SanityCheckResult, ensure_pipeline_schema; print('ok')"` output to `/tmp/ch_bulk_baseline_top_imports.txt`.
- [ ] **Do:** run `git status` and confirm working tree state matches expectations.
- [ ] **Verify:** all four baseline files exist and are non-empty.
- [ ] **Verify:** baseline `pytest` pass count is recorded (note any pre-existing failures so they don't get mis-attributed).

### Step 1 — Create empty subpackage skeletons

- [ ] **Do:** create `ch_bulk/core/__init__.py`, `ch_bulk/db/__init__.py`, `ch_bulk/companies_house/__init__.py`, `ch_bulk/cqc/__init__.py`, `ch_bulk/web/__init__.py`, `ch_bulk/matching/__init__.py`. Each is one line: a module docstring.
- [ ] **Verify:** `python -c "import ch_bulk; import ch_bulk.core; import ch_bulk.db; import ch_bulk.companies_house; import ch_bulk.cqc; import ch_bulk.web; import ch_bulk.matching; print('ok')"` succeeds.
- [ ] **Verify:** `pytest -q` passes with the same pass count as Step 0 baseline (no behavior changed yet — only empty `__init__.py` files added).

### Step 2 — Add `core/paths.py`

- [ ] **Do:** create `ch_bulk/core/paths.py` exposing `REPO_ROOT = Path(__file__).resolve().parents[2]`, `SQL_DIR = REPO_ROOT / "sql"`, `DATA_REFERENCE_DIR = REPO_ROOT / "data" / "reference"`.
- [ ] **Verify:** `python -c "from ch_bulk.core.paths import REPO_ROOT, SQL_DIR, DATA_REFERENCE_DIR; assert SQL_DIR.is_dir() and (SQL_DIR / 'ch').is_dir() and (SQL_DIR / 'cqc').is_dir() and DATA_REFERENCE_DIR.is_dir(), (REPO_ROOT, SQL_DIR, DATA_REFERENCE_DIR)"` succeeds.
- [ ] **Verify:** `pytest -q` still passes (file added but nothing imports from it yet).

### Step 3 — Move `core/` modules

- [ ] **Do:** `git mv ch_bulk/_logging.py ch_bulk/core/logging.py`.
- [ ] **Do:** `git mv ch_bulk/rate_limit.py ch_bulk/core/rate_limit.py`.
- [ ] **Do:** `git mv ch_bulk/settings.py ch_bulk/core/settings.py`.
- [ ] **Do:** rewrite imports across `ch_bulk/`, `scripts/`, `tests/`:
  - `from ch_bulk._logging import` → `from ch_bulk.core.logging import`
  - `from ch_bulk.rate_limit import` → `from ch_bulk.core.rate_limit import`
  - `from ch_bulk.settings import` → `from ch_bulk.core.settings import`
- [ ] **Verify:** `grep -rn "from ch_bulk\._logging\|from ch_bulk\.rate_limit\|from ch_bulk\.settings" ch_bulk/ scripts/ tests/` returns zero matches.
- [ ] **Verify:** `pytest -q` passes.

### Step 4 — Move `db/` modules

- [ ] **Do:** `git mv ch_bulk/bootstrap.py ch_bulk/db/bootstrap.py`.
- [ ] **Do:** `git mv ch_bulk/staging.py ch_bulk/db/staging.py`.
- [ ] **Do:** `git mv ch_bulk/sync_batches.py ch_bulk/db/sync_batches.py`.
- [ ] **Do:** `git mv ch_bulk/migration.py ch_bulk/db/migration.py`.
- [ ] **Do:** rewrite imports:
  - `from ch_bulk.bootstrap import` → `from ch_bulk.db.bootstrap import`
  - `from ch_bulk.staging import` → `from ch_bulk.db.staging import`
  - `from ch_bulk.sync_batches import` → `from ch_bulk.db.sync_batches import`
  - `from ch_bulk.migration import` → `from ch_bulk.db.migration import`
- [ ] **Do:** in `db/bootstrap.py`, replace `SQL_DIR = Path(__file__).resolve().parent.parent / "sql"` with `from ch_bulk.core.paths import SQL_DIR`.
- [ ] **Do:** in `db/migration.py`, replace `_repo_root()` body with `from ch_bulk.core.paths import REPO_ROOT` and `return REPO_ROOT`.
- [ ] **Do:** update `ch_bulk/__init__.py` eager import: `from ch_bulk.bootstrap import ensure_pipeline_schema` → `from ch_bulk.db.bootstrap import ensure_pipeline_schema`.
- [ ] **Verify:** `grep -rn "from ch_bulk\.bootstrap\|from ch_bulk\.staging\|from ch_bulk\.sync_batches\|from ch_bulk\.migration" ch_bulk/ scripts/ tests/` returns zero matches.
- [ ] **Verify:** `python -c "from ch_bulk import ensure_pipeline_schema; print(ensure_pipeline_schema.__module__)"` prints `ch_bulk.db.bootstrap`.
- [ ] **Verify:** `pytest -q` passes.

### Step 5 — Move `companies_house/` modules

- [ ] **Do:** `git mv ch_bulk/downloader.py ch_bulk/companies_house/downloader.py`.
- [ ] **Do:** `git mv ch_bulk/processor.py ch_bulk/companies_house/processor.py`.
- [ ] **Do:** `git mv ch_bulk/query.py ch_bulk/companies_house/query.py`.
- [ ] **Do:** `git mv ch_bulk/ch_enricher.py ch_bulk/companies_house/ch_enricher.py`.
- [ ] **Do:** `git mv ch_bulk/financials_enricher.py ch_bulk/companies_house/financials_enricher.py`.
- [ ] **Do:** `git mv ch_bulk/revenue_model.py ch_bulk/companies_house/revenue_model.py`.
- [ ] **Do:** rewrite imports:
  - `from ch_bulk.downloader import` → `from ch_bulk.companies_house.downloader import`
  - `from ch_bulk.processor import` → `from ch_bulk.companies_house.processor import`
  - `from ch_bulk.query import` → `from ch_bulk.companies_house.query import`
  - `from ch_bulk.ch_enricher import` → `from ch_bulk.companies_house.ch_enricher import`
  - `from ch_bulk.financials_enricher import` → `from ch_bulk.companies_house.financials_enricher import`
  - `from ch_bulk.revenue_model import` → `from ch_bulk.companies_house.revenue_model import`
- [ ] **Do:** in `companies_house/processor.py`, replace `SQL_DIR = Path(__file__).resolve().parent.parent / "sql" / "ch"` with `from ch_bulk.core.paths import SQL_DIR as _ROOT_SQL_DIR; SQL_DIR = _ROOT_SQL_DIR / "ch"`.
- [ ] **Do:** in `companies_house/ch_enricher.py`, replace `bands_path = Path(__file__).resolve().parent.parent / "data" / "reference" / "revenue_bands.csv"` with `from ch_bulk.core.paths import DATA_REFERENCE_DIR; bands_path = DATA_REFERENCE_DIR / "revenue_bands.csv"`.
- [ ] **Do:** update `ch_bulk/__init__.py` `__getattr__`: `SanityCheckError`/`SanityCheckResult` resolve from `ch_bulk.companies_house.processor` instead of `ch_bulk.processor`.
- [ ] **Verify:** `grep -rn "from ch_bulk\.downloader\|from ch_bulk\.processor\|from ch_bulk\.query \|from ch_bulk\.ch_enricher\|from ch_bulk\.financials_enricher\|from ch_bulk\.revenue_model" ch_bulk/ scripts/ tests/` returns zero matches.
- [ ] **Verify:** `python -c "from ch_bulk import SanityCheckError, SanityCheckResult; print(SanityCheckError.__module__)"` prints `ch_bulk.companies_house.processor`.
- [ ] **Verify:** `pytest -q` passes.

### Step 6 — Move `cqc/` modules

- [ ] **Do:** `git mv ch_bulk/cqc_downloader.py ch_bulk/cqc/downloader.py`.
- [ ] **Do:** `git mv ch_bulk/cqc_processor.py ch_bulk/cqc/processor.py`.
- [ ] **Do:** `git mv ch_bulk/cqc_query.py ch_bulk/cqc/query.py`.
- [ ] **Do:** `git mv ch_bulk/cqc_api_client.py ch_bulk/cqc/api_client.py`.
- [ ] **Do:** `git mv ch_bulk/cqc_api_enricher.py ch_bulk/cqc/api_enricher.py`.
- [ ] **Do:** rewrite imports:
  - `from ch_bulk.cqc_downloader import` → `from ch_bulk.cqc.downloader import`
  - `from ch_bulk.cqc_processor import` → `from ch_bulk.cqc.processor import`
  - `from ch_bulk.cqc_query import` → `from ch_bulk.cqc.query import`
  - `from ch_bulk.cqc_api_client import` → `from ch_bulk.cqc.api_client import`
  - `from ch_bulk.cqc_api_enricher import` → `from ch_bulk.cqc.api_enricher import`
- [ ] **Do:** in `cqc/processor.py`, replace `SQL_DIR = _CH_SQL_DIR.parent / "cqc"` derivation with `from ch_bulk.core.paths import SQL_DIR as _ROOT_SQL_DIR; SQL_DIR = _ROOT_SQL_DIR / "cqc"`. Remove the now-unused `SQL_DIR as _CH_SQL_DIR` from the `companies_house.processor` import; keep the other 5 shared symbols imported as-is.
- [ ] **Verify:** `grep -rn "from ch_bulk\.cqc_downloader\|from ch_bulk\.cqc_processor\|from ch_bulk\.cqc_query\|from ch_bulk\.cqc_api_client\|from ch_bulk\.cqc_api_enricher" ch_bulk/ scripts/ tests/` returns zero matches.
- [ ] **Verify:** `grep -n "_CH_SQL_DIR" ch_bulk/cqc/processor.py` returns zero matches.
- [ ] **Verify:** `pytest -q` passes.

### Step 7 — Move `web/` modules

- [ ] **Do:** `git mv ch_bulk/browser.py ch_bulk/web/browser.py`.
- [ ] **Do:** `git mv ch_bulk/web_search.py ch_bulk/web/search.py`.
- [ ] **Do:** `git mv ch_bulk/website_finder.py ch_bulk/web/website_finder.py`.
- [ ] **Do:** `git mv ch_bulk/classifier.py ch_bulk/web/classifier.py`.
- [ ] **Do:** rewrite imports:
  - `from ch_bulk.browser import` → `from ch_bulk.web.browser import`
  - `from ch_bulk.web_search import` → `from ch_bulk.web.search import`
  - `from ch_bulk.website_finder import` → `from ch_bulk.web.website_finder import`
  - `from ch_bulk.classifier import` → `from ch_bulk.web.classifier import`
- [ ] **Do:** manually fix `web/classifier.py:28` — `from ch_bulk import browser` → `from ch_bulk.web import browser`.
- [ ] **Verify:** `grep -rn "from ch_bulk\.browser\|from ch_bulk\.web_search\|from ch_bulk\.website_finder\|from ch_bulk\.classifier" ch_bulk/ scripts/ tests/` returns zero matches.
- [ ] **Verify:** `grep -n "from ch_bulk import browser" ch_bulk/` returns zero matches.
- [ ] **Verify:** `pytest -q` passes.

### Step 8 — Move `matching/`

- [ ] **Do:** `git mv ch_bulk/matcher.py ch_bulk/matching/ch_cqc.py`.
- [ ] **Do:** rewrite imports: `from ch_bulk.matcher import` → `from ch_bulk.matching.ch_cqc import`.
- [ ] **Verify:** `grep -rn "from ch_bulk\.matcher" ch_bulk/ scripts/ tests/` returns zero matches.
- [ ] **Verify:** `pytest -q` passes.

### Step 9 — Move root adhoc scripts + salvage

- [ ] **Do:** `mkdir -p scripts/adhoc`.
- [ ] **Do:** `git mv adhoc_batch_commit.py scripts/adhoc/batch_commit.py`.
- [ ] **Do:** `git mv adhoc_batch_prep.py scripts/adhoc/batch_prep.py`.
- [ ] **Do:** `git mv adhoc_fetch_batch.py scripts/adhoc/fetch_batch.py`.
- [ ] **Do:** `git mv fetch_shard_0.py scripts/adhoc/fetch_shard_0.py`.
- [ ] **Do:** `git mv classify_shard_adhoc.py scripts/adhoc/classify_shard.py`.
- [ ] **Do:** `git mv ch_bulk/salvage_classification_parse_errors.py scripts/adhoc/salvage_classification_parse_errors.py`. Update its imports: `from ch_bulk.bootstrap import ensure_pipeline_schema` → `from ch_bulk.db.bootstrap import ensure_pipeline_schema`; `from ch_bulk.classifier import (...)` → `from ch_bulk.web.classifier import (...)`; `from ch_bulk.staging import with_duckdb_connection` → `from ch_bulk.db.staging import with_duckdb_connection`.
- [ ] **Verify:** `ls /Users/nitinkum/Projects/nitkrar/ch_scraper/*.py 2>/dev/null | wc -l` returns 0 — no root-level Python files left.
- [ ] **Verify:** `ls scripts/adhoc/*.py | wc -l` returns 6.
- [ ] **Verify:** `python scripts/adhoc/salvage_classification_parse_errors.py --help 2>&1 | head -1` does not produce ImportError.

### Step 10 — Create `scripts/README.md`

- [ ] **Do:** create `scripts/README.md` with one-line descriptions per script. Two sections: "Maintained helpers" (top-level `scripts/`) and "Frozen one-shots" (`scripts/adhoc/`).
- [ ] **Verify:** `cat scripts/README.md` lists every `.py` file under `scripts/` and `scripts/adhoc/`.

### Step 11 — Delete stale backup

- [ ] **Do:** ASK USER for confirmation before deleting `ch_bulk.duckdb.bak-pre-trackA-201255` (2 GB). On approval: `rm ch_bulk.duckdb.bak-pre-trackA-201255`.
- [ ] **Verify:** `ls ch_bulk.duckdb*` shows only `ch_bulk.duckdb` (no `.bak`).

### Step 12 — Structural audit (file layout)

- [ ] **Verify:** `find ch_bulk -maxdepth 1 -type f -name "*.py" | sort` returns exactly `ch_bulk/__init__.py`, `ch_bulk/__main__.py`, `ch_bulk/api.py`, `ch_bulk/cli.py`, `ch_bulk/gui.py` (5 files, no others).
- [ ] **Verify:** `find ch_bulk/core -type f -name "*.py" | sort` returns exactly 5 files: `__init__.py`, `logging.py`, `paths.py`, `rate_limit.py`, `settings.py`.
- [ ] **Verify:** `find ch_bulk/db -type f -name "*.py" | sort` returns exactly 5 files: `__init__.py`, `bootstrap.py`, `migration.py`, `staging.py`, `sync_batches.py`.
- [ ] **Verify:** `find ch_bulk/companies_house -type f -name "*.py" | sort` returns exactly 7 files: `__init__.py`, `ch_enricher.py`, `downloader.py`, `financials_enricher.py`, `processor.py`, `query.py`, `revenue_model.py`.
- [ ] **Verify:** `find ch_bulk/cqc -type f -name "*.py" | sort` returns exactly 6 files: `__init__.py`, `api_client.py`, `api_enricher.py`, `downloader.py`, `processor.py`, `query.py`.
- [ ] **Verify:** `find ch_bulk/web -type f -name "*.py" | sort` returns exactly 5 files: `__init__.py`, `browser.py`, `classifier.py`, `search.py`, `website_finder.py`.
- [ ] **Verify:** `find ch_bulk/matching -type f -name "*.py" | sort` returns exactly 2 files: `__init__.py`, `ch_cqc.py`.

### Step 13 — Structural audit (imports)

- [ ] **Verify:** `grep -rn "from ch_bulk\." ch_bulk/ scripts/ tests/ | grep -v "ch_bulk\.\(core\|db\|companies_house\|cqc\|web\|matching\|api\|cli\|gui\)" | head` returns zero matches (every `ch_bulk.X` import resolves to one of the new subpackages or the kept top-level files).
- [ ] **Verify:** `grep -rn "__file__" ch_bulk/` returns only `ch_bulk/core/paths.py` (single source-of-truth for repo-root resolution).
- [ ] **Verify:** `grep -rn "Path(__file__).resolve()" ch_bulk/` returns only `ch_bulk/core/paths.py`.

### Step 14 — Behavioral verification (automated)

- [ ] **Verify:** `pytest -q` passes with the same pass count as Step 0 baseline.
- [ ] **Verify:** `diff <(ch-bulk --help) /tmp/ch_bulk_baseline_cli_help.txt` is empty (CLI surface unchanged).
- [ ] **Verify:** `diff <(ch-bulk info --db-path ch_bulk.duckdb) /tmp/ch_bulk_baseline_info.txt` is empty (info command output unchanged).
- [ ] **Verify:** `python -c "from ch_bulk import ChBulk, SanityCheckError, SanityCheckResult, ensure_pipeline_schema; ch = ChBulk(); print('ok')"` prints `ok` (all 4 top-level exports importable; `ChBulk` instantiates).

### Step 15 — Behavioral verification (manual, GUI)

- [ ] **Do:** launch `ch-bulk ui`. Confirm window opens, left rail shows CH/CQC/Settings panes, no Python tracebacks in terminal.
- [ ] **Do:** click each pane to confirm it renders. Close window.
- [ ] **Verify:** GUI launched and closed cleanly. (Manual — Tkinter not in pytest.)

### Step 16 — Commit (single, all-in-one)

- [ ] **Do:** `git add -A`.
- [ ] **Do:** `git commit -m "Refactor ch_bulk into subpackages (core/db/companies_house/cqc/web/matching) + centralize repo-root paths"` with a body that summarizes: structural-only refactor, no behavioral changes except `core/paths.py` extraction, public Python API and CLI surface preserved.
- [ ] **Verify:** `git log -1 --stat` shows all moved files as renames (`R100` or near-100) — history is preserved.
- [ ] **Verify:** `git status` is clean.

## Risks

1. **Hidden circular imports.** Both codex reviews verified no current cycles, but the new boundaries might surface accidental ones. Mitigation: `pytest -q` runs after every Step 3-8 move, not just at the end.
2. **`cqc/processor.py` keeps depending on `companies_house/processor.py`** for 5 shared symbols. This is intentional and honest (CQC ingest was built on top of CH ingest's helpers). Phase 2 may extract them to `db/` or a shared helpers module — not Phase 1.
3. **GUI launch is not covered by `pytest`.** Step 15 is manual.
4. **`__init__.py` `__getattr__` change is the only top-level Python API behavior diff.** External imports `from ch_bulk import {ChBulk, SanityCheckError, SanityCheckResult, ensure_pipeline_schema}` still work because the lazy targets are updated to new module paths. Verified in Step 14.
5. **Hardcoded `data/staging/` paths in 2 adhoc scripts** (`scripts/adhoc/batch_commit.py`, `scripts/adhoc/batch_prep.py`). Those paths are unchanged so the scripts keep working. No fix needed; flagged for awareness.

## Phase 2 outline (sketch only — not in scope)

1. Split `companies_house/financials_enricher.py` (94 KB) by seam: CH filings client vs. iXBRL/OCR parser vs. DuckDB loader.
2. Split `web/classifier.py` (66 KB) by seam: prompt construction vs. LLM call vs. response parsing vs. DuckDB loader.
3. Consider extracting shared ingest helpers (`SanityCheckError`, `SanityCheckResult`, `_escape_path`, `compact_database`, `timed_phase`) from `companies_house/processor.py` into `db/ingest_helpers.py` so `cqc/processor.py` doesn't cross-import from `companies_house/`. Verify the shape of the duplication first.
4. `gui.py` (53 KB) — defer; UI rewrites are high-risk and the GUI works.

Phase 2 decisions deferred until Phase 1 lands and the new structure is exercised for a few days.
