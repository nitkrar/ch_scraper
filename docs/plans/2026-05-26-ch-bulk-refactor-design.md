# ch_bulk refactor — folder structure + code cleanup

**Date:** 2026-05-26
**Status:** Phase 1 design v3 (after pool-codex-1 rounds 1-2 in req-0121 + ad-hoc codex independent proposal in req-0122 + pre-refactor bookmark commit `dee2501` tagged `pre-refactor-bookmark`)
**Scope:** Reorganize the `ch_bulk/` Python package and root-level adhoc scripts. Phase 1 is mostly structural, with one explicit behavior fix: centralize repo-root paths and replace lazy CWD-relative `data_dir` / `db_path` defaults with absolute repo-root defaults via `core/paths.py`; update top-level lazy-export targets in `__init__.py`.

## Changes from v2

1. **Primary organizing axis flipped from "by pipeline stage" to "by data source".** v2 split `ingest/{ch,cqc}/` vs `enrich/{ch,cqc}/`; v3 unifies into `companies_house/` and `cqc/` folders. Reason: ad-hoc codex's independent proposal converged on by-source; user picked it. Concrete win: the cross-folder coupling `cqc/processor.py` → `companies_house/processor.py` is now an honest by-source dependency rather than a violated "ingest subsystems are independent" claim.
2. **`core/ingest_helpers.py` extraction dropped.** Was needed in v2 to honor the "ingest/ch and ingest/cqc are independent" claim. Now that both processors sit under their source folders, the existing CQC→CH helper import is fine in place. Smaller diff, less code motion.
3. **`core/` split into `core/` + `db/`.** Codex round-1 and the independent proposal both flagged this. `core/` holds 4 files that don't touch DuckDB (logging, settings, rate_limit, paths); `db/` holds 4 DuckDB-layer files (bootstrap, staging, sync_batches, migration).
4. **`migration.py` → `db/migration.py`** (was top-level in v2). Consistent with the core/+db/ decision.
5. **`query.py` and `cqc_query.py` move inside their source folders** as `companies_house/query.py` and `cqc/query.py` (v2 had a separate `query/` subpackage). Consistent with by-source axis.
6. **`matcher.py` → `matching/ch_cqc.py`** (codex's naming; clearer than just `matcher.py` in a `matching/` folder).
7. **Public-API contract narrowed.** v2 said "public Python API unchanged" while removing old module paths — contradiction. v3 narrows the contract to: CLI commands, GUI, and top-level `from ch_bulk import {ChBulk, SanityCheckError, SanityCheckResult, ensure_pipeline_schema}`. Grep confirmed no external consumer (uk-homecare-deals or otherwise) imports `ch_bulk` submodules.
8. **Step 1 query-shadowing bug fixed.** v2's Step 1 created `ch_bulk/query/__init__.py` before Step 6 moved `query.py`, which shadows the module and breaks test/import execution immediately. v3 has no `query/` subpackage so the bug is moot; the moved files live in `companies_house/query.py` and `cqc/query.py`.
9. **`__file__` audit broadened.** v2 only grepped the exact `Path(__file__).resolve().parent.parent` string. v3 audits all `__file__` usage inside `ch_bulk/` to catch any drift to other relative-path expressions.
10. **`ch-bulk info` baseline captured in Step 0** so Step 14 can compare behavior with an explicit `--db-path` after the default-path fix.
11. **`ensure_pipeline_schema` added to final smoke** — it's one of the four top-level exports, must verify it imports cleanly.

## Problem (unchanged from v1/v2)

`ch_bulk/` has grown to 29 `.py` files in a flat layout. Concerns are mixed: bulk-CSV ingest, REST-API enrichment, web-discovery enrichment, matching, shared infra, CLI, GUI, migration tooling, and one operational salvage script all live as siblings. Several files are very large (`classifier.py` 66 KB, `financials_enricher.py` 94 KB, `gui.py` 53 KB). The project root has accumulated 5 one-off adhoc scripts plus a 2 GB stale DuckDB backup.

## Non-goals (Phase 1)

- **No broad product redesign.** Same command names, same GUI panes, same top-level `from ch_bulk import ...` imports. Phase 1 does intentionally fix path semantics: default `data_dir` / `db_path` stop being CWD-relative and become absolute repo-root defaults via `core/paths.py`.
- **No splitting of large files.** `classifier.py`, `financials_enricher.py`, `gui.py` stay single files this phase.
- **No new abstractions.** No base classes, no shared interfaces, no DI containers.
- **`data/staging/` is not touched.** Nothing under `data/` (staging, input, logs, output) moves, renames, or deletes.
- **`sql/`, `tests/`, `docs/` layouts unchanged.** Only module-path references in tests and scripts change (imports, function-local imports, patch-target strings, subprocess code strings, and a few stale docstrings).
- **Single commit.** All execution steps land in one git commit at the final commit step. Intermediate steps exist only for sequencing and verification, not separate commits.

## Public API contract

The following stay stable across Phase 1:

- **CLI surface**: same 15 top-level help entries (12 commands + 3 subgroups, 18 leaf commands total): `ch-bulk download`, `ch-bulk process`, `ch-bulk query`, `ch-bulk sync`, `ch-bulk match`, `ch-bulk info`, `ch-bulk ui`, `ch-bulk load-staging`, `ch-bulk classify`, `ch-bulk find-websites`, `ch-bulk cqc-enrich {providers,locations}`, `ch-bulk ch-enrich {directors,revenue}`, `ch-bulk enrich-financials`, `ch-bulk migration {export,import}`, `ch-bulk export-sqlite`. Flag names stay the same. The intentional CLI behavior change is that default `--data-dir` / `--db-path` values become absolute repo-root paths via `ch_bulk.core.paths`.
- **GUI**: launches via `ch-bulk ui`, with the same panes and workflows. Default path arguments now come from repo-root absolute defaults.
- **Top-level Python imports**: `from ch_bulk import ChBulk`, `from ch_bulk import SanityCheckError`, `from ch_bulk import SanityCheckResult`, `from ch_bulk import ensure_pipeline_schema` stay valid. `ChBulk()` intentionally picks up the new absolute default path semantics.

Direct submodule imports (`from ch_bulk.processor import ...`, `from ch_bulk.financials_enricher import ...`, etc.) are NOT part of the contract. Confirmed by grep: no external consumer in the local sibling nitkrar repos uses them. In-repo consumers (2 maintained scripts, 11 test files, function-local imports, patch-target strings, and subprocess code strings) are updated in lockstep.

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
│   └── paths.py                 # NEW — REPO_ROOT, SQL_DIR, DEFAULT_DATA_DIR, DATA_REFERENCE_DIR, DEFAULT_DB_PATH
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
4. **`cqc/`** — same pattern for CQC. Today `cqc/processor.py` imports 6 names from `companies_house/processor.py` (`SQL_DIR as _CH_SQL_DIR`, `SanityCheckError`, `SanityCheckResult`, `_escape_path`, `compact_database`, `timed_phase`). Step 6 switches CQC SQL-dir resolution to `core.paths`, which drops `_CH_SQL_DIR` and leaves 5 honest shared-symbol imports in place. Phase 2 may still extract them.
5. **`web/`** — `browser`, `search`, `website_finder`, `classifier` form the "discover and classify a company's website" subsystem. Logically a third source-like vertical (the website is the data).
6. **`matching/`** — `ch_cqc.py` is the only genuine cross-source workflow. Its own folder leaves room for future matching variants without crowding `companies_house/` or `cqc/`.
7. **Top level** — `__init__.py`, `__main__.py`, `api.py`, `cli.py`, `gui.py`. Public/operational surface; keeps the entrypoints discoverable.

## `core/paths.py`

```python
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]  # ch_bulk/core/paths.py → repo root
SQL_DIR = REPO_ROOT / "sql"
DEFAULT_DATA_DIR = REPO_ROOT / "data"
DATA_REFERENCE_DIR = DEFAULT_DATA_DIR / "reference"
DEFAULT_DB_PATH = DEFAULT_DATA_DIR / "db" / "ch_bulk.duckdb"
```

Callers that currently compute paths from `__file__`:

| Current file | Current line | Change |
|---|---|---|
| `ch_bulk/bootstrap.py:19` | `SQL_DIR = Path(__file__).resolve().parent.parent / "sql"` | `from ch_bulk.core.paths import SQL_DIR` |
| `ch_bulk/processor.py:69` | `SQL_DIR = Path(__file__).resolve().parent.parent / "sql" / "ch"` | `from ch_bulk.core.paths import SQL_DIR as _ROOT_SQL_DIR; SQL_DIR = _ROOT_SQL_DIR / "ch"` |
| `ch_bulk/migration.py:67-68` | `_repo_root()` body | `from ch_bulk.core.paths import REPO_ROOT` (replace body) |
| `ch_bulk/ch_enricher.py:966` | `bands_path = Path(__file__).resolve().parent.parent / "data" / "reference" / "revenue_bands.csv"` | `from ch_bulk.core.paths import DATA_REFERENCE_DIR; bands_path = DATA_REFERENCE_DIR / "revenue_bands.csv"` |

Phase 1 intentionally fixes the current lazy CWD-relative default bug by moving all default `data_dir` / `db_path` values onto `DEFAULT_DATA_DIR` / `DEFAULT_DB_PATH`.

Callers that currently hardcode the default DB path (25 occurrences across 7 files):

| Current file | Sites | Change |
|---|---|---|
| `ch_bulk/cli.py` | 17 `--db-path` Typer options, all defaulting to `Path("data/db/ch_bulk.duckdb")` | `Path("data/db/ch_bulk.duckdb")` → `DEFAULT_DB_PATH` |
| `ch_bulk/api.py` | 2 sites: docstring example + `ChBulk.__init__` default | docstring example becomes `ChBulk()` or uses constants; `db_path: str \| Path \| None = None` with `self.db_path = Path(db_path) if db_path is not None else DEFAULT_DB_PATH` |
| `ch_bulk/classifier.py:775` | `WebsiteClassifier.__init__` default | same `None`-default / `is not None` fallback pattern |
| `ch_bulk/cqc_api_enricher.py:767` | `CQCAPIEnricher.__init__` default | same pattern |
| `ch_bulk/website_finder.py:447` | `WebsiteFinder.__init__` default | same pattern |
| `ch_bulk/gui.py:946,1151` | `ChBulkApp.__init__` and `gui.main` defaults | same pattern |
| `ch_bulk/salvage_classification_parse_errors.py:196` | argparse default (moves to `scripts/adhoc/`) | `default="data/db/ch_bulk.duckdb"` → `default=str(DEFAULT_DB_PATH)` |

Callers that currently hardcode the default data dir (26 occurrences across 10 files):

| Current file | Sites | Change |
|---|---|---|
| `ch_bulk/cli.py` | 14 `--data-dir` Typer options, all defaulting to `Path("./data")` | `Path("./data")` → `DEFAULT_DATA_DIR` |
| `ch_bulk/api.py:95` | `ChBulk.__init__` default | `data_dir: str \| Path \| None = None` with `self.data_dir = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR` |
| `ch_bulk/ch_enricher.py:92,697,959` | `CompaniesHouseClient.__init__`, `enrich_directors`, `enrich_revenue` defaults | same `None`-default / `is not None` fallback pattern |
| `ch_bulk/classifier.py:774` | `WebsiteClassifier.__init__` default | same pattern |
| `ch_bulk/cqc_api_client.py:34` | `CQCAPIClient.__init__` default | same pattern |
| `ch_bulk/cqc_api_enricher.py:766` | `CQCAPIEnricher.__init__` default | same pattern |
| `ch_bulk/financials_enricher.py:2119` | `enrich_financials` default | same pattern |
| `ch_bulk/website_finder.py:446` | `WebsiteFinder.__init__` default | same pattern |
| `ch_bulk/gui.py:946,1151` | `ChBulkApp.__init__` and `gui.main` defaults | same pattern |
| `ch_bulk/salvage_classification_parse_errors.py:195` | argparse default | `default="data"` / `"./data"` → `default=str(DEFAULT_DATA_DIR)` |

Note: `cqc/processor.py` derives `SQL_DIR = _CH_SQL_DIR.parent / "cqc"` from its `companies_house.processor` import. After the move, that derivation still works because `companies_house/processor.py` exposes its `SQL_DIR` from `core.paths`. Cleaner to switch it to its own `from ch_bulk.core.paths import SQL_DIR as _ROOT_SQL_DIR; SQL_DIR = _ROOT_SQL_DIR / "cqc"` — done in Step 6.

## `__init__.py` updates

Current `__init__.py` exposes 3 lazy exports (`ChBulk`, `SanityCheckError`, `SanityCheckResult`) plus 1 eager export (`ensure_pipeline_schema`):

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

## Execution plan

**See [`2026-05-26-ch-bulk-refactor-implementation.md`](./2026-05-26-ch-bulk-refactor-implementation.md) for the executable step-by-step checklist** with exact commands, verify checks, and rollback paths. The implementation plan is the single source of truth for execution; this design doc only explains *what* and *why*.

## Risks

1. **Hidden circular imports.** Current flat-package import graph has no cycles, but the new boundaries might surface accidental ones. Mitigation: `unittest` runs after every move step, not just at the end (see implementation plan).
2. **`cqc/processor.py` keeps depending on `companies_house/processor.py`.** Today that dependency is 6 imported names; after Step 6 drops `_CH_SQL_DIR`, 5 shared symbols remain. This is intentional and honest (CQC ingest was built on top of CH ingest's helpers). Phase 2 may extract them — not Phase 1.
3. **The path-default fix is intentionally user-visible.** Default `data_dir` / `db_path` values become absolute repo-root paths. Command names and flag names stay stable, but help text and any output that prints the default path will change accordingly.
4. **String references to old flat module paths are easy to miss.** Tests and scripts contain patch targets, function-local imports, subprocess code strings, and stale docstrings that reference `ch_bulk.<oldmodule>`. The implementation plan includes an explicit audit step for them.
5. **GUI launch is not covered by automated tests.** Implementation plan Step 15 is manual.
6. **Module moves change logger names and object metadata.** Log records use `%(name)s`, so logger names will follow the new module paths. Object `__module__` strings also change (`ch_bulk.bootstrap` → `ch_bulk.db.bootstrap`, etc.). Accepted structural fallout.
7. **Hardcoded `data/staging/` paths in 2 adhoc scripts** (`scripts/adhoc/batch_commit.py`, `scripts/adhoc/batch_prep.py`). Those paths are unchanged so the scripts keep working. No fix needed; flagged for awareness.

## Phase 2 outline (sketch only — not in scope)

1. Split `companies_house/financials_enricher.py` (94 KB) by seam: CH filings client vs. iXBRL/OCR parser vs. DuckDB loader.
2. Split `web/classifier.py` (66 KB) by seam: prompt construction vs. LLM call vs. response parsing vs. DuckDB loader.
3. Consider extracting shared ingest helpers (`SanityCheckError`, `SanityCheckResult`, `_escape_path`, `compact_database`, `timed_phase`) from `companies_house/processor.py` into `db/ingest_helpers.py` so `cqc/processor.py` doesn't cross-import from `companies_house/` after Step 6 drops `_CH_SQL_DIR`. Verify the shape of the duplication first.
4. `gui.py` (53 KB) — defer; UI rewrites are high-risk and the GUI works.

Phase 2 decisions deferred until Phase 1 lands and the new structure is exercised for a few days.
