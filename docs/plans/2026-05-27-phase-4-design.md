# Phase 4: Central path config + producer/consumer bug fix

**Date:** 2026-05-27
**Status:** Design v2 (reviewed)
**Companion:** [`2026-05-27-phase-4-implementation.md`](./2026-05-27-phase-4-implementation.md) — verifiable execution checklist
**Predecessor:** Phase 3 commit `0844883` on `trunk`. Pre-Phase-4 bookmark: tag will be added in implementation Step 0.
**Audit source:** `/tmp/codex_path_audit.md` (req-0133)

## Problem

While testing Phase 3, user ran `ch-bulk` to download CH bulk data. Files landed in `data/BasicCompanyData-...` at repo root instead of `data/input/ch/` (the documented location per `ChBulk.ch_dir`). Investigation revealed a broader pattern:

1. **Producer/consumer mismatch (BUG):** `companies_house/downloader.py` writes to `data_dir/`; `api.py:108` (`self.ch_dir`) reads from `data_dir/input/ch/`. Split-phase use (`download` then `process`) breaks with `FileNotFoundError`. CQC downloader does it right — convention exists, just inconsistently applied.

2. **6 inline path sites across 4 runtime modules (SMELL):** `api.py`, `cqc/downloader.py`, `core/settings.py`, and `ch_enricher.py` reconstruct `data_dir / "input"`, `data_dir / "settings.json"`, and `DATA_REFERENCE_DIR / "revenue_bands.csv"` inline. Bypasses `core/paths.py` — same DRY problem Phase 1.5 fixed for staging/logs.

3. **`enrich_revenue(data_dir=...)` ignores its `data_dir` arg** — always reads from repo-root `data/reference/revenue_bands.csv`. Silent bug: parameter is documented but does nothing.

4. **`db_path` doesn't follow `data_dir`** — when caller passes a custom `data_dir`, `db_path` stays pinned to `DEFAULT_DB_PATH` (repo `data/db/`). Inconsistent with how `data_dir` drives logs, staging, inputs, settings.

User pushback (drove this scope): *"This should be in paths/settings not hardcoded in downloader script. Also the same for cqc downloader. So the paths are consistent across scripts. Processor will probably expect in data directory and it won't be present. Audit the scripts for hardcoded paths and the continuity is maintained using central configs not individually hardcoded paths in the scripts directly."*

## Approach

Three workstreams, single commit:

### Workstream A: Add the missing helpers in `core/paths.py`

```python
def input_dir(data_dir: Path | str, source: str) -> Path:
    return Path(data_dir) / "input" / source

def ch_input_dir(data_dir: Path | str) -> Path:
    return input_dir(data_dir, "ch")

def cqc_input_dir(data_dir: Path | str) -> Path:
    return input_dir(data_dir, "cqc")

def settings_path(data_dir: Path | str) -> Path:
    return Path(data_dir) / "settings.json"

def reference_dir(data_dir: Path | str) -> Path:
    return Path(data_dir) / "reference"

def revenue_bands_path(data_dir: Path | str) -> Path:
    return reference_dir(data_dir) / "revenue_bands.csv"

def db_dir(data_dir: Path | str) -> Path:
    return Path(data_dir) / "db"

def default_db_path(data_dir: Path | str) -> Path:
    return db_dir(data_dir) / "ch_bulk.duckdb"
```

The existing `DEFAULT_DB_PATH = DEFAULT_DATA_DIR / "db" / "ch_bulk.duckdb"` becomes a thin wrapper: `DEFAULT_DB_PATH = default_db_path(DEFAULT_DATA_DIR)`.

### Workstream B: Migrate ~35 call sites + fix the CH BUG

Per the audit at `/tmp/codex_path_audit.md`, the call sites group into:

| Cluster | Sites | Action |
|---|---|---|
| `ChBulk.ch_dir` / `.cqc_dir` properties | api.py:108, 113 | Use new helpers |
| `core/settings.py` settings path helper | settings.py:58-59 | Delegate the existing local helper to `core.paths.settings_path()` |
| GUI settings display | gui.py:908-911 | No direct path construction here today; GUI follows automatically once `core.settings.settings_path()` delegates to `core.paths` |
| CH downloader writes (BUG) | companies_house/downloader.py:247,268,302,333 | Use `ch_input_dir()` — fixes the bug |
| CQC downloader writes (SMELL) | cqc/downloader.py:167,193 | Use `cqc_input_dir()` |
| `ch_enricher.py` revenue bands path | ch_enricher.py:967-970 | Use `revenue_bands_path(data_dir)` — actually use the data_dir arg |
| `ChBulk.__init__` db_path | api.py:96-102 | `db_path = Path(db_path) if db_path is not None else default_db_path(self.data_dir)` |
| `WebsiteClassifier.__init__` db_path | classifier_pipeline.py | Same pattern |
| `CQCAPIEnricher.__init__` db_path | cqc/api_enricher.py | Same pattern |
| `WebsiteFinder.__init__` db_path | web/website_finder.py | Same pattern |
| `ChBulkApp.__init__` + `gui.main` db_path | gui.py | Same pattern |
| `salvage_classification_parse_errors.py` argparse | scripts/adhoc/salvage_classification_parse_errors.py | `default=str(default_db_path(DEFAULT_DATA_DIR))` |
| CLI `--db-path` defaults on commands that already take `--data-dir` (13 sites) | cli.py | Drop the static default, accept `None`, and let `ChBulk(data_dir=..., db_path=None)` resolve to `default_db_path(data_dir)` |
| CLI pure-DB commands without `--data-dir` (4 sites: `query`, `match`, `export-sqlite`, `info`) | cli.py | Accept `None` for a consistent help surface, but resolve omitted values back to `DEFAULT_DB_PATH` in the command body. No new CLI flags. |

Missed by the original audit but worth naming explicitly: `scripts/export_homecare_xlsx.py` also defaults `--db` to `DEFAULT_DB_PATH`. It is an ad hoc reporting script with no `data_dir` concept, so leave it repo-root-scoped in Phase 4 and call that out as a non-goal rather than silently widening the scope.

### Workstream C: db_path follows data_dir — design choice

The cleanest way to make CLI `--db-path` follow `--data-dir` without breaking explicit `--db-path` overrides: drop the static `Path("data/db/ch_bulk.duckdb")` default. Instead, accept `db_path: Path | None = None` in each Typer command. Inside the command body, resolve:

```python
ch = ChBulk(data_dir=data_dir, db_path=db_path)  # ChBulk handles None → default_db_path(data_dir)
```

For the 13 commands that already accept `--data-dir`, this means `--db-path` can be truly data-dir-relative. For the 4 pure-DB commands that do not accept `--data-dir`, omit the static Typer default for consistency but resolve `None` back to `DEFAULT_DB_PATH` in the command body. Do **not** add `--data-dir` to those commands in this phase.

This means `ch-bulk --help` no longer shows a concrete `--db-path` default. Acceptable trade-off — the runtime default is either "wherever the data goes" (commands with `--data-dir`) or the same repo-root `DEFAULT_DB_PATH` fallback as today (pure-DB commands).

**Behavior changes** (intentional, user-approved):
- `ch-bulk --data-dir /tmp/foo download` now writes DB to `/tmp/foo/db/ch_bulk.duckdb`, not `<repo>/data/db/ch_bulk.duckdb`. Today: DB is the repo's regardless of `--data-dir`.
- `enrich_revenue(data_dir=...)` now actually reads from `data_dir/reference/revenue_bands.csv`. Today: silently ignores `data_dir`.
- `ch-bulk --help` shows no concrete `--db-path` default (just the flag exists). For pure-DB commands without `--data-dir`, the runtime fallback remains the same repo-root `DEFAULT_DB_PATH`.

## Non-goals

- **No SQL subdir cleanup.** `companies_house/processor.py:73`, `cqc/processor.py:49`, `db/bootstrap.py:22,23,155` still construct `SQL_DIR / "ch"` etc. inline. Cosmetic; not causing bugs; defer.
- **No Phase 1.5 doc cleanup.** Docs still describe `archive/` as under `staging/` but live code says `data/archive/`. Code is canonical. Doc fix is a separate small commit later.
- **No changes to data layout on disk.** Empty subdirs (`data/staging/runs/`, etc.) stay where they are.
- **No CLI command additions** (HSCA/CQC bulk CLI wiring is Phase 5 scope).
- **No Phase 4 change to `scripts/export_homecare_xlsx.py`.** It keeps its repo-root `DEFAULT_DB_PATH` / `DEFAULT_DATA_DIR` defaults because it is an ad hoc exporter with no `data_dir` surface.
- **No test framework changes.** Add regression tests using existing unittest.
- **Single commit.** All workstream A+B+C land in one git commit.

## Public API contract

| Surface | Phase 4 change | Compatibility |
|---|---|---|
| `ChBulk()` (no args) | Same DB path (`DEFAULT_DB_PATH`) | ✓ unchanged |
| `ChBulk(data_dir="./custom")` | DB path becomes `./custom/db/ch_bulk.duckdb` (was repo `data/db/`) | ⚠ intentional change |
| `ChBulk(data_dir=X, db_path=Y)` | Both honored explicitly | ✓ unchanged |
| `ch-bulk` CLI without flags | Same defaults via `DEFAULT_DATA_DIR` / `DEFAULT_DB_PATH` | ✓ unchanged |
| `ch-bulk --data-dir /tmp/foo` | For commands that already take `--data-dir`, all paths (input, staging, logs, db) resolve under `/tmp/foo` | ⚠ intentional change |
| `query` / `match` / `export-sqlite` / `info` with no `--db-path` | Still fall back to repo-root `DEFAULT_DB_PATH` because those commands do not take `--data-dir` | ✓ unchanged runtime behavior |
| `ch-bulk --db-path Y` | Explicit override honored | ✓ unchanged |
| `enrich_revenue(data_dir=X)` | Reads bands from `X/reference/revenue_bands.csv` (was repo) | ⚠ intentional fix |
| Test patch paths | All current patches still resolve | ✓ unchanged |

The "intentional change" rows are the bug fixes the user explicitly approved.

## Rationale

1. **Producer/consumer must agree.** Phase 1.5 already centralized staging/logs because the same drift was happening there. Inputs and reference data are the leftover; finishing the job.
2. **`db_path` following `data_dir`** removes the "data goes here but DB goes there" surprise. Matches user expectation when passing custom `--data-dir`.
3. **`enrich_revenue(data_dir=...)` actually using its arg** is honest — currently the docstring lies about what the parameter does.
4. **Single commit** matches the established phase pattern.
5. **CLI default behavior change** (no static `--db-path` default) is acceptable: today's default is misleading anyway (it always points repo-root regardless of `--data-dir`).

## Risks

1. **`ChBulk(data_dir=X)` behavior change is observable.** Any caller (notebook, script outside this repo) that passes `data_dir=X` and expects the DB to stay at `DEFAULT_DB_PATH` breaks. Mitigation: explicit `db_path=` override still works. Per project memory, grep confirms no external in-tree consumer relies on the old behavior. uk-homecare-deals sibling repo uses CLI only.

2. **CLI `--help` default disappears.** `--db-path` no longer shows a concrete default. Cosmetic regression. Mitigation: help text should spell out the runtime rule: "`<data-dir>/db/ch_bulk.duckdb` when a command has `--data-dir`, otherwise the repo default DB."

3. **`enrich_revenue(data_dir=X)` reading new path.** Any future test that passes a custom `data_dir` must stage `revenue_bands.csv` into that temp tree. Current `tests/test_ch_enricher.py` only exercises the default-root reference CSV, so it is more of a coverage gap than a likely regression. Mitigation: add one explicit custom-`data_dir` regression test that copies the CSV into `revenue_bands_path(tmp)`.

4. **~35-call-site change is still large.** Higher risk of missing one, especially because the runtime work mixes 13 data-dir-aware CLI commands, 4 pure-DB CLI commands, and 6 constructor/entrypoint fallbacks. Mitigation: implementation plan is structured per cluster and explicitly distinguishes the 13-vs-4 CLI split.

5. **`DEFAULT_DB_PATH` constant kept (don't remove).** Some code/test may still import it directly. Keep it as `DEFAULT_DB_PATH = default_db_path(DEFAULT_DATA_DIR)` — same value, just routed through the helper.

## What this enables (later)

- Phase 5: HSCA + CQC bulk CLI/GUI wiring (clean path layer makes this easy)
- Phase 6: Extract shared enricher base (path layer no longer in the way)
- Fixing the 7 Playwright/browser-launch test errors (independent, parallel-able)
