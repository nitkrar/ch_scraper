# ch_bulk Phase 1 refactor — implementation plan

**Companion to:** [`2026-05-26-ch-bulk-refactor-design.md`](./2026-05-26-ch-bulk-refactor-design.md) (the design doc with rationale + target tree)
**For executor:** read this doc top-to-bottom. The design doc explains "what" and "why"; this doc gives you the "how" — exact commands, exact order, exact verification checks.

## Hard rules

1. **Code is the source of truth, not this doc.** If during execution you find the code doesn't match a doc claim — e.g. the doc says "5 shared symbols" but you find a 6th, or the doc says a `__file__` path is at line 19 but it's at line 21, or you find an import the doc didn't mention — **trust the code, fold the discrepancy in, fix it, and continue.** Do NOT blindly follow doc instructions when they contradict observable code state. After folding it in, leave a one-line note in your final handoff post so the doc can be corrected for next time.
2. **Single commit at end.** All steps below land in ONE git commit at Step 17. Intermediate steps run verification but do NOT commit. If you commit mid-phase, you've made a mistake — reset and start the failed step over.
3. **Do not introduce behavior changes outside the design-approved scope.** Phase 1 does intentionally fix path semantics: `core/paths.py` adds repo-root absolute `DEFAULT_DATA_DIR` / `DEFAULT_DB_PATH`, callers stop using lazy CWD-relative defaults, and `__init__.py` `__getattr__` is retargeted. Module moves will also change logger names and object `__module__` strings; that metadata drift is acceptable. Anything beyond that is out of scope.
4. **Verify before next step.** Every step has a `Verify:` block. If any verify fails, stop. `git restore` the failing step's changes, fix, re-attempt. Do NOT proceed.
5. **Rollback target.** If the whole refactor goes sideways: `git reset --hard pre-refactor-bookmark` returns to commit `dee2501`. The tag exists; use it.
6. **Do not touch `data/staging/`, `data/input/`, `data/logs/`, `data/db/`.** None of these directories are part of the refactor.
7. **Ask the user for confirmation** at Step 11 (already-deleted `.bak` — should already be gone; if not, ask). At Step 17 (commit), ask for the final go-ahead before the commit lands.

## Pre-flight checklist (before Step 0)

- [ ] Bootstrap the environment: `source .venv/bin/activate && pip install -e .`. All shell snippets below assume the repo `.venv` is active.
- [ ] Confirm working tree is clean: `git status` must show "nothing to commit, working tree clean".
- [ ] Confirm you're on `trunk` branch: `git branch --show-current` must print `trunk`.
- [ ] Confirm bookmark tag exists: `git tag --list pre-refactor-bookmark` must list it.
- [ ] Confirm DB file is at new location: `ls -lh data/db/ch_bulk.duckdb` must show the expected file.
- [ ] Confirm `ch-bulk --help` runs and shows the same 15 top-level help entries (12 commands + 3 subgroups, 18 leaf commands total).
- [ ] Confirm `.venv/bin/python -m unittest discover -s tests` runs. Record the current pass/fail count as baseline. There are 81 `unittest` tests total; 3 in `test_financials_enricher` depend on `data/staging/filings/07545840`, which is missing in this checkout, so accept the current pass count as baseline.

If any pre-flight check fails, stop and surface to user.

## Execution steps

Each step lists exact commands. For `git mv` you can run them as a batch per step. For edit operations, use the file:line references from the design doc.

### Step 0 — Baseline capture

```bash
.venv/bin/python -m unittest discover -s tests > /tmp/ch_bulk_baseline_tests.txt 2>&1 || true
ch-bulk --help > /tmp/ch_bulk_baseline_cli_help.txt 2>&1
ch-bulk info --db-path "$(pwd)/data/db/ch_bulk.duckdb" > /tmp/ch_bulk_baseline_info.txt 2>&1 || true
.venv/bin/python -c "from ch_bulk import ChBulk, SanityCheckError, SanityCheckResult, ensure_pipeline_schema; print('ok')" > /tmp/ch_bulk_baseline_top_imports.txt 2>&1
```

**Verify:** all 4 baseline files exist and are non-empty. Note the baseline `unittest` pass/fail count and treat it as the comparison target for later steps.

### Step 1 — Create empty subpackage skeletons

```bash
mkdir -p ch_bulk/core ch_bulk/db ch_bulk/companies_house ch_bulk/cqc ch_bulk/web ch_bulk/matching
for d in core db companies_house cqc web matching; do
  printf '"""ch_bulk.%s subpackage."""\n' "$d" > "ch_bulk/$d/__init__.py"
done
```

**Verify:**
```bash
.venv/bin/python -c "import ch_bulk.core, ch_bulk.db, ch_bulk.companies_house, ch_bulk.cqc, ch_bulk.web, ch_bulk.matching; print('ok')"
.venv/bin/python -m unittest discover -s tests  # must match baseline pass/fail count
```

### Step 2 — Add `core/paths.py`

Create `ch_bulk/core/paths.py`:
```python
"""Centralized path constants for ch_bulk."""
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = REPO_ROOT / "sql"
DEFAULT_DATA_DIR = REPO_ROOT / "data"
DATA_REFERENCE_DIR = DEFAULT_DATA_DIR / "reference"
DEFAULT_DB_PATH = DEFAULT_DATA_DIR / "db" / "ch_bulk.duckdb"
```

**Verify:**
```bash
.venv/bin/python -c "from ch_bulk.core.paths import REPO_ROOT, SQL_DIR, DEFAULT_DATA_DIR, DATA_REFERENCE_DIR, DEFAULT_DB_PATH; assert SQL_DIR.is_dir() and (SQL_DIR / 'ch').is_dir() and (SQL_DIR / 'cqc').is_dir() and DEFAULT_DATA_DIR.is_dir() and DATA_REFERENCE_DIR.is_dir() and DEFAULT_DB_PATH.parent.is_dir(); print('ok')"
.venv/bin/python -m unittest discover -s tests  # match baseline
```

### Step 2.5 — Centralize absolute default paths

Replace the 25 hardcoded `"data/db/ch_bulk.duckdb"` literals across 7 files with `DEFAULT_DB_PATH`, and replace all current CWD-relative `data_dir` defaults (`"./data"` / `"data"`) with `DEFAULT_DATA_DIR`. This is an intentional bug fix: defaults become absolute repo-root paths instead of depending on the current working directory.

Pattern A (Typer defaults — `cli.py`, 17 `--db-path` sites and 14 `--data-dir` sites):
```python
# Add at top of cli.py:
from ch_bulk.core.paths import DEFAULT_DATA_DIR, DEFAULT_DB_PATH

# Replace every:
db_path: Path = typer.Option(
    Path("data/db/ch_bulk.duckdb"), "--db-path", help="DuckDB database path."
)
# With:
db_path: Path = typer.Option(
    DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
)

# Replace every:
data_dir: Path = typer.Option(
    Path("./data"), "--data-dir", "-d", help="..."
)
# With:
data_dir: Path = typer.Option(
    DEFAULT_DATA_DIR, "--data-dir", "-d", help="..."
)
```

Pattern B (constructors/functions with both `data_dir` and `db_path` defaults — `api.py`, `classifier.py`, `cqc_api_enricher.py`, `website_finder.py`, `gui.py`):
```python
# Replace:
def __init__(
    self,
    data_dir: str | Path = "./data",
    db_path: str | Path = "data/db/ch_bulk.duckdb",
) -> None:
    self.data_dir = Path(data_dir)
    self.db_path = Path(db_path)

# With:
def __init__(
    self,
    data_dir: str | Path | None = None,
    db_path: str | Path | None = None,
) -> None:
    self.data_dir = (
        Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
    )
    self.db_path = (
        Path(db_path) if db_path is not None else DEFAULT_DB_PATH
    )
```
(Add `from ch_bulk.core.paths import DEFAULT_DATA_DIR, DEFAULT_DB_PATH` at top of each module.)

Pattern C (functions/classes with only `data_dir` defaults — `ch_enricher.py`, `cqc_api_client.py`, `financials_enricher.py`):
```python
# Replace:
def enrich_x(..., data_dir: str | Path = "./data", ...) -> None:
    data_dir = Path(data_dir)

# With:
def enrich_x(..., data_dir: str | Path | None = None, ...) -> None:
    data_dir = (
        Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
    )
```

Pattern D (argparse defaults — `salvage_classification_parse_errors.py`):
```python
parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
```

Pattern E (docstring example — `api.py:81`):
```python
# Remove the literal CWD-relative values from the example, or update it to
# use ChBulk() / the constants explicitly.
```

**Verify:**
```bash
grep -rn '"data/db/ch_bulk\.duckdb"' ch_bulk/   # must return zero matches
grep -rnE 'Path\("./data"\)|default="data"|data_dir: .*"\./data"' ch_bulk/   # must return zero matches
grep -rn "DEFAULT_DB_PATH" ch_bulk/ | wc -l       # must be ≥ 25
grep -rn "DEFAULT_DATA_DIR" ch_bulk/ | wc -l      # must be ≥ 26
ch-bulk --help | grep -E "db-path|data-dir" | head -4   # defaults render as absolute repo-root paths
.venv/bin/python -c "from ch_bulk import ChBulk; ch = ChBulk(); print(ch.data_dir); print(ch.db_path)"  # prints absolute repo-root data + db paths
.venv/bin/python -m unittest discover -s tests    # match baseline
```

### Step 3 — Move `core/` modules

```bash
git mv ch_bulk/_logging.py ch_bulk/core/logging.py
git mv ch_bulk/rate_limit.py ch_bulk/core/rate_limit.py
git mv ch_bulk/settings.py ch_bulk/core/settings.py
```

Rewrite imports across the repo:
- `from ch_bulk._logging import` → `from ch_bulk.core.logging import`
- `from ch_bulk.rate_limit import` → `from ch_bulk.core.rate_limit import`
- `from ch_bulk.settings import` → `from ch_bulk.core.settings import`

Apply to all files under `ch_bulk/`, `scripts/`, and `tests/`.

**Verify:**
```bash
grep -rn "from ch_bulk\._logging\|from ch_bulk\.rate_limit\|from ch_bulk\.settings" ch_bulk/ scripts/ tests/   # zero matches
.venv/bin/python -m unittest discover -s tests  # match baseline
```

### Step 4 — Move `db/` modules

```bash
git mv ch_bulk/bootstrap.py ch_bulk/db/bootstrap.py
git mv ch_bulk/staging.py ch_bulk/db/staging.py
git mv ch_bulk/sync_batches.py ch_bulk/db/sync_batches.py
git mv ch_bulk/migration.py ch_bulk/db/migration.py
```

Rewrite imports:
- `from ch_bulk.bootstrap import` → `from ch_bulk.db.bootstrap import`
- `from ch_bulk.staging import` → `from ch_bulk.db.staging import`
- `from ch_bulk.sync_batches import` → `from ch_bulk.db.sync_batches import`
- `from ch_bulk.migration import` → `from ch_bulk.db.migration import`

Patch `db/bootstrap.py`: replace `SQL_DIR = Path(__file__).resolve().parent.parent / "sql"` with `from ch_bulk.core.paths import SQL_DIR`.

Patch `db/migration.py:67-68`: replace `_repo_root()` body with `from ch_bulk.core.paths import REPO_ROOT` (at top) and `return REPO_ROOT` (body).

Patch `ch_bulk/__init__.py:3`: `from ch_bulk.bootstrap import ensure_pipeline_schema` → `from ch_bulk.db.bootstrap import ensure_pipeline_schema`.

**Verify:**
```bash
grep -rn "from ch_bulk\.bootstrap\|from ch_bulk\.staging\|from ch_bulk\.sync_batches\|from ch_bulk\.migration" ch_bulk/ scripts/ tests/   # zero matches
.venv/bin/python -c "from ch_bulk import ensure_pipeline_schema; print(ensure_pipeline_schema.__module__)"   # prints ch_bulk.db.bootstrap
.venv/bin/python -m unittest discover -s tests  # match baseline
```

### Step 5 — Move `companies_house/` modules

```bash
git mv ch_bulk/downloader.py ch_bulk/companies_house/downloader.py
git mv ch_bulk/processor.py ch_bulk/companies_house/processor.py
git mv ch_bulk/query.py ch_bulk/companies_house/query.py
git mv ch_bulk/ch_enricher.py ch_bulk/companies_house/ch_enricher.py
git mv ch_bulk/financials_enricher.py ch_bulk/companies_house/financials_enricher.py
git mv ch_bulk/revenue_model.py ch_bulk/companies_house/revenue_model.py
```

Rewrite imports across `ch_bulk/`, `scripts/`, and `tests/`:
- `from ch_bulk.downloader import` → `from ch_bulk.companies_house.downloader import`
- `from ch_bulk.processor import` → `from ch_bulk.companies_house.processor import`
- `from ch_bulk.query import` → `from ch_bulk.companies_house.query import`
- `from ch_bulk.ch_enricher import` → `from ch_bulk.companies_house.ch_enricher import`
- `from ch_bulk.financials_enricher import` → `from ch_bulk.companies_house.financials_enricher import`
- `from ch_bulk.revenue_model import` → `from ch_bulk.companies_house.revenue_model import`

Patch `companies_house/processor.py:69`: `SQL_DIR = Path(__file__).resolve().parent.parent / "sql" / "ch"` → `from ch_bulk.core.paths import SQL_DIR as _ROOT_SQL_DIR; SQL_DIR = _ROOT_SQL_DIR / "ch"`.

Patch `companies_house/ch_enricher.py:966`: `bands_path = Path(__file__).resolve().parent.parent / "data" / "reference" / "revenue_bands.csv"` → `from ch_bulk.core.paths import DATA_REFERENCE_DIR; bands_path = DATA_REFERENCE_DIR / "revenue_bands.csv"`.

Patch `ch_bulk/__init__.py` `__getattr__`: `from ch_bulk.processor import SanityCheckError, SanityCheckResult` → `from ch_bulk.companies_house.processor import SanityCheckError, SanityCheckResult`.

**Verify:**
```bash
grep -rn "from ch_bulk\.\(downloader\|processor\|query\|ch_enricher\|financials_enricher\|revenue_model\) import" ch_bulk/ scripts/ tests/   # zero
.venv/bin/python -c "from ch_bulk import SanityCheckError; print(SanityCheckError.__module__)"   # ch_bulk.companies_house.processor
.venv/bin/python -m unittest discover -s tests  # match baseline
```

### Step 6 — Move `cqc/` modules

```bash
git mv ch_bulk/cqc_downloader.py ch_bulk/cqc/downloader.py
git mv ch_bulk/cqc_processor.py ch_bulk/cqc/processor.py
git mv ch_bulk/cqc_query.py ch_bulk/cqc/query.py
git mv ch_bulk/cqc_api_client.py ch_bulk/cqc/api_client.py
git mv ch_bulk/cqc_api_enricher.py ch_bulk/cqc/api_enricher.py
```

Rewrite imports:
- `from ch_bulk.cqc_downloader import` → `from ch_bulk.cqc.downloader import`
- `from ch_bulk.cqc_processor import` → `from ch_bulk.cqc.processor import`
- `from ch_bulk.cqc_query import` → `from ch_bulk.cqc.query import`
- `from ch_bulk.cqc_api_client import` → `from ch_bulk.cqc.api_client import`
- `from ch_bulk.cqc_api_enricher import` → `from ch_bulk.cqc.api_enricher import`

Patch `cqc/processor.py`: `SQL_DIR = _CH_SQL_DIR.parent / "cqc"` → `from ch_bulk.core.paths import SQL_DIR as _ROOT_SQL_DIR; SQL_DIR = _ROOT_SQL_DIR / "cqc"`. Drop `SQL_DIR as _CH_SQL_DIR` from the `from ch_bulk.companies_house.processor import (...)` block. The other 5 shared symbols stay imported from there (`SanityCheckError`, `SanityCheckResult`, `_escape_path`, `compact_database`, `timed_phase`).

**Verify:**
```bash
grep -rn "from ch_bulk\.\(cqc_downloader\|cqc_processor\|cqc_query\|cqc_api_client\|cqc_api_enricher\) import" ch_bulk/ scripts/ tests/   # zero
grep -n "_CH_SQL_DIR" ch_bulk/cqc/processor.py   # zero matches
.venv/bin/python -m unittest discover -s tests  # match baseline
```

### Step 7 — Move `web/` modules

```bash
git mv ch_bulk/browser.py ch_bulk/web/browser.py
git mv ch_bulk/web_search.py ch_bulk/web/search.py
git mv ch_bulk/website_finder.py ch_bulk/web/website_finder.py
git mv ch_bulk/classifier.py ch_bulk/web/classifier.py
```

Rewrite imports:
- `from ch_bulk.browser import` → `from ch_bulk.web.browser import`
- `from ch_bulk.web_search import` → `from ch_bulk.web.search import`
- `from ch_bulk.website_finder import` → `from ch_bulk.web.website_finder import`
- `from ch_bulk.classifier import` → `from ch_bulk.web.classifier import`

**Manual fixup** at `ch_bulk/web/classifier.py:28`: `from ch_bulk import browser` → `from ch_bulk.web import browser`.

**Verify:**
```bash
grep -rn "from ch_bulk\.\(browser\|web_search\|website_finder\|classifier\) import" ch_bulk/ scripts/ tests/   # zero
grep -n "from ch_bulk import browser" ch_bulk/   # zero
.venv/bin/python -m unittest discover -s tests  # match baseline
```

### Step 8 — Move `matching/`

```bash
git mv ch_bulk/matcher.py ch_bulk/matching/ch_cqc.py
```

Rewrite imports: `from ch_bulk.matcher import` → `from ch_bulk.matching.ch_cqc import`.

**Verify:**
```bash
grep -rn "from ch_bulk\.matcher" ch_bulk/ scripts/ tests/   # zero
.venv/bin/python -m unittest discover -s tests  # match baseline
```

### Step 8.5 — Rewrite old flat-module string references

Now that all target subpackages exist, rewrite the non-import references that import-only greps miss:

- function-local imports in `ch_bulk/cli.py:98,250`
- function-local imports in `ch_bulk/gui.py:907,916,923`
- function-local import in `ch_bulk/processor.py:622`
- stale docstrings such as `ch_bulk/api.py` mentioning `ch_bulk.processor.SanityCheckError`
- `patch("ch_bulk.<oldmodule>...")` targets in tests
- subprocess code strings in `tests/test_staging.py`

Use this inventory command first:
```bash
grep -rn "patch.*ch_bulk\." tests/ scripts/
```

Then rewrite any remaining flat-module references to the new subpackage paths.

**Verify:**
```bash
grep -RInE 'ch_bulk\.(_logging|bootstrap|staging|sync_batches|migration|downloader|processor|query|ch_enricher|financials_enricher|revenue_model|cqc_downloader|cqc_processor|cqc_query|cqc_api_client|cqc_api_enricher|browser|web_search|website_finder|classifier|matcher|settings|rate_limit)([^A-Za-z0-9_]|$)' ch_bulk/ scripts/ tests/   # zero matches
grep -rn "patch.*ch_bulk\." tests/ scripts/   # spot-check that remaining patch targets point at new subpackage paths
.venv/bin/python -m unittest discover -s tests  # match baseline
```

### Step 9 — Move root adhoc scripts + salvage

```bash
mkdir -p scripts/adhoc
git mv adhoc_batch_commit.py scripts/adhoc/batch_commit.py
git mv adhoc_batch_prep.py scripts/adhoc/batch_prep.py
git mv adhoc_fetch_batch.py scripts/adhoc/fetch_batch.py
git mv fetch_shard_0.py scripts/adhoc/fetch_shard_0.py
git mv classify_shard_adhoc.py scripts/adhoc/classify_shard.py
git mv ch_bulk/salvage_classification_parse_errors.py scripts/adhoc/salvage_classification_parse_errors.py
```

Patch `scripts/adhoc/salvage_classification_parse_errors.py` imports:
- `from ch_bulk.bootstrap import ensure_pipeline_schema` → `from ch_bulk.db.bootstrap import ensure_pipeline_schema`
- `from ch_bulk.classifier import (...)` → `from ch_bulk.web.classifier import (...)`
- `from ch_bulk.staging import with_duckdb_connection` → `from ch_bulk.db.staging import with_duckdb_connection`

**Verify:**
```bash
find . -maxdepth 1 -type f -name "*.py" | wc -l   # zero
find scripts/adhoc -maxdepth 1 -type f -name "*.py" | wc -l   # 6
.venv/bin/python scripts/adhoc/salvage_classification_parse_errors.py --help 2>&1 | head -1   # no ImportError
```

### Step 10 — Create `scripts/README.md`

Write `scripts/README.md` with two sections: "Maintained helpers" (top-level `scripts/`) and "Frozen one-shots" (`scripts/adhoc/`). One line per script.

**Verify:** `cat scripts/README.md` lists every `.py` file under `scripts/` and `scripts/adhoc/`.

### Step 11 — Delete stale backup (likely already done)

```bash
find . -maxdepth 1 -name 'ch_bulk.duckdb.bak-pre-trackA-201255' -print
# If it exists: ASK USER for confirmation, then remove it.
# If it does not exist (likely — deleted during pre-bookmark), proceed.
```

**Verify:**
```bash
find . -maxdepth 1 -name 'ch_bulk.duckdb*' -print   # expected: no output
```

### Step 12 — Structural audit (file layout)

```bash
# Top-level ch_bulk/ should only have 5 .py files:
find ch_bulk -maxdepth 1 -type f -name "*.py" | sort
# Expected: __init__.py, __main__.py, api.py, cli.py, gui.py

# Each subpackage has expected count:
find ch_bulk/core -type f -name "*.py" | wc -l             # 5: __init__, logging, paths, rate_limit, settings
find ch_bulk/db -type f -name "*.py" | wc -l               # 5: __init__, bootstrap, migration, staging, sync_batches
find ch_bulk/companies_house -type f -name "*.py" | wc -l  # 7: __init__, ch_enricher, downloader, financials_enricher, processor, query, revenue_model
find ch_bulk/cqc -type f -name "*.py" | wc -l              # 6: __init__, api_client, api_enricher, downloader, processor, query
find ch_bulk/web -type f -name "*.py" | wc -l              # 5: __init__, browser, classifier, search, website_finder
find ch_bulk/matching -type f -name "*.py" | wc -l         # 2: __init__, ch_cqc
```

**Verify:** all counts match expected.

### Step 13 — Structural audit (imports + old-path references)

```bash
# No stragglers — every from ch_bulk.X import is to a current subpackage or top-level file:
grep -rn "from ch_bulk\." ch_bulk/ scripts/ tests/ | grep -vE "ch_bulk\.(core|db|companies_house|cqc|web|matching|api|cli|gui|__main__)"
# Expected: zero matches

# No stale old flat-module references anywhere in code/tests/scripts:
grep -RInE 'ch_bulk\.(_logging|bootstrap|staging|sync_batches|migration|downloader|processor|query|ch_enricher|financials_enricher|revenue_model|cqc_downloader|cqc_processor|cqc_query|cqc_api_client|cqc_api_enricher|browser|web_search|website_finder|classifier|matcher|settings|rate_limit)([^A-Za-z0-9_]|$)' ch_bulk/ scripts/ tests/
# Expected: zero matches

# Only one place uses __file__ for path resolution:
grep -rn "__file__" ch_bulk/
# Expected: only ch_bulk/core/paths.py
```

### Step 14 — Behavioral verification (automated)

```bash
.venv/bin/python -m unittest discover -s tests   # must match baseline
diff <(ch-bulk --help) /tmp/ch_bulk_baseline_cli_help.txt   # must be empty
diff <(ch-bulk info --db-path "$(pwd)/data/db/ch_bulk.duckdb") /tmp/ch_bulk_baseline_info.txt   # must be empty
.venv/bin/python -c "from ch_bulk import ChBulk, SanityCheckError, SanityCheckResult, ensure_pipeline_schema; ch = ChBulk(); print(ch.data_dir); print(ch.db_path); print('ok')"   # prints repo-root absolute paths + ok
```

### Step 15 — Behavioral verification (manual, GUI)

ASK USER to manually launch `ch-bulk ui` and confirm:
- window opens
- left rail shows CH / CQC / Settings panes
- no Python tracebacks in terminal
- each pane renders when clicked

User responds "GUI ok" or reports issues. Do not proceed to Step 17 until GUI is confirmed.

### Step 16 — Final diff review

```bash
git status
git diff --stat pre-refactor-bookmark --
git diff pre-refactor-bookmark -- ch_bulk/__init__.py
```

Surface the stats to the user. They should see ~29 file renames/moves, a handful of import-rewrite diffs, the new `core/paths.py`, and the expected path-default updates.

### Step 17 — Commit (single, all-in-one)

ASK USER for final go-ahead, then:

```bash
git add -A
git commit -m "$(cat <<'EOF'
Refactor ch_bulk into subpackages (core/db/companies_house/cqc/web/matching)

Structural refactor plus repo-root default-path fix:
- New core/paths.py exposes REPO_ROOT, SQL_DIR, DEFAULT_DATA_DIR,
  DATA_REFERENCE_DIR, DEFAULT_DB_PATH
- ch_bulk/__init__.py lazy __getattr__ retargeted to new module paths
- 25 hardcoded "data/db/ch_bulk.duckdb" literals and all CWD-relative
  data_dir defaults replaced with core.paths constants

CLI surface, GUI panes, and top-level Python API names unchanged.
Default data/db paths now resolve from repo root instead of the
current working directory.

See docs/plans/2026-05-26-ch-bulk-refactor-design.md for full rationale
and target structure.
EOF
)"

git log -1 --stat
git status
```

**Verify:** single commit on `trunk`. `git log --oneline -2` shows the new commit on top of the pre-refactor state.

## Failure modes and recovery

| Symptom | Likely cause | Fix |
|---|---|---|
| `unittest` fails after Step 3-8 | Forgot to rewrite an import in one file | Re-run the grep verify; find and fix the missed file |
| `ImportError: cannot import name X from ch_bulk.Y` | Top-level `__init__.py` `__getattr__` still points at an old path | Re-check the `__init__.py` patch in Steps 4-5 |
| `FileNotFoundError: sql/...` after Step 4 or 5 | `__file__`-relative path not updated | Re-check the `core.paths` import in `bootstrap.py` / `processor.py` |
| `FileNotFoundError: revenue_bands.csv` after Step 5 | `ch_enricher.py:966` path not updated | Apply the `DATA_REFERENCE_DIR` patch from Step 5 |
| `patch("ch_bulk.X...")` tests still fail after the moves | Old flat module names still live in patch targets or subprocess code strings | Re-run Step 8.5 and the broad old-module grep |
| `ch-bulk info` diff is noisy in Step 14 | You forgot the explicit `--db-path` argument on one side of the diff | Re-run Step 14 exactly as written |
| GUI crashes at launch in Step 15 | Probably an import error in `gui.py` or one of its indirect imports | Check `gui.py`, function-local settings imports, and the top-level `ChBulk` path |
| Whole phase looks broken | — | `git reset --hard pre-refactor-bookmark` and start over |

## Handoff

When done, post to the side room:
```
STATUS | <req-id> | Phase 1 refactor complete.
Commit: <hash>
Files moved: ~29
unittest: <pass count>/<total>
CLI diff vs baseline: <empty | N diffs>
Info diff vs baseline (--db-path explicit): <empty | N diffs>
GUI smoke: <ok | issues>
Tag pre-refactor-bookmark preserved for rollback.
```
