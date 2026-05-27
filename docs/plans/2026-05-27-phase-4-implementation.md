# Phase 4 implementation plan

**Companion to:** [`2026-05-27-phase-4-design.md`](./2026-05-27-phase-4-design.md) — rationale + symbol map
**For executor:** read this doc top-to-bottom. The design doc explains *what* and *why*; this doc gives you the *how*.

## Hard rules

1. **Code is the source of truth, not this doc.** If during execution you find the code doesn't match a doc claim (line numbers, additional call sites I missed, different signatures), **trust the code, fold the discrepancy in, fix it, continue**. Note discrepancies in your final handoff post.
2. **Single commit at the end.** All steps below land in ONE git commit at the final step. Intermediate steps run verification but do NOT commit.
3. **Behavior changes are limited to the design-approved set:**
   - `ChBulk(data_dir=X)` (no explicit db_path) → DB at `X/db/ch_bulk.duckdb` (was repo `data/db/`)
   - `enrich_revenue(data_dir=X)` → reads bands from `X/reference/revenue_bands.csv` (was always repo)
   - CLI `--db-path` shows no concrete default (depends on `--data-dir`)
   - CH downloader writes to `data_dir/input/ch/` (was `data_dir/`) — bug fix
   - Anything else is out of scope.
4. **Verify before next step.** Every step has a `Verify:` block. If any verify fails, stop. Revert tracked edits and remove untracked files before retrying.
5. **Rollback target.** Step 0 creates a `pre-phase-4-bookmark` tag. If anything goes wrong: `git reset --hard pre-phase-4-bookmark && git clean -fd`.
6. **No surprise behavior changes.** Test patches at facade paths must still resolve. Logger names don't change. Other Python imports unchanged.
7. **Ask the user for confirmation** at Step 11 (GUI smoke) and Step 13 (final commit).

## Pre-flight checklist (before Step 0)

- [ ] Confirm working tree is clean: `git status` shows "nothing to commit, working tree clean".
- [ ] If the 2 Phase 4 plan docs are still untracked, commit them FIRST as their own small docs commit (e.g. 'Add Phase 4 design + implementation plan') BEFORE Step 0. Same pattern as Phases 1/1.5/2/3.
- [ ] Confirm you're on `trunk` branch: `git branch --show-current` prints `trunk`.
- [ ] Confirm the Phase 3 commit is in history: `git merge-base --is-ancestor 0844883 HEAD && echo ok` prints `ok`.
- [ ] Confirm the bookmark tag does NOT already exist: `git tag --list pre-phase-4-bookmark` prints nothing.
- [ ] Confirm venv + deps: `source .venv/bin/activate && which python && .venv/bin/python -c "import duckdb, httpx, requests, trafilatura"` returns no errors.
- [ ] Confirm baseline test summary: `.venv/bin/python -m unittest discover -s tests > /tmp/phase4_preflight_tests.txt 2>&1 || true` then `grep -E "^(Ran |FAILED|OK$)" /tmp/phase4_preflight_tests.txt | tail -2` should show `80 tests / 0 fail / 7 errors` (Playwright errors, out of scope).
- [ ] If `data/BasicCompanyData-*.csv` exists at repo root (left from the buggy run user found), delete them: `rm -f data/BasicCompanyData-*.csv`. They're duplicates of `data/input/ch/`.

## Execution steps

### Step 0 — Baseline capture + bookmark tag

```bash
.venv/bin/python -m unittest discover -s tests > /tmp/phase4_baseline_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase4_baseline_tests.txt | tail -2
.venv/bin/ch-bulk --help > /tmp/phase4_baseline_cli_help.txt 2>&1
.venv/bin/ch-bulk info --db-path "$(pwd)/data/db/ch_bulk.duckdb" > /tmp/phase4_baseline_info.txt 2>&1 || true
.venv/bin/python -c "from ch_bulk import ChBulk, SanityCheckError, SanityCheckResult, ensure_pipeline_schema; print('ok')" > /tmp/phase4_baseline_top_imports.txt 2>&1
git tag pre-phase-4-bookmark HEAD
git tag --list pre-phase-4-bookmark
```

**Verify:** all baseline files non-empty. Tag exists.

### Step 1 — Add the new helpers in `core/paths.py`

Add to `ch_bulk/core/paths.py`:

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

Update the existing `DEFAULT_DB_PATH` to route through the helper:
```python
DEFAULT_DB_PATH = default_db_path(DEFAULT_DATA_DIR)
```

(Same value as before, just expressed via the helper.)

`DATA_REFERENCE_DIR` stays as-is — it's the default-root version of `reference_dir(DEFAULT_DATA_DIR)`. Keep it because tests and direct imports may reference it.

**Verify:**
```bash
.venv/bin/python -c "
from pathlib import Path
from ch_bulk.core.paths import (
    input_dir, ch_input_dir, cqc_input_dir, settings_path,
    reference_dir, revenue_bands_path, db_dir, default_db_path,
    DEFAULT_DB_PATH, DEFAULT_DATA_DIR, DATA_REFERENCE_DIR,
)
d = Path('/tmp/p4_test')
assert input_dir(d, 'x') == d / 'input' / 'x'
assert ch_input_dir(d) == d / 'input' / 'ch'
assert cqc_input_dir(d) == d / 'input' / 'cqc'
assert settings_path(d) == d / 'settings.json'
assert reference_dir(d) == d / 'reference'
assert revenue_bands_path(d) == d / 'reference' / 'revenue_bands.csv'
assert db_dir(d) == d / 'db'
assert default_db_path(d) == d / 'db' / 'ch_bulk.duckdb'
# DEFAULT_DB_PATH unchanged from before:
assert DEFAULT_DB_PATH == DEFAULT_DATA_DIR / 'db' / 'ch_bulk.duckdb'
# DATA_REFERENCE_DIR unchanged:
assert DATA_REFERENCE_DIR == DEFAULT_DATA_DIR / 'reference'
print('ok')
"
.venv/bin/python -m unittest discover -s tests > /tmp/phase4_step1_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase4_step1_tests.txt | tail -2
diff <(grep -E "^(Ran |FAILED|OK$)" /tmp/phase4_baseline_tests.txt | tail -2) <(grep -E "^(Ran |FAILED|OK$)" /tmp/phase4_step1_tests.txt | tail -2)
# Expected: empty diff (helpers added, nothing uses them yet)
```

### Step 2 — Migrate `core/settings.py` and `api.py` to helpers

In `ch_bulk/core/settings.py:58-59` (or wherever the function lives — verify with code):
- Replace inline `Path(data_dir) / SETTINGS_FILENAME` with `from ch_bulk.core.paths import settings_path; return settings_path(data_dir)`.
- The `SETTINGS_FILENAME` constant can stay or be inlined into the helper — your call (the constant is only referenced from this one place per the audit).

In `ch_bulk/api.py:108,113`:
- `self.data_dir / "input" / "ch"` → `from ch_bulk.core.paths import ch_input_dir, cqc_input_dir` at top, then `return ch_input_dir(self.data_dir)`.
- Same for cqc.

In `ch_bulk/gui.py`, verify the current behavior before editing:
- Today the GUI already calls `core.settings.settings_path(self.ch.data_dir)` when it renders the Settings pane.
- That means no standalone GUI path fix is required if `core.settings.settings_path()` becomes a thin wrapper over `core.paths.settings_path()`.
- Only touch `gui.py` in this step if you intentionally want the import to come directly from `core.paths`. Behavior must stay identical.

**Verify:**
```bash
.venv/bin/python -c "
from pathlib import Path
from ch_bulk import ChBulk
from ch_bulk.core.settings import settings_path
d = Path('/tmp/p4_step2')
ch = ChBulk(data_dir=d)
assert ch.ch_dir == d / 'input' / 'ch'
assert ch.cqc_dir == d / 'input' / 'cqc'
assert settings_path(d) == d / 'settings.json'
print('ok')
"
grep -n 'return self.data_dir / "input"' ch_bulk/api.py
grep -n 'return Path(data_dir) / SETTINGS_FILENAME' ch_bulk/core/settings.py
# Expected: both empty. Do NOT grep all of ch_bulk/ yet — cqc/downloader.py is Step 3.
.venv/bin/python -m unittest discover -s tests > /tmp/phase4_step2_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase4_step2_tests.txt | tail -2
diff <(grep -E "^(Ran |FAILED|OK$)" /tmp/phase4_baseline_tests.txt | tail -2) <(grep -E "^(Ran |FAILED|OK$)" /tmp/phase4_step2_tests.txt | tail -2)
```

### Step 3 — Migrate `cqc/downloader.py` (SMELL, no behavior change)

`cqc/downloader.py:167,193`:
- `target_dir = data_dir / "input" / "cqc"` → `target_dir = cqc_input_dir(data_dir)` (with the import at top).

**Verify:**
```bash
grep -rn 'data_dir / *"input" */ *"cqc"' ch_bulk/cqc/ | grep -v "core/paths.py"
# Expected: zero matches
.venv/bin/python -m unittest tests.test_cqc_hsca -v 2>&1 | tail -5
# Expected: no regression
```

### Step 4 — Fix the CH downloader BUG

In `ch_bulk/companies_house/downloader.py`:

At top: `from ch_bulk.core.paths import ch_input_dir`.

In `download_bulk_data(data_dir, ...)`:
- Replace `data_dir = Path(data_dir); data_dir.mkdir(...)` with:
  ```python
  data_dir = Path(data_dir)
  target_dir = ch_input_dir(data_dir)
  target_dir.mkdir(parents=True, exist_ok=True)
  ```
- Replace both `dest = data_dir / filename` lines (~line 268 and ~line 302) with `dest = target_dir / filename`.
- Replace `_extract_zip(zip_path, data_dir)` (~line 333) with `_extract_zip(zip_path, target_dir)`.

This is the actual user-visible bug fix.

**Verify:**
```bash
grep -n 'dest *= *data_dir /' ch_bulk/companies_house/downloader.py
# Expected: zero matches
grep -n 'data_dir = Path(data_dir)\|target_dir = ch_input_dir' ch_bulk/companies_house/downloader.py | head -5
# Expected: shows both the Path conversion and the ch_input_dir helper use

# Smoke test the download path resolution (without actually downloading anything):
.venv/bin/python -c "
import tempfile
from pathlib import Path
from ch_bulk.core.paths import ch_input_dir
with tempfile.TemporaryDirectory() as tmp:
    target = ch_input_dir(Path(tmp))
    assert target == Path(tmp) / 'input' / 'ch'
    target.mkdir(parents=True, exist_ok=True)
    assert target.is_dir()
print('ok')
"
```

### Step 5 — Migrate `enrich_revenue` to use the data_dir arg properly

In `ch_bulk/companies_house/ch_enricher.py:967-970` (verify line numbers):
- Remove the `del data_dir` line if present.
- Replace `bands_path = DATA_REFERENCE_DIR / "revenue_bands.csv"` with `bands_path = revenue_bands_path(data_dir)`.
- Add `from ch_bulk.core.paths import revenue_bands_path` at top (and remove `DATA_REFERENCE_DIR` import if no longer used elsewhere in this file).

**Behavior change:** `enrich_revenue(data_dir=X)` now reads from `X/reference/revenue_bands.csv` instead of repo. If `X` doesn't have that file, the function raises (this is correct — the parameter now means what it says).

**Verify:**
```bash
.venv/bin/python -c "
from pathlib import Path
import tempfile, shutil
from ch_bulk.core.paths import revenue_bands_path, DATA_REFERENCE_DIR
# Copy the reference CSV into a temp data_dir and verify enrich_revenue path resolves there:
with tempfile.TemporaryDirectory() as tmp:
    target = revenue_bands_path(tmp)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(DATA_REFERENCE_DIR / 'revenue_bands.csv', target)
    assert target.is_file()
print('ok')
"
.venv/bin/python -m unittest tests.test_ch_enricher -v 2>&1 | tail -5
# Expected: current tests should stay green because they only exercise the repo-default reference CSV.
# Do NOT treat that as sufficient coverage for the new custom-data_dir behavior — add an explicit regression in Step 8.
```

If a test breaks: read the test, decide if it should pass a custom data_dir (then copy the CSV into the test's tmpdir) or use DEFAULT_DATA_DIR. Don't roll back the production fix.

### Step 6 — Migrate `ChBulk.__init__` and 5 other constructor sites to `default_db_path`

In `ch_bulk/api.py` `ChBulk.__init__`:
- Currently: `self.db_path = Path(db_path) if db_path is not None else DEFAULT_DB_PATH`
- Change to: `self.db_path = Path(db_path) if db_path is not None else default_db_path(self.data_dir)`
- Add `from ch_bulk.core.paths import default_db_path` at top.

Apply the same pattern to:
- `ch_bulk/web/classifier_pipeline.py` `WebsiteClassifier.__init__`
- `ch_bulk/cqc/api_enricher.py` `CQCAPIEnricher.__init__`
- `ch_bulk/web/website_finder.py` `WebsiteFinder.__init__`
- `ch_bulk/gui.py` `ChBulkApp.__init__` and `gui.main`
- `scripts/adhoc/salvage_classification_parse_errors.py` argparse: `default=str(default_db_path(DEFAULT_DATA_DIR))` (no data_dir context for argparse — use default).

Do **not** pull `scripts/export_homecare_xlsx.py` into this phase. It is an ad hoc exporter with no `data_dir` surface and stays repo-root-scoped by design.

**Behavior change:** `ChBulk(data_dir="/tmp/foo")` now puts DB at `/tmp/foo/db/ch_bulk.duckdb` (was repo `data/db/`).

**Verify:**
```bash
.venv/bin/python -c "
import tempfile
from pathlib import Path
from ch_bulk import ChBulk
from ch_bulk.core.paths import DEFAULT_DB_PATH, DEFAULT_DATA_DIR

# Default behavior unchanged:
ch_default = ChBulk()
assert ch_default.db_path == DEFAULT_DB_PATH, ch_default.db_path
assert ch_default.data_dir == DEFAULT_DATA_DIR

# Custom data_dir → custom db_path (THIS IS THE BEHAVIOR CHANGE):
with tempfile.TemporaryDirectory() as tmp:
    ch_custom = ChBulk(data_dir=tmp)
    expected_db = Path(tmp) / 'db' / 'ch_bulk.duckdb'
    assert ch_custom.db_path == expected_db, (ch_custom.db_path, expected_db)

# Explicit db_path always wins:
ch_explicit = ChBulk(data_dir='/tmp/foo', db_path='/tmp/explicit.duckdb')
assert ch_explicit.db_path == Path('/tmp/explicit.duckdb')

print('ok')
"
.venv/bin/python -m unittest discover -s tests > /tmp/phase4_step6_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase4_step6_tests.txt | tail -2
diff <(grep -E "^(Ran |FAILED|OK$)" /tmp/phase4_baseline_tests.txt | tail -2) <(grep -E "^(Ran |FAILED|OK$)" /tmp/phase4_step6_tests.txt | tail -2)
# If a test breaks because it expected DB at repo path when passing custom data_dir, fix per Hard Rule #1.
```

### Step 7 — Migrate the 17 CLI `--db-path` defaults

In `ch_bulk/cli.py`, every Typer command currently has:
```python
db_path: Path = typer.Option(
    DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
)
```

(Or whatever the current form is — verify with code.)

For the 13 commands that already take `--data-dir`, change pattern to:
```python
db_path: Optional[Path] = typer.Option(
    None, "--db-path", help="DuckDB database path. Defaults to <data-dir>/db/ch_bulk.duckdb."
)
```

Inside each command body, the existing `ch = ChBulk(data_dir=data_dir, db_path=db_path)` already does the right thing — `ChBulk.__init__` resolves `None` → `default_db_path(data_dir)` per Step 6.

There are 17 `--db-path` options in `cli.py`, but they split into two groups:
- 13 commands already take `--data-dir`: switch `--db-path` to `Optional[Path] = None` and let `ChBulk(data_dir=..., db_path=None)` resolve to `default_db_path(data_dir)`.
- 4 pure-DB commands do **not** take `--data-dir`: `query`, `match`, `export-sqlite`, and `info`. Keep them backward-compatible without widening the CLI surface. Use `Optional[Path] = None` for a consistent help shape, but give them a different help string (for example: `"DuckDB database path. Defaults to the repo data/db/ch_bulk.duckdb when omitted."`) and resolve `db_path = db_path or DEFAULT_DB_PATH` in the command body.

Do **not** add `--data-dir` to the pure-DB commands in this phase. The design doc explicitly keeps CLI surface area unchanged.

**Verify:**
```bash
# No remaining static DEFAULT_DB_PATH defaults in typer.Option calls:
grep -nE 'typer\.Option\(\s*DEFAULT_DB_PATH|typer\.Option\(\s*Path\("data' ch_bulk/cli.py
# Expected: zero matches

# Pure-DB commands still pin omitted db_path to repo default in the body:
grep -n 'db_path = db_path or DEFAULT_DB_PATH' ch_bulk/cli.py
# Expected: 4 matches (query, match, export-sqlite, info), or equivalent pure-DB fallback logic if you factored it into a tiny helper.

# CLI still works (surface smoke):
.venv/bin/ch-bulk --help > /tmp/phase4_step7_cli_help.txt 2>&1
# Compare against baseline — expected diff is ONLY in --db-path help/default text
diff /tmp/phase4_baseline_cli_help.txt /tmp/phase4_step7_cli_help.txt | head -20

# Smoke one pure-DB command explicitly; do not rely on a non-existent --data-dir flag:
.venv/bin/ch-bulk info --db-path "$(pwd)/data/db/ch_bulk.duckdb" > /tmp/phase4_step7_info.txt 2>&1 || true
```

### Step 8 — Add regression tests

Create new test file `tests/test_paths.py` (or add to an existing one — `test_bootstrap.py` is a candidate):

```python
"""Regression tests for path helpers — proves producer/consumer continuity."""
import tempfile
import unittest
from pathlib import Path

from ch_bulk.core.paths import (
    ch_input_dir, cqc_input_dir, settings_path, reference_dir,
    revenue_bands_path, default_db_path, db_dir,
)


class PathHelperTests(unittest.TestCase):
    def test_ch_input_dir_matches_chbulk_ch_dir(self):
        from ch_bulk import ChBulk
        with tempfile.TemporaryDirectory() as tmp:
            ch = ChBulk(data_dir=tmp)
            self.assertEqual(ch.ch_dir, ch_input_dir(tmp))

    def test_cqc_input_dir_matches_chbulk_cqc_dir(self):
        from ch_bulk import ChBulk
        with tempfile.TemporaryDirectory() as tmp:
            ch = ChBulk(data_dir=tmp)
            self.assertEqual(ch.cqc_dir, cqc_input_dir(tmp))

    def test_chbulk_db_path_follows_data_dir(self):
        from ch_bulk import ChBulk
        with tempfile.TemporaryDirectory() as tmp:
            ch = ChBulk(data_dir=tmp)
            self.assertEqual(ch.db_path, default_db_path(tmp))

    def test_chbulk_explicit_db_path_overrides(self):
        from ch_bulk import ChBulk
        with tempfile.TemporaryDirectory() as tmp:
            explicit = Path(tmp) / "custom.duckdb"
            ch = ChBulk(data_dir=tmp, db_path=explicit)
            self.assertEqual(ch.db_path, explicit)
```

Also add one behavior-level regression in `tests/test_ch_enricher.py` for the new revenue-band semantics:

```python
def test_enrich_revenue_honors_custom_data_dir_reference_tree(self):
    import shutil
    from pathlib import Path
    from ch_bulk.companies_house.ch_enricher import enrich_revenue
    from ch_bulk.core.paths import DATA_REFERENCE_DIR, revenue_bands_path

    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir) / "custom_data"
        bands_path = revenue_bands_path(data_dir)
        bands_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(DATA_REFERENCE_DIR / "revenue_bands.csv", bands_path)
        # Build the minimal db fixture as in the existing revenue test, then:
        summary = enrich_revenue(db_path, data_dir=data_dir)
        self.assertEqual(summary["estimated"], 1)
```

Keep the existing default-root `load_bands(DATA_REFERENCE_DIR / "revenue_bands.csv")` test. It still documents the bundled default asset. The new test proves the Phase 4 custom-`data_dir` behavior.

Also add a regression test for the CH downloader writer path — either as a small mock-based test in `tests/test_ch_downloader.py` (new file) or inline in `tests/test_paths.py`:

```python
def test_ch_downloader_writes_to_input_ch(self):
    """download_bulk_data must write CSVs into <data_dir>/input/ch/."""
    from unittest.mock import patch
    from pathlib import Path
    import tempfile
    from ch_bulk.companies_house.downloader import download_bulk_data
    from ch_bulk.core.paths import ch_input_dir

    with tempfile.TemporaryDirectory() as tmp:
        # Mock the actual HTTP download but verify the target_dir gets created.
        with patch("ch_bulk.companies_house.downloader._download_file"), \
             patch("ch_bulk.companies_house.downloader._extract_zip", return_value=[]):
            try:
                download_bulk_data(data_dir=tmp, month="2026-05", strict=False)
            except Exception:
                pass  # We don't care about the full pipeline, just the dir creation.
        self.assertTrue(ch_input_dir(tmp).is_dir(),
                        f"download_bulk_data did not create {ch_input_dir(tmp)}")
```

**Verify:**
```bash
.venv/bin/python -m unittest tests.test_paths -v 2>&1 | tail -10
.venv/bin/python -m unittest tests.test_ch_enricher -v 2>&1 | tail -10
# All new tests should pass.
.venv/bin/python -m unittest discover -s tests > /tmp/phase4_step8_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase4_step8_tests.txt | tail -2
# Total tests should be baseline + the new path/revenue regressions, still 0 failures + 7 known errors.
```

### Step 9 — Structural audit

```bash
# All new helpers exist in core/paths.py:
.venv/bin/python -c "
from ch_bulk.core.paths import (
    input_dir, ch_input_dir, cqc_input_dir, settings_path,
    reference_dir, revenue_bands_path, db_dir, default_db_path,
    DEFAULT_DB_PATH, DEFAULT_DATA_DIR, DATA_REFERENCE_DIR,
)
print('ok')
"

# No remaining hardcoded inline path constructions for migrated patterns:
grep -rn 'data_dir / *"input"' ch_bulk/ | grep -v "core/paths.py"
grep -rn 'Path(data_dir) */ *"input"' ch_bulk/ | grep -v "core/paths.py"
grep -rn 'data_dir / *"settings\.json"\|Path(data_dir) */ *"settings\.json"' ch_bulk/ | grep -v "core/paths.py"
grep -rn 'DATA_REFERENCE_DIR */ *"revenue_bands\.csv"' ch_bulk/ | grep -v "core/paths.py"
# Expected: all empty.
```

### Step 10 — Behavioral diff vs baseline

```bash
.venv/bin/python -m unittest discover -s tests > /tmp/phase4_post_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase4_post_tests.txt | tail -2
# Expected: baseline_test_count + new_test_count run, 0 fail, 7 errors (Playwright pre-existing).

# CLI help diff — expected to differ ONLY in --db-path default text:
diff /tmp/phase4_baseline_cli_help.txt <(.venv/bin/ch-bulk --help) | head -30

# CLI info — default DB path same so output should match baseline (modulo row-tie ordering):
diff /tmp/phase4_baseline_info.txt <(.venv/bin/ch-bulk info --db-path "$(pwd)/data/db/ch_bulk.duckdb")

# Top-level imports unchanged:
.venv/bin/python -c "from ch_bulk import ChBulk, SanityCheckError, SanityCheckResult, ensure_pipeline_schema; print('ok')"
```

### Step 11 — GUI smoke (manual)

ASK USER to launch `.venv/bin/ch-bulk ui` and confirm:
- Window opens
- Left rail shows CH / CQC / Settings panes
- No Python tracebacks in terminal
- Settings pane shows the correct settings path (now resolved via `settings_path()`)
- Each pane renders when clicked

Do not proceed to Step 13 until confirmed.

### Step 12 — Final diff review

```bash
git status
git diff --stat pre-phase-4-bookmark --
ls -la ch_bulk/core/paths.py
wc -l ch_bulk/core/paths.py
```

Surface to user:
- `core/paths.py` grows by ~8 helpers (~30 LOC)
- ~35 call sites changed across api.py, settings.py, cli.py, downloaders, ch_enricher, classifier_pipeline, cqc/api_enricher, website_finder, gui.py, salvage
- new regression coverage in `tests/test_paths.py` plus one custom-`data_dir` revenue test in `tests/test_ch_enricher.py`
- CH BUG fixed: download_bulk_data writes to data_dir/input/ch/
- Behavior change: ChBulk(data_dir=X) → DB at X/db/, not repo

### Step 13 — Commit (single, all-in-one)

ASK USER for final go-ahead, then:

```bash
git add -A
git commit -m "$(cat <<'EOF'
Phase 4: Centralize path config + fix CH downloader producer/consumer bug

Adds 8 helpers to ch_bulk/core/paths.py (input_dir, ch_input_dir, cqc_input_dir,
settings_path, reference_dir, revenue_bands_path, db_dir, default_db_path).
Migrates ~35 inline path constructions across api.py, cli.py, settings.py,
downloaders, enrichers, and gui.py to use them.

Fixes:
- CH downloader bug: download_bulk_data wrote CSVs to data_dir/ instead of
  data_dir/input/ch/. Split-phase use (download then later process) broke
  with FileNotFoundError; only sync() worked because it bypassed discovery.
- enrich_revenue(data_dir=...) silently ignored its data_dir argument and
  always read repo-root reference. Now actually uses the argument.

Behavior changes (intentional, all derive from "data_dir is the runtime
data root, period"):
- ChBulk(data_dir=X) without explicit db_path → DB at X/db/ch_bulk.duckdb.
  Previously DB stayed at repo data/db/ regardless of data_dir.
- enrich_revenue(data_dir=X) reads bands from X/reference/revenue_bands.csv.
  Previously read repo regardless.
- CLI --db-path no longer shows a static default. Commands with --data-dir
  resolve to <data-dir>/db/; pure-DB commands keep the same repo-default
  runtime fallback.

Regression tests added: tests/test_paths.py covers ch_input_dir matches
ChBulk.ch_dir, db_path follows data_dir, explicit db_path overrides,
and download_bulk_data writes to the right directory. tests/test_ch_enricher.py
adds one explicit custom-data-dir revenue-bands regression.

CLI surface unchanged. GUI behavior unchanged when defaults used.
Test baseline: 80 tests / 0 fail / 7 errors → ~84 tests / 0 fail / 7 errors.

Decisions documented in docs/plans/2026-05-27-phase-4-design.md.
EOF
)"
git log -1 --stat | head -40
git status
```

**Verify:** Single commit. Working tree clean. Tag `pre-phase-4-bookmark` still exists for rollback.

## Rollback contract

If any verify fails: stop. Revert tracked + remove untracked files. If state is unclear: `git reset --hard pre-phase-4-bookmark && git clean -fd` returns to the Phase 3 end state (`0844883`).

## Failure modes and recovery

| Symptom | Likely cause | Fix |
|---|---|---|
| New or updated revenue test fails after Step 5 because reference CSV not found | Custom `data_dir` test did not stage `revenue_bands.csv` into the temp tree | Per Hard Rule #1: copy the CSV into the test's tmpdir using `revenue_bands_path(tmp)` and `DATA_REFERENCE_DIR`. |
| Test fails because db_path differs after Step 6 | Test instantiates `ChBulk(data_dir=tmp)` and expects repo's db_path | Update the test to pass an explicit `db_path=` OR assert on the new resolved path. |
| Sub-module ImportError | Missed an `from ch_bulk.core.paths import ...` add | Add the import. |
| GUI fails in Step 11 | settings_path migration broke a gui.py path | Read gui.py around 908-911, verify the helper resolves correctly. |
| Whole phase looks broken | — | `git reset --hard pre-phase-4-bookmark && git clean -fd` |

## Handoff

When done, post DONE in the side room with:
- Final commit hash
- Test pass count (expected: ~84 / 0 / 7)
- Discrepancy log if any (Hard Rule #1 finds)
- Tag `pre-phase-4-bookmark` preserved
