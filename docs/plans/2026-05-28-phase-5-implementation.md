# Phase 5 implementation plan

**Companion to:** [`2026-05-28-phase-5-design.md`](./2026-05-28-phase-5-design.md) — rationale + workstreams
**For executor:** read this doc top-to-bottom. The design doc explains *what* and *why*; this doc gives you the *how*.

## Hard rules

1. **Code is the source of truth, not this doc.** If during execution you find the code doesn't match a doc claim (line numbers, additional sites I missed, different signatures), **trust the code, fold the discrepancy in, fix it, continue**. Note discrepancies in your final handoff post.
2. **TWO intermediate commits, NOT one.** Workstream A (classifier fix) lands first as commit `5a`. Workstream B (HSCA/CQC surface) lands second as commit `5b`. **Main session will squash them later** — do not squash yourself. Verify-before-next-step still applies between sub-steps; just commit at the end of each workstream.
3. **No behavior changes outside the two design-approved sets:**
   - 5a: lazy `PlaywrightSession` init in `fallback_worker()`; one mock-target fix in `test_classify_uses_playwright_retry_for_tiny_bodies`.
   - 5b: new CLI commands (`cqc-bulk *`, `hsca-bulk *`); new GUI HSCA action row + HSCA status; new CLI runner tests.
   - Anything else is out of scope — note it and skip.
4. **Verify before next step.** Every step has a `Verify:` block. If any verify fails, stop. Revert tracked edits + remove untracked files before retrying.
5. **Rollback target.** Step 0 creates a `pre-phase-5-bookmark` tag. If anything goes wrong: `git reset --hard pre-phase-5-bookmark`. If you created `tests/test_cli_bulk_groups.py` but never committed it, remove that file explicitly before retrying.
6. **Ask the user for confirmation** at Step 8 (GUI smoke for HSCA buttons) and Step 10 (commit 5b go-ahead). Commit 5a can go through without GUI smoke since it has no GUI surface.
7. **Backend `ChBulk` methods stay as-is.** Do not change signatures, defaults, or behavior of `download_hsca`, `process_hsca`, `cqc_hsca_sync`, `download_cqc`, `process_cqc`, `sync_cqc`. CLI/GUI just expose them.

## Pre-flight checklist (before Step 0)

- [ ] Confirm working tree is clean: `git status` shows "nothing to commit, working tree clean".
- [ ] If the 2 Phase 5 plan docs are still untracked, commit them FIRST as their own small docs commit (same pattern as Phases 1/1.5/2/3/4).
- [ ] Confirm you're on `trunk` branch: `git branch --show-current` prints `trunk`.
- [ ] Confirm the Phase 4 commit is in history: `git merge-base --is-ancestor 96e6998 HEAD && echo ok` prints `ok`.
- [ ] Confirm the bookmark tag does NOT already exist: `git tag --list pre-phase-5-bookmark` prints nothing.
- [ ] Confirm venv + deps: `source .venv/bin/activate && which python && .venv/bin/python -c "import duckdb, httpx, requests, trafilatura"` returns no errors.
- [ ] Confirm Playwright availability: `.venv/bin/python -c "from playwright.sync_api import sync_playwright; print('ok')"` returns ok (the bug we're fixing depends on Playwright BEING importable; if it's not, the fix is moot).
- [ ] Confirm baseline: `.venv/bin/python -m unittest discover -s tests > /tmp/phase5_preflight_tests.txt 2>&1 || true` then `grep -E "^(Ran |FAILED|OK$)" /tmp/phase5_preflight_tests.txt` should show `Ran 85 tests ...` and `FAILED (errors=7)`.
- [ ] Confirm the 7 errors are all in `tests/test_classifier.py`: `grep "^ERROR:" /tmp/phase5_preflight_tests.txt | head -10` — every error should reference `tests.test_classifier`.

## Execution

Phase 5 has TWO sub-phases (5a, 5b). Each sub-phase is its own commit at the time of execution. Main session squashes them later.

---

## Phase 5a — Classifier eager-Playwright fix

### Step 0 — Baseline capture + bookmark tag

```bash
.venv/bin/python -m unittest discover -s tests > /tmp/phase5_baseline_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase5_baseline_tests.txt
.venv/bin/python - <<'PY'
from pathlib import Path
text = Path("/tmp/phase5_baseline_tests.txt").read_text()
print("has_classifier_errors=", "tests.test_classifier" in text and "ERROR" in text)
PY
.venv/bin/ch-bulk --help > /tmp/phase5_baseline_cli_help.txt 2>&1
.venv/bin/python -c "from ch_bulk.web.classifier import WebsiteClassifier; print('ok')" > /tmp/phase5_baseline_classifier_import.txt 2>&1
git tag pre-phase-5-bookmark HEAD
git tag --list pre-phase-5-bookmark
```

**Verify:** baseline files non-empty. `grep -E "^(Ran |FAILED|OK$)" /tmp/phase5_baseline_tests.txt` shows `Ran 85 tests ...` plus `FAILED (errors=7)`. Tag exists.

### Step 1 — Fix `fallback_worker()` in `classifier_pipeline.py`

Open `ch_bulk/web/classifier_pipeline.py`. Find `fallback_worker()` around line 547-633 (verify with code per Hard Rule #1).

**Current shape (approximate):**
```python
def fallback_worker():
    with browser.PlaywrightSession() as session:    # ← eager: opens session immediately
        while True:
            task = fallback_queue.get()
            if task is None:
                break
            # ... process task using session ...
```

**New shape:**
```python
def fallback_worker():
    session_cm = None
    session = None
    session_error = None
    try:
        while True:
            task = fallback_queue.get()
            if task is None:
                fallback_queue.task_done()
                break
            if session is None and session_error is None:
                try:
                    session_cm = browser.PlaywrightSession()
                    session = session_cm.__enter__()
                except Exception as exc:
                    logger.exception("Playwright session unavailable")
                    session_error = exc

            if session_error is not None:
                # Reuse the existing per-task fallback error path:
                # build an _unable_row(...) for this task, append it to
                # fallback_writer, and keep the lane alive for later tasks.
                ...
            else:
                # ... existing per-task processing using session ...
                ...
    finally:
        if session_cm is not None:
            session_cm.__exit__(None, None, None)
```

Adjust the exact shape to fit existing code conventions (e.g. if there's a different error-recording helper, use it). The key invariants:

- The thread blocks on `fallback_queue.get()` BEFORE opening any session.
- Session creation happens only when the first non-None task arrives.
- If session creation fails, do **not** append that exception to `fallback_failures` for the expected "browser unavailable" case; otherwise `raise_fallback_failure()` aborts the whole batch.
- Reuse the existing fallback-lane `_unable_row(...)` shape rather than inventing `_record_fallback_failure` (that helper does not exist in the code today).
- Preserve the current `fallback_writer`, `ready_path`, `load_requested`, logging, and `task_done()` behavior.
- Cleanup on shutdown (session close) still happens.

**Verify (Step 1):**
```bash
# Targeted test suite — only the tiny-bodies test should remain non-green:
.venv/bin/python -m unittest tests.test_classifier -v > /tmp/phase5_step1_classifier.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$|ERROR:|FAIL:)" /tmp/phase5_step1_classifier.txt
grep -n "test_classify_uses_playwright_retry_for_tiny_bodies" /tmp/phase5_step1_classifier.txt | tail -1
# Expected: the only remaining non-green test is test_classify_uses_playwright_retry_for_tiny_bodies

# Full suite count:
.venv/bin/python -m unittest discover -s tests > /tmp/phase5_step1_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$|ERROR:|FAIL:)" /tmp/phase5_step1_tests.txt
# Expected: exactly one remaining non-green test, and it is the tiny-bodies seam test
```

### Step 2 — Fix the wrong-seam mock in `test_classify_uses_playwright_retry_for_tiny_bodies`

Open `tests/test_classifier.py`, find `test_classify_uses_playwright_retry_for_tiny_bodies` (around line 243-310; verify).

Find what the existing passing `test_classify_reuses_playwright_session_for_multiple_handoffs` does: it patches both `browser.PlaywrightSession` and `browser.fetch_rendered`. Copy that pattern into the failing test.

Specifically: the test already patches `browser.fetch_rendered`, but does NOT patch `browser.PlaywrightSession` itself. After Step 1's lazy fix, the test enters fallback for real because it legitimately creates `retry_pages`. Need to also patch `browser.PlaywrightSession` so it doesn't try to launch real Chromium.

**Verify (Step 2):**
```bash
.venv/bin/python -m unittest tests.test_classifier.WebsiteClassifierTests.test_classify_uses_playwright_retry_for_tiny_bodies -v
# Expected: OK

# Full suite:
.venv/bin/python -m unittest discover -s tests > /tmp/phase5_step2_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase5_step2_tests.txt
# Expected: 85 tests / 0 fail / 0 errors  ← THE WIN
```

### Step 3 — Structural audit (5a)

```bash
# No code outside the fix touched:
git diff --stat pre-phase-5-bookmark -- ch_bulk/web/classifier_pipeline.py tests/test_classifier.py
# Expected: 2 files changed (classifier_pipeline.py + test_classifier.py), small line counts

# Behavioral diff vs baseline:
.venv/bin/ch-bulk --help > /tmp/phase5_step3_cli_help.txt 2>&1
diff /tmp/phase5_baseline_cli_help.txt /tmp/phase5_step3_cli_help.txt
# Expected: empty (CLI unchanged)

.venv/bin/python -c "from ch_bulk.web.classifier import WebsiteClassifier; c = WebsiteClassifier(); print('ok')"
# Expected: ok (still instantiates cleanly)
```

### Step 4 — Commit 5a

```bash
git add -A
git commit -m "$(cat <<'EOF'
Phase 5a: Lazy-init Playwright session in classifier fallback worker

WebsiteClassifier.classify() used to eagerly open a browser.PlaywrightSession
at fallback-thread startup whenever Playwright was importable, even if no
FallbackTask would ever arrive. On machines where Chromium launch is denied
(sandboxed, missing browser binaries, locked-down macOS), the entire
classification batch aborted before any non-fallback work could finish.

Fix: fallback_worker() now blocks on fallback_queue.get() first. Session
creation only happens when the first real FallbackTask arrives. Session is
reused for subsequent fallback tasks. If creation fails when a real task
arrives, failure is contained to the fallback lane — main batch continues.

Also fix test_classify_uses_playwright_retry_for_tiny_bodies to patch
browser.PlaywrightSession (the correct seam), mirroring the existing
test_classify_reuses_playwright_session_for_multiple_handoffs. Previously
it only patched browser.fetch_rendered and would still launch real Chromium.

Test baseline: 85 tests / 0 fail / 7 errors → 85 tests / 0 fail / 0 errors.
EOF
)"
git log -1 --stat | head -15
```

**Verify (5a done):**
- Working tree clean
- Last commit is 5a fix
- Tests fully green at 85/0/0

---

## Phase 5b — HSCA + CQC bulk CLI/GUI surface exposure

### Step 5 — Add CLI subgroups in `cli.py`

In `ch_bulk/cli.py`, add two new Typer subgroups using the existing `cqc_enrich_app` pattern as a template.

At the top with other subgroup declarations:
```python
cqc_bulk_app = typer.Typer(help="CQC bulk directory commands.")
hsca_bulk_app = typer.Typer(help="HSCA active locations bulk commands.")
app.add_typer(cqc_bulk_app, name="cqc-bulk")
app.add_typer(hsca_bulk_app, name="hsca-bulk")
```

For each subgroup, add 3 commands (download / process / sync) using the existing CLI option conventions from `cli.py`.

- `download` commands should mirror the current top-level CH `download` command: `--data-dir`, plus `--target-date` for HSCA, but no `--db-path`.
- `process` and `sync` commands should take `--data-dir`, optional `--db-path`, and `--force` so the CLI can reach the same sanity-override path the GUI already exposes.

CQC bulk:
```python
@cqc_bulk_app.command("download")
def cqc_bulk_download(
    data_dir: Path = typer.Option(...),
) -> None:
    """Download the latest CQC care directory CSV."""
    ch = ChBulk(data_dir=data_dir)
    output_path = ch.download_cqc()
    console.print(f"[bold green]CQC directory downloaded:[/] {output_path}")

@cqc_bulk_app.command("process")
def cqc_bulk_process(
    data_dir: Path = typer.Option(...),
    db_path: Optional[Path] = typer.Option(None, ...),
    force: bool = typer.Option(False, "--force", help="Proceed even if sanity checks fail."),
) -> None:
    """Process the latest downloaded CQC directory CSV into DuckDB."""
    ch = ChBulk(data_dir=data_dir, db_path=db_path)
    n_locations = ch.process_cqc(force=force)
    console.print(f"[bold green]CQC processed:[/] {n_locations:,} locations")

@cqc_bulk_app.command("sync")
def cqc_bulk_sync(
    data_dir: Path = typer.Option(...),
    db_path: Optional[Path] = typer.Option(None, ...),
    force: bool = typer.Option(False, "--force", help="Proceed even if sanity checks fail."),
) -> None:
    """Download and process the latest CQC directory in one step."""
    ch = ChBulk(data_dir=data_dir, db_path=db_path)
    n_locations = ch.sync_cqc(force=force)
    console.print(f"[bold green]CQC synced:[/] {n_locations:,} locations")
```

HSCA bulk (mirror with `--target-date` optional argument on download/sync, and `--force` on process/sync):
```python
@hsca_bulk_app.command("download")
def hsca_bulk_download(
    data_dir: Path = typer.Option(...),
    target_date: Optional[str] = typer.Option(None, "--target-date", help="Date string for ODS filename selection."),
) -> None:
    """Download the latest HSCA active locations ODS."""
    ch = ChBulk(data_dir=data_dir)
    output_path = ch.download_hsca(target_date=target_date)
    console.print(f"[bold green]HSCA downloaded:[/] {output_path}")

@hsca_bulk_app.command("process")
def hsca_bulk_process(
    data_dir: Path = typer.Option(...),
    db_path: Optional[Path] = typer.Option(None, ...),
    force: bool = typer.Option(False, "--force", help="Proceed even if sanity checks fail."),
) -> None:
    """Process the latest downloaded HSCA ODS into DuckDB."""
    ch = ChBulk(data_dir=data_dir, db_path=db_path)
    n_locations = ch.process_hsca(force=force)
    console.print(f"[bold green]HSCA processed:[/] {n_locations:,} locations")

@hsca_bulk_app.command("sync")
def hsca_bulk_sync(
    data_dir: Path = typer.Option(...),
    db_path: Optional[Path] = typer.Option(None, ...),
    target_date: Optional[str] = typer.Option(None, "--target-date", help="Date string for ODS filename selection."),
    force: bool = typer.Option(False, "--force", help="Proceed even if sanity checks fail."),
) -> None:
    """Download and process the latest HSCA ODS in one step."""
    ch = ChBulk(data_dir=data_dir, db_path=db_path)
    n_locations = ch.cqc_hsca_sync(target_date=target_date, force=force)
    console.print(f"[bold green]HSCA synced:[/] {n_locations:,} locations")
```

Return-value truth from `api.py`: all four process/sync methods above currently return a single integer location count, not tuples.

**Verify (Step 5):**
```bash
.venv/bin/ch-bulk --help | grep -E "cqc-bulk|hsca-bulk"
# Expected: 2 new subgroup entries

.venv/bin/ch-bulk cqc-bulk --help | head -15
.venv/bin/ch-bulk hsca-bulk --help | head -15
# Expected: 3 commands each (download/process/sync)

.venv/bin/ch-bulk cqc-bulk download --help | grep -E "data-dir"
.venv/bin/ch-bulk cqc-bulk process --help | grep -E "db-path|force"
.venv/bin/ch-bulk hsca-bulk sync --help | grep -E "target-date|force"
# Expected: options visible and aligned with actual signatures

.venv/bin/python -m unittest discover -s tests > /tmp/phase5_step5_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase5_step5_tests.txt
# Expected: 85 / 0 / 0 (no new failures)
```

### Step 6 — Add CLI runner tests

Create `tests/test_cli_bulk_groups.py` (or add to existing `tests/test_cqc_hsca.py`):

```python
"""Smoke tests for cqc-bulk and hsca-bulk CLI subgroups."""
import unittest
from unittest.mock import patch
from typer.testing import CliRunner

from ch_bulk.cli import app


class CqcBulkCliTests(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_cqc_bulk_group_listed_in_top_help(self):
        result = self.runner.invoke(app, ["--help"])
        self.assertIn("cqc-bulk", result.stdout)

    def test_hsca_bulk_group_listed_in_top_help(self):
        result = self.runner.invoke(app, ["--help"])
        self.assertIn("hsca-bulk", result.stdout)

    def test_cqc_bulk_subcommands_present(self):
        result = self.runner.invoke(app, ["cqc-bulk", "--help"])
        for cmd in ["download", "process", "sync"]:
            self.assertIn(cmd, result.stdout)

    def test_hsca_bulk_subcommands_present(self):
        result = self.runner.invoke(app, ["hsca-bulk", "--help"])
        for cmd in ["download", "process", "sync"]:
            self.assertIn(cmd, result.stdout)

    def test_cqc_bulk_download_invokes_chbulk(self):
        with patch("ch_bulk.cli.ChBulk") as mock_ch:
            mock_ch.return_value.download_cqc.return_value = "/tmp/dummy.csv"
            result = self.runner.invoke(app, ["cqc-bulk", "download", "--data-dir", "/tmp"])
            self.assertEqual(result.exit_code, 0, msg=result.stdout)
            mock_ch.return_value.download_cqc.assert_called_once()

    def test_hsca_bulk_download_passes_target_date(self):
        with patch("ch_bulk.cli.ChBulk") as mock_ch:
            mock_ch.return_value.download_hsca.return_value = "/tmp/dummy.ods"
            result = self.runner.invoke(app, ["hsca-bulk", "download", "--data-dir", "/tmp", "--target-date", "2026-05-01"])
            self.assertEqual(result.exit_code, 0, msg=result.stdout)
            mock_ch.return_value.download_hsca.assert_called_once()
            _, kwargs = mock_ch.return_value.download_hsca.call_args
            self.assertEqual(kwargs.get("target_date"), "2026-05-01")
```

That patch target is correct because `cli.py` does `from ch_bulk.api import ChBulk`, so the imported symbol lives at `ch_bulk.cli.ChBulk`.

**Verify (Step 6):**
```bash
.venv/bin/python -m unittest tests.test_cli_bulk_groups -v 2>&1 | tail -10
# Expected: 6 passed

.venv/bin/python -m unittest discover -s tests > /tmp/phase5_step6_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase5_step6_tests.txt
# Expected: 91 / 0 / 0 (added 6 tests)
```

### Step 7 — Add HSCA action row + status to GUI CQC pane

In `ch_bulk/gui.py`, find the CQC pane database card around line 472-489 (the row with `Sync / Download / Process` buttons).

Current shape (approximate):
```python
# In CQCPane._build() or similar
ttk.Button(action_row, text="Sync", command=self._on_sync).pack(...)
ttk.Button(action_row, text="Download", command=self._on_download).pack(...)
ttk.Button(action_row, text="Process", command=self._on_process).pack(...)
```

Restructure to add a pure status row plus new CQC / HSCA action rows:
```python
# existing status row stays first

# existing CQC row, now labeled
cqc_row = ttk.Frame(self.status_frame)
cqc_row.pack(fill="x", padx=8, pady=(0, 4))
ttk.Label(cqc_row, text="CQC:").pack(side="left", padx=(0, 8))
ttk.Button(cqc_row, text="Sync", command=self._on_sync).pack(side="left", padx=2)
ttk.Button(cqc_row, text="Download", command=self._on_download).pack(side="left", padx=2)
ttk.Button(cqc_row, text="Process", command=self._on_process).pack(side="left", padx=2)

# NEW: HSCA action row
hsca_row = ttk.Frame(self.status_frame)
hsca_row.pack(fill="x", padx=8, pady=(0, 6))
ttk.Label(hsca_row, text="HSCA:").pack(side="left", padx=(0, 8))
ttk.Button(hsca_row, text="Sync", command=self._on_hsca_sync).pack(side="left", padx=2)
ttk.Button(hsca_row, text="Download", command=self._on_hsca_download).pack(side="left", padx=2)
ttk.Button(hsca_row, text="Process", command=self._on_hsca_process).pack(side="left", padx=2)
```

Do **not** rename the existing CQC handlers unless the code gives you a compelling reason. There are no test references to the old handler names, but renaming them buys nothing. Minimal-churn path: keep `_on_sync/_on_download/_on_process` for CQC, add `_on_hsca_sync/_on_hsca_download/_on_hsca_process` beside them, and add `self.btn_hsca_*` widgets to `action_buttons()`.

Add 3 new handler methods that mirror the existing CQC ones but call `ch.download_hsca`, `ch.process_hsca`, `ch.cqc_hsca_sync` instead.

Update `CQCPane.refresh()` status text to also include HSCA row count. Around the existing status that shows `cqc_locations` / `cqc_providers` counts:

```python
con = duckdb.connect(str(self.ch.db_path), read_only=True)
try:
    loc = ...
    prov = ...
    try:
        hsca_count = con.execute("SELECT COUNT(*) FROM cqc_hsca_locations").fetchone()[0]
    except duckdb.CatalogException:
        hsca_count = 0
finally:
    con.close()

self.status_detail.configure(text=f"{active:,} active | HSCA {hsca_count:,}")
```

**Verify (Step 7):**
```bash
.venv/bin/python -m unittest discover -s tests > /tmp/phase5_step7_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase5_step7_tests.txt
# Expected: 91 / 0 / 0 (GUI changes don't break tests)

# Smoke import:
.venv/bin/python -c "from ch_bulk.gui import ChBulkApp; print('ok')"
# Expected: ok
```

### Step 8 — Manual GUI smoke (HSCA buttons + status)

ASK USER to launch `.venv/bin/ch-bulk ui` and confirm:
- Window opens, no tracebacks
- Click "CQC" in left rail
- See TWO action rows in the database card: "CQC: Sync / Download / Process" and "HSCA: Sync / Download / Process"
- Status text shows HSCA row count (e.g. "HSCA locations: 56,815" or "HSCA locations: 0" depending on DB state)
- DO NOT click the HSCA buttons (would trigger real download); just confirm visibility
- Click each existing pane (CH, CQC, Settings) — all render

Reply **"GUI ok"** before proceeding to commit.

### Step 9 — Structural + behavioral audit (5b)

```bash
# CLI surface changes only as expected:
.venv/bin/ch-bulk --help > /tmp/phase5_step9_cli_help.txt 2>&1
diff /tmp/phase5_baseline_cli_help.txt /tmp/phase5_step9_cli_help.txt | head -30
# Expected: ADDED entries for cqc-bulk and hsca-bulk subgroups, nothing else

# Full diff scope:
git diff --stat pre-phase-5-bookmark -- ch_bulk/cli.py ch_bulk/gui.py tests/test_cli_bulk_groups.py
# Expected: only those 3 Phase 5b files appear in this path-scoped diff

# Full tests:
.venv/bin/python -m unittest discover -s tests > /tmp/phase5_step9_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase5_step9_tests.txt
# Expected: 91 / 0 / 0
```

### Step 10 — Commit 5b

ASK USER for final commit go-ahead, then:

```bash
git add -A
git commit -m "$(cat <<'EOF'
Phase 5b: Expose CQC + HSCA bulk via CLI and GUI

Backend support has existed since prior phases (ChBulk.download_cqc /
process_cqc / sync_cqc / download_hsca / process_hsca / cqc_hsca_sync)
but was reachable only via the Python API. CLI had nothing; GUI only
exposed CQC.

Adds two new CLI subgroups (mirror cqc-enrich / ch-enrich style):
  ch-bulk cqc-bulk {download,process,sync}
  ch-bulk hsca-bulk {download,process,sync} [--target-date]

Adds HSCA action row to the CQC pane in the GUI:
  CQC:  Sync / Download / Process    (existing)
  HSCA: Sync / Download / Process    (new)

CQC pane status now includes HSCA location row count so the new
buttons have visible feedback.

Internal cqc_hsca_sync API name preserved (misleading but stable);
CLI surface labels it correctly as 'hsca-bulk sync'.

6 new tests in tests/test_cli_bulk_groups.py cover help text and
ChBulk method invocation per subgroup. No backend API changes.

Test baseline: 85 / 0 / 0 → 91 / 0 / 0.
EOF
)"
git log -1 --stat | head -15
```

**Verify (5b done):**
- Working tree clean
- Last 2 commits: 5b on top, 5a underneath, both ahead of `pre-phase-5-bookmark`

---

## Phase 5 wrap-up — main session squash

After codex posts DONE for 5b, main session (claude-nitin) will:

```bash
# Preferred non-interactive squash:
git reset --soft pre-phase-5-bookmark
git commit -m "Phase 5: Classifier lazy-Playwright fix + expose CQC/HSCA bulk via CLI/GUI"
```

Codex does NOT do the squash. Codex's job ends at "5b committed and verified."

## Failure modes and recovery

| Symptom | Likely cause | Fix |
|---|---|---|
| After Step 1, more than 1 test is still non-green | The lazy-init shape doesn't match the actual fallback_worker structure | Re-read the real queue/session flow in `classifier_pipeline.py`; preserve `task_done`, writer finalization, and don't route expected session-init failure through `fallback_failures` |
| After Step 2, the tiny-bodies test still tries to launch Chromium | Mock target still wrong | Compare exactly with `test_classify_reuses_playwright_session_for_multiple_handoffs` — patch `browser.PlaywrightSession` as well as `browser.fetch_rendered` |
| ChBulk method signatures differ from doc | Code drift / this doc was off | Read actual signatures, adjust the CLI commands per Hard Rule #1 |
| HSCA status query fails on first-run state | `cqc_hsca_locations` table doesn't exist | Use a narrow inner `duckdb.CatalogException` catch and still close the connection in `finally` |
| New HSCA buttons stay enabled during background tasks | Forgot to expose them via `action_buttons()` | Add `self.btn_hsca_sync`, `self.btn_hsca_download`, and `self.btn_hsca_process` to `CQCPane.action_buttons()` |
| Whole phase looks broken | — | `git reset --hard pre-phase-5-bookmark`; remove `tests/test_cli_bulk_groups.py` if it still exists untracked |

## Handoff (after 5b commit)

Post DONE in this side room with:
- Both commit hashes (5a + 5b)
- Test pass count (expected: 91 / 0 / 0)
- Brief discrepancy log if any
- Tag `pre-phase-5-bookmark` preserved
- Note: main session will squash 5a+5b before push
