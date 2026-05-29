# Phase 6 implementation plan

**Companion to:** [`2026-05-29-phase-6-design.md`](./2026-05-29-phase-6-design.md) — rationale + workstream breakdown
**For executor:** read this doc top-to-bottom. The design doc explains *what* and *why*; this doc gives you the *how*.

## Hard rules

1. **Code is the source of truth, not this doc.** If during execution you find the code doesn't match a doc claim (line numbers, method names, internal helper shapes), **trust the code, fold the discrepancy in, fix it, continue**. Note discrepancies in your final handoff post.
2. **Staged subcommits, then squash.** Workstreams A–E land as 5 separate intermediate commits during execution. **Main session squashes them into one Phase 6 commit before push.** Do NOT squash yourself.
3. **Optional kwarg only — never change existing signatures.** All `cancel_event` parameters are keyword-only with default `None`. Existing callers, tests, CLI all work unchanged.
4. **Verify before next step.** Every step has a `Verify:` block. If any verify fails, stop. Revert tracked + remove untracked files before retrying.
5. **Rollback target.** Step 0 creates a `pre-phase-6-bookmark` tag. If anything goes wrong: `git reset --hard pre-phase-6-bookmark`.
6. **Ask the user for confirmation** at Step 11 (manual quit-during-task smoke), Step 12 (manual cancel-during-task smoke), and Step 14 (final commit go-ahead for the last subcommit; main session squashes).
7. **No GUI framework change.** Tkinter stays. Don't introduce PySide6 / Streamlit / etc.

## Pre-flight checklist (before Step 0)

- [ ] Confirm working tree is clean: `git status` shows "nothing to commit, working tree clean".
- [ ] If the 2 Phase 6 plan docs are still untracked, commit them FIRST as their own small docs commit (same pattern as Phases 1/1.5/2/3/4/5).
- [ ] Confirm you're on `trunk` branch: `git branch --show-current` prints `trunk`.
- [ ] Confirm the GUI-fixes commit is in history: `git merge-base --is-ancestor 1181d0c HEAD && echo ok` prints `ok`.
- [ ] Confirm the bookmark tag does NOT already exist: `git tag --list pre-phase-6-bookmark` prints nothing.
- [ ] Confirm venv + deps: `source .venv/bin/activate && which python && .venv/bin/python -c "import duckdb, httpx, requests, trafilatura, playwright" 2>&1` returns no errors.
- [ ] Confirm baseline: `.venv/bin/python -m unittest discover -s tests > /tmp/phase6_preflight_tests.txt 2>&1 || true` then `grep -E "^(Ran |FAILED|OK$)" /tmp/phase6_preflight_tests.txt | tail -2` shows `91 tests / 0 fail / 0 errors`.
- [ ] Confirm `tests/test_rate_limit.py` exists — Workstream C must keep its existing `ch_bulk.core.rate_limit.time.sleep` patch passing after the throttle changes.

## Execution

Phase 6 has 5 staged sub-commits (A → B → C → D → E) followed by tests (F). Main session squashes all 6 into one Phase 6 commit before push.

---

## Subcommit A — GUI task registry + non-daemon threads + safe-quit dialog

### Step 0 — Baseline + bookmark

```bash
.venv/bin/python -m unittest discover -s tests > /tmp/phase6_baseline_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase6_baseline_tests.txt | tail -2
.venv/bin/ch-bulk --help > /tmp/phase6_baseline_cli_help.txt 2>&1
git tag pre-phase-6-bookmark HEAD
git tag --list pre-phase-6-bookmark
```

**Verify:** baseline files non-empty. Tag exists. Baseline = 91/0/0.

### Step 1 — Add `ActiveTask` registry to `ChBulkApp`

In `ch_bulk/gui.py`, in `ChBulkApp.__init__`:

```python
import uuid
from dataclasses import dataclass, field
from threading import Event, Thread, Lock
from typing import Optional

@dataclass
class ActiveTask:
    task_id: str
    label: str
    thread: Thread
    cancel_event: Optional[Event]
    can_cancel: bool
    started_monotonic: float
    last_message: str = ""
    phase: Optional[str] = None

# In __init__:
self._active_tasks: dict[str, ActiveTask] = {}
self._registry_lock = Lock()
```

Add helper methods:

```python
def _register_task(
    self,
    target,
    *,
    label: str,
    can_cancel: bool,
    cancel_event: Optional[Event] = None,
) -> tuple[str, Event | None]:
    """Create + register a non-daemon worker thread; return (task_id, cancel_event)."""
    task_id = str(uuid.uuid4())
    if can_cancel and cancel_event is None:
        cancel_event = Event()

    def _wrapped() -> None:
        try:
            # _register_task always passes one positional cancel_event arg.
            # Every wrapped worker closure must therefore accept
            # `cancel_event: Event | None = None`, even when can_cancel=False.
            target(cancel_event)
        finally:
            with self._registry_lock:
                self._active_tasks.pop(task_id, None)

    thread = Thread(target=_wrapped, daemon=False, name=f"task-{task_id[:8]}")
    task = ActiveTask(
        task_id=task_id,
        label=label,
        thread=thread,
        cancel_event=cancel_event,
        can_cancel=can_cancel,
        started_monotonic=time.monotonic(),
        last_message=label,
    )
    with self._registry_lock:
        self._active_tasks[task_id] = task
    thread.start()
    return task_id, cancel_event

def _active_task_snapshot(self) -> list[ActiveTask]:
    with self._registry_lock:
        return [t for t in self._active_tasks.values() if t.thread.is_alive()]
```

**Verify:**
```bash
.venv/bin/python -c "from ch_bulk.gui import ChBulkApp, ActiveTask; print('ok')"
.venv/bin/python -m unittest discover -s tests > /tmp/phase6_step1_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase6_step1_tests.txt | tail -2  # 91/0/0
```

### Step 2 — Migrate the 6 thread spawn sites to use `_register_task`

Per codex's sweep, the 6 call sites are:
- `gui.py:348-366` — `CHPane._run_query()` — read-only query, `can_cancel=False` (UX: too short to bother)
- `gui.py:378-397` — `CHPane._on_export()` — read-only export, `can_cancel=True` (writes file)
- `gui.py:759-781` — `CQCPane._run_query()` — read-only query, `can_cancel=False`
- `gui.py:796-818` — `CQCPane._on_export()` — read-only export, `can_cancel=True`
- `gui.py:1167-1179` — `ChBulkApp._run_task()` — generic worker, `can_cancel=True`
- `gui.py:1199-1224` — `ChBulkApp._dispatch_with_sanity()` — same with force=True, `can_cancel=True`

For each, replace the `threading.Thread(target=worker, daemon=True).start()` pattern. Every local worker closure that goes through `_register_task(...)` must adopt the uniform signature `def worker(cancel_event: Event | None = None) -> None`; for `can_cancel=False` sites the wrapper still passes `None`, so the call shape stays consistent without special-case introspection.

**Verify:**
```bash
if grep -n "daemon=True" ch_bulk/gui.py; then echo "unexpected daemon threads remain" && exit 1; else echo "ok"; fi
grep -rn "_register_task" ch_bulk/gui.py | wc -l  # at least 6 invocations + 1 def
.venv/bin/python -m unittest discover -s tests > /tmp/phase6_step2_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase6_step2_tests.txt | tail -2  # 91/0/0
```

### Step 3 — Rewrite `_on_close` with 3-button dialog

Replace current `_on_close`:

```python
def _on_close(self) -> None:
    active = self._active_task_snapshot()
    if not active:
        self.root.destroy()
        return

    # Build dialog text describing active tasks
    cancellable_count = sum(1 for t in active if t.can_cancel)
    uncancellable_count = len(active) - cancellable_count
    lines = [f"{len(active)} background task(s) running:"]
    for t in active:
        cancel_note = "" if t.can_cancel else " (cannot be cancelled)"
        lines.append(f"  • {t.label}{cancel_note}")
    if uncancellable_count > 0:
        lines.append("")
        lines.append("Uncancellable tasks (DB writes, compaction) will continue until current DB phase finishes.")
    lines.append("")
    lines.append("Choose: Wait for safe stop, Force quit, or Cancel?")

    # messagebox.askyesnocancel exists, but its labels are fixed to
    # Yes/No/Cancel. We want explicit Wait/Force quit/Cancel wording.
    result = _three_button_dialog(
        self.root,
        title="Background task running",
        message="\n".join(lines),
        buttons=("Wait for safe stop", "Force quit", "Cancel"),
    )
    if result is None or result == "Cancel":
        return
    if result == "Force quit":
        import os
        self.root.destroy()
        os._exit(0)
        return

    # "Wait for safe stop"
    for t in active:
        if t.cancel_event is not None:
            t.cancel_event.set()
    self._set_status(f"Stopping {len(active)} task(s); waiting for safe stop...")

    # Poll every 250ms up to 10s
    self._poll_shutdown(active, deadline_seconds=10.0)

def _poll_shutdown(self, active: list[ActiveTask], deadline_seconds: float) -> None:
    deadline = time.monotonic() + deadline_seconds
    def _tick() -> None:
        alive = [t for t in active if t.thread.is_alive()]
        if not alive:
            self.root.destroy()
            return
        if time.monotonic() >= deadline:
            # Show escalation dialog
            uncancellable_remaining = [t for t in alive if not t.can_cancel]
            escalation_msg = f"{len(alive)} task(s) still running after 10s."
            if uncancellable_remaining:
                escalation_msg += f" ({len(uncancellable_remaining)} cannot be interrupted.)"
            escalation_msg += "\n\nKeep waiting, Force quit, or Cancel?"
            choice = _three_button_dialog(
                self.root,
                title="Shutdown timeout",
                message=escalation_msg,
                buttons=("Keep waiting", "Force quit", "Cancel"),
            )
            if choice == "Force quit":
                import os
                self.root.destroy()
                os._exit(0)
                return
            if choice == "Keep waiting":
                self._poll_shutdown(active, deadline_seconds=10.0)
                return
            # Cancel: stay open
            return
        self.root.after(250, _tick)
    _tick()
```

Add `_three_button_dialog` helper at module level (use `tk.Toplevel` + 3 ttk.Buttons; return string of clicked button or None). Do not use `tk.simpledialog` here; it is an input prompt API, not a general action-choice dialog.

**Verify:**
```bash
.venv/bin/python -c "from ch_bulk.gui import ChBulkApp; print('ok')"
.venv/bin/python -m unittest discover -s tests > /tmp/phase6_step3_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase6_step3_tests.txt | tail -2  # 91/0/0
```

### Step 4 — Commit Subcommit A

```bash
git add -A
git commit -m "Phase 6a: GUI task registry + non-daemon threads + safe-quit dialog

Replaces 6 daemon-thread spawn sites with a tracked, non-daemon worker
that registers itself in ChBulkApp._active_tasks. _on_close now offers a
3-button dialog (Wait for safe stop / Force quit / Cancel) and polls
active threads with a 10s deadline before escalating.

Lays groundwork for cooperative cancellation in subsequent subcommits;
this commit alone removes the daemon-thread-dies-mid-DuckDB segfault
class because non-daemon threads block process exit cleanly."
```

---

## Subcommit B — Cancellation primitive

### Step 5 — Create `ch_bulk/core/cancellation.py`

```python
"""Cooperative cancellation primitives for long-running pipelines.

Pipelines accept an optional `cancel_event: threading.Event | None` and
check it at natural boundaries via `raise_if_cancelled(cancel_event)`.
Sleeps and rate-limit waits use `cancellable_sleep(cancel_event, secs)`
so cancellation can fire during throttle backoffs without waiting the
full sleep interval.
"""
from __future__ import annotations

import time
from collections.abc import Callable
from threading import Event


class OperationCancelled(RuntimeError):
    """Raised when a cooperative pipeline detects cancellation at a checkpoint."""


def is_cancelled(cancel_event: Event | None) -> bool:
    return cancel_event is not None and cancel_event.is_set()


def raise_if_cancelled(
    cancel_event: Event | None,
    *,
    reason: str = "operation cancelled",
) -> None:
    if is_cancelled(cancel_event):
        raise OperationCancelled(reason)


def cancellable_sleep(
    cancel_event: Event | None,
    seconds: float,
    *,
    reason: str = "operation cancelled",
    sleep_fn: Callable[[float], None] = time.sleep,
) -> None:
    """Sleep up to `seconds`, raising if `cancel_event` fires.

    `sleep_fn` exists so callers can pass their module-local time.sleep;
    that preserves tests such as tests/test_rate_limit.py which patch
    ch_bulk.core.rate_limit.time.sleep rather than patching this module.
    """
    seconds = max(seconds, 0.0)
    if cancel_event is None:
        sleep_fn(seconds)
        return
    if cancel_event.wait(timeout=seconds):
        raise OperationCancelled(reason)
```

**Verify:**
```bash
.venv/bin/python -c "
from threading import Event
from ch_bulk.core.cancellation import (
    OperationCancelled, is_cancelled, raise_if_cancelled, cancellable_sleep,
)
e = Event()
assert not is_cancelled(None)
assert not is_cancelled(e)
e.set()
assert is_cancelled(e)
try:
    raise_if_cancelled(e)
    raise AssertionError('should have raised')
except OperationCancelled:
    pass
# cancellable_sleep raises immediately when event already set
import time
t0 = time.monotonic()
try:
    cancellable_sleep(e, 5.0)
    raise AssertionError('should have raised')
except OperationCancelled:
    assert time.monotonic() - t0 < 0.5, 'should have raised immediately'
print('ok')
"
.venv/bin/python -m unittest discover -s tests > /tmp/phase6_step5_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase6_step5_tests.txt | tail -2  # 91/0/0
```

### Step 6 — Commit Subcommit B

```bash
git add -A
git commit -m "Phase 6b: Add ch_bulk/core/cancellation.py primitive

OperationCancelled exception + is_cancelled / raise_if_cancelled /
cancellable_sleep helpers. Pipelines accept optional cancel_event kwarg
(default None). cancellable_sleep preserves module-local time.sleep
mocks via an injected sleep_fn and raises OperationCancelled when a
cancel request fires during the wait."
```

---

## Subcommit C — Cancellable sleeps in rate-limit + HTTP retry helpers

### Step 7 — Update `core/rate_limit.SlidingWindowThrottle.wait()`

In `ch_bulk/core/rate_limit.py`, change `wait()` signature to accept optional `cancel_event` and use `cancellable_sleep`:

```python
def wait(self, *, cancel_event: Event | None = None) -> None:
    # ... existing throttle logic ...
    if sleep_for > 0:
        cancellable_sleep(
            cancel_event,
            sleep_for,
            reason="throttle wait cancelled",
            sleep_fn=time.sleep,
        )
    # ... existing post-sleep logic ...
```

Add `from ch_bulk.core.cancellation import cancellable_sleep` at top.

### Step 8 — Update HTTP client retry sites

Sites per codex's sweep:
- `CompaniesHouseClient._get()` — in `ch_bulk/companies_house/ch_enricher.py` or equivalent
- `CompaniesHouseFinancialsClient._get()` — in `ch_bulk/companies_house/financials_fetch.py`
- `CompaniesHouseFinancialsClient` document retry helper
- `CQCAPIClient._get()` — in `ch_bulk/cqc/api_client.py`
- `WebsiteFinder.find()` — inter-company pause in `ch_bulk/web/website_finder.py`

For each: thread `cancel_event` through the method signature and replace `time.sleep(backoff)` with `cancellable_sleep(cancel_event, backoff, reason=..., sleep_fn=time.sleep)`. Do **not** invent a new `self._cancel_event` instance attribute unless the class already owns task lifecycle state; the current CH/CQC API clients are short-lived per-call helpers, so method-param threading is the lower-churn fit.

**Verify (Step 7+8):**
```bash
if grep -n "time.sleep(" ch_bulk/core/rate_limit.py ch_bulk/companies_house/ch_enricher.py ch_bulk/companies_house/financials_fetch.py ch_bulk/cqc/api_client.py ch_bulk/web/website_finder.py; then echo "unexpected retry/throttle sleeps remain" && exit 1; else echo "ok"; fi
.venv/bin/python -m unittest tests.test_rate_limit -v 2>&1 | tail -5
.venv/bin/python -m unittest discover -s tests > /tmp/phase6_step8_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase6_step8_tests.txt | tail -2  # 91/0/0
```

### Step 9 — Commit Subcommit C

```bash
git add -A
git commit -m "Phase 6c: Cancellable rate-limit + HTTP retry sleeps

SlidingWindowThrottle.wait() now accepts optional cancel_event and uses
cancellable_sleep instead of time.sleep, so a cancellation request fires
within the throttle wait rather than blocking up to the full window.

Same pattern applied to retry backoffs in CompaniesHouseClient,
CompaniesHouseFinancialsClient, CQCAPIClient, and WebsiteFinder's
inter-company pause."
```

---

## Subcommit D — Per-pipeline cancellation hooks

### Step 10 — financials_pipeline + classifier_pipeline + 3 simpler pipelines + bulk processors

Apply per codex's priority order. For each pipeline:

1. **`enrich_financials(..., *, cancel_event=None)`** in `financials_pipeline.py`. Bridge the external GUI `cancel_event` into the existing internal `shutdown_event` / `request_shutdown(...)` flow. Stop feeding queue, stop assigning new parser work, drain in-flight responses, then send `None` sentinels to parser subprocesses. Do **not** add a second cross-process cancel channel unless the code proves you need one.

2. **`WebsiteClassifier.classify(..., *, cancel_event=None)`**. Per-company check + drain Playwright fallback queue + send sentinels to fallback thread.

3. **`WebsiteFinder.find(..., *, cancel_event=None)`** in `website_finder.py`. Per-company `raise_if_cancelled` in main loop + `cancellable_sleep` for inter-company pause.

4. **`CQCAPIEnricher.enrich_*(..., *, cancel_event=None)`** in `cqc/api_enricher.py`. This pipeline is sequential today. Add `raise_if_cancelled(...)` at per-entity boundaries in the main loop + cancellable throttle/retry waits.

5. **`enrich_directors(..., *, cancel_event=None)`** in `companies_house/ch_enricher.py`. This pipeline is also sequential today. Add per-company `raise_if_cancelled(...)` in the main loop + cancellable throttle/retry waits.

6. **`process_csvs(..., *, cancel_event=None)`** in `companies_house/processor.py`. Phase-boundary `raise_if_cancelled` calls between validate-files → ingest_to_staging → sanity_checks → bootstrap/upsert → create_indexes → compact_database. There is no download phase inside `process_csvs(...)`; download already happened before this function is called.

7. **`process_cqc_csv(..., *, cancel_event=None)`** + **`process_hsca_filters(..., *, cancel_event=None)`** in `cqc/processor.py`. Phase-boundary checks only. For CQC bulk the boundaries are validate-file → ingest → sanity → bootstrap/upsert → rollup_providers → indexes → compact. For HSCA the boundaries are validate-file → parse workbook → build Python rows → ingest staging → sanity → upsert → finish batch / compact.

For each, when cancel fires:
- After current DB phase completes, raise `OperationCancelled`.
- Skip subsequent phases (e.g. if cancelled after upsert but before compact, skip compact).
- Where the pipeline owns a `cqc_sync_batches` / `classification_batches` row, mark it `'cancelled'` instead of leaving it `'running'`. CH bulk `process_csvs(...)` does not own a sync-batch row today.

**Verify:**
```bash
.venv/bin/python -c "
from threading import Event
e = Event(); e.set()
from ch_bulk.companies_house.financials_pipeline import enrich_financials
from ch_bulk.web.classifier_pipeline import WebsiteClassifier
from ch_bulk.web.website_finder import WebsiteFinder
from ch_bulk.cqc.api_enricher import CQCAPIEnricher
from ch_bulk.companies_house.ch_enricher import enrich_directors
from ch_bulk.companies_house.processor import process_csvs
from ch_bulk.cqc.processor import process_cqc_csv, process_hsca_filters
# Verify all signatures accept cancel_event kwarg
import inspect
for fn in [enrich_financials, enrich_directors, process_csvs, process_cqc_csv, process_hsca_filters]:
    assert 'cancel_event' in inspect.signature(fn).parameters, f'{fn.__name__} missing cancel_event'
for cls in [WebsiteClassifier, WebsiteFinder, CQCAPIEnricher]:
    method = getattr(cls, 'classify', None) or getattr(cls, 'find', None) or getattr(cls, 'enrich_providers', None)
    assert method is not None and 'cancel_event' in inspect.signature(method).parameters, f'{cls.__name__} missing cancel_event'
print('ok')
"
.venv/bin/python -m unittest discover -s tests > /tmp/phase6_step10_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase6_step10_tests.txt | tail -2  # 91/0/0
```

### Step 11 — Manual quit-during-task smoke

ASK USER to test quit behavior:

1. `.venv/bin/ch-bulk ui`
2. Start a slow task (e.g. CQC Sync, HSCA Sync — these are the slow ones)
3. While the task is running, hit `Cmd-Q`
4. Confirm: 3-button dialog appears, says "1 task running" with the task label
5. Click `Wait for safe stop`. Confirm: status updates to "Stopping 1 task(s)..."; window closes within ~10s (if task hit a phase boundary) OR escalation dialog appears (if task is in an uncancellable phase like a long `duckdb.execute`)
6. If escalation appears, test `Force quit` — process exits immediately, no segfault popup

Reply "quit smoke ok" or report issue.

### Step 12 — Commit Subcommit D

```bash
git add -A
git commit -m "Phase 6d: Per-pipeline cooperative cancellation

All long-running pipelines accept optional cancel_event kwarg:
- financials_pipeline.enrich_financials
- classifier_pipeline.WebsiteClassifier.classify
- website_finder.WebsiteFinder.find
- cqc.api_enricher.CQCAPIEnricher.enrich_*
- companies_house.ch_enricher.enrich_directors
- companies_house.processor.process_csvs (phase boundaries)
- cqc.processor.process_cqc_csv + process_hsca_filters (phase boundaries)

When cancel fires, pipelines complete current durable unit (current
company / current DB phase) then raise OperationCancelled. Sync batches
marked 'cancelled' instead of 'running'. Subsequent phases skipped."
```

---

## Subcommit E — Compaction safety + recovery

### Step 13 — Compaction safety + pre-connect recovery

In `companies_house/processor.py` `compact_database()`:

1. `raise_if_cancelled(cancel_event)` BEFORE the export/import + unlink/rename. If cancelled here → don't start swap, leave original DB intact.
2. During swap (unlink → rename, ~ms), DON'T check cancel. Atomic from the user's POV.
3. After swap success, return normally.

Do **not** put swap recovery in `ensure_pipeline_schema(con)`. That hook is too late: by the time it runs, a direct `duckdb.connect(str(db_path))` may already have created a fresh empty DB at the missing path.

Instead add a small path-based helper such as `recover_interrupted_compaction(db_path: str | Path) -> None` in `db/bootstrap.py` (or another DB helper module), and call it **before** connecting.

```python
def recover_interrupted_compaction(db_path: str | Path) -> None:
    db_path = Path(db_path)
    tmp_db = db_path.with_suffix(db_path.suffix + ".compact.tmp")
    if tmp_db.exists() and not db_path.exists():
        logger.warning(
            "Recovering from interrupted compaction: renaming %s → %s",
            tmp_db, db_path,
        )
        tmp_db.rename(db_path)
```

Wire it into:

- `with_duckdb_connection(...)` before each `duckdb.connect(...)`
- any direct-connect runtime DB sites that bypass that helper

Use the actual code, not this doc's memory, to find those direct-connect sites:

```bash
grep -R -n "duckdb.connect(str(db_path)" ch_bulk
grep -R -n "duckdb.connect(str(self.ch.db_path)" ch_bulk
```

GUI dialog wording (in `_on_close` when an active task's `phase == 'compact_database'`):

> "Database compaction is in the swap window — cannot be cancelled safely. Wait or force quit?" (no "safe stop" option offered during this phase)

This requires the bulk processors to set `task.phase = 'compact_database'` via the existing `ActiveTask` registry. GUI's `_on_close` reads this phase before building the dialog.

### Step 14 — Commit Subcommit E + final test sweep

```bash
.venv/bin/python -m unittest discover -s tests > /tmp/phase6_post_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase6_post_tests.txt | tail -2  # 91+ tests / 0 fail / 0 errors
if grep -q "^FAILED" /tmp/phase6_post_tests.txt; then echo "tests failed" && exit 1; fi
grep -q "^OK$" /tmp/phase6_post_tests.txt

git add -A
git commit -m "Phase 6e: Compaction safety + bootstrap recovery from interrupted swap

compact_database() checks cancel BEFORE the unlink/rename swap window
and raises OperationCancelled if requested. Inside the swap, no
cancellation check (atomic from user POV). A pre-connect recovery
helper restores <db>.compact.tmp into place if the canonical DB is
missing after an interrupted swap.

GUI dialog detects 'compact_database' phase and omits 'safe stop' option
during the swap window, only offering 'Wait' or 'Force quit'."
```

---

## Tests (woven into Subcommits B–E)

Add to `tests/test_cancellation.py` (new file):

```python
"""Regression tests for cooperative cancellation primitives."""
import time
import unittest
from threading import Event, Thread

from ch_bulk.core.cancellation import (
    OperationCancelled,
    cancellable_sleep,
    is_cancelled,
    raise_if_cancelled,
)


class CancellationPrimitiveTests(unittest.TestCase):
    def test_is_cancelled_none(self):
        self.assertFalse(is_cancelled(None))

    def test_is_cancelled_set(self):
        e = Event()
        self.assertFalse(is_cancelled(e))
        e.set()
        self.assertTrue(is_cancelled(e))

    def test_raise_if_cancelled(self):
        e = Event()
        raise_if_cancelled(e)  # no-op
        e.set()
        with self.assertRaises(OperationCancelled):
            raise_if_cancelled(e)

    def test_cancellable_sleep_none(self):
        t0 = time.monotonic()
        cancellable_sleep(None, 0.1)
        self.assertGreater(time.monotonic() - t0, 0.05)

    def test_cancellable_sleep_already_cancelled(self):
        e = Event(); e.set()
        with self.assertRaises(OperationCancelled):
            cancellable_sleep(e, 5.0)

    def test_cancellable_sleep_cancelled_mid_sleep(self):
        e = Event()
        Thread(target=lambda: (time.sleep(0.1), e.set())).start()
        with self.assertRaises(OperationCancelled):
            cancellable_sleep(e, 5.0)
```

Optional further tests as `tests/test_pipeline_cancellation.py`:
- One per pipeline: instantiate with mocks + pre-set cancel_event, verify `OperationCancelled` raised
- Verify sync_batches row marked 'cancelled' not 'running' after cancel

**Verify (test suite total):**
```bash
.venv/bin/python -m unittest discover -s tests > /tmp/phase6_final_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase6_final_tests.txt | tail -2
# Expected: ~95-100 tests / 0 fail / 0 errors
```

---

## Main-session squash (after all 5 subcommits land)

After codex posts DONE for Subcommit E, main session:

```bash
git reset --soft pre-phase-6-bookmark
git commit -m "$(cat <<'EOF'
Phase 6: GUI task registry + cooperative cancellation across pipelines

Eliminates the daemon-thread + DuckDB-native-code segfault class on quit.

Workstreams (landed as staged subcommits, squashed here):

a. GUI task registry: ActiveTask dataclass + _active_tasks dict in
   ChBulkApp. All 6 thread spawn sites migrated from daemon=True ad-hoc
   threads to non-daemon registered workers. _on_close now offers a real
   3-button dialog (Wait for safe stop / Force quit / Cancel) with
   polling + escalation on timeout.

b. ch_bulk/core/cancellation.py: OperationCancelled exception +
   is_cancelled, raise_if_cancelled, cancellable_sleep helpers. Pipelines
   accept optional cancel_event=None kwarg; existing callers unaffected.

c. Cancellable rate-limit + HTTP retry sleeps. SlidingWindowThrottle.wait
   and all client _get() retry backoffs use cancellable_sleep so
   cancellation fires during throttle pauses instead of blocking the
   full sleep window.

d. Per-pipeline cancellation hooks in financials_pipeline,
   classifier_pipeline, website_finder, cqc/api_enricher,
   companies_house/ch_enricher, and the bulk processors (CH, CQC, HSCA).
   Bulk processors cancel at DB phase boundaries only (mid-statement
   duckdb.execute is atomic). All raise OperationCancelled after the
   current durable unit completes; sync_batches rows marked 'cancelled'
   instead of 'running'.

e. compact_database() checks cancel BEFORE the unlink/rename swap.
   A pre-connect recovery helper restores *.compact.tmp into place if
   the canonical DB is missing after an interrupted swap.

Tests: new tests/test_cancellation.py covers primitives. Test baseline
unchanged at 91/0/0 + new cancellation tests.

Behavioral changes:
- Worker threads are non-daemon; cooperative shutdown via cancel_event.
- "Wait for safe stop" actually waits up to 10s + escalates.
- Compaction recoverable from interrupted swap.

See docs/plans/2026-05-29-phase-6-design.md for full rationale.
EOF
)"
git log -1 --stat | head -20
git status
```

**Verify (final):**
- Single commit on `trunk`
- Working tree clean
- Tag `pre-phase-6-bookmark` preserved

## Rollback contract

If any subcommit fails: stop. Revert that subcommit only (`git reset --hard HEAD~1`). If multiple subcommits are in a bad state: `git reset --hard pre-phase-6-bookmark`.

## Failure modes and recovery

| Symptom | Likely cause | Fix |
|---|---|---|
| Tests fail after Subcommit A | Non-daemon thread doesn't exit on test teardown | Verify `_register_task`'s try/finally unregisters even on exception |
| Test `test_rate_limit.py` breaks after Subcommit C | `time.sleep` mock no longer intercepted | Pass the caller's module-local `time.sleep` into `cancellable_sleep(..., sleep_fn=time.sleep)`; `tests/test_rate_limit.py` patches `ch_bulk.core.rate_limit.time.sleep` |
| Pipeline test fails after Subcommit D | Pipeline raises `OperationCancelled` in a path tests don't expect | Either tests need updating, or the cancellation check is in the wrong place |
| GUI Cmd-Q hangs forever | Non-daemon thread didn't honor cancel_event | Worker function not checking `raise_if_cancelled` between durable units |
| Compaction half-runs after force-quit | Pre-connect recovery didn't fire | Verify the recovery helper runs before `duckdb.connect(...)`, not inside `ensure_pipeline_schema(con)` |
| Whole phase looks broken | — | `git reset --hard pre-phase-6-bookmark` |

## Handoff

When done (after Subcommit E commit), post DONE in this side room with:
- All 5 commit hashes (6a–6e)
- Test pass count (expected: 91 baseline + new cancellation tests, all green)
- Brief discrepancy log if any
- Tag `pre-phase-6-bookmark` preserved
- Note: main session will squash 6a–6e into one Phase 6 commit before push
