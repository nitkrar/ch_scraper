# Phase 6: GUI thread tracking + cooperative cancellation across pipelines

**Date:** 2026-05-29
**Status:** Design v2 (post-review)
**Companion:** [`2026-05-29-phase-6-implementation.md`](./2026-05-29-phase-6-implementation.md) — verifiable execution checklist
**Predecessor:** `1181d0c` "GUI fixes: read-only connection collision + Cmd-Q crash" on `trunk`. Pre-Phase-6 bookmark: tag will be added in implementation Step 0.
**Sweep source:** `/tmp/codex_phase6_sweep.md` (req-0139)

## Problem

After Phase 5 + the GUI fix commit (`1181d0c`), the deepest remaining bug class is **daemon-thread + DuckDB-native-code segfaults on quit**. Symptoms we've hit:

1. Cmd-Q during a long task → "Python quit unexpectedly" OS popup. Root cause: daemon thread is mid `duckdb.execute(...)`, Tk destroys root, DuckDB connection garbage-collected → SIGSEGV in C code before any of our cleanup runs.
2. Tab switch during a write → "Can't open connection with different configuration" — fixed in `1181d0c`.
3. `Cmd-Q` itself → fatal Python error from racing built-in Tk Cmd-Q handler — fixed in `1181d0c`.
4. `_on_close` claimed to "wait for background task" but immediately destroyed root — fixed in `1181d0c` to either wait or force-quit (still not graceful for in-flight write).

The fix in `1181d0c` is a band-aid. The deeper fix needs:
- **Real thread registry** (current `_task_running` boolean covers only 2 of 6 GUI thread spawn sites — query/export threads are invisible to quit logic)
- **Cooperative cancellation** in pipelines so user can actually cancel an in-flight task instead of only force-quitting
- **Non-daemon worker threads** so the interpreter waits for them to drain cleanly on exit
- **Cancellable rate-limit sleeps** so cancellation can fire even during throttle pauses (today: stuck up to full sleep interval)

## What the code actually looks like (per `/tmp/codex_phase6_sweep.md`)

Code-grounded sweep refuted three of my prior assumptions:

- **`cqc/api_enricher.py` is single-threaded** — not a worker-thread pool. Cancellation means cooperative per-entity check, not pool shutdown.
- **`companies_house/ch_enricher.py` is single-threaded** — same.
- **`website_finder.py` top-level is sequential** — parallelism lives one layer down in `web/search.py`.
- **`financials_pipeline.py` and `classifier_pipeline.py` already use `threading.Event` internally** — they're the natural model for the rest of the codebase.

Pipelines with explicit in-pipeline worker coordination are `financials_pipeline.py` (fetch threads + parser subprocesses) and `classifier_pipeline.py` (manually managed classifier threads + fallback thread). `website_finder.py` remains sequential at the top level even though `web/search.py` uses a `ThreadPoolExecutor` for URL checks.

**Bulk processors (CH/CQC/HSCA) are different:** most wall-clock time sits inside single `duckdb.execute(...)` calls (`COPY ... FROM read_csv_auto(...)`, upserts, compaction). Those are atomic from Python's POV — not interruptible. Phase 6 can only cancel them at phase boundaries.

**`compact_database()` has a dangerous force-quit window:** between `db_path.unlink()` and `tmp_db.rename(db_path)`, the canonical DB file does not exist. Force-quit there leaves only the `.compact.tmp` file. Phase 6 UI needs explicit wording for this.

## Approach

### Workstream A: GUI task registry + non-daemon threads + safe-quit dialog

Add an `ActiveTask` dataclass + registry in `ChBulkApp`. Replace all 6 `threading.Thread(...).start()` call sites with a helper that:
- Creates an optional `cancel_event: threading.Event`
- Creates a **non-daemon** thread
- Registers it
- Arranges unregister-on-finish

`_on_close()` gets a real 3-button dialog: `Wait for safe stop` / `Force quit` / `Cancel`.

Important implementation detail: the registry wrapper should always pass one positional `cancel_event` argument to the worker target, so every local worker closure it wraps must adopt the uniform signature `def worker(cancel_event: Event | None = None) -> None`, even when `can_cancel=False` and the passed value is always `None`.

Tk does have `messagebox.askyesnocancel`, but its button labels are fixed to `Yes / No / Cancel`; `tk.simpledialog` is an input prompt, not a multi-action choice dialog. If we want explicit `Wait for safe stop / Force quit / Cancel` wording, a small custom `Toplevel` dialog is justified.

### Workstream B: Cancellation primitive

A tiny module `ch_bulk/core/cancellation.py`:

```python
class OperationCancelled(RuntimeError):
    pass

def is_cancelled(cancel_event: threading.Event | None) -> bool: ...
def raise_if_cancelled(cancel_event: threading.Event | None, *, reason: str = "cancelled") -> None: ...
def cancellable_sleep(
    cancel_event: threading.Event | None,
    seconds: float,
    *,
    reason: str = "cancelled",
    sleep_fn: Callable[[float], None] = time.sleep,
) -> None: ...
```

Pipeline entrypoints accept optional `cancel_event: threading.Event | None = None`. Existing callers don't pass it → behavior unchanged. New GUI passes the registered event → cooperative cancellation works.

### Workstream C: Cancellable sleeps (rate-limit + retry backoffs)

`SlidingWindowThrottle.wait()` in `core/rate_limit.py` uses `time.sleep(...)` — non-cancellable. Without fixing this, cancellation requests during throttle pauses sit blocked for the full sleep window (~seconds to minutes for CH API at 600/window).

Switch `time.sleep(x)` → `cancellable_sleep(cancel_event, x, sleep_fn=time.sleep)` in:
- `core/rate_limit.SlidingWindowThrottle.wait()`
- `CompaniesHouseClient._get()` retry backoff
- `CompaniesHouseFinancialsClient._get()` + document retry helpers
- `CQCAPIClient._get()` retry backoff
- `WebsiteFinder.find()` inter-company pause
- `classifier_pipeline` Playwright retry-page loop (if any)

The `sleep_fn=time.sleep` injection is deliberate: `tests/test_rate_limit.py` currently patches `ch_bulk.core.rate_limit.time.sleep`, and a plain fallback inside `ch_bulk.core.cancellation` would not see that patch. `cancellable_sleep(...)` should also raise `OperationCancelled` when the event fires during the wait; otherwise the caller wakes early and can still continue its retry/throttle loop unless every call site remembers a second `raise_if_cancelled(...)`.

### Workstream D: Per-pipeline cancellation hooks

Apply per codex's priority order:

| Pipeline | Cancellation shape | Effort |
|---|---|---|
| `financials_pipeline.py` | Already has `shutdown_event`; route cancel through it. Stop feeding queue, drain in-flight responses, send sentinels to subprocess parsers. | Small (mostly plumbing) |
| `classifier_pipeline.py` | Already event-driven. Add per-company check + cancellable Playwright queue drain. | Small |
| `website_finder.py` | Per-company check between iterations + cancellable inter-company sleep. | Tiny |
| `cqc/api_enricher.py` | Per-entity check in the sequential loop + cancellable throttle. | Tiny |
| `companies_house/ch_enricher.py` | Same. | Tiny |
| `companies_house/processor.py` (CH bulk) | Phase-boundary cancel only. `raise_if_cancelled` between validate-files → ingest → sanity → bootstrap/upsert → indexes → compact. Can NOT interrupt mid-statement. | Small |
| `cqc/processor.py` (CQC + HSCA bulk) | Same — phase boundaries only. | Small |

### Workstream E: Compaction safety

Special handling in `compact_database()`:
- Check cancel BEFORE the unlink/rename window. If cancelled there → don't start the swap, raise `OperationCancelled`, leave original DB untouched.
- During the swap window (unlink → rename, ~ms), DON'T check cancel. Either complete the swap or leave it as torn state.
- If a process crashes during the swap window, add recovery in a **pre-connect helper that takes `db_path`**, not inside `ensure_pipeline_schema(con)`. `ensure_pipeline_schema` is too late: by the time it runs, a direct `duckdb.connect(str(db_path))` may already have created a fresh empty DB at the missing path.

GUI dialog wording during compact phase: "Database compaction is in the unlink/rename window — cannot be cancelled safely. Wait?"

## Non-goals

- **No GUI framework change.** Tkinter stays. After Phase 6 we'll re-evaluate whether the remaining flakiness justifies a rewrite.
- **No mid-`duckdb.execute()` cancellation.** Single SQL statements are atomic; we don't try to break them.
- **No new test framework.** Stay on stdlib unittest.
- **No backward-incompatible signature changes.** All pipeline cancellation params default to `None`; existing CLI/tests/callers work unchanged.
- **No abandonment of `os._exit(0)` force-quit path.** Cooperative shutdown is best-effort; user always has the escape hatch.

## Public API contract

| Surface | Phase 6 change | Compatibility |
|---|---|---|
| `from ch_bulk import ChBulk` | unchanged | ✓ |
| All `ChBulk.*` methods | unchanged signatures (cancel_event added as optional kwarg only on long-running ones) | ✓ |
| CLI commands | unchanged surface; new tasks just behave cooperatively when Cmd-Q'd | ✓ |
| GUI tab switching | unchanged | ✓ |
| GUI Cmd-Q during task | NEW: 3-button dialog (was: 2-button + immediate destroy) | UX improvement |
| Worker threads | NEW: non-daemon, registered, cancellable | invisible change unless quit fires |
| `core.rate_limit.SlidingWindowThrottle.wait()` | accepts optional `cancel_event` kwarg | additive |
| HTTP client `_get()` retry backoffs | accept optional `cancel_event` | additive |
| New module: `ch_bulk/core/cancellation.py` | New surface | additive |
| New exception: `OperationCancelled` | Pipelines raise on cancel after durable checkpoint | additive (callers don't catch unless they want to) |

## Rationale

1. **Daemon threads + DuckDB native code is the root of the crash class.** Non-daemon threads + cooperative cancellation removes the entire class.
2. **`threading.Event` matches existing code.** Two pipelines already use it. The primitive minimizes API churn.
3. **Optional kwarg = zero call-site churn for tests/CLI.** Tests don't have to pass `cancel_event=`; they get the existing behavior.
4. **Staged subcommits + main session squash.** Pattern used in Phase 5; lets codex verify each layer independently while keeping history clean.
5. **Honest UI wording per pipeline.** "Stops after current company" is true for API enrichers; "stops after current DB phase" is true for bulk; "cannot be cancelled" is true for compaction swap window. Phase 6 doesn't lie about what cancellation can do.

## Risks

1. **Non-daemon threads + buggy registry = process hang.** If a thread doesn't unregister itself on exception, the interpreter waits forever. Mitigation: registry helper uses try/finally for unregister; tests cover the exception path.
2. **Cancellation timing edge case.** If `cancel_event.set()` fires after `raise_if_cancelled` check but before `duckdb.execute(...)`, the statement runs to completion (atomic). User sees ~seconds-of-DB-phase delay before cancel takes effect. Documented in UI wording.
3. **`SlidingWindowThrottle` is shared across enrichers.** If GUI cancels one task that's sharing a throttle with another, both wake up. Mitigation: the cancel_event is per-task; throttle's `wait()` accepts it as a param, not a constructor state.
4. **Compaction swap window.** The ~ms window where DB doesn't exist is inherently unsafe. We can't make it cancellable. Adding bootstrap-time recovery (rename `*.compact.tmp` if main DB missing) is the safety net; document it.
5. **Multiprocessing parser cancellation.** `financials_pipeline.py` spawns parser subprocesses via multiprocessing.Process. Codex flagged: reuse existing parent-side `shutdown_event` rather than adding a second cross-process channel. Subprocess parsers receive `None` sentinel to exit cleanly.
6. **Tests using monkeypatch.** `tests/test_rate_limit.py` patches `ch_bulk.core.rate_limit.time.sleep`, not `ch_bulk.core.cancellation.time.sleep`. Preserve that by giving `cancellable_sleep(...)` an injectable `sleep_fn=time.sleep` and passing the caller's module-local `time.sleep` through.

## What this enables (later)

- GUI rewrite (PySide6 / Streamlit / etc.) becomes a UX decision, not a "fix the crashes" decision.
- Long-running CLI tasks could honor SIGINT cleanly (cancel_event hooked to a signal handler).
- Real "cancel" button in the GUI per task, not just on quit.
- Workflow improvements: queue multiple tasks with cancel-mid-queue support.

## Execution scope summary

| Workstream | Approx LOC | Risk | Sequential dependency |
|---|---|---|---|
| A. GUI registry + dialog | ~150 LOC in gui.py | Low | Independent |
| B. Cancellation primitive | ~30 LOC new module | Low | Independent |
| C. Cancellable sleeps | ~80 LOC across 5 files | Low | Depends on B |
| D1. financials_pipeline | ~50 LOC | Medium | Depends on B + C |
| D2. classifier_pipeline | ~50 LOC | Medium | Depends on B + C |
| D3. website_finder + 2 enrichers | ~60 LOC | Low | Depends on B + C |
| D4. Bulk processors (CH/CQC/HSCA) | ~40 LOC | Low | Depends on B |
| E. Compaction safety + recovery | ~40 LOC | Medium-high | Depends on B |
| Tests | ~150 LOC | Low | Last |

Estimated total: ~650 LOC across ~15 files. Single phase commit after staged subcommit execution + squash.
