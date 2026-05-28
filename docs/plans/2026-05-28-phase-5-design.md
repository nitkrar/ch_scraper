# Phase 5: Classifier eager-Playwright bug fix + HSCA/CQC bulk surface exposure

**Date:** 2026-05-28
**Status:** Design v1 (pre-review)
**Companion:** [`2026-05-28-phase-5-implementation.md`](./2026-05-28-phase-5-implementation.md) — verifiable execution checklist
**Predecessor:** Phase 4 commit `96e6998` on `trunk`. Pre-Phase-5 bookmark: tag will be added at Step 0.
**Sweep source:** `/tmp/codex_phase5_sweep.md` (req-0136)

## Problem

Two unrelated gaps surfaced after Phase 4:

1. **Classifier runtime bug.** `WebsiteClassifier.classify()` starts a fallback thread that eagerly opens `browser.PlaywrightSession` whenever Playwright is importable — regardless of whether any `FallbackTask` will actually arrive. On machines where Chromium launch is denied (locked-down macOS, headless CI without browser binaries), the entire batch aborts even when no fallback is needed. This is why we've carried 7 "Playwright errors" in `tests/test_classifier.py` since before Phase 1 — they were classified as "environment-dependent" but they're actually a real eager-init bug.

2. **HSCA + CQC bulk CLI/GUI gap.** Backend already exists in `ChBulk` (`download_cqc`/`process_cqc`/`sync_cqc`, `download_hsca`/`process_hsca`/`cqc_hsca_sync` per api.py:249-363). Nothing in `cli.py` exposes them. GUI has CQC buttons but no HSCA buttons. User flagged this — *"I asked it to pipe HSCA download and processing as part of CQC data/tab but this was never implemented."*

## What the file actually is (per code sweep at `/tmp/codex_phase5_sweep.md`)

A code-grounded sweep (req-0136) found:

- **6 of 7 failing tests need ONLY the eager-Playwright fix.** They never enqueue a `FallbackTask`; they fail at fallback-thread initialization before their real assertions even run.
- **1 of 7 (`test_classify_uses_playwright_retry_for_tiny_bodies`) also needs a mock-target fix.** It patches `browser.fetch_rendered` but not `browser.PlaywrightSession`, so even after the eager fix, it would still try to launch real Chromium when `_page_needs_playwright()` legitimately creates `retry_pages`.
- **Backend is complete for HSCA + CQC bulk.** This is pure surface-layer wiring.
- **No file overlap between workstreams.** Workstream A touches `classifier_pipeline.py` + `tests/test_classifier.py`. Workstream B touches `cli.py` + `gui.py` + new CLI tests.
- **`cqc_hsca_sync` is a misleading internal name.** It only syncs HSCA (not CQC+HSCA together as the name implies). CLI should expose as `hsca-bulk sync`, not `cqc-hsca-sync`.
- **GUI buttons without status feedback would be a UX regression.** If we add HSCA buttons, the CQC pane's `refresh()` only reports `cqc_locations`/`cqc_providers` row counts — must also surface HSCA row count or last-imported file date.

## Approach

Two workstreams, executed by codex as two intermediate commits, **squashed into one Phase 5 commit before push.**

### Workstream A: Classifier eager-Playwright fix + test repair (commit 5a)

Make `PlaywrightSession` creation lazy inside `fallback_worker()`:

- Block on `fallback_queue.get()` first.
- Only create+open the session when the first real `FallbackTask` arrives.
- Reuse session for subsequent tasks (preserves existing behavior covered by `test_classify_reuses_playwright_session_for_multiple_handoffs`).
- Preserve the existing fallback-lane writer/logging/finalize flow. The code already knows how to build an `_unable_row(...)` for a per-task Playwright failure; reuse that path instead of inventing a new helper.
- If session creation fails when a real task arrives, mark those handed-off items as fallback-lane unable rows rather than surfacing the error through `fallback_failures` and aborting the whole batch. `raise_fallback_failure()` is what kills the batch today.

Fix the one wrong-seam test: `test_classify_uses_playwright_retry_for_tiny_bodies` should patch `browser.PlaywrightSession` **and** keep patching `browser.fetch_rendered`, mirroring `test_classify_reuses_playwright_session_for_multiple_handoffs`. Patching `fetch_rendered` alone is insufficient because the fallback worker enters the session context before calling it.

### Workstream B: HSCA + CQC bulk CLI + GUI exposure (commit 5b)

**CLI (per codex's recommendation):**

```
ch-bulk cqc-bulk download
ch-bulk cqc-bulk process [--force]
ch-bulk cqc-bulk sync [--force]
ch-bulk hsca-bulk download [--target-date YYYY-MM-DD]
ch-bulk hsca-bulk process [--force]
ch-bulk hsca-bulk sync [--target-date YYYY-MM-DD] [--force]
```

Matches existing `cqc-enrich` / `ch-enrich` subgroup style. Avoids collision with top-level CH `download`/`process`/`sync`. Internal `cqc_hsca_sync` method name stays; CLI surface says `hsca-bulk sync` (honest about what it does).

Important code-truth detail: the current `ChBulk` methods return a single integer row count for `process_cqc`, `sync_cqc`, `process_hsca`, and `cqc_hsca_sync`, not `(locations, providers)` or `(locations, duals)` tuples. CLI success text should report location counts only unless we deliberately add extra DB queries.

Add focused `CliRunner` tests for command wiring + help text (using existing `tests/test_financials_enricher.py` `CliRunner` pattern).

**GUI:**

- Keep left rail unchanged (HSCA is companion ingest data, not a first-class browse surface yet).
- Extend the existing CQC database card in `gui.py` to add a second labeled action row.
- Layout:
  - Status summary row at top (current)
  - `CQC: Sync / Download / Process` (current row, just labeled "CQC:")
  - `HSCA: Sync / Download / Process` (NEW row)
- Update `CQCPane.refresh()` status text to also report HSCA row count (e.g. `cqc_hsca_locations` count) so the new buttons have visible feedback.
- Do NOT add an HSCA browse sub-tab in this commit (separate UX decision; out of scope).

## Non-goals

- **No backend API changes.** `ChBulk` methods stay as-is. We're exposing what's already there.
- **No HSCA browse UI.** Just ingest buttons + status; interactive browsing of `cqc_hsca_locations` is a future UX decision.
- **No classifier behavior change beyond lazy-init.** Same prompts, same staging, same loader behavior. Only the fallback-thread initialization order changes.
- **No fixes for other test errors.** Phase 5 only addresses the 7 Playwright errors. Any other tests are out of scope.
- **No CQC `cqc_hsca_sync` method rename.** Internal name stays; CLI/GUI just labels things correctly.

## Public API contract

| Surface | Phase 5 change | Compatibility |
|---|---|---|
| `from ch_bulk import ChBulk` | unchanged | ✓ |
| `ChBulk.download_hsca/process_hsca/cqc_hsca_sync/download_cqc/process_cqc/sync_cqc` | unchanged signatures + behavior | ✓ |
| `ch-bulk download/process/sync` (CH bulk) | unchanged | ✓ |
| `ch-bulk cqc-enrich {providers,locations}` | unchanged | ✓ |
| `ch-bulk cqc-bulk {download,process,sync}` | **NEW** | new feature |
| `ch-bulk hsca-bulk {download,process,sync}` | **NEW** | new feature |
| GUI CQC pane | adds HSCA action row + HSCA status in summary | additive |
| `WebsiteClassifier.classify()` | Playwright session lazy-init; classify batch succeeds when no fallback needed even on browser-less machines | bug fix; existing behavior preserved when fallback IS needed |

## Rationale

1. **Eager-Playwright fix is a bug, not just test cleanup.** Real users on locked-down machines (managed laptops, sandboxed runners) would hit "full batch aborts" even when their data never triggers fallback. Worth its own commit message.
2. **HSCA wiring closes a known user-facing gap.** User explicitly flagged it after seeing it missing in the current GUI.
3. **CLI groups (`cqc-bulk`, `hsca-bulk`) match repo style.** `cqc-enrich`, `ch-enrich`, `migration` already use this hyphenated-subgroup pattern.
4. **CLI `--force` belongs on the new process/sync commands.** The backend already exposes `force=` on CQC/HSCA ingest paths, and the GUI already surfaces that retry path through `_dispatch_with_sanity`. Hiding it in CLI would leave part of the existing behavior unreachable.
5. **GUI status row is required to avoid UX regression.** Adding HSCA buttons without visible feedback would be worse than nothing, and the new HSCA buttons also need to participate in the app's `action_buttons()` disable/re-enable cycle.
6. **Squash at end keeps phase history clean** while letting codex verify each concern independently during execution.

## Risks

1. **Lazy-init order matters.** The fix must preserve the current `fallback_queue.task_done()`, writer finalization, `load_requested`, and logging behavior. The queue itself will buffer work safely; the real risk is accidentally short-circuiting the existing fallback-lane bookkeeping.
2. **Session-creation failure path.** Do not report expected "browser unavailable for fallback lane" through `fallback_failures`, because `raise_fallback_failure()` aborts the whole batch. Treat that as a per-task fallback-lane failure and keep non-fallback items running.
3. **`test_classify_uses_playwright_retry_for_tiny_bodies` mock fix is independent.** The eager-init fix alone will still leave this test red because it explicitly enables Playwright fallback and legitimately produces `retry_pages`.
4. **HSCA status in CQC pane.** Need a narrow inner `duckdb.CatalogException` catch for `cqc_hsca_locations` on first-run state, while still closing the connection cleanly.
5. **HSCA buttons must be in `action_buttons()`.** Otherwise they remain clickable during background tasks while the rest of the pane is disabled.
6. **CLI subgroup surface should reflect real return values.** Success output should use the single location-count integer returned by the current API methods.
7. **CliRunner test pattern.** Existing `tests/test_financials_enricher.py` already patches `ch_bulk.cli.ChBulk...`, so `patch("ch_bulk.cli.ChBulk")` is the correct import-level seam here too. Phase 5 only needs 6 focused CLI tests, not a large matrix.

## What this enables (later)

- After Phase 5: 7 Playwright errors → 0. Full test suite green at `91/0/0`.
- HSCA browse UI (interactive `cqc_hsca_locations` view) becomes a separate small UX commit.
- Phase 6 candidate: extract shared enricher base across the 5 enrichers (path layer is finally clean enough to attempt).
- Phase 1.5 doc archive_dir mismatch fix (small doc-only commit).
- Push 11+ commits to `origin/trunk`.
