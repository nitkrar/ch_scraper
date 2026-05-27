# Phase 2 implementation plan

**Companion to:** [`2026-05-27-phase-2-design.md`](./2026-05-27-phase-2-design.md) — rationale + symbol-to-module map
**For executor:** read this doc top-to-bottom. The design doc explains *what* and *why*; this doc gives you the *how* — exact commands, exact order, exact verification.

## Hard rules

1. **Code is the source of truth, not this doc.** If during execution you find the code doesn't match a doc claim (line numbers, symbol names, dependency edges, what a test imports), **trust the code, fold the discrepancy in, fix it, continue**. Note discrepancies in your final handoff post.
2. **Single commit at the end.** All steps below land in ONE git commit at the final step. Intermediate steps run verification but do NOT commit.
3. **No business-logic or dataflow changes.** Pure mechanical extraction. Function bodies move; signatures, return values, database writes, network requests, and staging formats stay identical. Expected metadata-only changes are moved `__module__` values and logger names for moved helpers. If you find a bug while moving code, leave it (note it for follow-up); don't fix it in this phase.
4. **Verify before next step.** Every step has a `Verify:` block. If any verify fails, stop. Revert tracked edits with `git restore --source=HEAD --staged --worktree ...`; remove untracked new module files before retrying. If the tree gets confusing, use the full rollback target below.
5. **Rollback target.** Step 0 creates a `pre-phase-2-bookmark` tag. If anything goes wrong: `git reset --hard pre-phase-2-bookmark && git clean -fd`.
6. **Facade preserves test imports.** Every name currently imported by `tests/test_financials_enricher.py:25-42` MUST resolve via `from ch_bulk.companies_house.financials_enricher import X` after the split. Step 7 enumerates and verifies.
7. **Ask the user for confirmation** at Step 12 (GUI smoke) and Step 14 (final commit).

## Pre-flight checklist (before Step 0)

- [ ] Confirm working tree is clean: `git status` shows "nothing to commit, working tree clean".
- [ ] Confirm you're on `trunk` branch: `git branch --show-current` prints `trunk`.
- [ ] Confirm the Phase 1.5 commit is in history: `git merge-base --is-ancestor e666178 HEAD && echo ok` prints `ok`.
- [ ] Confirm the bookmark tag does NOT already exist: `git tag --list pre-phase-2-bookmark` prints nothing.
- [ ] Confirm venv + deps: `source .venv/bin/activate && which python && .venv/bin/python -c "import duckdb, ixbrlparse, pdfplumber, trafilatura, pandas, rapidfuzz"` returns no errors.
- [ ] Confirm baseline test summary robustly: `.venv/bin/python -m unittest discover -s tests > /tmp/phase2_preflight_tests.txt 2>&1 || true` then `grep -E "^(Ran |FAILED|OK$)" /tmp/phase2_preflight_tests.txt | tail -2` should show roughly `80 run / 0 fail / 7 errors` (the 7 are pre-existing Playwright errors, out of scope).
- [ ] Confirm no JSONL manifest stores class names by class path: `find data -type f -name '*.jsonl' | head -n 5` to locate sample manifests, then spot-check those paths. If any line contains `"FetchedFinancialRow"` as a string literal that gets `eval`'d or class-imported at load time, surface to user BEFORE moving the dataclass.

If any pre-flight check fails, stop and surface to user.

## Execution steps

### Step 0 — Baseline capture + bookmark tag

```bash
.venv/bin/python -m unittest discover -s tests > /tmp/phase2_baseline_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase2_baseline_tests.txt | tail -2
.venv/bin/ch-bulk --help > /tmp/phase2_baseline_cli_help.txt 2>&1
.venv/bin/ch-bulk info --db-path "$(pwd)/data/db/ch_bulk.duckdb" > /tmp/phase2_baseline_info.txt 2>&1 || true
.venv/bin/python -c "from ch_bulk import ChBulk, SanityCheckError, SanityCheckResult, ensure_pipeline_schema; print('ok')" > /tmp/phase2_baseline_top_imports.txt 2>&1
.venv/bin/python -c "from ch_bulk.companies_house.financials_enricher import enrich_financials, load_financials_staging; print('ok')" > /tmp/phase2_baseline_facade_imports.txt 2>&1
git tag pre-phase-2-bookmark HEAD
git tag --list pre-phase-2-bookmark
```

**Verify:** all 5 baseline files non-empty. Test-summary grep shows the expected baseline profile. Tag exists.

### Step 1 — Create `financials_contracts.py`

Move the following symbols from `financials_enricher.py` to `ch_bulk/companies_house/financials_contracts.py`:

```
Mode
FINANCIALS_SYNC_TYPE, FINANCIALS_FETCH_SYNC_TYPE
FILED_REVENUE_SOURCES, TERMINAL_REVENUE_SOURCES
ERROR_PARSE_STATUSES
IXBRL_RESOURCE, PDF_RESOURCE, IXBRL_EXTENSION, PDF_EXTENSION
FinancialTarget, FilingCandidate, ParsedFinancialFacts, StagedFinancialRow,
FetchedFinancialRow, FetchedFinancialWorkItem, FinancialsResult,
ParserProcessResponse, ParserProcessHandle, FinancialsFileStats
```

(Verify with code: codex's inventory line ranges may have drifted. Source of truth is the actual file at HEAD.)

In `financials_enricher.py`, replace those declarations with re-exports:

```python
from ch_bulk.companies_house.financials_contracts import (
    Mode,
    FINANCIALS_SYNC_TYPE,
    FINANCIALS_FETCH_SYNC_TYPE,
    FILED_REVENUE_SOURCES,
    TERMINAL_REVENUE_SOURCES,
    ERROR_PARSE_STATUSES,
    IXBRL_RESOURCE,
    PDF_RESOURCE,
    IXBRL_EXTENSION,
    PDF_EXTENSION,
    FinancialTarget,
    FilingCandidate,
    ParsedFinancialFacts,
    StagedFinancialRow,
    FetchedFinancialRow,
    FetchedFinancialWorkItem,
    FinancialsResult,
    ParserProcessResponse,
    ParserProcessHandle,
    FinancialsFileStats,
)
```

**Verify:**
```bash
.venv/bin/python -c "
from ch_bulk.companies_house.financials_contracts import (
    Mode, FINANCIALS_SYNC_TYPE, FINANCIALS_FETCH_SYNC_TYPE,
    FinancialTarget, FilingCandidate, ParsedFinancialFacts, StagedFinancialRow,
    FetchedFinancialRow, FetchedFinancialWorkItem, FinancialsResult,
    ParserProcessResponse, ParserProcessHandle, FinancialsFileStats,
)
from ch_bulk.companies_house.financials_enricher import (
    Mode, FinancialTarget, FetchedFinancialRow, StagedFinancialRow,
)
print('ok')
"
.venv/bin/python -m unittest discover -s tests > /tmp/phase2_step1_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase2_step1_tests.txt | tail -2
diff <(grep -E "^(Ran |FAILED|OK$)" /tmp/phase2_baseline_tests.txt | tail -2) <(grep -E "^(Ran |FAILED|OK$)" /tmp/phase2_step1_tests.txt | tail -2)
```

### Step 2 — Create `financials_parsers.py`

Move from `financials_enricher.py`:

```
REVENUE_FACTS, EMPLOYEE_COUNT_FACTS, GROSS_PROFIT_FACTS, PROFIT_BEFORE_TAX_FACTS,
PROFIT_AFTER_TAX_FACTS, FIXED_ASSETS_FACTS, CURRENT_ASSETS_FACTS, NET_ASSETS_FACTS,
NET_CURRENT_ASSETS_FACTS, PERIOD_FACT_GROUPS
IXBRL_PROFIT_LOSS_EXEMPTION_PATTERNS
REVENUE_REGEXES, EMPLOYEE_COUNT_REGEXES, GROSS_PROFIT_REGEXES, PROFIT_BEFORE_TAX_REGEXES,
PROFIT_AFTER_TAX_REGEXES, FIXED_ASSETS_REGEXES, CURRENT_ASSETS_REGEXES,
NET_ASSETS_REGEXES, NET_CURRENT_ASSETS_REGEXES
_empty_parsed_financials, _missing_financial_reasons, _classify_financial_facts
_parse_date
_has_ixbrl_profit_loss_exemption_marker
_derived_total_assets
_row_has_segments, _pick_ixbrl_row, _pick_ixbrl_value,
_row_period_start, _row_period_end, _duration_row_sort_key, _pick_ixbrl_period_bounds
_parse_ixbrl_bytes
_parse_numeric_text, _regex_number, _first_regex_value
_parse_pdf_bytes
```

Add at top of `financials_parsers.py`:
```python
from ch_bulk.companies_house.financials_contracts import (
    ParsedFinancialFacts,
)
```

In `financials_enricher.py`, add re-exports for the names tests import:
```python
from ch_bulk.companies_house.financials_parsers import (
    _parse_ixbrl_bytes,
    _parse_pdf_bytes,
)
```
(Plus any other private names you find tests importing — Step 7 has the full enumeration.)

**Verify:**
```bash
.venv/bin/python -c "
from ch_bulk.companies_house.financials_parsers import _parse_ixbrl_bytes, _parse_pdf_bytes
from ch_bulk.companies_house.financials_enricher import _parse_ixbrl_bytes, _parse_pdf_bytes
print('ok')
"
.venv/bin/python -m unittest tests.test_financials_enricher -v > /tmp/phase2_step2_financials_tests.txt 2>&1
tail -10 /tmp/phase2_step2_financials_tests.txt
grep -q "^OK$" /tmp/phase2_step2_financials_tests.txt
# All parser tests should still pass — they were the most directly tested cluster.
```

### Step 3 — Create `financials_fetch.py`

Move:

```
CH_API_BASE, DOCUMENT_API_BASE
DEFAULT_RETRY_AFTER_SECONDS, EFFECTIVE_CH_MAX_REQUESTS, CH_WINDOW_SECONDS
ANNUAL_ACCOUNTS_TYPES, ANNUAL_ACCOUNTS_DESCRIPTION_PREFIXES
CompaniesHouseFinancialsClient
_http_status_from_exception, _normalize_company_ids,
_filing_extension_for_format, _raw_filing_path
_select_targets, _select_latest_annual_accounts
_save_raw_filing, _build_fetched_row, _fetch_company_work_item
```

Do **not** move `_process_company` in this step. It composes `_fetch_company_work_item` with `_parse_fetched_row`, so forcing it into `financials_fetch.py` now would either create a fetch -> staging edge or break step ordering. Keep it in `financials_enricher.py` for now; Step 6 rewrites it as the one intentional facade compatibility wrapper.

Add at top:
```python
from ch_bulk.companies_house.financials_contracts import (
    FinancialTarget, FilingCandidate, FetchedFinancialRow, FetchedFinancialWorkItem,
    IXBRL_RESOURCE, PDF_RESOURCE, IXBRL_EXTENSION, PDF_EXTENSION,
)
from ch_bulk.companies_house.financials_parsers import (
    _parse_date,
)
```

Re-exports in `financials_enricher.py`:
```python
from ch_bulk.companies_house.financials_fetch import (
    CompaniesHouseFinancialsClient,
    _select_latest_annual_accounts,
    _select_targets,
)
```

**Verify:**
```bash
.venv/bin/python -c "
from ch_bulk.companies_house.financials_fetch import (
    CompaniesHouseFinancialsClient, _select_targets, _select_latest_annual_accounts,
)
from ch_bulk.companies_house.financials_enricher import (
    CompaniesHouseFinancialsClient, _select_targets, _select_latest_annual_accounts,
)
print('ok')
"
.venv/bin/python -m unittest tests.test_financials_enricher -v > /tmp/phase2_step3_financials_tests.txt 2>&1
tail -10 /tmp/phase2_step3_financials_tests.txt
grep -q "^OK$" /tmp/phase2_step3_financials_tests.txt
```

### Step 4 — Create `financials_staging.py`

Move:

```
FINANCIALS_INSERT_SQL, FINANCIALS_SCAN_SUMMARY_SQL
_months_between
_build_row, _fetched_row_to_target, _fetched_row_to_filing
_parse_fetched_row   # IMPORTANT: stays here, not parsers
_salvage_truncated_staging_file
_existing_staged_company_numbers
_replay_financials_fetch_staging_file, replay_financials_fetch_staging
_archive_financials_fetch_manifest
_scan_staged_financials_file
_sync_batch_status, _require_sync_batch, _mark_stale_running_batches
_load_financials_staging_file, load_financials_staging
```

Add at top:
```python
from ch_bulk.companies_house.financials_contracts import (
    FinancialTarget, FilingCandidate, ParsedFinancialFacts,
    StagedFinancialRow, FetchedFinancialRow, FinancialsFileStats,
    FINANCIALS_SYNC_TYPE, FINANCIALS_FETCH_SYNC_TYPE,
    FILED_REVENUE_SOURCES, TERMINAL_REVENUE_SOURCES,
)
from ch_bulk.companies_house.financials_parsers import (
    _parse_date, _parse_ixbrl_bytes, _parse_pdf_bytes,
)
```

`_replay_financials_fetch_staging_file` does **not** need `_raw_filing_path`; it reuses the serialized `raw_path` from `FetchedFinancialRow`. After this step, leave `_process_company` in the facade as a thin compatibility wrapper that calls `_fetch_company_work_item` (fetch) and `_parse_fetched_row` (staging).

Re-exports in `financials_enricher.py`:
```python
from ch_bulk.companies_house.financials_staging import (
    load_financials_staging,
    _replay_financials_fetch_staging_file,
    replay_financials_fetch_staging,
)
```

**Verify:**
```bash
.venv/bin/python -c "
from ch_bulk.companies_house.financials_staging import (
    load_financials_staging, _replay_financials_fetch_staging_file, replay_financials_fetch_staging,
)
from ch_bulk.companies_house.financials_enricher import (
    load_financials_staging, _replay_financials_fetch_staging_file, replay_financials_fetch_staging,
)
print('ok')
"
.venv/bin/python -m unittest tests.test_financials_enricher -v > /tmp/phase2_step4_financials_tests.txt 2>&1
tail -10 /tmp/phase2_step4_financials_tests.txt
grep -q "^OK$" /tmp/phase2_step4_financials_tests.txt
```

### Step 5 — Create `financials_pipeline.py`

Move:

```
DEFAULT_WORKERS, DEFAULT_PARSER_WORKERS, DEFAULT_BATCH_SIZE, DEFAULT_QUEUE_MAXSIZE,
DEFAULT_PARSE_INFLIGHT_PER_WORKER, PIPELINE_HEARTBEAT_INTERVAL_SECONDS
_validated_mode, _validated_batch_size, _validated_workers, _validated_parser_workers
_progress_text, _duration_text
_parser_process_main
enrich_financials
```

Add at top:
```python
from ch_bulk.companies_house.financials_contracts import (
    Mode, ParserProcessHandle, ParserProcessResponse,
    FetchedFinancialWorkItem, FinancialsResult,
    ERROR_PARSE_STATUSES,
    FINANCIALS_SYNC_TYPE, FINANCIALS_FETCH_SYNC_TYPE,
)
from ch_bulk.companies_house.financials_fetch import (
    CompaniesHouseFinancialsClient, _select_targets, _fetch_company_work_item,
)
from ch_bulk.companies_house.financials_staging import (
    _parse_fetched_row,  # used by _parser_process_main
    load_financials_staging, replay_financials_fetch_staging,
    _archive_financials_fetch_manifest, _mark_stale_running_batches,
)
```

Re-exports in `financials_enricher.py`:
```python
from ch_bulk.companies_house.financials_pipeline import enrich_financials
```

**Verify:**
```bash
.venv/bin/python -c "
from ch_bulk.companies_house.financials_pipeline import enrich_financials
from ch_bulk.companies_house.financials_enricher import enrich_financials
print('ok')
"
.venv/bin/python -c "
from ch_bulk.companies_house.financials_pipeline import _parser_process_main
print('ok')
"
.venv/bin/python -m unittest tests.test_financials_enricher -v > /tmp/phase2_step5_financials_tests.txt 2>&1
tail -10 /tmp/phase2_step5_financials_tests.txt
grep -q "^OK$" /tmp/phase2_step5_financials_tests.txt
```

### Step 6 — Verify facade is now minimal

The body of `ch_bulk/companies_house/financials_enricher.py` should now be ONLY:
- Module docstring
- `logger = logging.getLogger(__name__)` (keep facade logger for consistency; moved helpers still get new module logger names)
- 5 `from ch_bulk.companies_house.financials_<X> import (...)` blocks
- The thin `_process_company(...)` compatibility wrapper
- An `__all__` listing the public names (`enrich_financials`, `load_financials_staging`)

No other function bodies, no dataclass declarations, no constants.

**Verify:**
```bash
wc -l ch_bulk/companies_house/financials_enricher.py
# Should still be small (roughly < 120 lines)

# No class definitions in the facade:
grep -nE "^class " ch_bulk/companies_house/financials_enricher.py
# Expected: zero matches

# Exactly one function definition remains, the compatibility wrapper:
grep -nE "^def " ch_bulk/companies_house/financials_enricher.py
# Expected: one match, `def _process_company`

# Module-level logger preserved:
grep -n "logger = logging.getLogger" ch_bulk/companies_house/financials_enricher.py
# Expected: one match

.venv/bin/python -m unittest tests.test_financials_enricher -v > /tmp/phase2_step6_financials_tests.txt 2>&1
tail -10 /tmp/phase2_step6_financials_tests.txt
grep -q "^OK$" /tmp/phase2_step6_financials_tests.txt
# Same pass status as earlier targeted runs
```

### Step 7 — Test-imported private name audit + facade re-exports

Read `tests/test_financials_enricher.py:25-42` (or whatever line range the current code shows — Hard Rule #1). Enumerate every name imported from `ch_bulk.companies_house.financials_enricher`. For each one, confirm there's a corresponding re-export in the facade.

Expected names per codex's seam inventory:
```
IXBRL_RESOURCE, PDF_RESOURCE
CompaniesHouseFinancialsClient
FinancialTarget, FetchedFinancialRow, FetchedFinancialWorkItem,
ParsedFinancialFacts, StagedFinancialRow
_parse_ixbrl_bytes, _parse_pdf_bytes
_process_company
_replay_financials_fetch_staging_file
_select_latest_annual_accounts, _select_targets
enrich_financials, load_financials_staging
```

(There may be more — confirm by reading the actual test file.)

**Verify:**
```bash
# Every test import resolves:
.venv/bin/python -c "
import importlib
tests_module = importlib.import_module('tests.test_financials_enricher')
print('ok')
"

# Run the full test_financials_enricher suite:
.venv/bin/python -m unittest tests.test_financials_enricher -v > /tmp/phase2_step7_financials_tests.txt 2>&1
tail -10 /tmp/phase2_step7_financials_tests.txt
grep -q "^OK$" /tmp/phase2_step7_financials_tests.txt
# Expected: same pass count as baseline (parser tests pass, financials integration tests pass,
# 3 fixture-path-dependent tests already fixed in Phase 1.5 pass)
```

### Step 8 — Full suite verification

```bash
.venv/bin/python -m unittest discover -s tests > /tmp/phase2_post_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase2_post_tests.txt | tail -2
# Compare against baseline summary:
diff <(grep -E "^(Ran |FAILED|OK$)" /tmp/phase2_baseline_tests.txt | tail -2) <(grep -E "^(Ran |FAILED|OK$)" /tmp/phase2_post_tests.txt | tail -2)
# Expected: identical (80 run / 0 fail / 7 errors, where the 7 are Playwright pre-existing)
```

### Step 9 — Structural audit

```bash
# 5 new files exist:
ls ch_bulk/companies_house/financials_contracts.py
ls ch_bulk/companies_house/financials_parsers.py
ls ch_bulk/companies_house/financials_fetch.py
ls ch_bulk/companies_house/financials_staging.py
ls ch_bulk/companies_house/financials_pipeline.py

# Facade is small:
wc -l ch_bulk/companies_house/financials_enricher.py
# Expected: still small (roughly < 120 lines)

# No circular imports — none of the 5 new modules import from the facade:
grep -rn "from ch_bulk.companies_house.financials_enricher import" ch_bulk/companies_house/financials_*.py
# Expected: zero matches (none of the new modules should import from the facade)

# CQC / web / api / cli still work:
.venv/bin/python -c "from ch_bulk import ChBulk; ch = ChBulk(); print('ok')"
.venv/bin/ch-bulk --help > /dev/null && echo ok
```

### Step 10 — Behavioral diff vs baseline

```bash
.venv/bin/ch-bulk --help > /tmp/phase2_post_cli_help.txt 2>&1
diff /tmp/phase2_baseline_cli_help.txt /tmp/phase2_post_cli_help.txt
# Expected: empty

.venv/bin/ch-bulk info --db-path "$(pwd)/data/db/ch_bulk.duckdb" > /tmp/phase2_post_info.txt 2>&1 || true
diff /tmp/phase2_baseline_info.txt /tmp/phase2_post_info.txt
# Expected: empty

.venv/bin/python -c "from ch_bulk import ChBulk, SanityCheckError, SanityCheckResult, ensure_pipeline_schema; print('ok')"
.venv/bin/python -c "from ch_bulk.companies_house.financials_enricher import enrich_financials, load_financials_staging; print('ok')"
```

### Step 11 — Multiprocessing pickle smoke

`_parser_process_main` is invoked via `multiprocessing.Process` from `enrich_financials`, and the parser subprocess pipe pickles `FetchedFinancialRow`, `ParserProcessResponse`, and `StagedFinancialRow`. Verify both the function reference and representative payload objects are reachable from their new modules and round-trip through `pickle`.

```bash
.venv/bin/python -c "
import pickle
from ch_bulk.companies_house.financials_contracts import (
    FetchedFinancialRow, ParserProcessResponse, StagedFinancialRow,
)
from ch_bulk.companies_house.financials_pipeline import _parser_process_main
assert pickle.loads(pickle.dumps(_parser_process_main)).__module__ == 'ch_bulk.companies_house.financials_pipeline'

row = FetchedFinancialRow(
    company_number='12345678',
    accounts_last_made_up='2026-01-31',
    filing_id='fixture-one',
    filing_date='2026-03-12',
    filing_made_up_date='2026-01-31',
    paper_filed=False,
    filing_format='ixbrl',
    raw_path='/tmp/fixture.ixbrl',
    parse_status=None,
    parse_failure_reason=None,
    fetched_at='2026-05-27T00:00:00Z',
)
response = ParserProcessResponse(
    row=StagedFinancialRow(
        company_number='12345678',
        filing_id='fixture-one',
        filing_date='2026-03-12',
        filing_format='ixbrl',
        filing_period_start='2025-01-01',
        filing_period_end='2025-12-31',
        revenue=1.0,
        employee_count=2,
        gross_profit=3.0,
        profit_before_tax=4.0,
        profit_after_tax=5.0,
        fixed_assets=6.0,
        current_assets=7.0,
        total_assets=13.0,
        net_assets=8.0,
        net_current_assets=1.0,
        filing_age_months=3,
        parse_status='ok',
        parse_failure_reason=None,
        fetched_at='2026-05-27T00:00:00Z',
    ),
    error=None,
)
assert pickle.loads(pickle.dumps(row)).company_number == '12345678'
assert pickle.loads(pickle.dumps(response)).row.filing_id == 'fixture-one'
print('ok')
"
```

If this fails, `_parser_process_main` must stay at module-top-level, and the moved dataclasses must remain importable from a fresh interpreter under their new module paths.

### Step 12 — Behavioral verification (manual, GUI)

ASK USER to launch `.venv/bin/ch-bulk ui` and confirm:
- Window opens
- Left rail shows CH / CQC / Settings panes
- No Python tracebacks in terminal
- Each pane renders when clicked

User responds "GUI ok" or reports issues. Do not proceed to Step 14 until confirmed.

### Step 13 — Final diff review

```bash
git status
git diff --stat pre-phase-2-bookmark --
git diff pre-phase-2-bookmark -- ch_bulk/companies_house/financials_enricher.py | head -40
ls -la ch_bulk/companies_house/financials_*.py
wc -l ch_bulk/companies_house/financials_*.py
```

Surface to user:
- 5 new files added, sizes per `wc -l`
- 1 modified file (`financials_enricher.py` shrinks from 2,736 to roughly ~100 lines)
- Net diff is overwhelmingly move-heavy; small positive LOC drift from import blocks, `__all__`, and the `_process_company` compatibility wrapper is expected.
- No test changes expected (facade preserves all test imports).

### Step 14 — Commit (single, all-in-one)

ASK USER for final go-ahead, then:

```bash
git add -A
git commit -m "$(cat <<'EOF'
Phase 2: Split companies_house/financials_enricher.py into 5 modules + facade

Pure mechanical extraction. Function bodies move, signatures and behavior
unchanged. The 94KB file becomes:

- financials_enricher.py  (~100 lines, facade re-exporting public + test-imported names and keeping _process_company as a compatibility wrapper)
- financials_contracts.py  (dataclasses + sync-type/policy constants)
- financials_parsers.py    (iXBRL + PDF parsers — cleanest extraction)
- financials_fetch.py      (CH client + target/filing selection + raw-file save + fetch)
- financials_staging.py    (row adapters + replay + load + recovery; _parse_fetched_row HERE)
- financials_pipeline.py   (validation/progress helpers + _parser_process_main + enrich_financials)

Public Python API unchanged via facade. CLI surface unchanged. Test suite
result unchanged: 80 run / 0 fail / 7 errors (the 7 are pre-existing
Playwright/browser-launch failures, out of scope for Phase 2).

Decisions documented in docs/plans/2026-05-27-phase-2-design.md.
Notably: not a clean 3-way fetch/parse/load split — _parse_fetched_row is
dual-use (live parser + crash-recovery), so it stays in staging, not parsers.
EOF
)"
git log -1 --stat | head -40
git status
```

**Verify:** Single commit. Working tree clean. Tag `pre-phase-2-bookmark` still exists for rollback.

## Rollback contract

If any verify fails: stop. Revert tracked edits with `git restore --source=HEAD --staged --worktree ...`. Remove untracked module files before retrying. If state is unclear: `git reset --hard pre-phase-2-bookmark && git clean -fd` returns to the Phase 1.5 end state (`e666178`) and removes leftover new files.

## Failure modes and recovery

| Symptom | Likely cause | Fix |
|---|---|---|
| ImportError after Step N | A name was moved out of the facade but tests still import it | Add re-export to facade. Check `tests/test_financials_enricher.py:25-42` for the full import list. |
| Circular import detected | A new module imports from the facade, or `_parse_date` / `_process_company` got placed on the wrong side of the graph | Internal contract is `contracts -> parsers -> {fetch, staging} -> pipeline`; facade depends on all five and never the other way around. Keep `_process_company` as facade compatibility wrapper. |
| `_parser_process_main` fails in subprocess | Module not importable from fresh interpreter, or moved pipe payload dataclasses no longer pickle cleanly | Verify both `from ch_bulk.companies_house.financials_pipeline import _parser_process_main` and representative `FetchedFinancialRow` / `ParserProcessResponse` payloads round-trip through `pickle`. |
| Test asserts on `X.__module__` and breaks | Dataclass module path changed (Phase 2 is the first time this happens) | Either accept it (best — tests asserting on `__module__` are brittle) or add `__module__` alias in contracts. |
| GUI fails to launch | Indirect import path broken somewhere | Check `ch_bulk.api` imports `enrich_financials` and `load_financials_staging` from `financials_enricher` (the facade). |
| Whole phase looks broken | — | `git reset --hard pre-phase-2-bookmark && git clean -fd` |

## Handoff

When done, post DONE in this side room with:
- Final commit hash
- Test pass count (expected: 80/0/7, same as baseline)
- Brief discrepancy log if any (Hard Rule #1 finds)
- Tag `pre-phase-2-bookmark` preserved
