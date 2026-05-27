# Phase 2: Split `companies_house/financials_enricher.py`

**Date:** 2026-05-27
**Status:** Design v2 (reviewed)
**Companion:** [`2026-05-27-phase-2-implementation.md`](./2026-05-27-phase-2-implementation.md) — verifiable execution checklist
**Predecessor:** Phase 1.5 commit `e666178` on `trunk`. Pre-Phase-2 bookmark: tag will be added in implementation Step 0.

## Problem

`ch_bulk/companies_house/financials_enricher.py` is 2,736 lines / 94 KB — the largest file in the codebase. It owns:

- request + selection
- raw filing download and manifesting
- iXBRL / PDF parsing
- fetched-row to staged-row adaptation
- staging replay / load + sync-batch recovery
- concurrent orchestration

Recent OCR + iXBRL extraction work piled on; navigating the file is now hard. Phase 1 flagged it as a Phase 2 split target.

## What the file actually is (per code sweep at `/tmp/codex_financials_seams.md`)

A code-grounded sweep (req-0127) refuted the original "clean 3-way fetch/parse/load" assumption:

- **`_parse_fetched_row` is dual-use** — called from both the live parser subprocess AND crash-recovery replay. The "parse" boundary spans both flows.
- **`_fetch_company_work_item` knows parser policy** — it decides terminal status `pdf_no_text_layer` for paper-filed and 406-fallback PDFs. Fetch knows about parse statuses.
- **`_select_targets` knows filesystem layout** — incremental mode checks the on-disk raw-file cache via `_raw_filing_path`.
- **`load_financials_staging` is not a pure loader** — it finalizes stale running batches, aggregates outcome counts.
- **`enrich_financials` spans recovery** — at startup it calls `replay_financials_fetch_staging(...)` + `load_financials_staging(...)` BEFORE selecting new work. At end it calls `load_financials_staging(..., batch_id=batch_id)` + `_archive_financials_fetch_manifest(...)`.

Forcing a literal `fetch.py / parse.py / load.py` split would create circular imports or a "misc adapters" file still too large.

## Approach

5 modules behind a compatibility facade, all in `companies_house/`:

```
ch_bulk/companies_house/
├── financials_enricher.py        # NOW: facade — re-exports public API + test-imported privates
├── financials_contracts.py       # NEW — dataclasses + policy constants + statuses
├── financials_parsers.py         # NEW — iXBRL + PDF parsers (cleanest extraction)
├── financials_fetch.py           # NEW — CH client + selection + raw save + fetch work item
├── financials_staging.py         # NEW — row adapters + replay + load + recovery
└── financials_pipeline.py        # NEW — _parser_process_main + enrich_financials orchestration
```

**Why a facade, not a clean cut:** `tests/test_financials_enricher.py:25-42` imports 10+ private symbols directly (`_parse_ixbrl_bytes`, `FetchedFinancialRow`, `_replay_financials_fetch_staging_file`, `_process_company`, etc.). Re-exports prevent a giant test-churn diff and let us migrate the test imports in a follow-up phase if we want.

## Non-goals

- **No business-logic or dataflow changes.** Pure mechanical extraction. Function bodies move; signatures, return values, database writes, network requests, and staging formats stay identical. Expected metadata-only changes are documented below.
- **No new abstractions.** No base classes, no protocol types, no DI containers. Don't change what the code does, just where it lives.
- **No test-file edits are expected.** Test imports continue to resolve via the facade.
- **No changes to `api.py` or `cli.py`.** They import only `enrich_financials` and `load_financials_staging`, both re-exported by the facade.
- **No changes to other enrichers.** `ch_enricher.py`, `revenue_model.py`, CQC/web enrichers are out of scope.
- **Single commit.** All 5 module extractions + facade rewrites land in one git commit.

## Symbol-to-module map

Per codex's seam inventory:

### `financials_contracts.py`
- `Mode`
- All dataclasses: `FinancialTarget`, `FilingCandidate`, `ParsedFinancialFacts`, `StagedFinancialRow`, `FetchedFinancialRow`, `FetchedFinancialWorkItem`, `FinancialsResult`, `ParserProcessResponse`, `ParserProcessHandle`, `FinancialsFileStats`
- Sync types: `FINANCIALS_SYNC_TYPE`, `FINANCIALS_FETCH_SYNC_TYPE`
- Policy constants: `FILED_REVENUE_SOURCES`, `TERMINAL_REVENUE_SOURCES`, `ERROR_PARSE_STATUSES`
- Resource constants: `IXBRL_RESOURCE`, `PDF_RESOURCE`, `IXBRL_EXTENSION`, `PDF_EXTENSION`

### `financials_parsers.py`
- All iXBRL fact-name groups (`REVENUE_FACTS`, `EMPLOYEE_COUNT_FACTS`, etc.) and `PERIOD_FACT_GROUPS`
- `IXBRL_PROFIT_LOSS_EXEMPTION_PATTERNS`
- All PDF regex packs (`REVENUE_REGEXES`, etc.)
- `_empty_parsed_financials`, `_missing_financial_reasons`, `_classify_financial_facts`
- `_parse_date` (parser helpers already call it directly; fetch and staging can import it upward)
- `_has_ixbrl_profit_loss_exemption_marker`
- All iXBRL parser helpers + `_parse_ixbrl_bytes`
- All PDF parser helpers + `_parse_pdf_bytes`
- `_derived_total_assets`

### `financials_fetch.py`
- `CH_API_BASE`, `DOCUMENT_API_BASE`
- Worker/retry/rate-limit constants: `DEFAULT_RETRY_AFTER_SECONDS`, `EFFECTIVE_CH_MAX_REQUESTS`, `CH_WINDOW_SECONDS`
- Filing type/description constants: `ANNUAL_ACCOUNTS_TYPES`, `ANNUAL_ACCOUNTS_DESCRIPTION_PREFIXES`
- `CompaniesHouseFinancialsClient`
- Helpers: `_http_status_from_exception`, `_normalize_company_ids`, `_filing_extension_for_format`, `_raw_filing_path`
- `_select_targets`, `_select_latest_annual_accounts`
- `_save_raw_filing`, `_build_fetched_row`, `_fetch_company_work_item`

### `financials_staging.py`
- `FINANCIALS_INSERT_SQL`, `FINANCIALS_SCAN_SUMMARY_SQL`
- `_months_between`
- `_build_row`, `_fetched_row_to_target`, `_fetched_row_to_filing`
- `_parse_fetched_row` (intentionally HERE not in parsers — recovery + live both go through this)
- `_salvage_truncated_staging_file`
- `_existing_staged_company_numbers`
- `_replay_financials_fetch_staging_file`, `replay_financials_fetch_staging`
- `_archive_financials_fetch_manifest`
- `_scan_staged_financials_file`
- `_sync_batch_status`, `_require_sync_batch`, `_mark_stale_running_batches`
- `_load_financials_staging_file`, `load_financials_staging`

### `financials_pipeline.py`
- Worker/batching/queue constants: `DEFAULT_WORKERS`, `DEFAULT_PARSER_WORKERS`, `DEFAULT_BATCH_SIZE`, `DEFAULT_QUEUE_MAXSIZE`, `DEFAULT_PARSE_INFLIGHT_PER_WORKER`, `PIPELINE_HEARTBEAT_INTERVAL_SECONDS`
- Validation helpers: `_validated_mode`, `_validated_batch_size`, `_validated_workers`, `_validated_parser_workers`
- Progress formatting: `_progress_text`, `_duration_text`
- `_parser_process_main`
- `enrich_financials`

### `financials_enricher.py` (facade)
- `from ch_bulk.companies_house.financials_pipeline import enrich_financials`
- `from ch_bulk.companies_house.financials_staging import load_financials_staging`
- Plus re-exports for every test-imported private name (see implementation Step 7).
- `_process_company` stays here as a thin compatibility wrapper around `_fetch_company_work_item` + `_parse_fetched_row`. It is test-only, it spans fetch and staging, and keeping it in the facade avoids inventing a fetch -> staging edge just for compatibility.
- Keeping `logger = logging.getLogger(__name__)` in the facade is fine for consistency, but it does **not** preserve old logger names for moved helpers by itself. Moved functions will log under their new module names unless we explicitly thread a shared logger through them, which this phase does not do.

## Cross-module imports

The 5 new modules form a DAG, but not a linear chain:

```text
contracts
   ↑
parsers
  ↙   ↘
fetch  staging
   \   /
  pipeline
```

Actual dependency contract:

- `financials_contracts.py`: no internal imports.
- `financials_parsers.py`: depends on contracts.
- `financials_fetch.py`: depends on contracts + parsers (`_parse_date` is shared upward from parsers).
- `financials_staging.py`: depends on contracts + parsers (`_parse_date`, `_parse_ixbrl_bytes`, `_parse_pdf_bytes`).
- `financials_pipeline.py`: depends on contracts + fetch + staging.
- `financials_enricher.py` facade sits above all five. None of the five new modules may import from the facade.

Import-time side effects are limited to logger construction, regex compilation, and constant / SQL definition. No module does network, database, or filesystem I/O at import time, so the DAG is safe from that angle too.

## Public API contract

Unchanged across Phase 2:

- **CLI surface:** same commands, same flags. `ch-bulk enrich-financials` invokes `enrich_financials` via the facade.
- **Python imports:**
  - `from ch_bulk.companies_house.financials_enricher import enrich_financials` — works via facade re-export.
  - `from ch_bulk.companies_house.financials_enricher import load_financials_staging` — same.
  - `from ch_bulk.companies_house.financials_enricher import _parse_ixbrl_bytes` etc. (test imports) — work via facade re-export.
- **Logger names:** moved helpers will log under their new module names such as `ch_bulk.companies_house.financials_fetch` or `...financials_pipeline`. The facade logger alone does not preserve historical helper logger names. This is an acceptable metadata-only change, but it should be called out rather than hand-waved away.
- **`__module__` / pickle paths:** `_parser_process_main.__module__` moves to `ch_bulk.companies_house.financials_pipeline`. `FetchedFinancialRow`, `StagedFinancialRow`, and `ParserProcessResponse` move to `ch_bulk.companies_house.financials_contracts`. The JSONL manifests are plain JSON and should not care, but the parser subprocess pipe definitely pickles those payloads, so Step 11 must verify the function and the payload dataclasses round-trip cleanly.
- **Import-time behavior:** regex compilation and module-level constants move, but there is still no import-time I/O.

## Rationale

1. **Parser extraction is the cleanest seam.** Pure bytes → `ParsedFinancialFacts`, no DB or filesystem.
2. **Contracts file is needed to avoid circular imports.** Every cluster references the dataclasses; pulling them into a shared module breaks any potential cycle.
3. **`_parse_date` belongs with parsers, not fetch.** Parser helpers already call it directly, and both fetch and staging can import it upward without creating a cycle.
4. **`_parse_fetched_row` stays in staging, not parsers.** It's a cross-flow adapter. Putting it in parsers would force parsers to know about `FetchedFinancialRow` (a manifest contract), tangling concerns again.
5. **`_process_company` stays as a facade compatibility wrapper.** It is test-only and composes fetch + staging. Keeping it out of the 5 internal modules avoids inventing a fetch -> staging dependency just for compatibility.
6. **Facade preserves test compatibility.** ~10 test-imported private symbols continue to resolve via re-export or the compatibility wrapper. Day-1 test diff is zero. Test imports can migrate to direct sub-module paths in a follow-up if/when we want.
7. **Single commit matches Phase 1 + 1.5 pattern.** Structural-only changes ship atomically.

## Risks

1. **Circular import via `_parse_fetched_row` or `_parse_date`.** If fetch or staging reach back into the facade, or if `_parse_date` stays in fetch while parsers still need it, we cycle. Mitigation: `financials_enricher.py` stays leaf-only, `_parse_date` lives in parsers, and `_process_company` remains the one facade wrapper instead of forcing fetch to import staging.

2. **Test imports break despite facade.** If a test does `from ch_bulk.companies_house.financials_enricher import X` where X doesn't get a re-export, ImportError. Mitigation: implementation Step 7 explicitly enumerates every test-imported name and adds a re-export for each.

3. **Multiprocessing path drift.** `_parser_process_main` moves modules, and the subprocess pipe payloads (`FetchedFinancialRow`, `ParserProcessResponse`, `StagedFinancialRow`) move too. `multiprocessing.Connection.send()` definitely pickles those payloads. Mitigation: verify both the function and representative payload objects round-trip via `pickle` in the smoke step.

4. **Log-name churn from sub-module loggers.** Each new sub-module creates `logger = logging.getLogger(__name__)` — that means new log names like `ch_bulk.companies_house.financials_pipeline`. Existing log consumers don't break (they grep on substrings) but log volume per channel changes. Acceptable.

5. **Largest pure-refactor diff so far.** ~2,000-2,600 changed lines across 6 files (1 shrinks dramatically, 5 new). Mostly mechanical moves. The verify gate at each step catches breakage early.

## What this enables (Phase 3+)

After Phase 2 lands:
- Splitting `web/classifier.py` (66 KB) — similar structure, similar approach.
- Extracting a shared enricher base if duplication across all 5 enrichers is real (verify first).
- Migrating test imports from facade re-exports to direct sub-module paths (optional cleanup).
- Fixing the 7 pre-existing Playwright/browser-launch test errors (independent, can run in parallel).
