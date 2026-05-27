# Phase 3: Split `web/classifier.py`

**Date:** 2026-05-27
**Status:** Design v1 (pre-review)
**Companion:** [`2026-05-27-phase-3-implementation.md`](./2026-05-27-phase-3-implementation.md) — verifiable execution checklist
**Predecessor:** Phase 2 commit `b778306` on `trunk`. Pre-Phase-3 bookmark: tag will be added in implementation Step 0.

## Problem

`ch_bulk/web/classifier.py` is 1,801 lines / 66 KB — the second-largest file in the codebase after the now-split `financials_enricher.py`. It owns:

- HTTP fetch + Playwright-fallback rendering
- LLM prompt construction + JSON response parsing + verdict normalization
- Per-company classification orchestration
- Parallel worker pool + chunked staging
- Incremental staging load + classification_batches lifecycle

Phase 1 flagged it as a Phase 3 target. Phase 2 proved the pattern (5-module split of `financials_enricher.py`) works.

## What the file actually is (per code sweep at `/tmp/codex_classifier_seams.md`)

A code-grounded sweep (req-0130) found:

- **`load_classification_staging` is cleanly extractable** — independent of website-fetch or LLM. Depends only on contracts + `db.staging` helpers + SQL.
- **URL/JSON helpers are mostly pure** — depend only on stdlib + `trafilatura` + constants.
- **`_classify_site_content` knows the `StagedClassification` schema** — LLM/parse layer already entangled with staging-row contract.
- **`_classify_company` mixes fetch policy with fallback handoff** — returns either a staged row OR a `FallbackTask` for the Playwright slow lane.
- **`classify()` loads chunks mid-run, not just at the end** — at startup, during execution (after each chunk closes), and on crash. Orchestration + load aren't separable into "do everything then load."
- **Playwright fallback is a dedicated slow lane** — one fallback thread, one shared `browser.PlaywrightSession()`, not just "fetch again."

A naïve 7-way split (`fetch / render / prompt / call / parse / stage / load / orchestrate`) would over-fragment and force dependency injection. Codex's recommendation: medium grain.

## Approach

4 modules behind a compatibility facade, all in `web/`:

```
ch_bulk/web/
├── classifier.py               # NOW: thin facade — re-imports + re-exports
├── classifier_staging.py       # NEW — staging contract + batch lifecycle + loader (cleanest extraction)
├── classifier_content.py       # NEW — URL/page fetch + Playwright fallback prep
├── classifier_llm.py           # NEW — prompt + LLM call + JSON/verdict parsing
└── classifier_pipeline.py      # NEW — WebsiteClassifier class + orchestration
```

**Why a facade, not a clean cut:** tests at `tests/test_classifier.py:18-24` import `StagedClassification`, `WebsiteClassifier`, `_extract_json_object`, `insert_classification_batch`, `load_classification_staging` from `ch_bulk.web.classifier`. Tests also patch `ch_bulk.web.classifier.requests.Session.get`, `ch_bulk.web.classifier.httpx.Client.post`, `ch_bulk.web.classifier.browser.fetch_rendered`, etc. The facade must:
1. Re-export the 5 imported names so test imports still resolve.
2. `import requests`, `import httpx`, `from ch_bulk.web import browser` at the top so `patch("ch_bulk.web.classifier.requests...")` still finds the same module objects (Python `sys.modules` ensures shared identity).

**Important difference from Phase 2:** classifier tests don't patch moved helper functions (no `patch("ch_bulk.web.classifier._classify_company")` etc.). They patch module-level imports and one instance method on the class. That means **no thin wrapper functions are needed** in the facade — re-imports + re-exports are sufficient. Phase 2's 7-wrapper-function facade was an artifact of financials tests; classifier tests are tamer.

**Extraction rule:** the content + llm clusters should become module-level functions that take their runtime dependencies explicitly:
- content functions take a `requests.Session` and, for the Playwright path, a `browser.PlaywrightSession`
- llm functions take an `httpx.Client`, `model`, `llm_config`, and `classifier_name`
- pipeline keeps the resource-owning + orchestration methods (`__init__`, lifecycle, input selection, `_classify_company`, `_classify_company_with_playwright`, `classify`)

The live code has no hidden queue/stats state inside `_fetch_page`, `_collect_site_content*`, `_call_llm`, `_unable_row`, or `_classify_site_content`. The queues, locks, counters, and fallback thread all live inside `classify()`. So the right boundary is "extract with explicit args", not "only extract methods that don't touch `self` at all."

## Non-goals

- **No business-logic or dataflow changes.** Pure mechanical extraction. Function bodies move; signatures, return values, DuckDB writes, HTTP calls, LLM prompts stay identical.
- **No fix for the 7 Playwright/browser-launch test errors.** They're pre-existing failures that fail at Playwright launch time (out of scope for Phase 3). They use this same test file — Phase 3 must preserve the same error shape, not improve or worsen it.
- **No new abstractions.** No base classes, no protocol types, no dependency injection.
- **No test-file edits.** Test imports continue to resolve via the facade.
- **No changes to `api.py` or `cli.py`.** They route through `ChBulk.classify()` which instantiates `WebsiteClassifier` — re-exported by the facade.
- **No changes to `web/browser.py`, `web/search.py`, `web/website_finder.py`.** These are siblings to `classifier.py`, not part of the split.
- **Single commit.** All 4 module extractions + facade rewrite land in one git commit.

## Symbol-to-module map

Per codex's seam inventory:

### `classifier_staging.py` (~500-650 LOC, cleanest extraction)
- Constants: `CLASSIFICATION_SYNC_TYPE`, `VERDICT_SQL_EXPRESSION`, `CLASSIFICATION_INSERT_SQL`, `CLASSIFICATION_BATCH_AGGREGATE_SQL`
- Dataclasses: `StagedClassification`, `StagedClassificationFileStats`
- Writer: `ClassificationChunkWriter`
- Helpers: `_batch_error_delta`, `_classification_batch_id_from_path`, `_pending_classification_staging_files`, `_classification_batch_totals`, `_classification_batch_progress`
- Batch lifecycle: `insert_classification_batch`, `finish_classification_batch`
- Loaders: `_scan_staged_classification_file`, `_load_classification_staging_file`, `load_classification_staging`

### `classifier_content.py` (~250-350 LOC)
- Constants: `MIN_CLASSIFIABLE_TEXT_LEN`, `PAGE_PATHS`
- Dataclasses: `PageFetch`, `SiteContent`, `FallbackTask`
- Pure helpers: `_normalize_url`, `_page_url`, `_strip_https_www`, `_extract_markdown`
- Free-function extractions of: `_fetch_page`, `_page_needs_playwright`, `_assemble_site_content`, `_fetch_site_pages_parallel`, `_collect_site_content`, `_collect_site_content_with_playwright`

### `classifier_llm.py` (~200-300 LOC)
- Constants: `VALID_VERDICTS`, `LLM_REQUEST_FAILURE_REASON`, `CLASSIFICATION_ERROR_REASONS`, `VERDICT_NORMALIZATION`, `PROMPT_TEMPLATE`
- Verdict helpers: `_canonical_verdict`
- LLM-text helpers: `_extract_text_content`, `_parse_json_object_candidate`, `_extract_json_object`
- Free-function extractions of: `_call_llm`, `_unable_row`, `_classify_site_content`

### `classifier_pipeline.py` (~600-750 LOC)
- Type alias + constants: `Mode`, `DEFAULT_BATCH_SIZE`, `USER_AGENT`
- Validation helpers: `_validated_mode`, `_validated_batch_size`
- Progress formatting: `_progress_text`, `_duration_text`
- Resource + lifecycle methods: `__init__`, `__enter__`, `__exit__`, `__del__`, `close`, `_require_http_session`, `_require_llm_client`, `_playwright_enabled`, `_classifier_workers`
- Input selection: `_normalized_company_urls`, `_select_company_inputs`
- Company-classify methods: `_classify_company`, `_classify_company_with_playwright`
- Class: `WebsiteClassifier` with `.classify(...)` method

### `classifier.py` (facade, ~40-80 LOC)
```python
"""Website classification — facade for compatibility with existing imports + patch targets."""
from __future__ import annotations

# Keep these module-level imports so test patches like
# patch("ch_bulk.web.classifier.requests.Session.get") still resolve.
import httpx
import requests

from ch_bulk.web import browser

# Re-export production + test-imported names from the new modules.
from ch_bulk.web.classifier_staging import (
    StagedClassification,
    insert_classification_batch,
    load_classification_staging,
)
from ch_bulk.web.classifier_llm import (
    _extract_json_object,
)
from ch_bulk.web.classifier_pipeline import (
    WebsiteClassifier,
)

# Keep a facade-level logger for compatibility with the old module shape.
import logging
logger = logging.getLogger(__name__)

__all__ = [
    "WebsiteClassifier",
    "StagedClassification",
    "load_classification_staging",
    "insert_classification_batch",
    "_extract_json_object",
]
```

## Cross-module imports

The 4 new modules form a small DAG:

```text
classifier_staging   (no internal deps — leaf)
classifier_content   (no internal deps — leaf)
classifier_llm       (imports classifier_content + classifier_staging)
       ↘     ↓     ↙
       classifier_pipeline   (imports all 3)
```

Actual dependency contract:

- `classifier_staging.py`: depends on `ch_bulk.db.*`, `ch_bulk.core.*`, stdlib, duckdb. NO classifier-* dep.
- `classifier_content.py`: depends on stdlib + `requests` + `trafilatura` + `ch_bulk.web.browser`. NO classifier-* dep.
- `classifier_llm.py`: depends on stdlib + `httpx` + `ch_bulk.db.staging.isoformat_utc` + `classifier_content.SiteContent` + `classifier_staging.StagedClassification`.
- `classifier_pipeline.py`: depends on stdlib + `duckdb` + `requests` + `httpx` + `ch_bulk.web.browser` + `ch_bulk.core.*` + `ch_bulk.db.*` + all 3 new classifier modules. It does **not** import `ch_bulk.api`.
- `classifier.py` (facade): imports `requests`, `httpx`, `browser` so test patches resolve. Re-exports from the 4 new modules.

No cycles. None of the 4 new modules import from the facade.

## Public API contract

Unchanged across Phase 3:

- **CLI surface:** same commands, same flags. `ch-bulk classify` invokes `ChBulk.classify()` which instantiates `WebsiteClassifier` via the facade.
- **GUI:** unchanged.
- **Python imports:**
  - `from ch_bulk.web.classifier import WebsiteClassifier` — works via facade re-export.
  - `from ch_bulk.web.classifier import load_classification_staging` — same.
  - `from ch_bulk.web.classifier import StagedClassification, insert_classification_batch, _extract_json_object` — work via facade re-exports.
- **Test monkey-patches:**
  - `patch("ch_bulk.web.classifier.requests.Session.get")` — works because facade `import requests`.
  - `patch("ch_bulk.web.classifier.httpx.Client.post")` — works because facade `import httpx`.
  - `patch("ch_bulk.web.classifier.browser.fetch_rendered")` — works because facade `from ch_bulk.web import browser`.
  - `patch("ch_bulk.web.classifier.browser.is_playwright_available")` — same.
  - `patch("ch_bulk.web.classifier.browser.PlaywrightSession")` — same.
  - `patch.object(classifier, "_classify_company", ...)` — works on a `WebsiteClassifier` instance created from the facade re-export.
- **Metadata drift that is acceptable:**
  - re-exported objects keep their import path compatibility, but their `__module__` strings change to the implementation module (`classifier_pipeline`, `classifier_staging`, `classifier_llm`)
  - log records emitted from moved code naturally come from the new module logger names, not `ch_bulk.web.classifier`
  - stack traces / `inspect.getsource()` point at the new files

The public API is the import path and runtime behavior, not the implementation module string or logger channel.

## Rationale

1. **Staging extraction is the cleanest seam.** Pure DuckDB + JSONL handling, no fetch or LLM dependency.
2. **Content and LLM grouped honestly with what travels together.** `_classify_site_content` already knows the staged-row contract; splitting prompt/call/parse apart would just create three files all importing the same types.
3. **Pipeline keeps the class.** `WebsiteClassifier` owns long-lived `requests.Session`, `httpx.Client`, and the fallback thread. Splitting class state across multiple files would be more complex than keeping it in one.
4. **Thin facade (no wrappers).** Classifier tests don't patch moved helpers, so re-imports + re-exports suffice. Facade ends up ~50 lines (vs Phase 2's 192).
5. **Single commit matches Phase 1/1.5/2 pattern.** Structural-only changes ship atomically with the `pre-phase-3-bookmark` rollback target.

## Risks

1. **Circular import via `SiteContent`.** `classifier_llm.py` will import `SiteContent` and `StagedClassification`; that direction is fine. The forbidden direction is `classifier_content -> classifier_llm`. Mitigation: keep content as a leaf; if a cycle appears, move the llm-touching call back up into pipeline rather than inventing a fifth helper module.

2. **Test patches at facade path must keep working.** Specific patches: `requests.Session.get`, `httpx.Client.post`, `browser.fetch_rendered`, `browser.is_playwright_available`, `browser.PlaywrightSession`. Each needs the corresponding module imported in the facade. Mitigation: implementation Step 7 explicitly verifies each patch path resolves.

3. **Implementation-module strings change for all moved objects.** `WebsiteClassifier`, `StagedClassification`, `load_classification_staging`, `insert_classification_batch`, `_extract_json_object`, and the moved helper dataclasses all keep import compatibility via the facade, but their `__module__` values point at the new file. Any code asserting those exact strings breaks. Verify there is no runtime path that serializes/imports those names dynamically.

4. **Explicit-dependency extraction can be botched if the args are incomplete.** `_fetch_page` needs a `requests.Session`; `_collect_site_content_with_playwright` needs a `browser.PlaywrightSession`; `_call_llm` needs `httpx.Client` + model/config; `_unable_row` needs `classifier_name`. Mitigation: pass those dependencies explicitly and keep `_classify_company` / `_classify_company_with_playwright` as the only class-owned call sites the tests care about.

5. **Playwright fallback thread creates `browser.PlaywrightSession()` inside `classifier_pipeline.py`.** Tests that mock `PlaywrightSession` at the facade path must still intercept the import resolved inside pipeline. Mitigation: pipeline imports `from ch_bulk.web import browser` (same shared module reference as the facade), then uses `browser.PlaywrightSession`. Patches on `ch_bulk.web.classifier.browser.PlaywrightSession` (facade path) AND `ch_bulk.web.classifier_pipeline.browser.PlaywrightSession` (pipeline path) both resolve via the shared `sys.modules["ch_bulk.web.browser"]`.

6. **Largest test file in the codebase.** `tests/test_classifier.py` is 1,038 lines, 12 test methods. After the split, the same 12 tests must run with the same outcomes (most pass; 7 Playwright errors stay errors; nothing changes shape). Mitigation: full suite verify after each module extraction.

## What this enables (later phases)

After Phase 3:
- Fix the 7 Playwright/browser-launch test errors (smaller, focused task — possibly headless setup or fixture changes).
- Extract a shared enricher base across all 5 enrichers (verify duplication first; financials and classifier both have batched-staging-write patterns).
- Migrate test imports from facade re-exports to direct sub-module paths (optional cleanup; not blocking anything).
- Web/`browser.py`, `web/search.py`, `web/website_finder.py` reorganization if warranted (none are over 30KB; probably fine as-is).
