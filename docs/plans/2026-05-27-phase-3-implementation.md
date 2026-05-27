# Phase 3 implementation plan

**Companion to:** [`2026-05-27-phase-3-design.md`](./2026-05-27-phase-3-design.md) — rationale + symbol-to-module map
**For executor:** read this doc top-to-bottom. The design doc explains *what* and *why*; this doc gives you the *how* — exact commands, exact order, exact verification.

## Hard rules

1. **Code is the source of truth, not this doc.** If during execution you find the code doesn't match a doc claim (line numbers, symbol names, dependency edges, what a test imports), **trust the code, fold the discrepancy in, fix it, continue**. Note discrepancies in your final handoff post.
2. **Single commit at the end.** All steps below land in ONE git commit at the final step. Intermediate steps run verification but do NOT commit.
3. **No business-logic or dataflow changes.** Pure mechanical extraction. Function bodies move; signatures, return values, HTTP calls, DuckDB writes, LLM prompts stay identical. Expected metadata-only changes are moved `__module__` values, new logger channel names, and traceback/source-file locations. If you find a bug while moving code, leave it (note it for follow-up); don't fix it in this phase.
4. **Verify before next step.** Every step has a `Verify:` block. If any verify fails, stop. Revert tracked edits and remove untracked new module files before retrying. If the tree gets confusing, use the full rollback target below.
5. **Rollback target.** Step 0 creates a `pre-phase-3-bookmark` tag. If anything goes wrong: `git reset --hard pre-phase-3-bookmark && git clean -fd`.
6. **Facade preserves test imports AND test patches.** Every name currently imported by `tests/test_classifier.py:18-24` MUST resolve via `from ch_bulk.web.classifier import X` after the split. Every patch path currently used (`ch_bulk.web.classifier.requests.*`, `httpx.*`, `browser.*`) MUST still resolve via the facade's module-level imports of `requests`, `httpx`, `browser`. Step 7 enumerates and verifies.
7. **Ask the user for confirmation** at Step 12 (GUI smoke) and Step 14 (final commit).

## Pre-flight checklist (before Step 0)

- [ ] Confirm working tree is clean: `git status` shows "nothing to commit, working tree clean".
- [ ] If the two Phase 3 plan docs are still untracked, stop and commit them first as their own small docs commit before Step 0. Do **not** fold plan-doc additions into the Phase 3 refactor commit.
- [ ] Confirm you're on `trunk` branch: `git branch --show-current` prints `trunk`.
- [ ] Confirm the Phase 2 commit is in history: `git merge-base --is-ancestor b778306 HEAD && echo ok` prints `ok`.
- [ ] Confirm the bookmark tag does NOT already exist: `git tag --list pre-phase-3-bookmark` prints nothing.
- [ ] Confirm venv + deps: `source .venv/bin/activate && which python && .venv/bin/python -c "import duckdb, httpx, requests, trafilatura"` returns no errors.
- [ ] Confirm baseline test summary: `.venv/bin/python -m unittest discover -s tests > /tmp/phase3_preflight_tests.txt 2>&1 || true` then `grep -E "^(Ran |FAILED|OK$)" /tmp/phase3_preflight_tests.txt | tail -2` should show roughly `80 tests / 0 fail / 7 errors` (the 7 are pre-existing Playwright errors, out of scope; verify they're all in `test_classifier.py` since classifier is what we're splitting).
- [ ] Spot-check no JSONL data has hardcoded class paths: `find data -type f -name '*.jsonl' 2>/dev/null | head -n 3` then quick eyeball — if any line contains literal `"ch_bulk.web.classifier.StagedClassification"` as a class path that would `eval`/`import` at load time, surface to user before moving the dataclass.

If any pre-flight check fails, stop and surface to user.

## Execution steps

### Step 0 — Baseline capture + bookmark tag

```bash
.venv/bin/python -m unittest discover -s tests > /tmp/phase3_baseline_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase3_baseline_tests.txt | tail -2
.venv/bin/ch-bulk --help > /tmp/phase3_baseline_cli_help.txt 2>&1
.venv/bin/ch-bulk info --db-path "$(pwd)/data/db/ch_bulk.duckdb" > /tmp/phase3_baseline_info.txt 2>&1 || true
.venv/bin/python -c "from ch_bulk import ChBulk, SanityCheckError, SanityCheckResult, ensure_pipeline_schema; print('ok')" > /tmp/phase3_baseline_top_imports.txt 2>&1
.venv/bin/python -c "from ch_bulk.web.classifier import WebsiteClassifier, load_classification_staging, StagedClassification, insert_classification_batch, _extract_json_object; print('ok')" > /tmp/phase3_baseline_facade_imports.txt 2>&1
git tag pre-phase-3-bookmark HEAD
git tag --list pre-phase-3-bookmark
```

**Verify:** all 5 baseline files non-empty. Test-summary grep shows the expected baseline profile. Tag exists.

### Step 1 — Create `classifier_staging.py` (cleanest seam)

Move the following symbols from `ch_bulk/web/classifier.py` to `ch_bulk/web/classifier_staging.py`:

- Constants: `CLASSIFICATION_SYNC_TYPE`, `VERDICT_SQL_EXPRESSION`, `CLASSIFICATION_INSERT_SQL`, `CLASSIFICATION_BATCH_AGGREGATE_SQL`
- Dataclasses: `StagedClassification`, `StagedClassificationFileStats`
- `ClassificationChunkWriter`
- Helpers: `_batch_error_delta`, `_classification_batch_id_from_path`, `_pending_classification_staging_files`, `_classification_batch_totals`, `_classification_batch_progress`
- Batch lifecycle: `insert_classification_batch`, `finish_classification_batch`
- Loaders: `_scan_staged_classification_file`, `_load_classification_staging_file`, `load_classification_staging`

(Verify with code — codex's line ranges may have drifted. Source of truth is the actual file at HEAD.)

Add at top of `classifier_staging.py`:
```python
"""Website classification staging contract + batch lifecycle + DuckDB loader."""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb

from ch_bulk.core.paths import runs_dir
from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.db.staging import (
    LoadedBatch,
    STAGED_JSONL_SCAN_SQL,
    isoformat_utc,
    mark_staging_file_loaded,
    parse_staged_datetime,
    with_duckdb_connection,
)

logger = logging.getLogger(__name__)
```

(Exact import set: verify against what the moved functions actually use. Don't import unused names.)

In `classifier.py`, replace the moved declarations with re-exports:
```python
from ch_bulk.web.classifier_staging import (
    CLASSIFICATION_SYNC_TYPE,
    StagedClassification,
    StagedClassificationFileStats,
    ClassificationChunkWriter,
    _classification_batch_totals,
    _pending_classification_staging_files,
    insert_classification_batch,
    finish_classification_batch,
    load_classification_staging,
)
```

(Tests directly import 3 of these — `StagedClassification`, `insert_classification_batch`, `load_classification_staging`. `_classification_batch_totals` and `_pending_classification_staging_files` also stay imported temporarily because `classify()` still uses them until Step 4 moves the class.)

**Verify:**
```bash
.venv/bin/python -c "
from ch_bulk.web.classifier_staging import (
    CLASSIFICATION_SYNC_TYPE, StagedClassification, ClassificationChunkWriter,
    insert_classification_batch, finish_classification_batch, load_classification_staging,
)
from ch_bulk.web.classifier import (
    StagedClassification, insert_classification_batch, load_classification_staging,
)
print('ok')
"
.venv/bin/python -m unittest discover -s tests > /tmp/phase3_step1_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase3_step1_tests.txt | tail -2
diff <(grep -E "^(Ran |FAILED|OK$)" /tmp/phase3_baseline_tests.txt | tail -2) <(grep -E "^(Ran |FAILED|OK$)" /tmp/phase3_step1_tests.txt | tail -2)
```

### Step 2 — Create `classifier_content.py`

Move:
- Constants: `MIN_CLASSIFIABLE_TEXT_LEN`, `PAGE_PATHS`
- Dataclasses: `PageFetch`, `SiteContent`, `FallbackTask`
- Pure helpers: `_normalize_url`, `_page_url`, `_strip_https_www`, `_extract_markdown`

For the methods currently bound to `WebsiteClassifier` (`_fetch_page`, `_page_needs_playwright`, `_assemble_site_content`, `_fetch_site_pages_parallel`, `_collect_site_content`, `_collect_site_content_with_playwright`):
- Extract them into **module-level free functions** in `classifier_content.py`.
- Pass dependencies explicitly:
  - `_fetch_page` / `_fetch_site_pages_parallel` take a `requests.Session`
  - `_collect_site_content_with_playwright` takes a `browser.PlaywrightSession`
  - `_page_needs_playwright` / `_assemble_site_content` stay pure over `PageFetch` / `SiteContent`
- During the Step 2 transition, keep temporary delegating methods on `WebsiteClassifier` so the class still works before Step 4 moves it into `classifier_pipeline.py`.
- Do **not** use the rule "if it touches `self`, keep it as a method." In the live code these methods have no hidden queue/lock/stats dependencies; they only need explicit runtime resources passed in.

Add at top:
```python
"""Page fetch + Playwright fallback prep for website classification."""
from __future__ import annotations

import logging
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
import trafilatura

from ch_bulk.web import browser

logger = logging.getLogger(__name__)
```

In `classifier.py` facade, keep `import requests` and `from ch_bulk.web import browser` at top (so test patches still resolve at the facade path).

**Verify:**
```bash
.venv/bin/python -c "
from ch_bulk.web.classifier_content import (
    PageFetch, SiteContent, FallbackTask, MIN_CLASSIFIABLE_TEXT_LEN, PAGE_PATHS,
    _normalize_url, _page_url,
)
print('ok')
"
.venv/bin/python -m unittest tests.test_classifier > /tmp/phase3_step2_classifier_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase3_step2_classifier_tests.txt | tail -2
grep -c "^ERROR: test_" /tmp/phase3_step2_classifier_tests.txt
# Expected: same module-level profile as baseline classifier tests; 7 Playwright/browser-launch errors, nothing new.
```

### Step 3 — Create `classifier_llm.py`

Move:
- Constants: `VALID_VERDICTS`, `LLM_REQUEST_FAILURE_REASON`, `CLASSIFICATION_ERROR_REASONS`, `VERDICT_NORMALIZATION`, `PROMPT_TEMPLATE`
- Helpers: `_canonical_verdict`, `_extract_text_content`, `_parse_json_object_candidate`, `_extract_json_object`
- Extract `_call_llm`, `_unable_row`, and `_classify_site_content` into free functions too.
- Pass dependencies explicitly:
  - `_call_llm` takes `httpx.Client`, `model`, and `llm_config`
  - `_unable_row` takes `classifier_name`
  - `_classify_site_content` takes either the above explicit args or helper callables wired from them
- During the Step 3 transition, keep temporary delegating methods on `WebsiteClassifier` so the class still works before Step 4 moves it into `classifier_pipeline.py`.

Add at top:
```python
"""LLM prompt + call + JSON/verdict parsing for website classification."""
from __future__ import annotations

import ast
import json
import logging
import re
import time
from typing import Any

import httpx

from ch_bulk.db.staging import isoformat_utc
from ch_bulk.web.classifier_content import SiteContent
from ch_bulk.web.classifier_staging import StagedClassification

logger = logging.getLogger(__name__)
```

In `classifier.py` facade, add:
```python
from ch_bulk.web.classifier_llm import _extract_json_object
```

(Tests import `_extract_json_object` directly — must resolve via facade.)

Also in facade, keep `import httpx` at top (so `patch("ch_bulk.web.classifier.httpx.Client.post")` resolves).

**Verify:**
```bash
.venv/bin/python -c "
from ch_bulk.web.classifier_llm import (
    VALID_VERDICTS, PROMPT_TEMPLATE, _canonical_verdict, _extract_json_object,
)
from ch_bulk.web.classifier import _extract_json_object
print('ok')
"
.venv/bin/python -m unittest \
  tests.test_classifier.WebsiteClassifierTests.test_extract_json_object_accepts_strict_and_python_dict_syntax \
  > /tmp/phase3_step3_extract_json_test.txt 2>&1 || true
cat /tmp/phase3_step3_extract_json_test.txt
.venv/bin/python -m unittest tests.test_classifier > /tmp/phase3_step3_classifier_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase3_step3_classifier_tests.txt | tail -2
grep -c "^ERROR: test_" /tmp/phase3_step3_classifier_tests.txt
# Expected: same module-level profile as baseline classifier tests; 7 Playwright/browser-launch errors, nothing new.
```

### Step 4 — Create `classifier_pipeline.py`

Move the remaining production code:
- Type alias + constants: `Mode`, `DEFAULT_BATCH_SIZE`, `USER_AGENT`
- Validation helpers: `_validated_mode`, `_validated_batch_size`
- Progress formatting: `_progress_text`, `_duration_text`
- Input selection: `_normalized_company_urls`, `_select_company_inputs`
- `_classify_company`, `_classify_company_with_playwright`
- `WebsiteClassifier` class (resource/lifecycle helpers + orchestration methods)
- Remove the temporary Step 2/3 delegating methods once `_classify_company*` and `classify()` call the imported content/llm functions directly.

Add at top:
```python
"""Website classification orchestration — WebsiteClassifier class + select/classify helpers."""
from __future__ import annotations

import logging
import queue
import threading
import time
from pathlib import Path
from typing import Any, Literal

import duckdb
import httpx
import requests
from requests.adapters import HTTPAdapter

from ch_bulk.web import browser
from ch_bulk.core.logging import FsyncLineLogger
from ch_bulk.core.paths import DEFAULT_DATA_DIR, DEFAULT_DB_PATH
from ch_bulk.core.settings import load_settings
from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.db.staging import with_duckdb_connection
from ch_bulk.web.classifier_content import (
    FallbackTask,
    MIN_CLASSIFIABLE_TEXT_LEN,
    SiteContent,
    _collect_site_content,
    _collect_site_content_with_playwright,
    _normalize_url,
)
from ch_bulk.web.classifier_llm import (
    _classify_site_content,
    _unable_row,
)
from ch_bulk.web.classifier_staging import (
    CLASSIFICATION_SYNC_TYPE,
    ClassificationChunkWriter,
    StagedClassification,
    _classification_batch_totals,
    _pending_classification_staging_files,
    finish_classification_batch,
    insert_classification_batch,
    load_classification_staging,
)

logger = logging.getLogger(__name__)
Mode = Literal["incremental", "all", "list"]
```

(Exact import set: verify against what `WebsiteClassifier` actually uses. Don't import unused names.)

In `classifier.py` facade, add:
```python
from ch_bulk.web.classifier_pipeline import WebsiteClassifier
```

**Verify:**
```bash
.venv/bin/python -c "
from ch_bulk.web.classifier_pipeline import WebsiteClassifier
from ch_bulk.web.classifier import WebsiteClassifier
print('ok')
"
.venv/bin/python -m unittest tests.test_classifier > /tmp/phase3_step4_classifier_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase3_step4_classifier_tests.txt | tail -2
grep -c "^ERROR: test_" /tmp/phase3_step4_classifier_tests.txt
# Expected: same module-level profile as baseline classifier tests; 7 Playwright/browser-launch errors, nothing new.
```

### Step 5 — Verify facade is now minimal and complete

The body of `ch_bulk/web/classifier.py` should now be ONLY:
- Module docstring
- `from __future__ import annotations`
- `import requests`, `import httpx`, `from ch_bulk.web import browser` (so test patches resolve at facade path)
- Re-import blocks from the 4 new modules
- `import logging; logger = logging.getLogger(__name__)` (retain a facade-level logger for compatibility)
- `__all__` listing the public re-exports

No function bodies, no class definitions, no dataclasses, no constants.

Expected facade size: 40-80 lines.

**Verify:**
```bash
wc -l ch_bulk/web/classifier.py
# Should be < 100 lines

grep -E "^def |^class |^@dataclass" ch_bulk/web/classifier.py
# Expected: zero matches (no defs/classes/dataclasses in facade)

grep -n "logger = logging.getLogger" ch_bulk/web/classifier.py
# Expected: one match

grep -n "^import requests" ch_bulk/web/classifier.py
grep -n "^import httpx" ch_bulk/web/classifier.py
grep -n "^from ch_bulk.web import browser" ch_bulk/web/classifier.py
# Each: one match (needed for test patches)
```

### Step 6 — Patch-path resolution audit

The most important Phase 3 invariant: test monkey-patches at the facade path must still intercept calls. Verify each patch target:

```bash
.venv/bin/python -c "
import ch_bulk.web.classifier as facade

# Module-object patches (these are the ones tests use most):
assert hasattr(facade, 'requests'), 'facade.requests missing'
assert hasattr(facade, 'httpx'), 'facade.httpx missing'
assert hasattr(facade, 'browser'), 'facade.browser missing'

# Attribute paths used by tests:
assert hasattr(facade.requests, 'Session'), 'facade.requests.Session missing'
assert hasattr(facade.httpx, 'Client'), 'facade.httpx.Client missing'
assert hasattr(facade.browser, 'fetch_rendered'), 'facade.browser.fetch_rendered missing'
assert hasattr(facade.browser, 'is_playwright_available'), 'facade.browser.is_playwright_available missing'
assert hasattr(facade.browser, 'PlaywrightSession'), 'facade.browser.PlaywrightSession missing'

# Re-exported symbols (5 from tests/test_classifier.py:18-24):
assert hasattr(facade, 'WebsiteClassifier')
assert hasattr(facade, 'StagedClassification')
assert hasattr(facade, '_extract_json_object')
assert hasattr(facade, 'insert_classification_batch')
assert hasattr(facade, 'load_classification_staging')

# Same module objects? (critical for patches to intercept across modules)
import sys
assert facade.requests is sys.modules['requests']
assert facade.httpx is sys.modules['httpx']
assert facade.browser is sys.modules['ch_bulk.web.browser']

print('ok')
"
```

### Step 7 — Test-imported private name audit

Read `tests/test_classifier.py:18-24` (or wherever the imports actually live — Hard Rule #1). Enumerate every name imported from `ch_bulk.web.classifier`.

Expected names per codex's sweep:
```
StagedClassification
WebsiteClassifier
_extract_json_object
insert_classification_batch
load_classification_staging
```

Verify each resolves:
```bash
.venv/bin/python -c "
from ch_bulk.web.classifier import (
    StagedClassification, WebsiteClassifier,
    _extract_json_object, insert_classification_batch, load_classification_staging,
)
print('ok')
"
```

Also verify the `patch.object(classifier, '_classify_company', ...)` path still resolves to the class method (test does this in `test_classify_parallel_workers_preserve_complete_jsonl_rows`):
```bash
.venv/bin/python -c "
from ch_bulk.web import classifier
assert hasattr(classifier.WebsiteClassifier, '_classify_company'), 'WebsiteClassifier._classify_company missing'
print('ok')
"
```

### Step 8 — Full suite verification

```bash
.venv/bin/python -m unittest discover -s tests > /tmp/phase3_post_tests.txt 2>&1 || true
grep -E "^(Ran |FAILED|OK$)" /tmp/phase3_post_tests.txt | tail -2
diff <(grep -E "^(Ran |FAILED|OK$)" /tmp/phase3_baseline_tests.txt | tail -2) <(grep -E "^(Ran |FAILED|OK$)" /tmp/phase3_post_tests.txt | tail -2)
# Expected: empty diff (80 tests / 0 fail / 7 errors, where the 7 are Playwright pre-existing)
```

### Step 9 — Structural audit

```bash
# 4 new files exist:
ls ch_bulk/web/classifier_staging.py
ls ch_bulk/web/classifier_content.py
ls ch_bulk/web/classifier_llm.py
ls ch_bulk/web/classifier_pipeline.py

# Facade is small:
wc -l ch_bulk/web/classifier.py
# Expected: < 100 lines (probably 40-80)

# No circular imports — none of the 4 new modules import from the facade:
grep -rn "from ch_bulk.web.classifier import\|from ch_bulk.web import classifier" ch_bulk/web/classifier_*.py
# Expected: zero matches

# Verify the 4 modules form a clean DAG (no cycles):
.venv/bin/python -c "
import ast, sys
for mod in ['classifier_staging', 'classifier_content', 'classifier_llm', 'classifier_pipeline']:
    src = open(f'ch_bulk/web/{mod}.py').read()
    tree = ast.parse(src)
    deps = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith('ch_bulk.web.classifier'):
            deps.append(node.module)
    print(f'{mod}: depends on {deps}')
"
# Expected:
# - classifier_staging: depends on []
# - classifier_content: depends on []
# - classifier_llm: depends on ['ch_bulk.web.classifier_content', 'ch_bulk.web.classifier_staging']
# - classifier_pipeline: depends on ['ch_bulk.web.classifier_content', 'ch_bulk.web.classifier_llm', 'ch_bulk.web.classifier_staging']
```

### Step 10 — Behavioral diff vs baseline

```bash
.venv/bin/ch-bulk --help > /tmp/phase3_post_cli_help.txt 2>&1
diff /tmp/phase3_baseline_cli_help.txt /tmp/phase3_post_cli_help.txt
# Expected: empty

.venv/bin/ch-bulk info --db-path "$(pwd)/data/db/ch_bulk.duckdb" > /tmp/phase3_post_info.txt 2>&1 || true
diff /tmp/phase3_baseline_info.txt /tmp/phase3_post_info.txt
# Expected: empty

.venv/bin/python -c "from ch_bulk import ChBulk, SanityCheckError, SanityCheckResult, ensure_pipeline_schema; print('ok')"
.venv/bin/python -c "from ch_bulk.web.classifier import WebsiteClassifier, load_classification_staging, StagedClassification, insert_classification_batch, _extract_json_object; print('ok')"
```

### Step 11 — Classification smoke (instance lifecycle)

`WebsiteClassifier` owns `requests.Session` and `httpx.Client`. Verify instantiation works end-to-end without launching real network/LLM:

```bash
.venv/bin/python -c "
from ch_bulk.web.classifier import WebsiteClassifier
c = WebsiteClassifier()  # uses DEFAULT_DATA_DIR + DEFAULT_DB_PATH
assert c is not None
print('ok')
"
```

If this fails, the issue is likely an import-time side effect in `classifier_pipeline.py` (network call at module load, etc.) — investigate and fix per Hard Rule #1.

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
git diff --stat pre-phase-3-bookmark --
git diff pre-phase-3-bookmark -- ch_bulk/web/classifier.py | head -40
ls -la ch_bulk/web/classifier_*.py ch_bulk/web/classifier.py
wc -l ch_bulk/web/classifier_*.py ch_bulk/web/classifier.py
```

Surface to user:
- 4 new files added, sizes per `wc -l`
- 1 modified file (`classifier.py` shrinks from 1,801 to ~50-80 lines)
- Net production code delta: ~0 LOC (pure move + re-export adds)
- No test changes expected
- No SQL or data file changes

### Step 14 — Commit (single, all-in-one)

ASK USER for final go-ahead, then:

```bash
git add -A
git commit -m "$(cat <<'EOF'
Phase 3: Split web/classifier.py into 4 modules + thin facade

Pure mechanical extraction. Function bodies move, signatures and behavior
unchanged. The 66KB file becomes:

- classifier.py             (thin facade, ~50 lines, re-imports + re-exports)
- classifier_staging.py     (staging contract + batch lifecycle + DuckDB loader — cleanest extraction)
- classifier_content.py     (URL/page fetch + Playwright fallback prep)
- classifier_llm.py         (prompt + LLM call + JSON/verdict parsing)
- classifier_pipeline.py    (WebsiteClassifier class + orchestration)

Public Python API unchanged via facade. CLI surface unchanged. Test suite
result unchanged: 80 tests / 0 fail / 7 errors (the 7 are pre-existing
Playwright/browser-launch failures, out of scope for Phase 3).

The facade keeps module-level imports of requests, httpx, and browser so
existing test patches at ch_bulk.web.classifier.<module>.<attr> still
intercept calls without test edits.

Decisions documented in docs/plans/2026-05-27-phase-3-design.md.
EOF
)"
git log -1 --stat | head -40
git status
```

**Verify:** Single commit. Working tree clean. Tag `pre-phase-3-bookmark` still exists for rollback.

## Rollback contract

If any verify fails: stop. Revert tracked + remove untracked new module files. If state is unclear: `git reset --hard pre-phase-3-bookmark && git clean -fd` returns to the Phase 2 end state (`b778306`).

## Failure modes and recovery

| Symptom | Likely cause | Fix |
|---|---|---|
| ImportError on `_extract_json_object` after Step 3 | Facade re-export missing | Add `from ch_bulk.web.classifier_llm import _extract_json_object` to facade. |
| Test patch on `ch_bulk.web.classifier.requests.Session.get` doesn't intercept | Facade missing `import requests` | Add it. |
| Test patch on `ch_bulk.web.classifier.browser.fetch_rendered` doesn't intercept | Facade missing `from ch_bulk.web import browser` | Add it. Verify `facade.browser is sys.modules['ch_bulk.web.browser']`. |
| Circular import between content and llm | `classifier_llm` imports `SiteContent`, then `classifier_content` accidentally imports back from `classifier_llm` | Keep content as a leaf. If a cycle appears, move the llm-touching call back into pipeline; do **not** invent a fifth helper module. |
| `WebsiteClassifier` calls fail with `AttributeError: 'NoneType' has no attribute X` | A method was extracted as free function but still expected `self.something` | Convert back to method, or pass `self` attrs explicitly. |
| GUI fails to launch | Probably an import chain broken — `ch_bulk.api` imports through `ChBulk.classify()` → `WebsiteClassifier` | Read the chain top-down, find the missing re-export. |
| Whole phase looks broken | — | `git reset --hard pre-phase-3-bookmark && git clean -fd` |

## Handoff

When done, post DONE in this side room with:
- Final commit hash
- Test pass count (expected: 80 / 0 / 7, same as baseline)
- Brief discrepancy log if any (Hard Rule #1 finds)
- Tag `pre-phase-3-bookmark` preserved
