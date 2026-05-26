# Audit follow-up WP — code hygiene + parallelism fixes

Two adversarial audits (req-0065 pool-claude; req-0067 ad-hoc codex) identified Excel-era patterns and Single-writer assumptions that limit throughput and parallelism. Findings re-checked against current code; reflects post-req-0064 (JSONL refactor) state.

## Verdict map (10 findings still actionable)

| # | File:line | Severity | Status | Issue |
|---|---|---|---|---|
| 2 | staging.py:46,61-64; ch_enricher.py:528-531; cqc_api_enricher.py:598-602 | High | Partial | `pending_staging_files()` wildcard loads all `*.jsonl` for a sync_type at startup; consumes another process's in-flight file (parallel-unsafe) |
| 8 | matcher.py:761-763 | High | Open | `match_companies_to_cqc()` opens a write DuckDB connection without the `with_duckdb_connection` lock-retry wrapper; dies on conflict |
| 5 | cqc_api_client.py:43-48; cqc_api_enricher.py:692-742 | High | Open | Sync httpx, no HTTP/2; caps CQC at ~5 rps vs 33 rps API budget |
| 9 | bootstrap.py:122-139; sql/cqc/bootstrap_hsca_*.sql; sql/macros_and_views.sql:102-196 | Medium | Open | `ensure_pipeline_schema()` runs schema + CREATE INDEX + view rebuild in one transaction on every enrich start |
| 6 | matcher.py:698-724,798-815 | Medium | Open | `_insert_matches()` per-row INSERT OR REPLACE in loop; ~10K writes where one `executemany` suffices |
| 7 | ch_enricher.py:209-234,289-303,814-829 | Medium | Open | `enrich_revenue` is N+1: per-row SELECT + per-row INSERT; batched helpers exist (237-279, 306-323) but unused |
| 3 | processor.py:450-457; cqc_processor.py:648-655,748-755; api.py:127-132,248-253,307-312 | Medium | Partial | `compact=True` defaults at low-level and API entry points; three full-DB rebuilds per refresh cycle |
| 4 | _logging.py:78-87; staging.py:186-188 | Low | Open | Per-line write+flush remains on FsyncLineLogger and StagingWriter.append; ~20K syscalls per enrich run |
| 10 | rate_limit.py:25-49; cqc_api_client.py:49-54; ch_enricher.py:91-99 | Low | Open | `SlidingWindowThrottle` per-instance + non-thread-safe; blocks shared-budget design across processes |
| 12 | docs/plans/2026-05-24-homecare-pipeline.md:477-479 | Low | Open | Plan PipelinePane sequences match-after-API-enrich; matcher hard inputs are only companies + cqc_providers + cqc_hsca_locations (matcher.py:169-178) |

Findings dropped by re-check: #1 (with_duckdb_connection now read-only-aware), #11 (sanity-halt is consistent across CH/CQC/HSCA — pool-claude was wrong).

## Concrete fixes to ship in this WP

### Fix #2 — Namespaced staging files (Critical for parallel safety)
- `staging.py:46` `pending_staging_files()` — make `batch_id` required OR add a `.complete` marker file convention (writer touches `<batch>.complete` when JSONL is fully written; loader only consumes files with a marker)
- `ch_enricher.py:528-531` and `cqc_api_enricher.py:598-602` startup load — scope to current batch_id OR only files with `.complete` marker
- Test: two enrichers running concurrently, neither ingests the other's in-flight file

### Fix #8 — Matcher lock retry
- `matcher.py:761-763` — wrap the connection opening with the same `with_duckdb_connection` helper used by staging
- Test: matcher runs while another writer holds the lock; retries and succeeds

### Fix #5 — Async CQC client
- `cqc_api_client.py:43-48` — switch to `httpx.AsyncClient(http2=True)`
- `cqc_api_enricher.py:692-742` — convert inner loop to `asyncio.gather` with semaphore=10 (concurrent in-flight requests)
- Throttle: keep the per-process SlidingWindowThrottle but make it async-aware (asyncio.Lock around deque mutation). 33 req/sec API budget is the wall; concurrency=10 saturates it.
- DON'T touch CH client (`ch_enricher.py:91-99`) — CH rate limit is 2/sec, sync is fine
- Test: smoke run on 1000 providers, measure rps vs current sync baseline; target 25-30 rps

### Fix #9 — Idempotent schema bootstrap
- `bootstrap.py:122-139` — add a fingerprint check: a one-row `pipeline_schema_version` table; if version matches, skip the schema apply
- Bump version manually when SQL files change (semver-style)
- View rebuilds (`CREATE OR REPLACE VIEW`) are cheap and can stay; the expensive part is the CREATE INDEX + FK repair
- Test: second `ensure_pipeline_schema()` call short-circuits without touching DDL

### Fix #6 — Batch matcher writes
- `matcher.py:698-724` — replace per-row loop with `con.executemany("INSERT OR REPLACE INTO ch_cqc_matches VALUES (?, ?, ?, ?, ?, ?)", rows)`
- All within one transaction (already wrapped at L798-815)
- Test: matcher writes 1000 matches in <1s

### Fix #7 — Batch revenue enrichment
- `ch_enricher.py:814-829` `enrich_revenue` — rewrite as:
  - One `SELECT company_number, employee_count FROM company_enrichment WHERE revenue IS NULL AND employee_count IS NOT NULL`
  - Build (cn, est_revenue) pairs in memory using bands lookup
  - One `executemany` UPDATE: `UPDATE company_enrichment SET revenue=?, revenue_source='employee_band_lookup' WHERE company_number=?`
  - OR a single SQL UPDATE join against a bands table (faster; requires bands as a DuckDB table not Python list)
- Test: revenue enrich on 10K rows completes in <5s

### Fix #3 — Default compact=False
- `processor.py:450-457`, `cqc_processor.py:648-655,748-755`, `api.py:127-132,248-253,307-312` — change default to `compact=False`
- Add `ChBulk.compact()` public method that explicitly triggers compaction
- Update the GUI to expose a "Compact DB" button on the Settings pane
- Test: full pipeline run does not auto-compact; manual `ch.compact()` still works

### Fix #4 — Drop per-line flush
- `_logging.py:78-87` `FsyncLineLogger.write_line` — remove `.flush()` from per-line write; rely on Python's line-buffered stdout (`buffering=1`) which already flushes on `\n`. Keep `flush_and_fsync()` at batch boundaries.
- `staging.py:186-188` `StagingWriter.append` — same. Remove per-line flush; flush+fsync at batch boundaries only.
- Test: log lines still appear within ~seconds of write (line buffering); fsync still landing at checkpoints

### Fix #10 — Centralize throttle (small, future-proofing)
- `rate_limit.py:25-49` — add `asyncio.Lock` around deque mutation; document that the throttle is per-process. Inter-process shared budget deferred to a follow-up if we ever build multi-machine runners.
- Skip if Fix #5 (async client) already makes the throttle async-safe as a side effect — verify after Fix #5.

### Fix #12 — Plan DAG decoupling (doc only, no code)
- `docs/plans/2026-05-24-homecare-pipeline.md:477-479` — re-sequence PipelinePane: bulk → match (now) → CQC API enrich (parallel with director enrich) → re-run match (picks up API-only CH numbers) → seed websites → classify → tier view
- Document that match-before-enrich is supported and recommended for fast partial results

## Things explicitly NOT in this WP

- Multi-process shared throttle (requires inter-process IPC; defer until we run on multiple machines)
- DuckDB → Postgres migration (not warranted; DuckDB handles our scale)
- Replacing JSONL staging with a proper queue (Kafka/Celery — wildly overengineered for this)

## Verification
- All existing tests pass
- New tests per fix above
- Real smoke: full pipeline run (bulk sync skipped — already loaded) + match + CQC API enrich + CH director enrich, with CQC + CH in parallel. Report wall-clock + comparison to pre-fix baselines (CQC enrich was 45min for 29K providers; target post-async ~15min)
