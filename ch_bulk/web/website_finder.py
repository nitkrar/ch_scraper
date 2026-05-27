"""DDG-based website discovery with JSONL staging and bulk DuckDB load."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import duckdb

from ch_bulk.core.logging import FsyncLineLogger
from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.core.paths import DEFAULT_DATA_DIR, DEFAULT_DB_PATH
from ch_bulk.db.staging import (
    LoadedBatch,
    StagingWriter,
    batch_id_from_staging_path,
    isoformat_utc,
    mark_staging_file_loaded,
    pending_staging_files,
    summarize_loaded_batches,
    with_duckdb_connection,
)
from ch_bulk.db.sync_batches import finish_sync_batch, insert_sync_batch
from ch_bulk.web.search import (
    DEFAULT_MIN_SCORE,
    DuckDuckGoSearcher,
    SearchOutcome,
    build_company_query,
)

logger = logging.getLogger(__name__)

Mode = Literal["incremental", "all", "list"]
WEBSITE_FINDER_SYNC_TYPE = "website_finder"
DEFAULT_PAUSE_SECONDS = 0.7
WEBSITE_FINDER_FILE_STATS_SQL = """
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (
        WHERE nullif(trim(coalesce(picked_url, '')), '') IS NOT NULL
    ) AS picked_rows,
    COUNT(*) FILTER (
        WHERE lower(trim(coalesce(picked_reason, ''))) = 'no_match_above_threshold'
    ) AS no_match_rows,
    COUNT(*) FILTER (
        WHERE lower(trim(coalesce(picked_reason, ''))) = 'search_error'
    ) AS error_rows
FROM read_json_auto(?, format='newline_delimited')
"""
WEBSITE_FINDER_INSERT_SQL = """
WITH staged AS (
    SELECT
        trim(company_number) AS company_number,
        nullif(trim(picked_url), '') AS picked_url,
        CAST(fetched_at AS TIMESTAMP) AS fetched_at,
        coalesce(CAST(picked_reachable AS BOOLEAN), FALSE) AS picked_reachable
    FROM read_json_auto(?, format='newline_delimited')
    WHERE nullif(trim(picked_url), '') IS NOT NULL
),
ranked AS (
    SELECT
        company_number,
        picked_url,
        fetched_at,
        picked_reachable,
        ROW_NUMBER() OVER (
            PARTITION BY company_number
            ORDER BY fetched_at, picked_url
        ) AS company_row_num
    FROM staged
),
existing_primary AS (
    SELECT DISTINCT company_number
    FROM company_websites
    WHERE is_primary
)
INSERT OR IGNORE INTO company_websites (
    company_number,
    discovered_via,
    source_entity_id,
    url,
    is_primary,
    last_seen_reachable_at,
    discovered_at,
    discovered_by_batch
)
SELECT
    ranked.company_number,
    'web_search' AS discovered_via,
    NULL AS source_entity_id,
    ranked.picked_url AS url,
    CASE
        WHEN existing_primary.company_number IS NULL
         AND ranked.company_row_num = 1
        THEN TRUE
        ELSE FALSE
    END AS is_primary,
    CASE
        WHEN ranked.picked_reachable THEN ranked.fetched_at
        ELSE NULL
    END AS last_seen_reachable_at,
    ranked.fetched_at,
    CAST(? AS UUID)
FROM ranked
LEFT JOIN existing_primary USING (company_number)
"""


@dataclass(frozen=True)
class WebsiteTarget:
    company_number: str
    company_name: str
    postcode: str | None


@dataclass(frozen=True)
class StagedWebsiteSearch:
    company_number: str
    name: str
    postcode: str | None
    query: str
    fetched_at: str
    search_results: list[dict[str, object]]
    picked_url: str | None
    picked_reason: str
    picked_score: int | None
    picked_rank: int | None
    picked_reachable: bool | None
    min_score: int
    error: str | None = None

    @classmethod
    def from_json_line(cls, line: str) -> "StagedWebsiteSearch":
        payload = json.loads(line)
        return cls(
            company_number=str(payload["company_number"]),
            name=str(payload["name"]),
            postcode=(
                str(payload["postcode"])
                if payload.get("postcode") not in {None, ""}
                else None
            ),
            query=str(payload["query"]),
            fetched_at=str(payload["fetched_at"]),
            search_results=list(payload.get("search_results") or []),
            picked_url=(
                str(payload["picked_url"])
                if payload.get("picked_url") not in {None, ""}
                else None
            ),
            picked_reason=str(payload["picked_reason"]),
            picked_score=(
                int(payload["picked_score"])
                if payload.get("picked_score") not in {None, ""}
                else None
            ),
            picked_rank=(
                int(payload["picked_rank"])
                if payload.get("picked_rank") not in {None, ""}
                else None
            ),
            picked_reachable=(
                bool(payload["picked_reachable"])
                if payload.get("picked_reachable") is not None
                else None
            ),
            min_score=int(payload["min_score"]),
            error=(
                str(payload["error"])
                if payload.get("error") not in {None, ""}
                else None
            ),
        )

    @classmethod
    def from_outcome(
        cls,
        target: WebsiteTarget,
        outcome: SearchOutcome,
        *,
        fetched_at: str,
    ) -> "StagedWebsiteSearch":
        return cls(
            company_number=target.company_number,
            name=target.company_name,
            postcode=target.postcode,
            query=outcome.query,
            fetched_at=fetched_at,
            search_results=[
                result.to_payload() for result in outcome.search_results
            ],
            picked_url=outcome.picked_url,
            picked_reason=outcome.picked_reason,
            picked_score=outcome.picked_score,
            picked_rank=outcome.picked_rank,
            picked_reachable=outcome.picked_reachable,
            min_score=outcome.min_score,
            error=None,
        )

    def to_json_line(self) -> str:
        return json.dumps(
            {
                "company_number": self.company_number,
                "name": self.name,
                "postcode": self.postcode,
                "query": self.query,
                "fetched_at": self.fetched_at,
                "search_results": self.search_results,
                "picked_url": self.picked_url,
                "picked_reason": self.picked_reason,
                "picked_score": self.picked_score,
                "picked_rank": self.picked_rank,
                "picked_reachable": self.picked_reachable,
                "min_score": self.min_score,
                "error": self.error,
            },
            ensure_ascii=True,
            sort_keys=True,
        )


@dataclass(frozen=True)
class StagedWebsiteFileStats:
    total_rows: int
    picked_rows: int
    no_match_rows: int
    error_rows: int


@dataclass(frozen=True)
class PendingRunState:
    batch_id: str
    path: Path
    processed_company_numbers: set[str]
    total_rows: int
    error_rows: int


def _validated_mode(mode: str) -> Mode:
    normalized = mode.strip().lower()
    if normalized not in {"incremental", "all", "list"}:
        raise ValueError(f"Unsupported mode: {mode}")
    return normalized  # type: ignore[return-value]


def _validated_pause_seconds(pause_seconds: float) -> float:
    if pause_seconds < 0:
        raise ValueError("pause_seconds must be >= 0")
    return pause_seconds


def _progress_text(done: int, total: int) -> str:
    if total <= 0:
        return "0/0 0.0%"
    percent = (done / total) * 100
    return f"{done}/{total} {percent:.1f}%"


def _duration_text(seconds: float) -> str:
    rounded = max(0, int(round(seconds)))
    hours, remainder = divmod(rounded, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _scan_staged_file_stats(path: str | Path) -> StagedWebsiteFileStats:
    staged_path = Path(path)
    if not staged_path.exists() or staged_path.stat().st_size == 0:
        return StagedWebsiteFileStats(0, 0, 0, 0)

    con = duckdb.connect(":memory:")
    try:
        row = con.execute(
            WEBSITE_FINDER_FILE_STATS_SQL,
            [str(staged_path)],
        ).fetchone()
    finally:
        con.close()

    if row is None:
        return StagedWebsiteFileStats(0, 0, 0, 0)
    return StagedWebsiteFileStats(
        total_rows=int(row[0]),
        picked_rows=int(row[1]),
        no_match_rows=int(row[2]),
        error_rows=int(row[3]),
    )


def _read_pending_run_state(path: str | Path) -> PendingRunState:
    staged_path = Path(path)
    processed_company_numbers: set[str] = set()
    total_rows = 0
    error_rows = 0
    if staged_path.exists():
        with staged_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                staged = StagedWebsiteSearch.from_json_line(line)
                processed_company_numbers.add(staged.company_number)
                total_rows += 1
                if staged.picked_reason == "search_error":
                    error_rows += 1
    return PendingRunState(
        batch_id=batch_id_from_staging_path(WEBSITE_FINDER_SYNC_TYPE, staged_path),
        path=staged_path,
        processed_company_numbers=processed_company_numbers,
        total_rows=total_rows,
        error_rows=error_rows,
    )


def _load_website_finder_staging_file(
    db_path: str | Path,
    *,
    path: Path,
) -> LoadedBatch:
    batch_id = batch_id_from_staging_path(WEBSITE_FINDER_SYNC_TYPE, path)
    stats = _scan_staged_file_stats(path)
    final_records_fetched = stats.total_rows
    final_records_updated = 0
    final_error_count = stats.error_rows

    def ensure_batch_row(con: duckdb.DuckDBPyConnection) -> None:
        ensure_pipeline_schema(con)
        row = con.execute(
            """
            SELECT COUNT(*)
            FROM cqc_sync_batches
            WHERE batch_id = CAST(? AS UUID)
            """,
            [batch_id],
        ).fetchone()
        if row is None or not int(row[0]):
            raise ValueError(f"Unknown website_finder batch: {batch_id}")

    with_duckdb_connection(db_path, ensure_batch_row)

    try:
        def run_load(con: duckdb.DuckDBPyConnection) -> None:
            nonlocal final_records_updated

            ensure_pipeline_schema(con)
            con.execute("BEGIN TRANSACTION")
            try:
                if stats.total_rows:
                    con.execute(
                        WEBSITE_FINDER_INSERT_SQL,
                        [str(path), batch_id],
                    )
                    row = con.execute(
                        """
                        SELECT COUNT(*)
                        FROM company_websites
                        WHERE discovered_by_batch = CAST(? AS UUID)
                        """,
                        [batch_id],
                    ).fetchone()
                    final_records_updated = int(row[0]) if row else 0
                finish_sync_batch(
                    con,
                    batch_id,
                    status="succeeded",
                    records_fetched=final_records_fetched,
                    records_updated=final_records_updated,
                    error_count=final_error_count,
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

        with_duckdb_connection(db_path, run_load)
    except Exception:
        def mark_failed(con: duckdb.DuckDBPyConnection) -> None:
            finish_sync_batch(
                con,
                batch_id,
                status="failed",
                records_fetched=final_records_fetched,
                records_updated=final_records_updated,
                error_count=final_error_count + 1,
            )

        with_duckdb_connection(db_path, mark_failed)
        raise

    loaded_path = mark_staging_file_loaded(path)
    return LoadedBatch(
        batch_id=batch_id,
        path=str(loaded_path),
        records_fetched=final_records_fetched,
        records_updated=final_records_updated,
        error_count=final_error_count,
        extra={"no_match_count": stats.no_match_rows},
    )


def load_website_finder_staging(
    data_dir: str | Path,
    db_path: str | Path,
    *,
    batch_id: str | None = None,
) -> dict[str, object]:
    loaded: list[LoadedBatch] = []
    for path in pending_staging_files(
        data_dir,
        sync_type=WEBSITE_FINDER_SYNC_TYPE,
        batch_id=batch_id,
    ):
        loaded.append(
            _load_website_finder_staging_file(
                db_path,
                path=path,
            )
        )
    return summarize_loaded_batches(
        WEBSITE_FINDER_SYNC_TYPE,
        loaded,
    )


class WebsiteFinder:
    def __init__(
        self,
        data_dir: str | Path | None = None,
        db_path: str | Path | None = None,
        *,
        min_score: int = DEFAULT_MIN_SCORE,
    ) -> None:
        self.data_dir = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
        self.db_path = Path(db_path) if db_path is not None else DEFAULT_DB_PATH
        self.min_score = int(min_score)
        self.searcher = DuckDuckGoSearcher()

    def close(self) -> None:
        self.searcher.close()

    def __enter__(self) -> "WebsiteFinder":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _select_targets(
        self,
        con: duckdb.DuckDBPyConnection,
        *,
        mode: Mode,
        ids: list[str] | None,
    ) -> list[WebsiteTarget]:
        if mode == "list":
            if not ids:
                raise ValueError("mode=list requires one or more ids")
            ordered_ids = _dedupe_preserve_order(
                [value.strip() for value in ids if value.strip()]
            )
            placeholders = ", ".join(["?"] * len(ordered_ids))
            rows = con.execute(
                f"""
                SELECT company_number, company_name, postcode
                FROM companies
                WHERE company_number IN ({placeholders})
                """,
                ordered_ids,
            ).fetchall()
            row_by_company = {
                str(company_number): WebsiteTarget(
                    company_number=str(company_number),
                    company_name=str(company_name or ""),
                    postcode=str(postcode) if postcode not in {None, ""} else None,
                )
                for company_number, company_name, postcode in rows
            }
            return [
                row_by_company[company_number]
                for company_number in ordered_ids
                if company_number in row_by_company
            ]

        filters = ""
        if mode == "incremental":
            filters = """
            LEFT JOIN company_websites cw
              ON cw.company_number = m.company_number
            WHERE cw.company_number IS NULL
            """
        rows = con.execute(
            f"""
            SELECT DISTINCT
                c.company_number,
                c.company_name,
                c.postcode
            FROM current_company_match m
            JOIN companies c USING (company_number)
            {filters}
            ORDER BY c.company_number
            """
        ).fetchall()
        return [
            WebsiteTarget(
                company_number=str(company_number),
                company_name=str(company_name or ""),
                postcode=str(postcode) if postcode not in {None, ""} else None,
            )
            for company_number, company_name, postcode in rows
        ]

    def find(
        self,
        *,
        mode: str = "incremental",
        ids: list[str] | None = None,
        pause_seconds: float = DEFAULT_PAUSE_SECONDS,
    ) -> dict[str, object]:
        validated_mode = _validated_mode(mode)
        validated_pause_seconds = _validated_pause_seconds(pause_seconds)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        resumable = validated_mode == "incremental" and not ids
        pending_paths = pending_staging_files(
            self.data_dir,
            sync_type=WEBSITE_FINDER_SYNC_TYPE,
        )
        if len(pending_paths) > 1:
            raise ValueError(
                "Multiple pending website_finder staging files found; "
                "replay them with load-staging or clean them up before running again."
            )

        pending_state: PendingRunState | None = None
        if pending_paths:
            if resumable:
                pending_state = _read_pending_run_state(pending_paths[0])
            else:
                load_website_finder_staging(self.data_dir, self.db_path)

        batch_id: str | None = None
        targets: list[WebsiteTarget] = []
        resumed = pending_state is not None
        prior_processed = pending_state.total_rows if pending_state else 0
        total_fetched = prior_processed
        total_errors = pending_state.error_rows if pending_state else 0
        load_attempted = False
        run_log: FsyncLineLogger | None = None
        staging_writer: StagingWriter | None = None
        started_monotonic = time.monotonic()

        try:
            def prepare_batch(
                con: duckdb.DuckDBPyConnection,
            ) -> tuple[list[WebsiteTarget], str]:
                ensure_pipeline_schema(con)
                selected = self._select_targets(
                    con,
                    mode=validated_mode,
                    ids=ids,
                )
                if pending_state is not None:
                    row = con.execute(
                        """
                        SELECT COUNT(*)
                        FROM cqc_sync_batches
                        WHERE batch_id = CAST(? AS UUID)
                        """,
                        [pending_state.batch_id],
                    ).fetchone()
                    if row is None or not int(row[0]):
                        raise ValueError(
                            f"Unknown website_finder batch: {pending_state.batch_id}"
                        )
                    con.execute(
                        """
                        UPDATE cqc_sync_batches
                        SET
                            status = 'running',
                            finished_at = NULL
                        WHERE batch_id = CAST(? AS UUID)
                        """,
                        [pending_state.batch_id],
                    )
                    if pending_state.processed_company_numbers:
                        selected = [
                            target
                            for target in selected
                            if target.company_number
                            not in pending_state.processed_company_numbers
                        ]
                    return selected, pending_state.batch_id
                batch_id = insert_sync_batch(
                    con,
                    sync_type=WEBSITE_FINDER_SYNC_TYPE,
                    mode=validated_mode,
                )
                return selected, batch_id

            targets, batch_id = with_duckdb_connection(
                self.db_path,
                prepare_batch,
            )
            requested = prior_processed + len(targets)

            run_log = FsyncLineLogger(
                self.data_dir,
                sync_type=WEBSITE_FINDER_SYNC_TYPE,
                batch_id=batch_id,
                filename_prefix="website_finder",
            )
            run_log.write_line(
                f"{'resume' if resumed else 'start'} "
                f"sync_type={WEBSITE_FINDER_SYNC_TYPE} "
                f"mode={validated_mode} "
                f"requested={requested} "
                f"remaining={len(targets)} "
                f"pause_seconds={validated_pause_seconds:.1f} "
                f"min_score={self.min_score}"
            )
            run_log.flush_and_fsync()

            if requested == 0:
                def finalize_empty(con: duckdb.DuckDBPyConnection) -> None:
                    finish_sync_batch(
                        con,
                        batch_id,
                        status="succeeded",
                        records_fetched=0,
                        records_updated=0,
                        error_count=0,
                    )

                with_duckdb_connection(self.db_path, finalize_empty)
                run_log.write_line("complete requested=0 records_fetched=0 records_updated=0 no_match=0 errors=0 elapsed=0s")
                run_log.flush_and_fsync()
                return {
                    "batch_id": batch_id,
                    "requested": 0,
                    "records_fetched": 0,
                    "records_updated": 0,
                    "no_match_count": 0,
                    "error_count": 0,
                    "mode": validated_mode,
                    "pause_seconds": validated_pause_seconds,
                    "min_score": self.min_score,
                    "log_path": str(run_log.path),
                    "resumed": resumed,
                }

            staging_writer = StagingWriter(
                self.data_dir,
                sync_type=WEBSITE_FINDER_SYNC_TYPE,
                batch_id=batch_id,
            )

            for index, target in enumerate(targets, start=1):
                fetched_at = isoformat_utc()
                status = "error"
                picked_reason = "search_error"
                query = build_company_query(target.company_name, target.postcode)

                try:
                    if not target.company_name.strip():
                        raise ValueError("missing company_name")
                    outcome = self.searcher.search_company(
                        target.company_name,
                        target.postcode,
                        min_score=self.min_score,
                    )
                    staged = StagedWebsiteSearch.from_outcome(
                        target,
                        outcome,
                        fetched_at=fetched_at,
                    )
                    picked_reason = staged.picked_reason
                    if staged.picked_url:
                        status = "ok"
                    else:
                        status = "no_match"
                except Exception as exc:
                    logger.exception(
                        "DDG website search failed for company %s",
                        target.company_number,
                    )
                    staged = StagedWebsiteSearch(
                        company_number=target.company_number,
                        name=target.company_name,
                        postcode=target.postcode,
                        query=query,
                        fetched_at=fetched_at,
                        search_results=[],
                        picked_url=None,
                        picked_reason="search_error",
                        picked_score=None,
                        picked_rank=None,
                        picked_reachable=None,
                        min_score=self.min_score,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                    picked_reason = staged.picked_reason
                    total_errors += 1

                staging_writer.append(staged)
                staging_writer.flush_and_fsync()
                total_fetched += 1

                completed = prior_processed + index
                run_log.write_line(
                    f"company_number={target.company_number} "
                    f"status={status} "
                    f"picked_reason={picked_reason} "
                    f"progress={_progress_text(completed, requested)}"
                )
                run_log.flush_and_fsync()

                if index < len(targets) and validated_pause_seconds > 0:
                    time.sleep(validated_pause_seconds)

            load_attempted = True
            load_summary = load_website_finder_staging(
                self.data_dir,
                self.db_path,
                batch_id=batch_id,
            )
            records_fetched = int(load_summary["records_fetched"])
            records_updated = int(load_summary["records_updated"])
            no_match_count = int(load_summary.get("no_match_count", 0))
            error_count = int(load_summary["error_count"])
            elapsed_seconds = time.monotonic() - started_monotonic
            run_log.write_line(
                "complete "
                f"requested={requested} "
                f"records_fetched={records_fetched} "
                f"records_updated={records_updated} "
                f"no_match={no_match_count} "
                f"errors={error_count} "
                f"elapsed={_duration_text(elapsed_seconds)}"
            )
            run_log.flush_and_fsync()

            logger.info(
                "Website discovery complete: requested=%d fetched=%d updated=%d no_match=%d errors=%d batch_id=%s",
                requested,
                records_fetched,
                records_updated,
                no_match_count,
                error_count,
                batch_id,
            )
            return {
                "batch_id": batch_id,
                "requested": requested,
                "records_fetched": records_fetched,
                "records_updated": records_updated,
                "no_match_count": no_match_count,
                "error_count": error_count,
                "mode": validated_mode,
                "pause_seconds": validated_pause_seconds,
                "min_score": self.min_score,
                "log_path": str(run_log.path),
                "resumed": resumed,
            }
        except BaseException:
            if staging_writer is not None:
                staging_writer.flush_and_fsync()
            if batch_id is not None and not load_attempted:
                def mark_failed(con: duckdb.DuckDBPyConnection) -> None:
                    finish_sync_batch(
                        con,
                        batch_id,
                        status="failed",
                        records_fetched=total_fetched,
                        records_updated=0,
                        error_count=total_errors + 1,
                    )

                with_duckdb_connection(self.db_path, mark_failed)
            if run_log is not None:
                elapsed_seconds = time.monotonic() - started_monotonic
                run_log.write_line(
                    "crash "
                    f"records_fetched={total_fetched} "
                    f"records_updated=0 "
                    f"errors={total_errors + 1} "
                    f"elapsed={_duration_text(elapsed_seconds)}"
                )
                run_log.flush_and_fsync()
            raise
        finally:
            if run_log is not None:
                run_log.close()
            if staging_writer is not None:
                staging_writer.close()
