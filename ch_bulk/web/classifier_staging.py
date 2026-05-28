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

CLASSIFICATION_SYNC_TYPE = "classifications"
_BATCH_ERROR_REASONS = {
    "all_pages_unreachable",
    "llm_request_error",
    "parse_error",
}
VERDICT_SQL_EXPRESSION = """
CASE
    WHEN lower(trim(coalesce(raw_json->>'verdict', ''))) = 'majority domiciliary care'
    THEN 'Majority domiciliary'
    WHEN lower(trim(coalesce(raw_json->>'verdict', ''))) = 'majority domiciliary'
    THEN 'Majority domiciliary'
    WHEN lower(trim(coalesce(raw_json->>'verdict', ''))) = 'majority supported living'
    THEN 'Majority Supported living'
    WHEN lower(trim(coalesce(raw_json->>'verdict', ''))) = 'majority residential'
    THEN 'Majority residential'
    WHEN lower(trim(coalesce(raw_json->>'verdict', ''))) = 'mixed_domiciliary_supported'
    THEN 'Mixed_domiciliary_supported'
    WHEN lower(trim(coalesce(raw_json->>'verdict', ''))) = 'mixed residential domiciliary'
    THEN 'Mixed_residential_domiciliary'
    WHEN lower(trim(coalesce(raw_json->>'verdict', ''))) = 'mixed_residential_domiciliary'
    THEN 'Mixed_residential_domiciliary'
    WHEN lower(trim(coalesce(raw_json->>'verdict', ''))) = 'unable to classify'
    THEN 'Unable to classify'
    ELSE coalesce(nullif(trim(raw_json->>'verdict'), ''), 'Unable to classify')
END
"""
CLASSIFICATION_INSERT_SQL = f"""
INSERT OR REPLACE INTO classifications (
    company_number,
    source_type,
    verdict,
    verdict_reason,
    evidence_quote,
    source_url,
    classifier,
    classified_at,
    batch_id
)
SELECT
    entity_id AS company_number,
    'website' AS source_type,
    {VERDICT_SQL_EXPRESSION} AS verdict,
    raw_json->>'failure_reason' AS verdict_reason,
    raw_json->>'evidence' AS evidence_quote,
    raw_json->>'source_url' AS source_url,
    coalesce(raw_json->>'classifier', 'llm:unknown') AS classifier,
    CAST(fetched_at AS TIMESTAMP) AS classified_at,
    CAST(? AS UUID) AS batch_id
FROM read_json_auto(?, format='newline_delimited')
"""
CLASSIFICATION_BATCH_AGGREGATE_SQL = """
SELECT
    COUNT(*) AS records_updated,
    COUNT(*) FILTER (
        WHERE lower(trim(coalesce(verdict, ''))) = 'unable to classify'
    ) AS unable_count,
COUNT(*) FILTER (
    WHERE lower(trim(coalesce(verdict_reason, ''))) IN (
        'llm_request_error',
        'parse_error',
        'all_pages_unreachable'
    )
) AS error_count
FROM classifications
WHERE batch_id = CAST(? AS UUID)
"""


@dataclass(frozen=True)
class StagedClassification:
    entity_id: str
    entity_type: str
    fetched_at: str
    http_status: int | None
    raw_json: dict[str, Any]

    @classmethod
    def from_json_line(cls, line: str) -> "StagedClassification":
        payload = json.loads(line)
        http_status = payload.get("http_status")
        return cls(
            entity_id=str(payload["entity_id"]),
            entity_type=str(payload["entity_type"]),
            fetched_at=str(payload["fetched_at"]),
            http_status=int(http_status) if http_status is not None else None,
            raw_json=dict(payload["raw_json"]),
        )

    def to_json_line(self) -> str:
        return json.dumps(
            {
                "entity_id": self.entity_id,
                "entity_type": self.entity_type,
                "fetched_at": self.fetched_at,
                "http_status": self.http_status,
                "raw_json": self.raw_json,
            },
            ensure_ascii=True,
            sort_keys=True,
        )

    def fetched_at_value(self) -> datetime:
        return parse_staged_datetime(self.fetched_at)


@dataclass(frozen=True)
class StagedClassificationFileStats:
    total_rows: int
    invalid_entity_rows: int
    invalid_payload_rows: int


class ClassificationChunkWriter:
    """Write classifier rows to chunked JSONL files and finalize closed chunks."""

    def __init__(
        self,
        data_dir: str | Path,
        *,
        batch_id: str,
        lane: str,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.batch_id = batch_id
        self.lane = lane
        self.stage_dir = runs_dir(self.data_dir, CLASSIFICATION_SYNC_TYPE) / self.batch_id
        self.stage_dir.mkdir(parents=True, exist_ok=True)
        self._part = 1
        self._path: Path | None = None
        self._handle: Any | None = None
        self._rows_in_file = 0

    @property
    def rows_in_file(self) -> int:
        return self._rows_in_file

    def _next_path(self) -> Path:
        return self.stage_dir / f"lane-{self.lane}_p{self._part:04d}.jsonl.open"

    def _ensure_handle(self) -> None:
        if self._handle is not None:
            return
        self._path = self._next_path()
        self._handle = open(
            self._path,
            "a",
            encoding="utf-8",
            buffering=1,
        )

    def append(self, row: StagedClassification) -> None:
        self._ensure_handle()
        assert self._handle is not None
        self._handle.write(f"{row.to_json_line()}\n")
        self._handle.flush()
        self._rows_in_file += 1

    def flush_and_fsync(self) -> None:
        if self._handle is None:
            return
        self._handle.flush()
        os.fsync(self._handle.fileno())

    def finalize(self) -> Path | None:
        if self._handle is None or self._path is None:
            return None
        self.flush_and_fsync()
        self._handle.close()
        ready_path = Path(str(self._path)[:-5])
        if ready_path.exists():
            ready_path.unlink()
        self._path.rename(ready_path)
        self._handle = None
        self._path = None
        self._rows_in_file = 0
        self._part += 1
        return ready_path

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None


def _batch_error_delta(row: StagedClassification) -> int:
    failure_reason = str(row.raw_json.get("failure_reason") or "").strip().lower()
    if failure_reason in _BATCH_ERROR_REASONS:
        return 1
    if row.raw_json.get("error"):
        return 1
    return 0


def _classification_batch_id_from_path(path: str | Path) -> str:
    stage_path = Path(path)
    name = stage_path.name
    if name.endswith(".open"):
        name = name[:-5]
    if name.endswith(".loaded"):
        name = name[: -len(".loaded")]
    if stage_path.parent.parent.name != CLASSIFICATION_SYNC_TYPE or not name.endswith(".jsonl"):
        raise ValueError(f"Unexpected classification staging file: {path}")
    return stage_path.parent.name


def _pending_classification_staging_files(
    data_dir: str | Path,
    *,
    batch_id: str | None = None,
    include_open: bool = False,
) -> list[Path]:
    stage_dir = runs_dir(data_dir, CLASSIFICATION_SYNC_TYPE)
    if not stage_dir.exists():
        return []
    target_dir = stage_dir / batch_id if batch_id is not None else stage_dir
    if not target_dir.exists():
        return []
    patterns = ["*.jsonl"] if batch_id is not None else ["*/*.jsonl"]
    if include_open:
        patterns.append("*.jsonl.open" if batch_id is not None else "*/*.jsonl.open")
    paths: list[Path] = []
    for pattern in patterns:
        paths.extend(target_dir.glob(pattern))
    deduped = sorted({path.resolve(): path for path in paths}.values(), key=str)
    return deduped


def _classification_batch_totals(
    con: duckdb.DuckDBPyConnection,
    batch_id: str,
) -> tuple[int, int, int]:
    row = con.execute(
        CLASSIFICATION_BATCH_AGGREGATE_SQL,
        [batch_id],
    ).fetchone()
    if row is None:
        return (0, 0, 0)
    return (int(row[0]), int(row[1]), int(row[2]))


def insert_classification_batch(
    con: duckdb.DuckDBPyConnection,
    *,
    classifier: str,
    source_type: str,
    input_count: int,
    model_version: str,
) -> str:
    batch_id = str(uuid4())
    con.execute(
        """
        INSERT INTO classification_batches (
            batch_id,
            classifier,
            source_type,
            started_at,
            finished_at,
            status,
            input_count,
            classified_count,
            unable_count,
            error_count,
            model_version
        )
        VALUES (?, ?, ?, ?, NULL, 'running', ?, 0, 0, 0, ?)
        """,
        [
            batch_id,
            classifier,
            source_type,
            parse_staged_datetime(isoformat_utc()),
            input_count,
            model_version,
        ],
    )
    return batch_id


def finish_classification_batch(
    con: duckdb.DuckDBPyConnection,
    *,
    batch_id: str,
    status: str,
    classified_count: int | None = None,
    unable_count: int | None = None,
    error_count: int | None = None,
) -> None:
    con.execute(
        """
        UPDATE classification_batches
        SET
            finished_at = CASE
                WHEN ? THEN ?
                ELSE finished_at
            END,
            status = ?,
            classified_count = COALESCE(?, classified_count),
            unable_count = COALESCE(?, unable_count),
            error_count = COALESCE(?, error_count)
        WHERE batch_id = ?
        """,
        [
            status != "running",
            parse_staged_datetime(isoformat_utc()),
            status,
            classified_count,
            unable_count,
            error_count,
            batch_id,
        ],
    )


def _classification_batch_progress(
    con: duckdb.DuckDBPyConnection,
    batch_id: str,
) -> tuple[int, int, int]:
    row = con.execute(
        """
        SELECT
            COALESCE(input_count, 0),
            COALESCE(classified_count, 0),
            COALESCE(error_count, 0)
        FROM classification_batches
        WHERE batch_id = ?
        """,
        [batch_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"Unknown classification batch: {batch_id}")
    return (int(row[0]), int(row[1]), int(row[2]))


def _scan_staged_classification_file(
    path: str | Path,
) -> StagedClassificationFileStats:
    con = duckdb.connect(":memory:")
    try:
        row = con.execute(
            f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (
                    WHERE entity_type != ?
                ) AS invalid_entity_rows,
                COUNT(*) FILTER (
                    WHERE json_type(raw_json) != 'OBJECT'
                ) AS invalid_payload_rows
            FROM {STAGED_JSONL_SCAN_SQL}
            """,
            ["classification", str(path)],
        ).fetchone()
    finally:
        con.close()

    if row is None:
        return StagedClassificationFileStats(0, 0, 0)
    return StagedClassificationFileStats(
        total_rows=int(row[0]),
        invalid_entity_rows=int(row[1]),
        invalid_payload_rows=int(row[2]),
    )


def _load_classification_staging_file(
    db_path: str | Path,
    *,
    path: Path,
    finalize_batch: bool,
) -> LoadedBatch:
    batch_id = _classification_batch_id_from_path(path)

    def ensure_batch_exists(con: duckdb.DuckDBPyConnection) -> None:
        ensure_pipeline_schema(con)
        _classification_batch_progress(con, batch_id)

    with_duckdb_connection(db_path, ensure_batch_exists)

    try:
        staged_stats = _scan_staged_classification_file(path)
        if staged_stats.invalid_entity_rows:
            raise ValueError(
                f"Unexpected entity_type rows in classification staging: {path}"
            )
        if staged_stats.invalid_payload_rows:
            raise ValueError(
                "Expected object payloads in classification staging rows"
            )

        def run_load(con: duckdb.DuckDBPyConnection) -> tuple[int, int, int]:
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute(
                    CLASSIFICATION_INSERT_SQL,
                    [batch_id, str(path)],
                )
                (
                    records_updated,
                    unable_count,
                    error_count,
                ) = _classification_batch_totals(con, batch_id)
                finish_classification_batch(
                    con,
                    batch_id=batch_id,
                    status="succeeded" if finalize_batch else "running",
                    classified_count=records_updated,
                    unable_count=unable_count,
                    error_count=error_count,
                )
                con.execute("COMMIT")
                return (records_updated, unable_count, error_count)
            except Exception:
                con.execute("ROLLBACK")
                raise

        (
            records_updated,
            unable_count,
            error_count,
        ) = with_duckdb_connection(db_path, run_load)
    except Exception:

        def mark_failed(con: duckdb.DuckDBPyConnection) -> None:
            (
                records_updated,
                unable_count,
                error_count,
            ) = _classification_batch_totals(con, batch_id)
            finish_classification_batch(
                con,
                batch_id=batch_id,
                status="failed",
                classified_count=records_updated,
                unable_count=unable_count,
                error_count=error_count + 1,
            )

        with_duckdb_connection(db_path, mark_failed)
        raise

    loaded_path = mark_staging_file_loaded(path)
    return LoadedBatch(
        batch_id=batch_id,
        path=str(loaded_path),
        records_fetched=staged_stats.total_rows,
        records_updated=records_updated,
        error_count=error_count,
        extra={"unable_count": unable_count},
    )


def load_classification_staging(
    data_dir: str | Path,
    db_path: str | Path,
    *,
    batch_id: str | None = None,
    include_open: bool = False,
    finalize_batch: bool = True,
) -> dict[str, object]:
    data_dir = Path(data_dir)
    db_path = Path(db_path)
    loaded: list[LoadedBatch] = []
    loaded_paths: list[str] = []
    touched_batches: set[str] = set()

    for path in _pending_classification_staging_files(
        data_dir,
        batch_id=batch_id,
        include_open=include_open,
    ):
        result = _load_classification_staging_file(
            db_path,
            path=path,
            finalize_batch=finalize_batch,
        )
        loaded.append(result)
        loaded_paths.append(result.path)
        touched_batches.add(result.batch_id)

    batch_totals: list[tuple[str, int, int, int]] = []
    if touched_batches:

        def read_totals(
            con: duckdb.DuckDBPyConnection,
        ) -> list[tuple[str, int, int, int]]:
            totals: list[tuple[str, int, int, int]] = []
            for touched_batch_id in sorted(touched_batches):
                records_updated, unable_count, error_count = _classification_batch_totals(
                    con,
                    touched_batch_id,
                )
                totals.append(
                    (
                        touched_batch_id,
                        records_updated,
                        unable_count,
                        error_count,
                    )
                )
            return totals

        batch_totals = with_duckdb_connection(db_path, read_totals)

    return {
        "sync_type": CLASSIFICATION_SYNC_TYPE,
        "loaded_batches": len(touched_batches),
        "records_fetched": sum(result.records_fetched for result in loaded),
        "records_updated": sum(total[1] for total in batch_totals),
        "unable_count": sum(total[2] for total in batch_totals),
        "error_count": sum(total[3] for total in batch_totals),
        "batch_ids": sorted(touched_batches),
        "loaded_paths": loaded_paths,
    }
