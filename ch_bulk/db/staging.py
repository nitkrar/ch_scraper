"""JSONL staging helpers for parallel enrichment runs."""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TypeVar

import duckdb

from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.db.sync_batches import finish_sync_batch, update_sync_batch_progress

DB_LOCK_RETRY_ATTEMPTS = 120
DB_LOCK_RETRY_DELAY_SECONDS = 0.25

T = TypeVar("T")

STAGED_JSONL_SCAN_SQL = """
read_json(
    ?,
    format='newline_delimited',
    columns={
        entity_type:'VARCHAR',
        entity_id:'VARCHAR',
        fetched_at:'TIMESTAMP',
        http_status:'BIGINT',
        raw_json:'JSON'
    }
)
"""

RAW_API_RESPONSE_INSERT_SQL = f"""
INSERT OR IGNORE INTO cqc_api_responses (
    batch_id,
    entity_type,
    entity_id,
    fetched_at,
    scrape_date,
    http_status,
    raw_json
)
SELECT
    CAST(? AS UUID),
    entity_type,
    entity_id,
    fetched_at,
    CAST(fetched_at AS DATE),
    CAST(http_status AS INTEGER),
    raw_json
FROM {STAGED_JSONL_SCAN_SQL}
"""

_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def isoformat_utc(value: datetime | None = None) -> str:
    if value is None:
        value = datetime.now(timezone.utc)
    elif value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def parse_staged_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def staging_path(
    data_dir: str | Path,
    *,
    sync_type: str,
    batch_id: str,
) -> Path:
    stage_dir = Path(data_dir) / "staging"
    stage_dir.mkdir(parents=True, exist_ok=True)
    return stage_dir / f"{sync_type}_{batch_id}.jsonl"


def pending_staging_files(
    data_dir: str | Path,
    *,
    sync_type: str,
    batch_id: str | None = None,
) -> list[Path]:
    stage_dir = Path(data_dir) / "staging"
    if not stage_dir.exists():
        return []
    if batch_id is not None:
        candidate = stage_dir / f"{sync_type}_{batch_id}.jsonl"
        return [candidate] if candidate.exists() else []
    return sorted(stage_dir.glob(f"{sync_type}_*.jsonl"))


def batch_id_from_staging_path(sync_type: str, path: str | Path) -> str:
    name = Path(path).name
    prefix = f"{sync_type}_"
    suffix = ".jsonl"
    if not name.startswith(prefix) or not name.endswith(suffix):
        raise ValueError(f"Unexpected staging file for {sync_type}: {path}")
    return name[len(prefix) : -len(suffix)]


def truncate_incomplete_jsonl_tail(path: str | Path) -> int:
    file_path = Path(path)
    if not file_path.exists():
        return 0
    file_size = file_path.stat().st_size
    if file_size == 0:
        return 0

    truncated_bytes = 0
    with open(file_path, "rb+") as handle:
        last_good_offset = 0
        while True:
            line = handle.readline()
            if not line:
                break
            line_end = handle.tell()
            if not line.strip():
                last_good_offset = line_end
                continue
            try:
                json.loads(line)
            except json.JSONDecodeError:
                if line_end != file_size:
                    raise
                handle.truncate(last_good_offset)
                handle.flush()
                os.fsync(handle.fileno())
                truncated_bytes = file_size - last_good_offset
                break
            last_good_offset = line_end
    return truncated_bytes


def _is_lock_conflict(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(
        needle in text
        for needle in (
            "lock",
            "locked",
            "busy",
            "another process",
            "conflict",
            "concurrent",
        )
    )


def with_duckdb_connection(
    db_path: str | Path,
    callback: Callable[[duckdb.DuckDBPyConnection], T],
    *,
    read_only: bool = False,
    attempts: int = DB_LOCK_RETRY_ATTEMPTS,
    delay_seconds: float = DB_LOCK_RETRY_DELAY_SECONDS,
) -> T:
    last_exc: Exception | None = None
    for attempt in range(attempts):
        con: duckdb.DuckDBPyConnection | None = None
        try:
            con = duckdb.connect(str(db_path), read_only=read_only)
            return callback(con)
        except Exception as exc:
            last_exc = exc
            if not _is_lock_conflict(exc) or attempt == attempts - 1:
                raise
            time.sleep(delay_seconds)
        finally:
            if con is not None:
                con.close()
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("DuckDB connection retry loop exited without a result")


@dataclass(frozen=True)
class StagedAPIResponse:
    entity_type: str
    entity_id: str
    fetched_at: str
    http_status: int
    raw_json: Any

    @classmethod
    def from_json_line(cls, line: str) -> "StagedAPIResponse":
        payload = json.loads(line)
        return cls(
            entity_type=str(payload["entity_type"]),
            entity_id=str(payload["entity_id"]),
            fetched_at=str(payload["fetched_at"]),
            http_status=int(payload["http_status"]),
            raw_json=payload["raw_json"],
        )

    def to_json_line(self) -> str:
        return json.dumps(
            {
                "entity_type": self.entity_type,
                "entity_id": self.entity_id,
                "fetched_at": self.fetched_at,
                "http_status": self.http_status,
                "raw_json": self.raw_json,
            },
            ensure_ascii=True,
            sort_keys=True,
        )

    def fetched_at_value(self) -> datetime:
        return parse_staged_datetime(self.fetched_at)

    def raw_json_text(self) -> str:
        return json.dumps(self.raw_json, ensure_ascii=True, sort_keys=True)


class StagingWriter:
    """Append-only JSONL writer with per-line flush + explicit fsync."""

    def __init__(
        self,
        data_dir: str | Path,
        *,
        sync_type: str,
        batch_id: str,
    ) -> None:
        self.path = staging_path(
            data_dir,
            sync_type=sync_type,
            batch_id=batch_id,
        )
        self.buffering = 1
        self._handle = open(
            self.path,
            "a",
            encoding="utf-8",
            buffering=self.buffering,
        )

    def __enter__(self) -> "StagingWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def append(self, row: Any) -> None:
        self._handle.write(f"{row.to_json_line()}\n")
        self._handle.flush()

    def flush_and_fsync(self) -> None:
        self._handle.flush()
        os.fsync(self._handle.fileno())

    def close(self) -> None:
        self._handle.close()


@dataclass(frozen=True)
class LoadOutcome:
    records_updated: int
    error_count_delta: int = 0
    extra: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class LoadedBatch:
    batch_id: str
    path: str
    records_fetched: int
    records_updated: int
    error_count: int
    extra: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class StagedFileStats:
    total_rows: int
    success_rows: int
    invalid_entity_rows: int
    invalid_payload_rows: int


def summarize_loaded_batches(
    sync_type: str,
    results: list[LoadedBatch],
) -> dict[str, object]:
    extra_totals: dict[str, int] = {}
    for result in results:
        for key, value in result.extra.items():
            extra_totals[key] = extra_totals.get(key, 0) + value

    summary: dict[str, object] = {
        "sync_type": sync_type,
        "loaded_batches": len(results),
        "records_fetched": sum(result.records_fetched for result in results),
        "records_updated": sum(result.records_updated for result in results),
        "error_count": sum(result.error_count for result in results),
        "batch_ids": [result.batch_id for result in results],
        "loaded_paths": [result.path for result in results],
    }
    summary.update(extra_totals)
    return summary


def sync_batch_progress(
    con: duckdb.DuckDBPyConnection,
    batch_id: str,
) -> tuple[int, int, int]:
    row = con.execute(
        """
        SELECT
            COALESCE(records_fetched, 0),
            COALESCE(records_updated, 0),
            COALESCE(error_count, 0)
        FROM cqc_sync_batches
        WHERE batch_id = ?
        """,
        [batch_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"Unknown sync batch: {batch_id}")
    return (int(row[0]), int(row[1]), int(row[2]))


def _safe_sql_identifier(identifier: str) -> str:
    if not _SQL_IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Unsafe SQL identifier: {identifier}")
    return identifier


def create_temp_staged_api_responses(
    con: duckdb.DuckDBPyConnection,
    *,
    path: str | Path,
    table_name: str,
) -> str:
    safe_table_name = _safe_sql_identifier(table_name)
    con.execute(f"DROP TABLE IF EXISTS {safe_table_name}")
    con.execute(
        f"""
        CREATE TEMP TABLE {safe_table_name} AS
        SELECT
            entity_type,
            entity_id,
            fetched_at,
            CAST(http_status AS INTEGER) AS http_status,
            raw_json
        FROM {STAGED_JSONL_SCAN_SQL}
        """,
        [str(path)],
    )
    return safe_table_name


def staged_api_response_stats(
    con: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    expected_entity_type: str,
    success_json_type: str,
) -> StagedFileStats:
    safe_table_name = _safe_sql_identifier(table_name)
    row = con.execute(
        f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE http_status = 200) AS success_rows,
            COUNT(*) FILTER (WHERE entity_type != ?) AS invalid_entity_rows,
            COUNT(*) FILTER (
                WHERE http_status = 200
                  AND json_type(raw_json) != ?
            ) AS invalid_payload_rows
        FROM {safe_table_name}
        """,
        [expected_entity_type, success_json_type],
    ).fetchone()
    if row is None:
        return StagedFileStats(0, 0, 0, 0)
    return StagedFileStats(
        total_rows=int(row[0]),
        success_rows=int(row[1]),
        invalid_entity_rows=int(row[2]),
        invalid_payload_rows=int(row[3]),
    )


def insert_api_responses_from_stage(
    con: duckdb.DuckDBPyConnection,
    *,
    batch_id: str,
    table_name: str,
) -> None:
    safe_table_name = _safe_sql_identifier(table_name)
    con.execute(
        f"""
        INSERT OR IGNORE INTO cqc_api_responses (
            batch_id,
            entity_type,
            entity_id,
            fetched_at,
            scrape_date,
            http_status,
            raw_json
        )
        SELECT
            CAST(? AS UUID),
            entity_type,
            entity_id,
            fetched_at,
            CAST(fetched_at AS DATE),
            http_status,
            raw_json
        FROM {safe_table_name}
        """,
        [batch_id],
    )


def scan_staged_api_responses(
    path: str | Path,
    *,
    expected_entity_type: str,
    success_json_type: str,
) -> StagedFileStats:
    con = duckdb.connect(":memory:")
    try:
        row = con.execute(
            f"""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (WHERE CAST(http_status AS INTEGER) = 200) AS success_rows,
                COUNT(*) FILTER (WHERE entity_type != ?) AS invalid_entity_rows,
                COUNT(*) FILTER (
                    WHERE CAST(http_status AS INTEGER) = 200
                      AND json_type(raw_json) != ?
                ) AS invalid_payload_rows
            FROM {STAGED_JSONL_SCAN_SQL}
            """,
            [expected_entity_type, success_json_type, str(path)],
        ).fetchone()
    finally:
        con.close()

    if row is None:
        return StagedFileStats(0, 0, 0, 0)
    return StagedFileStats(
        total_rows=int(row[0]),
        success_rows=int(row[1]),
        invalid_entity_rows=int(row[2]),
        invalid_payload_rows=int(row[3]),
    )


def mark_staging_file_loaded(path: str | Path) -> Path:
    pending_path = Path(path)
    loaded_path = pending_path.with_name(f"{pending_path.name}.loaded")
    if loaded_path.exists():
        loaded_path.unlink()
    pending_path.rename(loaded_path)
    return loaded_path
