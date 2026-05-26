"""Helpers for writing cqc_sync_batches rows."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import duckdb


def utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def insert_sync_batch(
    con: duckdb.DuckDBPyConnection,
    *,
    sync_type: str,
    mode: str,
) -> str:
    batch_id = str(uuid4())
    con.execute(
        """
        INSERT INTO cqc_sync_batches (
            batch_id,
            sync_type,
            mode,
            started_at,
            finished_at,
            status,
            records_fetched,
            records_updated,
            error_count
        )
        VALUES (?, ?, ?, ?, NULL, 'running', NULL, NULL, 0)
        """,
        [batch_id, sync_type, mode, utcnow_naive()],
    )
    return batch_id


def finish_sync_batch(
    con: duckdb.DuckDBPyConnection,
    batch_id: str,
    *,
    status: str,
    records_fetched: int | None = None,
    records_updated: int | None = None,
    error_count: int | None = None,
) -> None:
    con.execute(
        """
        UPDATE cqc_sync_batches
        SET
            finished_at = ?,
            status = ?,
            records_fetched = COALESCE(?, records_fetched),
            records_updated = COALESCE(?, records_updated),
            error_count = COALESCE(?, error_count)
        WHERE batch_id = ?
        """,
        [
            utcnow_naive(),
            status,
            records_fetched,
            records_updated,
            error_count,
            batch_id,
        ],
    )


def update_sync_batch_progress(
    con: duckdb.DuckDBPyConnection,
    batch_id: str,
    *,
    records_fetched: int,
    records_updated: int,
    error_count: int,
) -> None:
    con.execute(
        """
        UPDATE cqc_sync_batches
        SET
            status = 'running',
            records_fetched = ?,
            records_updated = ?,
            error_count = ?
        WHERE batch_id = ?
        """,
        [
            records_fetched,
            records_updated,
            error_count,
            batch_id,
        ],
    )
