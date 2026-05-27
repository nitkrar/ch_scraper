"""Schema bootstrap helpers for the homecare pipeline extensions.

The SQL files live in ``sql/`` so they remain runnable in DuckDB
directly. This module only orchestrates the order and the one piece of
branching the SQL should not own: ``tiered_targets`` depends on the CH
``companies`` table, so view creation is deferred until that table
exists.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from ch_bulk.core.paths import SQL_DIR

logger = logging.getLogger(__name__)
SCHEMA_FILES = [
    SQL_DIR / "cqc" / "bootstrap_hsca_locations.sql",
    SQL_DIR / "cqc" / "bootstrap_hsca_dual_registrations.sql",
    SQL_DIR / "bootstrap_pipeline.sql",
]
VIEWS_FILE = SQL_DIR / "macros_and_views.sql"


def _run_sql_file(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    **subs: str,
) -> None:
    sql = path.read_text()
    for key, value in subs.items():
        sql = sql.replace("{{" + key + "}}", str(value))
    con.execute(sql)


def _table_exists(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _column_exists(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    column_name: str,
) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = ?
          AND column_name = ?
        """,
        [table_name, column_name],
    ).fetchone()
    return bool(row and row[0])


def _ensure_column(
    con: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    column_name: str,
    column_type: str,
) -> None:
    if not _table_exists(con, table_name):
        return
    if _column_exists(con, table_name, column_name):
        return
    con.execute(
        f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
    )


def _rename_column_if_exists(
    con: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    old_name: str,
    new_name: str,
) -> None:
    if not _table_exists(con, table_name):
        return
    if not _column_exists(con, table_name, old_name):
        return
    if _column_exists(con, table_name, new_name):
        return
    con.execute(
        f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}"
    )


def _table_has_fk_to(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    referenced_table: str,
) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM duckdb_constraints()
        WHERE table_name = ?
          AND constraint_type = 'FOREIGN KEY'
          AND referenced_table = ?
        """,
        [table_name, referenced_table],
    ).fetchone()
    return bool(row and row[0])


def repair_pipeline_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Apply targeted in-place schema repairs for existing databases.

    This is intentionally narrower than ``ensure_pipeline_schema()``:
    it does not create the broader pipeline tables, it only fixes known
    on-disk schema issues that block runtime operations such as
    compaction. Safe to run repeatedly.
    """
    if not _table_exists(con, "cqc_hsca_dual_registrations"):
        return
    if not _table_has_fk_to(
        con,
        "cqc_hsca_dual_registrations",
        "cqc_hsca_locations",
    ):
        return

    logger.info(
        "Migrating cqc_hsca_dual_registrations to remove the HSCA FK"
    )
    con.execute(
        """
        CREATE TEMP TABLE cqc_hsca_dual_registrations_backup AS
        SELECT * FROM cqc_hsca_dual_registrations
        """
    )
    con.execute("DROP TABLE cqc_hsca_dual_registrations")
    _run_sql_file(
        con,
        SQL_DIR / "cqc" / "bootstrap_hsca_dual_registrations.sql",
    )
    con.execute(
        """
        INSERT INTO cqc_hsca_dual_registrations BY NAME
        SELECT * FROM cqc_hsca_dual_registrations_backup
        """
    )
    con.execute("DROP TABLE cqc_hsca_dual_registrations_backup")


def ensure_pipeline_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create the homecare pipeline schema objects if they are missing.

    Safe to run repeatedly. Table creation is fully idempotent. The
    views/macros file is only applied once the CH ``companies`` table
    exists; before that point, a fresh bootstrap still succeeds, but the
    derived views are intentionally deferred rather than creating a fake
    placeholder ``companies`` table that would break the real CH ingest.
    """
    con.execute("BEGIN TRANSACTION")
    try:
        for path in SCHEMA_FILES:
            logger.info("Applying schema SQL: %s", path.name)
            _run_sql_file(con, path)

        repair_pipeline_schema(con)
        _rename_column_if_exists(
            con,
            table_name="company_enrichment",
            old_name="profit_loss",
            new_name="profit_before_tax",
        )
        _rename_column_if_exists(
            con,
            table_name="company_enrichment",
            old_name="period_start_date",
            new_name="filing_period_start",
        )
        _rename_column_if_exists(
            con,
            table_name="company_enrichment",
            old_name="period_end_date",
            new_name="filing_period_end",
        )
        _ensure_column(
            con,
            table_name="company_enrichment",
            column_name="gross_profit",
            column_type="DOUBLE",
        )
        _ensure_column(
            con,
            table_name="company_enrichment",
            column_name="profit_before_tax",
            column_type="DOUBLE",
        )
        _ensure_column(
            con,
            table_name="company_enrichment",
            column_name="profit_after_tax",
            column_type="DOUBLE",
        )
        _ensure_column(
            con,
            table_name="company_enrichment",
            column_name="filing_period_start",
            column_type="DATE",
        )
        _ensure_column(
            con,
            table_name="company_enrichment",
            column_name="filing_period_end",
            column_type="DATE",
        )
        _ensure_column(
            con,
            table_name="company_enrichment",
            column_name="fixed_assets",
            column_type="DOUBLE",
        )
        _ensure_column(
            con,
            table_name="company_enrichment",
            column_name="current_assets",
            column_type="DOUBLE",
        )
        _ensure_column(
            con,
            table_name="company_enrichment",
            column_name="total_assets",
            column_type="DOUBLE",
        )
        _ensure_column(
            con,
            table_name="company_enrichment",
            column_name="net_assets",
            column_type="DOUBLE",
        )
        _ensure_column(
            con,
            table_name="company_enrichment",
            column_name="net_current_assets",
            column_type="DOUBLE",
        )
        _ensure_column(
            con,
            table_name="company_enrichment",
            column_name="filing_id",
            column_type="TEXT",
        )
        _ensure_column(
            con,
            table_name="company_enrichment",
            column_name="filing_format",
            column_type="TEXT",
        )
        _ensure_column(
            con,
            table_name="company_enrichment",
            column_name="filing_age_months",
            column_type="INTEGER",
        )

        if _table_exists(con, "companies"):
            logger.info("Applying macros/views SQL: %s", VIEWS_FILE.name)
            _run_sql_file(con, VIEWS_FILE)
        else:
            logger.info(
                "Skipping %s until the companies table exists",
                VIEWS_FILE.name,
            )

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
