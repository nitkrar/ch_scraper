"""Query the Companies House DuckDB database by SIC code."""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from ch_bulk.db.bootstrap import recover_interrupted_compaction

logger = logging.getLogger(__name__)


def _normalise_sic_codes(sic_codes: str | list[str]) -> list[str]:
    """Normalise SIC codes input to a flat list of strings.

    Args:
        sic_codes: A single SIC code string, a comma-separated string,
            or a list of SIC code strings.

    Returns:
        List of individual SIC code strings.
    """
    if isinstance(sic_codes, str):
        return [s.strip() for s in sic_codes.split(",") if s.strip()]
    return [str(s).strip() for s in sic_codes]


def _build_sic_where(sic_codes: list[str]) -> tuple[str, list[str]]:
    """Build a WHERE clause fragment matching any of the 4 SIC columns.

    Uses parameterized placeholders to prevent SQL injection.

    Args:
        sic_codes: List of SIC code strings to match.

    Returns:
        Tuple of (SQL WHERE fragment, list of parameter values).
    """
    placeholders = ", ".join("?" for _ in sic_codes)
    conditions = " OR ".join(
        f"sic_code_{i} IN ({placeholders})" for i in range(1, 5)
    )
    # Each condition repeats the full set of sic_codes
    params = sic_codes * 4
    return f"({conditions})", params


def query_by_sic(
    db_path: str | Path,
    sic_codes: str | list[str],
    status: str | None = "Active",
    limit: int | None = None,
) -> list[dict]:
    """Query companies by SIC code.

    Searches across all four SIC code columns (a company can have up
    to 4 SIC codes).  Uses parameterized queries to prevent SQL injection.

    Args:
        db_path: Path to the DuckDB database file.
        sic_codes: One or more SIC codes to search for.  Can be a single
            string, a comma-separated string, or a list of strings.
        status: Filter by company status (e.g. ``"Active"``).
            Pass ``None`` to include all statuses.
        limit: Maximum number of results to return, or ``None`` for all.

    Returns:
        List of company records as dictionaries.

    Raises:
        FileNotFoundError: If the database file does not exist.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    codes = _normalise_sic_codes(sic_codes)
    where_clause, params = _build_sic_where(codes)

    sql = f"SELECT * FROM companies WHERE {where_clause}"
    if status:
        sql += " AND company_status = ?"
        params.append(status)
    sql += " ORDER BY company_name"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        result = con.execute(sql, params)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        con.close()


def export_query_csv(
    db_path: str | Path,
    sic_codes: str | list[str],
    output_path: str | Path,
    status: str | None = "Active",
    limit: int | None = None,
) -> int:
    """Export SIC code query results directly to a CSV file.

    Uses DuckDB's native COPY to stream results to disk without
    loading them into Python memory.

    Args:
        db_path: Path to the DuckDB database file.
        sic_codes: One or more SIC codes to search for.
        output_path: Path for the output CSV file.
        status: Filter by company status, or ``None`` for all.
        limit: Maximum number of results, or ``None`` for all.

    Returns:
        Number of rows exported.

    Raises:
        FileNotFoundError: If the database file does not exist.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    codes = _normalise_sic_codes(sic_codes)
    where_clause, params = _build_sic_where(codes)

    inner_sql = f"SELECT * FROM companies WHERE {where_clause}"
    if status:
        inner_sql += " AND company_status = ?"
        params.append(status)
    inner_sql += " ORDER BY company_name"
    if limit is not None:
        inner_sql += f" LIMIT {int(limit)}"

    # Escape path for safe SQL embedding (backslashes then single quotes)
    safe_path = str(output_path).replace("\\", "\\\\").replace("'", "''")

    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        # Materialize into temp table (DuckDB doesn't allow params in CREATE VIEW)
        con.execute("DROP TABLE IF EXISTS _export_tmp")
        con.execute(f"CREATE TEMP TABLE _export_tmp AS {inner_sql}", params)
        con.execute(
            f"COPY _export_tmp TO '{safe_path}' (HEADER, DELIMITER ',')"
        )
        row_count: int = con.execute(
            "SELECT COUNT(*) FROM _export_tmp"
        ).fetchone()[0]  # type: ignore[index]
        con.execute("DROP TABLE IF EXISTS _export_tmp")

        logger.info("Exported %d records to %s", row_count, output_path)
        return row_count
    finally:
        con.close()


def get_db_info(db_path: str | Path) -> dict:
    """Get summary statistics about the Companies House database.

    Args:
        db_path: Path to the DuckDB database file.

    Returns:
        Dictionary with keys:

        - ``total_companies`` (int): Total number of company records.
        - ``status_breakdown`` (dict[str, int]): Count per company status.
        - ``top_sic_codes`` (list[dict]): Top 20 SIC codes with counts.

    Raises:
        FileNotFoundError: If the database file does not exist.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        # Total companies
        total: int = con.execute(
            "SELECT COUNT(*) FROM companies"
        ).fetchone()[0]  # type: ignore[index]

        # Status breakdown
        status_rows = con.execute(
            "SELECT company_status, COUNT(*) AS cnt "
            "FROM companies "
            "GROUP BY company_status "
            "ORDER BY cnt DESC"
        ).fetchall()
        status_breakdown = {row[0]: row[1] for row in status_rows}

        # Top SIC codes — union across all 4 columns
        top_sic = con.execute("""
            SELECT sic_code, COUNT(*) AS cnt
            FROM (
                SELECT sic_code_1 AS sic_code FROM companies WHERE sic_code_1 IS NOT NULL
                UNION ALL
                SELECT sic_code_2 FROM companies WHERE sic_code_2 IS NOT NULL
                UNION ALL
                SELECT sic_code_3 FROM companies WHERE sic_code_3 IS NOT NULL
                UNION ALL
                SELECT sic_code_4 FROM companies WHERE sic_code_4 IS NOT NULL
            )
            GROUP BY sic_code
            ORDER BY cnt DESC
            LIMIT 20
        """).fetchall()
        top_sic_codes = [
            {"sic_code": row[0], "count": row[1]} for row in top_sic
        ]

        return {
            "total_companies": total,
            "status_breakdown": status_breakdown,
            "top_sic_codes": top_sic_codes,
        }

    finally:
        con.close()


# ---------------------------------------------------------------------------
# Multi-filter query helpers
# ---------------------------------------------------------------------------

SORTABLE_COLUMNS = {
    "company_number",
    "company_name",
    "company_status",
    "company_type",
    "postcode",
    "incorporation_date",
    "sic_code_1",
    "country_of_origin",
}


def _build_filter_where(
    *,
    sic_codes: str | list[str] | None = None,
    status: str | None = None,
    company_type: str | None = None,
    postcode_prefix: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    country: str | None = None,
    is_active: bool | None = None,
) -> tuple[str, list]:
    """Build a WHERE clause from the common filter parameters.

    Returns (where_sql, params) where *where_sql* includes the leading
    ``WHERE`` keyword when at least one filter is active.
    """
    conditions: list[str] = []
    params: list = []

    if sic_codes is not None:
        codes = _normalise_sic_codes(sic_codes)
        sic_where, sic_params = _build_sic_where(codes)
        conditions.append(sic_where)
        params.extend(sic_params)

    if status is not None:
        conditions.append("company_status = ?")
        params.append(status)

    if company_type is not None:
        conditions.append("company_type = ?")
        params.append(company_type)

    if postcode_prefix is not None:
        conditions.append("STARTS_WITH(postcode, ?)")
        params.append(postcode_prefix)

    if year_from is not None:
        conditions.append("EXTRACT(YEAR FROM incorporation_date) >= ?")
        params.append(year_from)

    if year_to is not None:
        conditions.append("EXTRACT(YEAR FROM incorporation_date) <= ?")
        params.append(year_to)

    if country is not None:
        conditions.append("country_of_origin = ?")
        params.append(country)

    if is_active is not None:
        conditions.append("is_active = ?")
        params.append(bool(is_active))

    if conditions:
        where_sql = " WHERE " + " AND ".join(conditions)
    else:
        where_sql = ""

    return where_sql, params


def query_companies(
    db_path: str | Path,
    *,
    sic_codes: str | list[str] | None = None,
    status: str | None = None,
    company_type: str | None = None,
    postcode_prefix: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    country: str | None = None,
    is_active: bool | None = None,
    sort_by: str = "company_name",
    sort_order: str = "ASC",
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    """Multi-filter paginated query. Returns (rows, total_count)."""
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    where_sql, params = _build_filter_where(
        sic_codes=sic_codes,
        status=status,
        company_type=company_type,
        postcode_prefix=postcode_prefix,
        year_from=year_from,
        year_to=year_to,
        country=country,
        is_active=is_active,
    )

    # Validate sorting
    if sort_by not in SORTABLE_COLUMNS:
        sort_by = "company_name"
    if sort_order.upper() not in ("ASC", "DESC"):
        sort_order = "ASC"
    else:
        sort_order = sort_order.upper()

    offset = (page - 1) * page_size

    data_sql = (
        f"SELECT * FROM companies{where_sql} "
        f"ORDER BY {sort_by} {sort_order} "
        f"LIMIT {int(page_size)} OFFSET {int(offset)}"
    )
    count_sql = f"SELECT COUNT(*) FROM companies{where_sql}"

    con = duckdb.connect(str(db_path))
    try:
        # Fetch total count
        total_count: int = con.execute(
            count_sql, params
        ).fetchone()[0]  # type: ignore[index]

        # Fetch page of results
        result = con.execute(data_sql, params)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        records = [dict(zip(columns, row)) for row in rows]

        return records, total_count
    finally:
        con.close()


def get_filter_options(db_path: str | Path) -> dict:
    """Returns distinct values for UI dropdown population."""
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        statuses = [
            row[0]
            for row in con.execute(
                "SELECT DISTINCT company_status FROM companies "
                "WHERE company_status IS NOT NULL ORDER BY 1"
            ).fetchall()
        ]
        company_types = [
            row[0]
            for row in con.execute(
                "SELECT DISTINCT company_type FROM companies "
                "WHERE company_type IS NOT NULL ORDER BY 1"
            ).fetchall()
        ]
        countries = [
            row[0]
            for row in con.execute(
                "SELECT DISTINCT country_of_origin FROM companies "
                "WHERE country_of_origin IS NOT NULL ORDER BY 1"
            ).fetchall()
        ]
        return {
            "statuses": statuses,
            "company_types": company_types,
            "countries": countries,
        }
    finally:
        con.close()


def export_filtered_csv(
    db_path: str | Path,
    output_path: str | Path,
    *,
    sic_codes: str | list[str] | None = None,
    status: str | None = None,
    company_type: str | None = None,
    postcode_prefix: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    country: str | None = None,
    is_active: bool | None = None,
) -> int:
    """Export filtered results via DuckDB COPY. Returns row count."""
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    where_sql, params = _build_filter_where(
        sic_codes=sic_codes,
        status=status,
        company_type=company_type,
        postcode_prefix=postcode_prefix,
        year_from=year_from,
        year_to=year_to,
        country=country,
        is_active=is_active,
    )

    inner_sql = f"SELECT * FROM companies{where_sql} ORDER BY company_name"

    # Escape path for safe SQL embedding (backslashes then single quotes)
    safe_path = str(output_path).replace("\\", "\\\\").replace("'", "''")

    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        # Materialize into temp table (DuckDB doesn't allow params in CREATE VIEW)
        con.execute("DROP TABLE IF EXISTS _export_filtered_tmp")
        con.execute(f"CREATE TEMP TABLE _export_filtered_tmp AS {inner_sql}", params)
        con.execute(
            f"COPY _export_filtered_tmp TO '{safe_path}' (HEADER, DELIMITER ',')"
        )
        row_count: int = con.execute(
            "SELECT COUNT(*) FROM _export_filtered_tmp"
        ).fetchone()[0]  # type: ignore[index]
        con.execute("DROP TABLE IF EXISTS _export_filtered_tmp")

        logger.info("Exported %d filtered records to %s", row_count, output_path)
        return row_count
    finally:
        con.close()
