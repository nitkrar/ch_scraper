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


def _build_sic_where(
    sic_codes: list[str],
    *,
    alias: str = "",
) -> tuple[str, list[str]]:
    """Build a WHERE clause fragment matching any of the 4 SIC columns.

    Uses parameterized placeholders to prevent SQL injection.

    Args:
        sic_codes: List of SIC code strings to match.

    Returns:
        Tuple of (SQL WHERE fragment, list of parameter values).
    """
    def q(column: str) -> str:
        return f"{alias}.{column}" if alias else column

    placeholders = ", ".join("?" for _ in sic_codes)
    conditions = " OR ".join(
        f"{q(f'sic_code_{i}')} IN ({placeholders})" for i in range(1, 5)
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

DIRECTORS_SORT_COLUMNS = {
    "company_number": "c.company_number",
    "company_name": "c.company_name",
    "company_status": "c.company_status",
    "provider_name": "p.provider_name",
    "avg_director_age": "e.avg_director_age",
    "min_director_age": "e.min_director_age",
    "max_director_age": "e.max_director_age",
    "directors_over_60": "e.directors_over_60",
    "all_directors_60_plus": "e.all_directors_60_plus",
    "sic_code_1": "c.sic_code_1",
    "postcode": "c.postcode",
    "incorporation_date": "c.incorporation_date",
    "is_active": "c.is_active",
}

DIRECTORS_SELECT_SQL = (
    "SELECT "
    "c.company_number AS company_number, "
    "c.company_name AS company_name, "
    "c.company_status AS company_status, "
    "c.company_type AS company_type, "
    "c.sic_code_1 AS sic_code_1, "
    "c.postcode AS postcode, "
    "c.incorporation_date AS incorporation_date, "
    "c.is_active AS is_active, "
    "p.provider_name AS provider_name, "
    "e.avg_director_age AS avg_director_age, "
    "e.min_director_age AS min_director_age, "
    "e.max_director_age AS max_director_age, "
    "e.total_active_directors AS total_active_directors, "
    "e.directors_over_60 AS directors_over_60, "
    "e.all_directors_60_plus AS all_directors_60_plus"
)

FINANCIALS_PRESENCE_SQL = (
    "COALESCE("
    "e.revenue, "
    "e.employee_count, "
    "e.gross_profit, "
    "e.profit_before_tax, "
    "e.profit_after_tax, "
    "e.fixed_assets, "
    "e.current_assets, "
    "e.total_assets, "
    "e.net_assets, "
    "e.net_current_assets"
    ") IS NOT NULL"
)

FINANCIALS_SORT_COLUMNS = {
    "company_number": "c.company_number",
    "company_name": "c.company_name",
    "company_status": "c.company_status",
    "provider_name": "p.provider_name",
    "revenue": "e.revenue",
    "revenue_source": "e.revenue_source",
    "employee_count": "e.employee_count",
    "filing_period_end": "e.filing_period_end",
    "gross_profit": "e.gross_profit",
    "profit_before_tax": "e.profit_before_tax",
    "profit_after_tax": "e.profit_after_tax",
    "fixed_assets": "e.fixed_assets",
    "current_assets": "e.current_assets",
    "total_assets": "e.total_assets",
    "net_assets": "e.net_assets",
    "net_current_assets": "e.net_current_assets",
    "sic_code_1": "c.sic_code_1",
    "postcode": "c.postcode",
    "incorporation_date": "c.incorporation_date",
    "is_active": "c.is_active",
}

FINANCIALS_SELECT_SQL = (
    "SELECT "
    "c.company_number AS company_number, "
    "c.company_name AS company_name, "
    "c.company_status AS company_status, "
    "c.company_type AS company_type, "
    "c.sic_code_1 AS sic_code_1, "
    "c.postcode AS postcode, "
    "c.incorporation_date AS incorporation_date, "
    "c.is_active AS is_active, "
    "p.provider_name AS provider_name, "
    "e.revenue AS revenue, "
    "e.revenue_source AS revenue_source, "
    "e.employee_count AS employee_count, "
    "e.filing_period_end AS filing_period_end, "
    "e.gross_profit AS gross_profit, "
    "e.profit_before_tax AS profit_before_tax, "
    "e.profit_after_tax AS profit_after_tax, "
    "e.fixed_assets AS fixed_assets, "
    "e.current_assets AS current_assets, "
    "e.total_assets AS total_assets, "
    "e.net_assets AS net_assets, "
    "e.net_current_assets AS net_current_assets, "
    "e.filing_format AS filing_format, "
    "e.filing_age_months AS filing_age_months"
)


def _enrichment_from_join() -> str:
    return (
        " FROM company_enrichment e "
        "INNER JOIN companies c ON e.company_number = c.company_number "
        "LEFT JOIN current_company_match m ON e.company_number = m.company_number "
        "LEFT JOIN cqc_providers p ON m.cqc_provider_id = p.provider_id"
    )


def _enrichment_search_where(search: str | None) -> tuple[str, list[str]]:
    if search is None or not search.strip():
        return "", []
    pattern = f"%{search.strip()}%"
    return (
        "("
        "LOWER(c.company_name) LIKE LOWER(?) "
        "OR LOWER(c.company_number) LIKE LOWER(?) "
        "OR LOWER(p.provider_name) LIKE LOWER(?)"
        ")",
        [pattern, pattern, pattern],
    )


def _directors_where(
    *,
    search: str | None = None,
    sic_codes: str | list[str] | None = None,
    status: str | None = None,
    company_type: str | None = None,
    postcode_prefix: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    country: str | None = None,
    is_active: bool | None = None,
    min_director_age: int | None = None,
) -> tuple[str, list]:
    conditions = ["e.avg_director_age IS NOT NULL"]
    params: list = []

    filter_where, filter_params = _build_filter_where(
        sic_codes=sic_codes,
        status=status,
        company_type=company_type,
        postcode_prefix=postcode_prefix,
        year_from=year_from,
        year_to=year_to,
        country=country,
        is_active=is_active,
        alias="c",
    )
    if filter_where:
        conditions.append(filter_where.removeprefix(" WHERE "))
        params.extend(filter_params)

    search_where, search_params = _enrichment_search_where(search)
    if search_where:
        conditions.append(search_where)
        params.extend(search_params)

    if min_director_age is not None:
        conditions.append("e.max_director_age >= ?")
        params.append(int(min_director_age))

    return f" WHERE {' AND '.join(conditions)}", params


def _financials_where(
    *,
    search: str | None = None,
    sic_codes: str | list[str] | None = None,
    status: str | None = None,
    company_type: str | None = None,
    postcode_prefix: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    country: str | None = None,
    is_active: bool | None = None,
    min_revenue: float | int | None = None,
    min_employees: int | None = None,
) -> tuple[str, list]:
    conditions = [FINANCIALS_PRESENCE_SQL]
    params: list = []

    filter_where, filter_params = _build_filter_where(
        sic_codes=sic_codes,
        status=status,
        company_type=company_type,
        postcode_prefix=postcode_prefix,
        year_from=year_from,
        year_to=year_to,
        country=country,
        is_active=is_active,
        alias="c",
    )
    if filter_where:
        conditions.append(filter_where.removeprefix(" WHERE "))
        params.extend(filter_params)

    search_where, search_params = _enrichment_search_where(search)
    if search_where:
        conditions.append(search_where)
        params.extend(search_params)

    if min_revenue is not None:
        conditions.append("e.revenue >= ?")
        params.append(min_revenue)

    if min_employees is not None:
        conditions.append("e.employee_count >= ?")
        params.append(int(min_employees))

    return f" WHERE {' AND '.join(conditions)}", params


def _companies_from_where(
    *,
    search: str | None = None,
    sic_codes: str | list[str] | None = None,
    status: str | None = None,
    company_type: str | None = None,
    postcode_prefix: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    country: str | None = None,
    is_active: bool | None = None,
) -> tuple[str, str, list]:
    if search is None or not search.strip():
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
        return " FROM companies", where_sql, params

    conditions: list[str] = []
    params: list = []
    filter_where, filter_params = _build_filter_where(
        sic_codes=sic_codes,
        status=status,
        company_type=company_type,
        postcode_prefix=postcode_prefix,
        year_from=year_from,
        year_to=year_to,
        country=country,
        is_active=is_active,
        alias="c",
    )
    if filter_where:
        conditions.append(filter_where.removeprefix(" WHERE "))
        params.extend(filter_params)

    search_where, search_params = _enrichment_search_where(search)
    if search_where:
        conditions.append(search_where)
        params.extend(search_params)

    where_sql = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    from_sql = (
        " FROM companies c "
        "LEFT JOIN current_company_match m ON c.company_number = m.company_number "
        "LEFT JOIN cqc_providers p ON m.cqc_provider_id = p.provider_id"
    )
    return from_sql, where_sql, params


def _export_copy_sql(
    db_path: str | Path,
    output_path: str | Path,
    *,
    select_sql: str,
    from_sql: str,
    where_sql: str,
    order_by_sql: str,
    params: list,
) -> int:
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    safe_path = str(output_path).replace("\\", "\\\\").replace("'", "''")
    count_sql = f"SELECT COUNT(*){from_sql}{where_sql}"
    copy_sql = (
        f"COPY ({select_sql}{from_sql}{where_sql} ORDER BY {order_by_sql}) "
        f"TO '{safe_path}' (HEADER, DELIMITER ',')"
    )

    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        row_count: int = con.execute(
            count_sql, params
        ).fetchone()[0]  # type: ignore[index]
        con.execute(copy_sql, params)
        return row_count
    finally:
        con.close()


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
    alias: str = "",
) -> tuple[str, list]:
    """Build a WHERE clause from the common filter parameters.

    Returns (where_sql, params) where *where_sql* includes the leading
    ``WHERE`` keyword when at least one filter is active.
    """
    conditions: list[str] = []
    params: list = []

    def q(column: str) -> str:
        return f"{alias}.{column}" if alias else column

    if sic_codes is not None:
        codes = _normalise_sic_codes(sic_codes)
        sic_where, sic_params = _build_sic_where(codes, alias=alias)
        conditions.append(sic_where)
        params.extend(sic_params)

    if status is not None:
        conditions.append(f"{q('company_status')} = ?")
        params.append(status)

    if company_type is not None:
        conditions.append(f"{q('company_type')} = ?")
        params.append(company_type)

    if postcode_prefix is not None:
        conditions.append(f"STARTS_WITH({q('postcode')}, ?)")
        params.append(postcode_prefix)

    if year_from is not None:
        conditions.append(f"EXTRACT(YEAR FROM {q('incorporation_date')}) >= ?")
        params.append(year_from)

    if year_to is not None:
        conditions.append(f"EXTRACT(YEAR FROM {q('incorporation_date')}) <= ?")
        params.append(year_to)

    if country is not None:
        conditions.append(f"{q('country_of_origin')} = ?")
        params.append(country)

    if is_active is not None:
        conditions.append(f"{q('is_active')} = ?")
        params.append(bool(is_active))

    if conditions:
        where_sql = " WHERE " + " AND ".join(conditions)
    else:
        where_sql = ""

    return where_sql, params


def query_companies(
    db_path: str | Path,
    *,
    search: str | None = None,
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

    from_sql, where_sql, params = _companies_from_where(
        search=search,
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
    use_joined_search = search is not None and bool(search.strip())
    select_sql = "SELECT c.*" if use_joined_search else "SELECT *"
    sort_col = f"c.{sort_by}" if use_joined_search else sort_by

    data_sql = (
        f"{select_sql}{from_sql}{where_sql} "
        f"ORDER BY {sort_col} {sort_order} "
        f"LIMIT {int(page_size)} OFFSET {int(offset)}"
    )
    count_sql = f"SELECT COUNT(*){from_sql}{where_sql}"

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


def query_directors_age(
    db_path: str | Path,
    *,
    search: str | None = None,
    sic_codes: str | list[str] | None = None,
    status: str | None = None,
    company_type: str | None = None,
    postcode_prefix: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    country: str | None = None,
    is_active: bool | None = None,
    min_director_age: int | None = None,
    sort_by: str = "company_name",
    sort_order: str = "ASC",
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    """Paginated browse query for CH director age enrichment data."""
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    where_sql, params = _directors_where(
        search=search,
        sic_codes=sic_codes,
        status=status,
        company_type=company_type,
        postcode_prefix=postcode_prefix,
        year_from=year_from,
        year_to=year_to,
        country=country,
        is_active=is_active,
        min_director_age=min_director_age,
    )

    sort_col = DIRECTORS_SORT_COLUMNS.get(sort_by, "c.company_name")
    sort_order = sort_order.upper() if sort_order.upper() in ("ASC", "DESC") else "ASC"
    offset = (page - 1) * page_size
    from_join_sql = _enrichment_from_join()
    data_sql = (
        f"{DIRECTORS_SELECT_SQL}{from_join_sql}{where_sql} "
        f"ORDER BY {sort_col} {sort_order} "
        f"LIMIT {int(page_size)} OFFSET {int(offset)}"
    )
    count_sql = f"SELECT COUNT(*){from_join_sql}{where_sql}"

    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        try:
            total_count: int = con.execute(
                count_sql, params
            ).fetchone()[0]  # type: ignore[index]
            result = con.execute(data_sql, params)
        except duckdb.CatalogException:
            return [], 0
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        records = [dict(zip(columns, row)) for row in rows]
        return records, total_count
    finally:
        con.close()


def query_financials(
    db_path: str | Path,
    *,
    search: str | None = None,
    sic_codes: str | list[str] | None = None,
    status: str | None = None,
    company_type: str | None = None,
    postcode_prefix: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    country: str | None = None,
    is_active: bool | None = None,
    min_revenue: float | int | None = None,
    min_employees: int | None = None,
    sort_by: str = "company_name",
    sort_order: str = "ASC",
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    """Paginated browse query for CH financial enrichment data."""
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    where_sql, params = _financials_where(
        search=search,
        sic_codes=sic_codes,
        status=status,
        company_type=company_type,
        postcode_prefix=postcode_prefix,
        year_from=year_from,
        year_to=year_to,
        country=country,
        is_active=is_active,
        min_revenue=min_revenue,
        min_employees=min_employees,
    )

    sort_col = FINANCIALS_SORT_COLUMNS.get(sort_by, "c.company_name")
    sort_order = sort_order.upper() if sort_order.upper() in ("ASC", "DESC") else "ASC"
    offset = (page - 1) * page_size
    from_join_sql = _enrichment_from_join()
    data_sql = (
        f"{FINANCIALS_SELECT_SQL}{from_join_sql}{where_sql} "
        f"ORDER BY {sort_col} {sort_order} "
        f"LIMIT {int(page_size)} OFFSET {int(offset)}"
    )
    count_sql = f"SELECT COUNT(*){from_join_sql}{where_sql}"

    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        try:
            total_count: int = con.execute(
                count_sql, params
            ).fetchone()[0]  # type: ignore[index]
            result = con.execute(data_sql, params)
        except duckdb.CatalogException:
            return [], 0
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
    search: str | None = None,
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
    from_sql, where_sql, params = _companies_from_where(
        search=search,
        sic_codes=sic_codes,
        status=status,
        company_type=company_type,
        postcode_prefix=postcode_prefix,
        year_from=year_from,
        year_to=year_to,
        country=country,
        is_active=is_active,
    )
    use_joined_search = search is not None and bool(search.strip())
    select_sql = "SELECT c.*" if use_joined_search else "SELECT *"
    order_by_sql = "c.company_name" if use_joined_search else "company_name"
    row_count = _export_copy_sql(
        db_path,
        output_path,
        select_sql=select_sql,
        from_sql=from_sql,
        where_sql=where_sql,
        order_by_sql=order_by_sql,
        params=params,
    )
    logger.info("Exported %d filtered records to %s", row_count, output_path)
    return row_count


def export_directors_age_csv(
    db_path: str | Path,
    output_path: str | Path,
    *,
    search: str | None = None,
    sic_codes: str | list[str] | None = None,
    status: str | None = None,
    company_type: str | None = None,
    postcode_prefix: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    country: str | None = None,
    is_active: bool | None = None,
    min_director_age: int | None = None,
    sort_by: str = "company_name",
    sort_order: str = "ASC",
) -> int:
    where_sql, params = _directors_where(
        search=search,
        sic_codes=sic_codes,
        status=status,
        company_type=company_type,
        postcode_prefix=postcode_prefix,
        year_from=year_from,
        year_to=year_to,
        country=country,
        is_active=is_active,
        min_director_age=min_director_age,
    )
    sort_col = DIRECTORS_SORT_COLUMNS.get(sort_by, "c.company_name")
    sort_dir = sort_order.upper() if sort_order.upper() in ("ASC", "DESC") else "ASC"
    row_count = _export_copy_sql(
        db_path,
        output_path,
        select_sql=DIRECTORS_SELECT_SQL,
        from_sql=_enrichment_from_join(),
        where_sql=where_sql,
        order_by_sql=f"{sort_col} {sort_dir}",
        params=params,
    )
    logger.info("Exported %d directors rows to %s", row_count, output_path)
    return row_count


def export_financials_csv(
    db_path: str | Path,
    output_path: str | Path,
    *,
    search: str | None = None,
    sic_codes: str | list[str] | None = None,
    status: str | None = None,
    company_type: str | None = None,
    postcode_prefix: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    country: str | None = None,
    is_active: bool | None = None,
    min_revenue: float | int | None = None,
    min_employees: int | None = None,
    sort_by: str = "company_name",
    sort_order: str = "ASC",
) -> int:
    where_sql, params = _financials_where(
        search=search,
        sic_codes=sic_codes,
        status=status,
        company_type=company_type,
        postcode_prefix=postcode_prefix,
        year_from=year_from,
        year_to=year_to,
        country=country,
        is_active=is_active,
        min_revenue=min_revenue,
        min_employees=min_employees,
    )
    sort_col = FINANCIALS_SORT_COLUMNS.get(sort_by, "c.company_name")
    sort_dir = sort_order.upper() if sort_order.upper() in ("ASC", "DESC") else "ASC"
    row_count = _export_copy_sql(
        db_path,
        output_path,
        select_sql=FINANCIALS_SELECT_SQL,
        from_sql=_enrichment_from_join(),
        where_sql=where_sql,
        order_by_sql=f"{sort_col} {sort_dir}",
        params=params,
    )
    logger.info("Exported %d financial rows to %s", row_count, output_path)
    return row_count
