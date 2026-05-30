"""Query helpers for the tiered_targets screening view.

`tiered_targets` is the headline M&A screening output: each matched company
scored on classification + director-age + size and bucketed into Tier 1/2/3 /
Excluded. This module is a thin paginated/filterable reader over that view,
mirroring the cqc/companies_house query helpers.

`provider_name` is joined from ``cqc_providers`` at query time (rather than in
the view) so the view carries no hard dependency on a table that
``rollup_providers.sql`` periodically DROP+recreates.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from ch_bulk.db.bootstrap import recover_interrupted_compaction

logger = logging.getLogger(__name__)

# tiered_targets aliased ``t``; cqc_providers aliased ``p``.
_FROM_JOIN = (
    " FROM tiered_targets t "
    "LEFT JOIN cqc_providers p ON p.provider_id = t.cqc_provider_id"
)
_SELECT = "SELECT t.*, p.provider_name" + _FROM_JOIN

# GUI-clickable sort keys → safe qualified column expressions.
TARGETS_SORT_COLUMNS = {
    "tier": "t.tier",
    "total_score": "t.total_score",
    "company_name": "t.company_name",
    "provider_name": "p.provider_name",
    "address_post_town": "t.address_post_town",
    "revenue": "t.revenue",
    "employee_count": "t.employee_count",
    "directors_over_60": "t.directors_over_60",
    "total_active_directors": "t.total_active_directors",
    "match_status": "t.match_status",
}

DEFAULT_TIERS = ("Tier 1", "Tier 2", "Tier 3")


def _build_targets_where(
    *,
    search: str | None = None,
    tiers: list[str] | tuple[str, ...] | None = DEFAULT_TIERS,
    any_director_over_60: bool | None = None,
    min_revenue: float | None = None,
    min_employees: int | None = None,
) -> tuple[str, list]:
    conditions: list[str] = []
    params: list = []

    if search:
        like = f"%{search}%"
        conditions.append(
            "(LOWER(t.company_name) LIKE LOWER(?) "
            "OR t.company_number LIKE ? "
            "OR LOWER(p.provider_name) LIKE LOWER(?))"
        )
        params.extend([like, like, like])

    # tiers=None means "all tiers" (no filter); the default is Tier 1-3.
    if tiers:
        placeholders = ", ".join(["?"] * len(tiers))
        conditions.append(f"t.tier IN ({placeholders})")
        params.extend(list(tiers))

    if any_director_over_60:
        conditions.append("t.directors_over_60 > 0")

    if min_revenue is not None:
        conditions.append("t.revenue >= ?")
        params.append(min_revenue)

    if min_employees is not None:
        conditions.append("t.employee_count >= ?")
        params.append(min_employees)

    if conditions:
        return " WHERE " + " AND ".join(conditions), params
    return "", params


def query_tiered_targets(
    db_path: str | Path,
    *,
    search: str | None = None,
    tiers: list[str] | tuple[str, ...] | None = DEFAULT_TIERS,
    any_director_over_60: bool | None = None,
    min_revenue: float | None = None,
    min_employees: int | None = None,
    sort_by: str = "total_score",
    sort_order: str = "DESC",
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    """Paginated read over tiered_targets (+ provider_name). Returns (rows, total).

    Defaults to Tier 1-3 (Excluded hidden) and best-first (total_score DESC).
    Returns ([], 0) if the screening view / cqc_providers don't exist yet
    (e.g. a CH-only DB).
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    where_sql, params = _build_targets_where(
        search=search, tiers=tiers, any_director_over_60=any_director_over_60,
        min_revenue=min_revenue, min_employees=min_employees,
    )
    sort_col = TARGETS_SORT_COLUMNS.get(sort_by, "t.total_score")
    sort_order = sort_order.upper() if sort_order.upper() in ("ASC", "DESC") else "DESC"
    offset = (page - 1) * page_size

    data_sql = (
        f"{_SELECT}{where_sql} "
        f"ORDER BY {sort_col} {sort_order} "
        f"LIMIT {int(page_size)} OFFSET {int(offset)}"
    )
    count_sql = f"SELECT COUNT(*){_FROM_JOIN}{where_sql}"

    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        total = con.execute(count_sql, params).fetchone()[0]
        result = con.execute(data_sql, params)
        cols = [d[0] for d in result.description]
        rows = [dict(zip(cols, row)) for row in result.fetchall()]
        return rows, int(total)
    except duckdb.CatalogException:
        # Screening view / cqc_providers not present yet (e.g. CH-only DB).
        return [], 0
    finally:
        con.close()


def export_tiered_targets_csv(
    db_path: str | Path,
    output_path: str | Path,
    **filters,
) -> int:
    db_path = Path(db_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    where_sql, params = _build_targets_where(**filters)
    safe_path = str(output_path).replace(chr(39), chr(39) * 2)
    sql = (
        f"COPY (({_SELECT}{where_sql})) "
        f"TO '{safe_path}' (HEADER, DELIMITER ',')"
    )
    count_sql = f"SELECT COUNT(*){_FROM_JOIN}{where_sql}"
    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        n = con.execute(count_sql, params).fetchone()[0]
        con.execute(sql, params)
        return int(n)
    except duckdb.CatalogException:
        return 0
    finally:
        con.close()
