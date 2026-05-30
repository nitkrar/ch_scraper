"""Query helpers for the CQC locations / providers tables."""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from ch_bulk.db.bootstrap import recover_interrupted_compaction

logger = logging.getLogger(__name__)

CQC_LOCATION_SORT_COLUMNS = {
    "name", "postcode", "service_types", "provider_name",
    "local_authority", "region", "is_active", "last_scrape_date",
}
CQC_PROVIDER_SORT_COLUMNS = {
    "provider_name", "active_location_count", "total_location_count",
    "is_active", "last_scrape_date",
}
HSCA_LOCATION_SORT_COLUMNS = {
    "location_id": "h.location_id",
    "name": "l.name",
    "provider_name": "l.provider_name",
    "company_name": "c.company_name",
    "company_status": "c.company_status",
    "provider_companies_house_number": "h.provider_companies_house_number",
    "number_of_beds": "h.number_of_beds",
    "care_home": "h.care_home",
    "dormant": "h.dormant",
    "provider_ownership_type": "h.provider_ownership_type",
    "provider_brand_name": "h.provider_brand_name",
    "st_domiciliary_care_service": "h.st_domiciliary_care_service",
    "st_supported_living_service": "h.st_supported_living_service",
    "st_care_home_with_nursing": "h.st_care_home_with_nursing",
    "st_care_home_without_nursing": "h.st_care_home_without_nursing",
    "st_extra_care_housing_services": "h.st_extra_care_housing_services",
    "st_hospice_services_at_home": "h.st_hospice_services_at_home",
    "postcode": "l.postcode",
    "region": "l.region",
    "local_authority": "l.local_authority",
    "service_types": "l.service_types",
    "is_active": "l.is_active",
}
HSCA_LOCATION_SELECT_SQL = (
    "SELECT "
    "h.location_id AS location_id, "
    "l.name AS name, "
    "l.provider_name AS provider_name, "
    "c.company_name AS company_name, "
    "c.company_status AS company_status, "
    "h.provider_companies_house_number AS provider_companies_house_number, "
    "h.number_of_beds AS number_of_beds, "
    "h.care_home AS care_home, "
    "h.dormant AS dormant, "
    "h.provider_ownership_type AS provider_ownership_type, "
    "h.provider_brand_name AS provider_brand_name, "
    "h.st_domiciliary_care_service AS st_domiciliary_care_service, "
    "h.st_supported_living_service AS st_supported_living_service, "
    "h.st_care_home_with_nursing AS st_care_home_with_nursing, "
    "h.st_care_home_without_nursing AS st_care_home_without_nursing, "
    "h.st_extra_care_housing_services AS st_extra_care_housing_services, "
    "h.st_hospice_services_at_home AS st_hospice_services_at_home, "
    "l.postcode AS postcode, "
    "l.region AS region, "
    "l.local_authority AS local_authority, "
    "l.service_types AS service_types, "
    "l.is_active AS is_active, "
    "h.service_user_bands AS service_user_bands, "
    "h.regulated_activities AS regulated_activities"
)


def _list_any(col: str, values: list[str], params: list) -> str:
    """SQL fragment matching `col` (a LIST column) against ANY of values."""
    if not values:
        return "TRUE"
    # list_has_any(col, [v1, v2, ...])
    placeholders = ", ".join(["?"] * len(values))
    params.extend(values)
    return f"list_has_any({col}, [{placeholders}])"


def _string_any_like(col: str, values: list[str], params: list) -> str:
    """SQL fragment matching `col` (a VARCHAR with pipe-separated values)
    against ANY of values via substring match."""
    if not values:
        return "TRUE"
    parts = []
    for v in values:
        parts.append(f"LOWER({col}) LIKE LOWER(?)")
        params.append(f"%{v}%")
    return "(" + " OR ".join(parts) + ")"


def _build_cqc_locations_where(
    *,
    name_contains: str | None = None,
    service_types: list[str] | str | None = None,
    regions: list[str] | str | None = None,
    local_authorities: list[str] | str | None = None,
    provider_name_contains: str | None = None,
    postcode_prefix: str | None = None,
    is_active: bool | None = None,
    alias: str = "",
) -> tuple[str, list]:
    conditions: list[str] = []
    params: list = []
    q = (lambda c: f"{alias}.{c}" if alias else c)

    if name_contains:
        conditions.append(f"LOWER({q('name')}) LIKE LOWER(?)")
        params.append(f"%{name_contains}%")

    # Multi-value filters: accept str (single), list (multi), None (skip)
    if service_types:
        if isinstance(service_types, str):
            service_types = [service_types]
        conditions.append(_string_any_like(q("service_types"), service_types, params))
    if regions:
        if isinstance(regions, str):
            regions = [regions]
        placeholders = ", ".join(["?"] * len(regions))
        conditions.append(f"{q('region')} IN ({placeholders})")
        params.extend(regions)
    if local_authorities:
        if isinstance(local_authorities, str):
            local_authorities = [local_authorities]
        placeholders = ", ".join(["?"] * len(local_authorities))
        conditions.append(f"{q('local_authority')} IN ({placeholders})")
        params.extend(local_authorities)

    if provider_name_contains:
        conditions.append(f"LOWER({q('provider_name')}) LIKE LOWER(?)")
        params.append(f"%{provider_name_contains}%")
    if postcode_prefix:
        conditions.append(f"STARTS_WITH({q('postcode')}, ?)")
        params.append(postcode_prefix)
    if is_active is not None:
        conditions.append(f"{q('is_active')} = ?")
        params.append(bool(is_active))

    if conditions:
        return " WHERE " + " AND ".join(conditions), params
    return "", params


def _build_cqc_providers_where(
    *,
    provider_name_contains: str | None = None,
    service_types: list[str] | str | None = None,
    regions: list[str] | str | None = None,
    local_authorities: list[str] | str | None = None,
    is_active: bool | None = None,
    min_active_location_count: int | None = None,
    max_active_location_count: int | None = None,
) -> tuple[str, list]:
    """Filter cqc_providers. Service / region / LA filters operate on the
    rollup LIST columns via list_has_any (multi-value = OR semantics)."""
    conditions: list[str] = []
    params: list = []

    if provider_name_contains:
        conditions.append("LOWER(provider_name) LIKE LOWER(?)")
        params.append(f"%{provider_name_contains}%")

    if service_types:
        if isinstance(service_types, str):
            service_types = [service_types]
        conditions.append(_list_any("service_types_list", service_types, params))
    if regions:
        if isinstance(regions, str):
            regions = [regions]
        conditions.append(_list_any("regions_list", regions, params))
    if local_authorities:
        if isinstance(local_authorities, str):
            local_authorities = [local_authorities]
        conditions.append(_list_any("local_authorities_list", local_authorities, params))

    if is_active is not None:
        conditions.append("is_active = ?")
        params.append(bool(is_active))

    if min_active_location_count is not None:
        conditions.append("active_location_count >= ?")
        params.append(int(min_active_location_count))
    if max_active_location_count is not None:
        conditions.append("active_location_count <= ?")
        params.append(int(max_active_location_count))

    if conditions:
        return " WHERE " + " AND ".join(conditions), params
    return "", params


def query_cqc_locations(
    db_path: str | Path,
    *,
    name_contains: str | None = None,
    service_types: list[str] | str | None = None,
    regions: list[str] | str | None = None,
    local_authorities: list[str] | str | None = None,
    provider_name_contains: str | None = None,
    postcode_prefix: str | None = None,
    is_active: bool | None = None,
    sort_by: str = "name",
    sort_order: str = "ASC",
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    where_sql, params = _build_cqc_locations_where(
        name_contains=name_contains, service_types=service_types,
        regions=regions, local_authorities=local_authorities,
        provider_name_contains=provider_name_contains,
        postcode_prefix=postcode_prefix, is_active=is_active,
    )
    if sort_by not in CQC_LOCATION_SORT_COLUMNS:
        sort_by = "name"
    sort_order = sort_order.upper() if sort_order.upper() in ("ASC", "DESC") else "ASC"
    offset = (page - 1) * page_size

    data_sql = (
        f"SELECT * FROM cqc_locations{where_sql} "
        f"ORDER BY {sort_by} {sort_order} "
        f"LIMIT {int(page_size)} OFFSET {int(offset)}"
    )
    count_sql = f"SELECT COUNT(*) FROM cqc_locations{where_sql}"

    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        total = con.execute(count_sql, params).fetchone()[0]
        result = con.execute(data_sql, params)
        cols = [d[0] for d in result.description]
        rows = [dict(zip(cols, row)) for row in result.fetchall()]
        return rows, int(total)
    finally:
        con.close()


def query_cqc_providers(
    db_path: str | Path,
    *,
    provider_name_contains: str | None = None,
    service_types: list[str] | str | None = None,
    regions: list[str] | str | None = None,
    local_authorities: list[str] | str | None = None,
    is_active: bool | None = None,
    min_active_location_count: int | None = None,
    max_active_location_count: int | None = None,
    sort_by: str = "provider_name",
    sort_order: str = "ASC",
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    where_sql, params = _build_cqc_providers_where(
        provider_name_contains=provider_name_contains,
        service_types=service_types,
        regions=regions,
        local_authorities=local_authorities,
        is_active=is_active,
        min_active_location_count=min_active_location_count,
        max_active_location_count=max_active_location_count,
    )
    if sort_by not in CQC_PROVIDER_SORT_COLUMNS:
        sort_by = "provider_name"
    sort_order = sort_order.upper() if sort_order.upper() in ("ASC", "DESC") else "ASC"
    offset = (page - 1) * page_size

    data_sql = (
        f"SELECT * FROM cqc_providers{where_sql} "
        f"ORDER BY {sort_by} {sort_order} "
        f"LIMIT {int(page_size)} OFFSET {int(offset)}"
    )
    count_sql = f"SELECT COUNT(*) FROM cqc_providers{where_sql}"

    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        total = con.execute(count_sql, params).fetchone()[0]
        result = con.execute(data_sql, params)
        cols = [d[0] for d in result.description]
        rows = [dict(zip(cols, row)) for row in result.fetchall()]
        return rows, int(total)
    finally:
        con.close()


def _hsca_from_where(
    *,
    name_contains: str | None = None,
    service_types: list[str] | str | None = None,
    regions: list[str] | str | None = None,
    local_authorities: list[str] | str | None = None,
    provider_name_contains: str | None = None,
    postcode_prefix: str | None = None,
    is_active: bool | None = None,
    has_ch_number: bool | None = None,
) -> tuple[str, str, list]:
    where_sql, params = _build_cqc_locations_where(
        name_contains=name_contains,
        service_types=service_types,
        regions=regions,
        local_authorities=local_authorities,
        provider_name_contains=provider_name_contains,
        postcode_prefix=postcode_prefix,
        is_active=is_active,
        alias="l",
    )
    conditions: list[str] = []
    if where_sql:
        conditions.append(where_sql.removeprefix(" WHERE "))
    if has_ch_number is True:
        conditions.append("h.provider_companies_house_number IS NOT NULL")
    elif has_ch_number is False:
        conditions.append("h.provider_companies_house_number IS NULL")
    final_where_sql = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    from_join_sql = (
        " FROM cqc_hsca_locations h "
        "INNER JOIN cqc_locations l ON h.location_id = l.location_id "
        "LEFT JOIN companies c ON h.provider_companies_house_number = c.company_number"
    )
    return from_join_sql, final_where_sql, params


def query_hsca_locations(
    db_path: str | Path,
    *,
    name_contains: str | None = None,
    service_types: list[str] | str | None = None,
    regions: list[str] | str | None = None,
    local_authorities: list[str] | str | None = None,
    provider_name_contains: str | None = None,
    postcode_prefix: str | None = None,
    is_active: bool | None = None,
    has_ch_number: bool | None = None,
    sort_by: str = "name",
    sort_order: str = "ASC",
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    from_join_sql, where_sql, params = _hsca_from_where(
        name_contains=name_contains,
        service_types=service_types,
        regions=regions,
        local_authorities=local_authorities,
        provider_name_contains=provider_name_contains,
        postcode_prefix=postcode_prefix,
        is_active=is_active,
        has_ch_number=has_ch_number,
    )
    sort_col = HSCA_LOCATION_SORT_COLUMNS.get(sort_by, "l.name")
    sort_order = sort_order.upper() if sort_order.upper() in ("ASC", "DESC") else "ASC"
    offset = (page - 1) * page_size
    data_sql = (
        f"{HSCA_LOCATION_SELECT_SQL}{from_join_sql}{where_sql} "
        f"ORDER BY {sort_col} {sort_order} "
        f"LIMIT {int(page_size)} OFFSET {int(offset)}"
    )
    count_sql = f"SELECT COUNT(*){from_join_sql}{where_sql}"

    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        total = con.execute(count_sql, params).fetchone()[0]
        result = con.execute(data_sql, params)
        cols = [d[0] for d in result.description]
        rows = [dict(zip(cols, row)) for row in result.fetchall()]
        return rows, int(total)
    finally:
        con.close()


def export_hsca_locations_csv(
    db_path: str | Path,
    output_path: str | Path,
    **filters,
) -> int:
    db_path = Path(db_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    from_join_sql, where_sql, params = _hsca_from_where(**filters)
    count_sql = f"SELECT COUNT(*){from_join_sql}{where_sql}"
    sql = (
        f"COPY ({HSCA_LOCATION_SELECT_SQL}{from_join_sql}{where_sql}) "
        f"TO '{str(output_path).replace(chr(39), chr(39)*2)}' (HEADER, DELIMITER ',')"
    )
    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        n = con.execute(count_sql, params).fetchone()[0]
        con.execute(sql, params)
        return int(n)
    finally:
        con.close()


def get_cqc_filter_options(db_path: str | Path) -> dict:
    """Distinct values for CQC filter dropdowns. Source = cqc_locations
    (one source of truth; provider rollup values are a subset)."""
    db_path = Path(db_path)
    if not db_path.exists():
        return {"service_types": [], "regions": [], "local_authorities": []}
    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        service_types = [
            r[0] for r in con.execute("""
                SELECT DISTINCT trim(unnest(string_split(service_types, '|'))) AS s
                FROM cqc_locations
                WHERE service_types IS NOT NULL
                ORDER BY s
            """).fetchall() if r[0]
        ]
        regions = [
            r[0] for r in con.execute(
                "SELECT DISTINCT region FROM cqc_locations "
                "WHERE region IS NOT NULL ORDER BY region"
            ).fetchall() if r[0]
        ]
        local_authorities = [
            r[0] for r in con.execute(
                "SELECT DISTINCT local_authority FROM cqc_locations "
                "WHERE local_authority IS NOT NULL ORDER BY local_authority"
            ).fetchall() if r[0]
        ]
        return {
            "service_types": service_types,
            "regions": regions,
            "local_authorities": local_authorities,
        }
    except duckdb.CatalogException:
        return {"service_types": [], "regions": [], "local_authorities": []}
    finally:
        con.close()


def export_cqc_locations_csv(
    db_path: str | Path,
    output_path: str | Path,
    **filters,
) -> int:
    db_path = Path(db_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    where_sql, params = _build_cqc_locations_where(**filters)
    sql = (
        f"COPY (SELECT * FROM cqc_locations{where_sql}) "
        f"TO '{str(output_path).replace(chr(39), chr(39)*2)}' (HEADER, DELIMITER ',')"
    )
    count_sql = f"SELECT COUNT(*) FROM cqc_locations{where_sql}"
    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        n = con.execute(count_sql, params).fetchone()[0]
        con.execute(sql, params)
        return int(n)
    finally:
        con.close()


def export_cqc_providers_csv(
    db_path: str | Path,
    output_path: str | Path,
    **filters,
) -> int:
    db_path = Path(db_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    where_sql, params = _build_cqc_providers_where(**filters)
    sql = (
        f"COPY (SELECT * FROM cqc_providers{where_sql}) "
        f"TO '{str(output_path).replace(chr(39), chr(39)*2)}' (HEADER, DELIMITER ',')"
    )
    count_sql = f"SELECT COUNT(*) FROM cqc_providers{where_sql}"
    recover_interrupted_compaction(db_path)
    con = duckdb.connect(str(db_path))
    try:
        n = con.execute(count_sql, params).fetchone()[0]
        con.execute(sql, params)
        return int(n)
    finally:
        con.close()


# Backwards-compat alias for older code using a single ‘service_type’ kwarg
export_cqc_filtered_csv = export_cqc_locations_csv
