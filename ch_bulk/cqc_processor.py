"""CQC care directory ingest + upsert into DuckDB.

Mirrors the CH pipeline in processor.py but for the CQC weekly CSV
(location-level rows with provider_id linkage).

Tables produced:
    cqc_locations  — one row per CQC-registered location (~57k UK-wide)
    cqc_providers  — rolled up per provider_id from locations

The CSV's "office use only" CQC IDs are renamed to clean column names:
    "CQC Location ID (for office use only)"  → location_id
    "CQC Provider ID (for office use only)"  → provider_id
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
from rich.console import Console

from ch_bulk.bootstrap import ensure_pipeline_schema
from ch_bulk.processor import (
    SQL_DIR as _CH_SQL_DIR,
    SanityCheckError,
    SanityCheckResult,
    _escape_path,
    compact_database,
    timed_phase,
)
from ch_bulk.sync_batches import finish_sync_batch, insert_sync_batch

logger = logging.getLogger(__name__)
console = Console()

# Reuse the same thresholds the CH pipeline uses
ROW_COUNT_PCT_THRESHOLD = 5.0
INACTIVE_CHURN_PCT_THRESHOLD = 5.0
HSCA_ROW_COUNT_PCT_THRESHOLD = 10.0
HSCA_CH_NUMBER_PCT_THRESHOLD = 10.0

SQL_DIR = _CH_SQL_DIR.parent / "cqc"

HSCA_SHEET_NAME = "HSCA_Active_Locations"
HSCA_DUAL_SHEET_NAME = "Dual_Registration_Locations"
HSCA_FILENAME_RE = re.compile(
    r"hsca_active_locations_(\d{4}-\d{2}-\d{2})\.ods$"
)
HSCA_LOCATION_REQUIRED_COLUMNS = [
    "Location ID",
    "Provider ID",
    "Provider Companies House Number",
    "Provider Charity Number",
    "Provider Ownership Type",
    "Brand ID",
    "Brand Name",
    "Provider Web Address",
    "Location Web Address",
    "Care home?",
    "Care homes beds",
    "Dormant (Y/N)",
    "Registered manager",
    "Service type - Domiciliary care service",
    "Service type - Supported living service",
    "Service type - Care home service with nursing",
    "Service type - Care home service without nursing",
    "Service type - Extra Care housing services",
    "Service type - Hospice services at home",
]
HSCA_DUAL_REQUIRED_COLUMNS = [
    "Location ID",
    "Location Name",
    "Location HSCA Start Date",
    "Location Type/Sector",
    "Provider ID",
    "Provider Name",
    "Linked Organisation ID",
    "Linked Organisation Name",
    "Relationship",
    "Relationship Start Date",
    "Primary ID",
]
HSCA_SERVICE_TYPE_TO_FIELD = {
    "Service type - Domiciliary care service": "st_domiciliary_care_service",
    "Service type - Supported living service": "st_supported_living_service",
    "Service type - Care home service with nursing": "st_care_home_with_nursing",
    "Service type - Care home service without nursing": "st_care_home_without_nursing",
    "Service type - Extra Care housing services": "st_extra_care_housing_services",
    "Service type - Hospice services at home": "st_hospice_services_at_home",
}
HSCA_LOCATION_STAGE_COLUMNS = [
    "location_id",
    "provider_id",
    "provider_companies_house_number",
    "provider_charity_number",
    "provider_ownership_type",
    "provider_brand_id",
    "provider_brand_name",
    "provider_web_address",
    "location_web_address",
    "care_home",
    "number_of_beds",
    "dormant",
    "registered_manager_name",
    "st_domiciliary_care_service",
    "st_supported_living_service",
    "st_care_home_with_nursing",
    "st_care_home_without_nursing",
    "st_extra_care_housing_services",
    "st_hospice_services_at_home",
    "bulk_imported_at",
    "bulk_file_date",
    "raw_row",
]
HSCA_DUAL_STAGE_COLUMNS = [
    "location_id",
    "location_name",
    "location_hsca_start_date",
    "location_type_sector",
    "provider_id",
    "provider_name",
    "linked_organisation_id",
    "linked_organisation_name",
    "relationship",
    "relationship_start_date",
    "primary_id",
]


@dataclass(frozen=True)
class HSCASanityResult:
    table_exists: bool
    row_count: int
    dup_distinct_location_ids: int
    dup_sample: list[str]
    old_total: int
    row_delta: int
    row_pct: float | None
    ch_numbers_populated: int
    ch_numbers_pct: float


def _run_sql_file(con: duckdb.DuckDBPyConnection, name: str, **subs) -> None:
    sql = (SQL_DIR / name).read_text()
    for key, value in subs.items():
        sql = sql.replace("{{" + key + "}}", str(value))
    con.execute(sql)


def _fetchone_sql_file(
    con: duckdb.DuckDBPyConnection, name: str, **subs
) -> tuple:
    sql = (SQL_DIR / name).read_text()
    for key, value in subs.items():
        sql = sql.replace("{{" + key + "}}", str(value))
    return con.execute(sql).fetchone()


def scrape_date_from_cqc_filename(csv_file: Path) -> date:
    """Extract YYYY-MM-DD from ``cqc_directory_YYYY-MM-DD.csv``."""
    import re
    m = re.match(r"cqc_directory_(\d{4}-\d{2}-\d{2})\.csv$", csv_file.name)
    if not m:
        raise ValueError(
            f"Filename does not match expected pattern "
            f"cqc_directory_YYYY-MM-DD.csv: {csv_file.name}"
        )
    return date.fromisoformat(m.group(1))


def scrape_date_from_hsca_filename(ods_file: Path) -> date:
    """Extract YYYY-MM-DD from ``hsca_active_locations_YYYY-MM-DD.ods``."""
    m = HSCA_FILENAME_RE.match(ods_file.name)
    if not m:
        raise ValueError(
            "Filename does not match expected pattern "
            f"hsca_active_locations_YYYY-MM-DD.ods: {ods_file.name}"
        )
    return date.fromisoformat(m.group(1))


def _ingest_cqc_to_staging(
    con: duckdb.DuckDBPyConnection,
    csv_file: Path,
) -> None:
    """Load the CQC directory CSV into cqc_locations_staging.

    The published CSV has 4 preamble rows before the header — we skip
    them with ``skip=4`` and supply explicit column names because the
    "CQC Location ID (for office use only)" header has parentheses
    that duckdb auto-detection sometimes mangles.
    """
    con.execute("DROP TABLE IF EXISTS cqc_locations_staging")
    src = _escape_path(csv_file)
    con.execute(rf"""
        CREATE TABLE cqc_locations_staging AS
        SELECT
            "Name"                                           AS name,
            "Also known as"                                  AS also_known_as,
            "Address"                                        AS address,
            "Postcode"                                       AS postcode,
            "Phone number"                                   AS phone_number,
            "Service's website (if available)"               AS website,
            "Service types"                                  AS service_types,
            "Date of latest check"                           AS date_of_latest_check_raw,
            "Specialisms/services"                           AS specialisms,
            "Provider name"                                  AS provider_name,
            "Local authority"                                AS local_authority,
            "Region"                                         AS region,
            "Location URL"                                   AS location_url,
            "CQC Location ID (for office use only)"          AS location_id,
            "CQC Provider ID (for office use only)"          AS provider_id
        FROM read_csv_auto(
            '{src}',
            header = true,
            ignore_errors = true,
            all_varchar = true,
            skip = 4
        )
        WHERE location_id IS NOT NULL AND location_id != ''
    """)


def _load_hsca_sheet(file_path: Path, sheet_name: str) -> pd.DataFrame:
    try:
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            engine="odf",
            dtype=str,
            keep_default_na=False,
        )
    except ValueError as exc:
        raise ValueError(
            f"Sheet {sheet_name!r} not found in {file_path.name}"
        ) from exc
    return df.fillna("")


def _missing_columns(
    df: pd.DataFrame,
    required_columns: list[str],
) -> list[str]:
    present = set(df.columns.tolist())
    return [name for name in required_columns if name not in present]


def _clean_text(value: object) -> str | None:
    text = str(value or "").strip()
    if text in {"", "-", "*"}:
        return None
    return text


def _parse_flag(value: object) -> bool:
    text = str(value or "").strip().upper()
    return text in {"Y", "YES", "TRUE", "1"}


def _parse_int(value: object) -> int | None:
    text = _clean_text(value)
    if text is None:
        return None
    text = text.replace(",", "")
    try:
        return int(float(text))
    except ValueError:
        return None


def _parse_uk_date(value: object) -> date | None:
    text = _clean_text(value)
    if text is None:
        return None
    try:
        return datetime.strptime(text, "%d/%m/%Y").date()
    except ValueError:
        return None


def _build_hsca_location_rows(
    df: pd.DataFrame,
    scrape_date: date,
    imported_at: datetime,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for record in df.to_dict(orient="records"):
        location_id = _clean_text(record.get("Location ID"))
        provider_id = _clean_text(record.get("Provider ID"))
        if location_id is None or provider_id is None:
            continue

        row: dict[str, object] = {
            "location_id": location_id,
            "provider_id": provider_id,
            "provider_companies_house_number": _clean_text(
                record.get("Provider Companies House Number")
            ),
            "provider_charity_number": _clean_text(
                record.get("Provider Charity Number")
            ),
            "provider_ownership_type": _clean_text(
                record.get("Provider Ownership Type")
            ),
            "provider_brand_id": _clean_text(record.get("Brand ID")),
            "provider_brand_name": _clean_text(record.get("Brand Name")),
            "provider_web_address": _clean_text(
                record.get("Provider Web Address")
            ),
            "location_web_address": _clean_text(
                record.get("Location Web Address")
            ),
            "care_home": _parse_flag(record.get("Care home?")),
            "number_of_beds": _parse_int(record.get("Care homes beds")),
            "dormant": _parse_flag(record.get("Dormant (Y/N)")),
            "registered_manager_name": _clean_text(
                record.get("Registered manager")
            ),
            "bulk_imported_at": imported_at,
            "bulk_file_date": scrape_date,
            "raw_row": json.dumps(record, ensure_ascii=True, sort_keys=True),
        }
        for column_name, field_name in HSCA_SERVICE_TYPE_TO_FIELD.items():
            row[field_name] = _parse_flag(record.get(column_name))
        rows.append(row)
    return rows


def _build_hsca_dual_rows(df: pd.DataFrame) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for record in df.to_dict(orient="records"):
        location_id = _clean_text(record.get("Location ID"))
        provider_id = _clean_text(record.get("Provider ID"))
        linked_org_id = _clean_text(record.get("Linked Organisation ID"))
        if location_id is None or provider_id is None or linked_org_id is None:
            continue

        rows.append(
            {
                "location_id": location_id,
                "location_name": _clean_text(record.get("Location Name")),
                "location_hsca_start_date": _parse_uk_date(
                    record.get("Location HSCA Start Date")
                ),
                "location_type_sector": _clean_text(
                    record.get("Location Type/Sector")
                ),
                "provider_id": provider_id,
                "provider_name": _clean_text(record.get("Provider Name")),
                "linked_organisation_id": linked_org_id,
                "linked_organisation_name": _clean_text(
                    record.get("Linked Organisation Name")
                ),
                "relationship": _clean_text(record.get("Relationship")),
                "relationship_start_date": _parse_uk_date(
                    record.get("Relationship Start Date")
                ),
                "primary_id": _parse_flag(record.get("Primary ID")),
            }
        )
    return rows


def _ingest_hsca_to_staging(
    con: duckdb.DuckDBPyConnection,
    locations_rows: list[dict[str, object]],
    dual_rows: list[dict[str, object]],
) -> None:
    con.execute("DROP TABLE IF EXISTS cqc_hsca_locations_staging")
    con.execute("DROP TABLE IF EXISTS cqc_hsca_dual_registrations_staging")

    locations_df = pd.DataFrame(locations_rows, columns=HSCA_LOCATION_STAGE_COLUMNS)
    dual_df = pd.DataFrame(dual_rows, columns=HSCA_DUAL_STAGE_COLUMNS)

    con.register("hsca_locations_df", locations_df)
    con.register("hsca_dual_df", dual_df)
    try:
        con.execute(
            """
            CREATE TABLE cqc_hsca_locations_staging AS
            SELECT
                CAST(location_id AS TEXT) AS location_id,
                CAST(provider_id AS TEXT) AS provider_id,
                CAST(provider_companies_house_number AS TEXT) AS provider_companies_house_number,
                CAST(provider_charity_number AS TEXT) AS provider_charity_number,
                CAST(provider_ownership_type AS TEXT) AS provider_ownership_type,
                CAST(provider_brand_id AS TEXT) AS provider_brand_id,
                CAST(provider_brand_name AS TEXT) AS provider_brand_name,
                CAST(provider_web_address AS TEXT) AS provider_web_address,
                CAST(location_web_address AS TEXT) AS location_web_address,
                CAST(care_home AS BOOLEAN) AS care_home,
                CAST(number_of_beds AS INTEGER) AS number_of_beds,
                CAST(dormant AS BOOLEAN) AS dormant,
                CAST(registered_manager_name AS TEXT) AS registered_manager_name,
                CAST(st_domiciliary_care_service AS BOOLEAN) AS st_domiciliary_care_service,
                CAST(st_supported_living_service AS BOOLEAN) AS st_supported_living_service,
                CAST(st_care_home_with_nursing AS BOOLEAN) AS st_care_home_with_nursing,
                CAST(st_care_home_without_nursing AS BOOLEAN) AS st_care_home_without_nursing,
                CAST(st_extra_care_housing_services AS BOOLEAN) AS st_extra_care_housing_services,
                CAST(st_hospice_services_at_home AS BOOLEAN) AS st_hospice_services_at_home,
                CAST(bulk_imported_at AS TIMESTAMP) AS bulk_imported_at,
                CAST(bulk_file_date AS DATE) AS bulk_file_date,
                CAST(raw_row AS JSON) AS raw_row
            FROM hsca_locations_df
            """
        )
        con.execute(
            """
            CREATE TABLE cqc_hsca_dual_registrations_staging AS
            SELECT
                CAST(location_id AS TEXT) AS location_id,
                CAST(location_name AS TEXT) AS location_name,
                CAST(location_hsca_start_date AS DATE) AS location_hsca_start_date,
                CAST(location_type_sector AS TEXT) AS location_type_sector,
                CAST(provider_id AS TEXT) AS provider_id,
                CAST(provider_name AS TEXT) AS provider_name,
                CAST(linked_organisation_id AS TEXT) AS linked_organisation_id,
                CAST(linked_organisation_name AS TEXT) AS linked_organisation_name,
                CAST(relationship AS TEXT) AS relationship,
                CAST(relationship_start_date AS DATE) AS relationship_start_date,
                CAST(primary_id AS BOOLEAN) AS primary_id
            FROM hsca_dual_df
            """
        )
    finally:
        con.unregister("hsca_locations_df")
        con.unregister("hsca_dual_df")


def _check_hsca_sanity(
    con: duckdb.DuckDBPyConnection,
) -> HSCASanityResult:
    table_exists = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'cqc_hsca_locations'"
    ).fetchone()[0] > 0

    row_count = con.execute(
        "SELECT COUNT(*) FROM cqc_hsca_locations_staging"
    ).fetchone()[0]
    dup_rows = con.execute(
        """
        SELECT location_id
        FROM cqc_hsca_locations_staging
        GROUP BY location_id
        HAVING COUNT(*) > 1
        ORDER BY location_id
        """
    ).fetchall()
    dup_sample = [row[0] for row in dup_rows[:5]]
    ch_numbers_populated = con.execute(
        """
        SELECT COUNT(*)
        FROM cqc_hsca_locations_staging
        WHERE provider_companies_house_number IS NOT NULL
          AND trim(provider_companies_house_number) != ''
        """
    ).fetchone()[0]
    ch_numbers_pct = (
        0.0 if row_count == 0 else (ch_numbers_populated / row_count) * 100.0
    )

    old_total = 0
    row_delta = 0
    row_pct = None
    if table_exists:
        old_total = con.execute(
            "SELECT COUNT(*) FROM cqc_hsca_locations"
        ).fetchone()[0]
        row_delta = abs(row_count - old_total)
        if old_total > 0:
            row_pct = (row_delta / old_total) * 100.0

    return HSCASanityResult(
        table_exists=table_exists,
        row_count=int(row_count),
        dup_distinct_location_ids=len(dup_rows),
        dup_sample=dup_sample,
        old_total=int(old_total),
        row_delta=int(row_delta),
        row_pct=None if row_pct is None else float(row_pct),
        ch_numbers_populated=int(ch_numbers_populated),
        ch_numbers_pct=float(ch_numbers_pct),
    )


def _enforce_hsca_sanity(
    result: HSCASanityResult,
    force: bool,
) -> None:
    hard_failures: list[str] = []
    soft_failures: list[str] = []

    if result.row_count == 0:
        hard_failures.append("No HSCA rows were parsed from the ODS file.")

    if result.dup_distinct_location_ids > 0:
        sample = ", ".join(result.dup_sample)
        hard_failures.append(
            "DUPLICATE location_ids in HSCA staging "
            f"({result.dup_distinct_location_ids} duplicated IDs). "
            f"Sample: [{sample}]. force=True does NOT override."
        )

    if (
        result.table_exists
        and result.row_pct is not None
        and result.row_pct > HSCA_ROW_COUNT_PCT_THRESHOLD
    ):
        soft_failures.append(
            f"ROW COUNT delta {result.row_pct:.2f}% exceeds "
            f"{HSCA_ROW_COUNT_PCT_THRESHOLD:.2f}% threshold "
            f"({result.old_total:,} → {result.row_count:,})."
        )

    if result.ch_numbers_pct < HSCA_CH_NUMBER_PCT_THRESHOLD:
        soft_failures.append(
            "Provider Companies House Number populated percentage "
            f"{result.ch_numbers_pct:.2f}% is below the "
            f"{HSCA_CH_NUMBER_PCT_THRESHOLD:.2f}% threshold "
            f"({result.ch_numbers_populated:,}/{result.row_count:,})."
        )

    if hard_failures:
        raise SanityCheckError(
            SanityCheckResult(
                companies_exists=result.table_exists,
                dup_distinct_numbers=result.dup_distinct_location_ids,
                dup_excess_rows=result.dup_distinct_location_ids,
                dup_sample=result.dup_sample,
                old_total=result.old_total,
                new_total=result.row_count,
                row_delta=result.row_delta,
                row_pct=result.row_pct,
                currently_active=0,
                would_be_inactivated=0,
                inactive_pct=None,
            ),
            "HSCA sanity check FAILED:\n  - " + "\n  - ".join(hard_failures),
        )

    if soft_failures and not force:
        raise SanityCheckError(
            SanityCheckResult(
                companies_exists=result.table_exists,
                dup_distinct_numbers=result.dup_distinct_location_ids,
                dup_excess_rows=result.dup_distinct_location_ids,
                dup_sample=result.dup_sample,
                old_total=result.old_total,
                new_total=result.row_count,
                row_delta=result.row_delta,
                row_pct=result.row_pct,
                currently_active=0,
                would_be_inactivated=0,
                inactive_pct=None,
            ),
            "HSCA sanity check FAILED:\n  - "
            + "\n  - ".join(soft_failures)
            + "\n\nRe-run with force=True to override.",
        )


def _check_cqc_sanity(con: duckdb.DuckDBPyConnection) -> SanityCheckResult:
    """Run the CQC sanity SQL files. Reuses the CH SanityCheckResult shape."""
    cqc_exists = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'cqc_locations'"
    ).fetchone()[0] > 0

    dup_distinct_ids, dup_excess_rows, dup_sample = _fetchone_sql_file(
        con, "sanity_staging.sql"
    )

    if cqc_exists:
        (
            old_total,
            new_total,
            row_delta,
            row_pct,
            currently_active,
            would_be_inactivated,
            inactive_pct,
        ) = _fetchone_sql_file(con, "sanity_upsert.sql")
    else:
        old_total = new_total = row_delta = 0
        row_pct = None
        currently_active = would_be_inactivated = 0
        inactive_pct = None

    return SanityCheckResult(
        companies_exists=cqc_exists,  # field name kept generic
        dup_distinct_numbers=int(dup_distinct_ids or 0),
        dup_excess_rows=int(dup_excess_rows or 0),
        dup_sample=list(dup_sample or []),
        old_total=int(old_total or 0),
        new_total=int(new_total or 0),
        row_delta=int(row_delta or 0),
        row_pct=None if row_pct is None else float(row_pct),
        currently_active=int(currently_active or 0),
        would_be_inactivated=int(would_be_inactivated or 0),
        inactive_pct=None if inactive_pct is None else float(inactive_pct),
    )


def _enforce_cqc_sanity(result: SanityCheckResult, force: bool) -> None:
    failures: list[str] = []
    if result.dup_distinct_numbers > 0:
        sample = ", ".join(str(s) for s in result.dup_sample[:5])
        failures.append(
            f"DUPLICATE location_ids in staging "
            f"({result.dup_distinct_numbers} distinct IDs duplicated, "
            f"{result.dup_excess_rows} excess rows). Sample: [{sample}]. "
            f"force=True does NOT override."
        )
    if result.companies_exists:
        if result.row_pct is not None and result.row_pct > ROW_COUNT_PCT_THRESHOLD:
            failures.append(
                f"ROW COUNT delta {result.row_pct:.2f}% exceeds "
                f"{ROW_COUNT_PCT_THRESHOLD:.2f}% threshold "
                f"({result.old_total:,} → {result.new_total:,})."
            )
        if result.inactive_pct is not None and result.inactive_pct > INACTIVE_CHURN_PCT_THRESHOLD:
            failures.append(
                f"INACTIVE CHURN {result.inactive_pct:.2f}% exceeds "
                f"{INACTIVE_CHURN_PCT_THRESHOLD:.2f}% threshold "
                f"(would mark {result.would_be_inactivated:,} of "
                f"{result.currently_active:,} active rows inactive)."
            )
    if not failures:
        return
    if result.dup_distinct_numbers > 0:
        raise SanityCheckError(result, "CQC sanity check FAILED:\n  - " + "\n  - ".join(failures))
    if not force:
        raise SanityCheckError(
            result,
            "CQC sanity check FAILED:\n  - " + "\n  - ".join(failures)
            + "\n\nRe-run with force=True to override.",
        )


def process_cqc_csv(
    csv_file: str | Path,
    db_path: str | Path,
    progress_callback: callable | None = None,
    force: bool = False,
    scrape_date_override: date | None = None,
    compact: bool = True,
) -> int:
    """Ingest one CQC directory CSV into the DuckDB database.

    Returns total rows in cqc_locations after the operation.
    """
    csv_file = Path(csv_file)
    db_path = Path(db_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_file}")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    scrape_date = scrape_date_override or scrape_date_from_cqc_filename(csv_file)

    def _notify(msg: str) -> None:
        if progress_callback:
            progress_callback(msg)
        else:
            console.print(f"[bold magenta]{msg}[/]")

    @timed_phase("cqc_ingest_to_staging")
    def _do_ingest(con):
        _ingest_cqc_to_staging(con, csv_file)

    @timed_phase("cqc_sanity_checks")
    def _do_sanity(con):
        return _check_cqc_sanity(con)

    @timed_phase("cqc_bootstrap_locations")
    def _do_bootstrap(con):
        _run_sql_file(con, "bootstrap_locations.sql", scrape_date=scrape_date.isoformat())

    @timed_phase("cqc_upsert_locations")
    def _do_upsert(con):
        con.execute("BEGIN TRANSACTION")
        try:
            _run_sql_file(con, "upsert_locations.sql", scrape_date=scrape_date.isoformat())
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    @timed_phase("cqc_rollup_providers")
    def _do_rollup(con):
        _run_sql_file(con, "rollup_providers.sql")

    @timed_phase("cqc_create_indexes")
    def _do_indexes(con):
        _run_sql_file(con, "indexes.sql")

    _notify(f"CQC: processing scrape_date {scrape_date.isoformat()}")
    t_total = time.perf_counter()
    con = duckdb.connect(str(db_path))
    try:
        _do_ingest(con)
        result = _do_sanity(con)
        _enforce_cqc_sanity(result, force=force)

        if not result.companies_exists:
            _do_bootstrap(con)
        else:
            _notify(
                f"  CQC sanity OK: row delta {result.row_pct:.2f}%, "
                f"inactive churn {result.inactive_pct:.2f}%"
            )
            _do_upsert(con)

        _do_rollup(con)
        _do_indexes(con)
        con.execute("DROP TABLE IF EXISTS cqc_locations_staging")

        loc_count = con.execute("SELECT COUNT(*) FROM cqc_locations").fetchone()[0]
        prov_count = con.execute("SELECT COUNT(*) FROM cqc_providers").fetchone()[0]
        active_loc = con.execute("SELECT COUNT(*) FROM cqc_locations WHERE is_active = TRUE").fetchone()[0]

        total = time.perf_counter() - t_total
        _notify(
            f"CQC done! {loc_count:,} locations ({active_loc:,} active) "
            f"across {prov_count:,} providers in {total:.1f}s"
        )
        logger.info(
            "CQC processed %d locations (%d active), %d providers for "
            "scrape_date=%s in %.1fs",
            loc_count, active_loc, prov_count, scrape_date.isoformat(), total,
        )
    finally:
        con.close()

    if compact:
        compact_database(db_path, progress_callback=progress_callback)

    return loc_count


def process_hsca_filters(
    file_path: str | Path,
    db_path: str | Path,
    progress_callback: callable | None = None,
    force: bool = False,
    scrape_date_override: date | None = None,
    compact: bool = True,
) -> int:
    """Ingest one HSCA active locations ODS into the DuckDB database.

    Returns total rows in ``cqc_hsca_locations`` after the operation.
    """
    file_path = Path(file_path)
    db_path = Path(db_path)
    if not file_path.exists():
        raise FileNotFoundError(f"ODS file not found: {file_path}")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    scrape_date = scrape_date_override or scrape_date_from_hsca_filename(file_path)

    def _notify(msg: str) -> None:
        if progress_callback:
            progress_callback(msg)
        else:
            console.print(f"[bold magenta]{msg}[/]")

    _notify(f"HSCA: processing scrape_date {scrape_date.isoformat()}")
    t_total = time.perf_counter()
    con = duckdb.connect(str(db_path))
    batch_id: str | None = None
    try:
        ensure_pipeline_schema(con)
        batch_id = insert_sync_batch(con, sync_type="bulk_hsca", mode="all")

        @timed_phase("hsca_parse_workbook")
        def _do_parse():
            locations_df = _load_hsca_sheet(file_path, HSCA_SHEET_NAME)
            dual_df = _load_hsca_sheet(file_path, HSCA_DUAL_SHEET_NAME)

            missing_locations = _missing_columns(
                locations_df, HSCA_LOCATION_REQUIRED_COLUMNS
            )
            if missing_locations:
                raise ValueError(
                    "HSCA locations sheet is missing required columns: "
                    + ", ".join(missing_locations)
                )

            missing_dual = _missing_columns(dual_df, HSCA_DUAL_REQUIRED_COLUMNS)
            if missing_dual:
                raise ValueError(
                    "HSCA dual-registration sheet is missing required columns: "
                    + ", ".join(missing_dual)
                )

            imported_at = datetime.now(timezone.utc).replace(tzinfo=None)
            return (
                _build_hsca_location_rows(locations_df, scrape_date, imported_at),
                _build_hsca_dual_rows(dual_df),
            )

        @timed_phase("hsca_ingest_to_staging")
        def _do_ingest(
            locations_rows: list[dict[str, object]],
            dual_rows: list[dict[str, object]],
        ) -> None:
            _ingest_hsca_to_staging(con, locations_rows, dual_rows)

        @timed_phase("hsca_sanity_checks")
        def _do_sanity() -> HSCASanityResult:
            return _check_hsca_sanity(con)

        @timed_phase("hsca_upsert")
        def _do_upsert() -> None:
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute("DROP TABLE IF EXISTS cqc_hsca_dual_registrations")
                con.execute("DELETE FROM cqc_hsca_locations")
                _run_sql_file(con, "bootstrap_hsca_dual_registrations.sql")
                _run_sql_file(con, "upsert_hsca_locations.sql")
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

        locations_rows, dual_rows = _do_parse()
        _do_ingest(locations_rows, dual_rows)
        result = _do_sanity()
        _enforce_hsca_sanity(result, force=force)

        if result.table_exists and result.row_pct is not None:
            _notify(
                f"  HSCA sanity OK: row delta {result.row_pct:.2f}%, "
                f"CH-number populated {result.ch_numbers_pct:.2f}%"
            )
        else:
            _notify(
                "  HSCA sanity OK: "
                f"{result.row_count:,} rows, "
                f"CH-number populated {result.ch_numbers_pct:.2f}%"
            )

        _do_upsert()
        con.execute("DROP TABLE IF EXISTS cqc_hsca_locations_staging")
        con.execute("DROP TABLE IF EXISTS cqc_hsca_dual_registrations_staging")

        locations_count = con.execute(
            "SELECT COUNT(*) FROM cqc_hsca_locations"
        ).fetchone()[0]
        dual_count = con.execute(
            "SELECT COUNT(*) FROM cqc_hsca_dual_registrations"
        ).fetchone()[0]

        finish_sync_batch(
            con,
            batch_id,
            status="succeeded",
            records_fetched=locations_count,
            records_updated=locations_count,
            error_count=0,
        )

        total = time.perf_counter() - t_total
        _notify(
            f"HSCA done! {locations_count:,} locations and "
            f"{dual_count:,} dual registrations in {total:.1f}s"
        )
        logger.info(
            "HSCA processed %d locations and %d dual registrations for "
            "scrape_date=%s in %.1fs (batch_id=%s)",
            locations_count,
            dual_count,
            scrape_date.isoformat(),
            total,
            batch_id,
        )
    except Exception:
        if batch_id is not None:
            finish_sync_batch(
                con,
                batch_id,
                status="failed",
                error_count=1,
            )
        raise
    finally:
        con.execute("DROP TABLE IF EXISTS cqc_hsca_locations_staging")
        con.execute("DROP TABLE IF EXISTS cqc_hsca_dual_registrations_staging")
        con.close()

    if compact:
        compact_database(db_path, progress_callback=progress_callback)

    return locations_count
