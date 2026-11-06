"""Load companies missing from the bulk snapshot via the CH profile API.

Rows inserted here carry ``record_source = 'api'``; bulk rows have NULL.
Each fetch sets ``last_scrape_date`` to the fetch day, like a bulk snapshot.
"""

from __future__ import annotations

import tempfile
import threading
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable

import duckdb

from ch_bulk.companies_house.ch_enricher import CompaniesHouseClient
from ch_bulk.companies_house.processor import (
    BULK_CSV_COLUMNS,
    _ingest_to_staging,
    _run_sql_file,
)
from ch_bulk.core.cancellation import raise_if_cancelled
from ch_bulk.db.bootstrap import (
    _column_exists,
    _table_exists,
    ensure_companies_record_source,
)
from ch_bulk.db.staging import with_duckdb_connection

RECORD_SOURCE_API = "api"

# API enum -> bulk CSV vocabulary, so API rows filter like bulk rows.
COMPANY_STATUS_LABELS = {
    "active": "Active",
    "dissolved": "Dissolved",
    "liquidation": "Liquidation",
    "administration": "In Administration",
    "voluntary-arrangement": "Voluntary Arrangement",
    "receivership": "RECEIVERSHIP",
}
COMPANY_TYPE_LABELS = {
    "ltd": "Private Limited Company",
    "plc": "Public Limited Company",
    "llp": "Limited Liability Partnership",
    "limited-partnership": "Limited Partnership",
    "private-limited-guarant-nsc": (
        "PRI/LTD BY GUAR/NSC (Private, limited by guarantee, no share capital)"
    ),
    "private-limited-guarant-nsc-limited-exemption": (
        "PRI/LBG/NSC (Private, Limited by guarantee, no share capital, "
        "use of 'Limited' exemption)"
    ),
    "private-unlimited": "Private Unlimited Company",
    "charitable-incorporated-organisation": "Charitable Incorporated Organisation",
    "scottish-charitable-incorporated-organisation": (
        "Scottish Charitable Incorporated Organisation"
    ),
    "registered-overseas-entity": "Overseas Entity",
    "royal-charter": "Royal Charter Company",
    "scottish-partnership": "Scottish Partnership",
    "old-public-company": "Old Public Company",
}
INACTIVE_STATUSES = {"dissolved", "converted-closed", "removed", "closed"}


@dataclass(frozen=True)
class ProfileLoadResult:
    inserted: list[str]
    not_found: list[str]
    already_present: list[str]


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _date(value: object) -> date | None:
    text = _text(value)
    return date.fromisoformat(text) if text else None


def _int(value: object) -> int | None:
    text = _text(value)
    return int(text) if text else None


def _company_status(status: str | None, detail: str | None) -> str | None:
    if status is None:
        return None
    if status == "active" and detail == "active-proposal-to-strike-off":
        return "Active - Proposal to Strike off"
    if status in COMPANY_STATUS_LABELS:
        return COMPANY_STATUS_LABELS[status]
    return status.replace("-", " ").capitalize()


def profile_to_companies_row(profile: dict) -> dict[str, object]:
    """Map a ``GET /company/{number}`` payload onto ``companies`` columns."""
    status = _text(profile.get("company_status"))
    company_type = _text(profile.get("type"))
    sic_codes = profile.get("sic_codes") or []
    address = profile.get("registered_office_address") or {}
    accounts = profile.get("accounts") or {}
    last_accounts = accounts.get("last_accounts") or {}
    reference_date = accounts.get("accounting_reference_date") or {}
    confirmation = profile.get("confirmation_statement") or {}

    address_parts = [
        _text(address.get(key))
        for key in (
            "care_of",
            "po_box",
            "address_line_1",
            "address_line_2",
            "locality",
            "region",
            "country",
        )
    ]
    non_empty_parts = [part for part in address_parts if part]

    row: dict[str, object] = {
        "company_number": profile["company_number"],
        "company_name": profile.get("company_name"),
        "company_status": _company_status(
            status, _text(profile.get("company_status_detail"))
        ),
        "company_type": (
            None
            if company_type is None
            else COMPANY_TYPE_LABELS.get(company_type, company_type)
        ),
        "incorporation_date": _date(profile.get("date_of_creation")),
        "dissolution_date": _date(profile.get("date_of_cessation")),
        "address_care_of": address_parts[0],
        "address_po_box": address_parts[1],
        "address_line_1": address_parts[2],
        "address_line_2": address_parts[3],
        "address_post_town": address_parts[4],
        "address_county": address_parts[5],
        "address_country": address_parts[6],
        "postcode": _text(address.get("postal_code")),
        "registered_address": ", ".join(non_empty_parts) or None,
        "accounts_last_made_up": _date(last_accounts.get("made_up_to")),
        "accounts_next_due": _date(accounts.get("next_due")),
        "accounts_ref_day": _int(reference_date.get("day")),
        "accounts_ref_month": _int(reference_date.get("month")),
        "conf_stmt_next_due": _date(confirmation.get("next_due")),
        "conf_stmt_last_made_up": _date(confirmation.get("last_made_up_to")),
        "is_active": status is not None and status not in INACTIVE_STATUSES,
        "record_source": RECORD_SOURCE_API,
    }
    for position in range(4):
        row[f"sic_code_{position + 1}"] = (
            sic_codes[position] if position < len(sic_codes) else None
        )
    return row


def ensure_companies_table(con: duckdb.DuckDBPyConnection) -> None:
    """Create an empty bulk-shaped ``companies`` table if missing.

    Runs the real staging ingest + bootstrap over a header-only CSV so the
    columns and types match a bulk-built table exactly.
    """
    if not _table_exists(con, "companies"):
        with tempfile.TemporaryDirectory() as tmp:
            header_csv = Path(tmp) / "header_only.csv"
            header_csv.write_text(",".join(BULK_CSV_COLUMNS) + "\n")
            _ingest_to_staging(con, [header_csv])
        _run_sql_file(
            con, "bootstrap_companies.sql", scrape_date=date.today().isoformat()
        )
        _run_sql_file(con, "indexes.sql")
        con.execute("DROP TABLE IF EXISTS companies_staging")
    ensure_companies_record_source(con)


def has_bulk_companies(con: duckdb.DuckDBPyConnection) -> bool:
    if not _table_exists(con, "companies"):
        return False
    if not _column_exists(con, "companies", "record_source"):
        # Older DBs predate API loading, so every row is a bulk row.
        return con.execute("SELECT EXISTS (SELECT 1 FROM companies)").fetchone()[0]
    return con.execute(
        "SELECT EXISTS (SELECT 1 FROM companies WHERE record_source IS NULL)"
    ).fetchone()[0]


def _insert_company_row(
    con: duckdb.DuckDBPyConnection,
    row: dict[str, object],
) -> None:
    columns = list(row)
    con.execute(
        f"INSERT INTO companies ({', '.join(columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)})",
        [row[column] for column in columns],
    )


def _replace_company_row(
    con: duckdb.DuckDBPyConnection,
    row: dict[str, object],
) -> None:
    # UPDATE rather than DELETE + INSERT: DuckDB rejects re-inserting a
    # deleted key in one transaction against a persisted unique index.
    # Columns the API does not supply (bulk-only fields, first_scrape_date)
    # keep their stored values.
    columns = [name for name in row if name != "company_number"]
    con.execute(
        f"UPDATE companies SET {', '.join(f'{c} = ?' for c in columns)} "
        "WHERE company_number = ?",
        [row[column] for column in columns] + [row["company_number"]],
    )


def load_missing_companies(
    db_path: str | Path,
    data_dir: str | Path | None,
    company_numbers: list[str],
    *,
    refresh: bool = False,
    client: object | None = None,
    cancel_event: threading.Event | None = None,
    progress: Callable[[str], None] | None = None,
) -> ProfileLoadResult:
    """Insert ``companies`` rows for numbers not already stored.

    Stored rows whose ``last_scrape_date`` is more than a month old (or
    missing) are re-fetched and updated in place; bulk rows stay bulk rows.
    ``refresh=True`` also re-fetches every row previously loaded from the API.
    """

    def _existing_rows(
        con: duckdb.DuckDBPyConnection,
    ) -> dict[str, tuple[str | None, bool]]:
        ensure_companies_table(con)
        if not company_numbers:
            return {}
        rows = con.execute(
            "SELECT company_number, record_source, "
            "coalesce(last_scrape_date < CURRENT_DATE - INTERVAL 1 MONTH, TRUE) "
            "FROM companies "
            "WHERE company_number IN (SELECT UNNEST(?::VARCHAR[]))",
            [company_numbers],
        ).fetchall()
        return {number: (source, stale) for number, source, stale in rows}

    existing = with_duckdb_connection(db_path, _existing_rows)

    already_present: list[str] = []
    to_fetch: list[str] = []
    for number in company_numbers:
        if number not in existing:
            to_fetch.append(number)
            continue
        source, stale = existing[number]
        if stale or (refresh and source == RECORD_SOURCE_API):
            to_fetch.append(number)
        else:
            already_present.append(number)

    inserted: list[str] = []
    not_found: list[str] = []
    owns_client = client is None and bool(to_fetch)
    if owns_client:
        client = CompaniesHouseClient(data_dir)
    try:
        for index, number in enumerate(to_fetch, start=1):
            raise_if_cancelled(cancel_event, reason="company lookup cancelled")
            if progress is not None:
                progress(f"Looking up {index}/{len(to_fetch)}: {number}")
            profile = client.get_company_profile(number, cancel_event=cancel_event)
            if profile is None:
                not_found.append(number)
                continue
            row = profile_to_companies_row(profile)
            row["last_scrape_date"] = date.today()
            if number in existing:
                if existing[number][0] != RECORD_SOURCE_API:
                    del row["record_source"]
                write = _replace_company_row
            else:
                write = _insert_company_row
            with_duckdb_connection(db_path, lambda con: write(con, row))
            inserted.append(number)
    finally:
        if owns_client:
            client.close()

    return ProfileLoadResult(
        inserted=inserted,
        not_found=not_found,
        already_present=already_present,
    )
