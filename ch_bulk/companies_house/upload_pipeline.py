"""Upload List orchestrator: company-number file in, profile workbook out."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable

import duckdb

from ch_bulk.companies_house import ch_enricher, financials_enricher
from ch_bulk.companies_house.company_profile import load_missing_companies
from ch_bulk.companies_house.upload_export import build_profile_rows, write_profile_xlsx
from ch_bulk.companies_house.upload_list import read_company_numbers
from ch_bulk.core.cancellation import raise_if_cancelled
from ch_bulk.db.bootstrap import _table_exists
from ch_bulk.db.staging import with_duckdb_connection

_CANCEL_REASON = "upload cancelled"


@dataclass(frozen=True)
class UploadSummary:
    output_path: Path
    requested: int
    invalid: int
    not_found: int
    looked_up: int
    directors_fetched: int
    financials_fetched: int


def _enrichment_state(
    db_path: str | Path,
    company_numbers: list[str],
) -> dict[str, tuple[object, object, object, object, bool]]:
    """Map number -> (directors_dob_years, directors, filing_id, revenue_source, stale).

    ``stale``: last enriched more than a month ago, so fetch directors and
    accounts again.
    """

    def query(con: duckdb.DuckDBPyConnection):
        if not company_numbers or not _table_exists(con, "company_enrichment"):
            return {}
        rows = con.execute(
            """
            SELECT company_number, directors_dob_years, directors,
                   filing_id, revenue_source,
                   coalesce(CAST(last_enriched_at AS DATE)
                            < CURRENT_DATE - INTERVAL 1 MONTH, TRUE)
            FROM company_enrichment
            WHERE company_number IN (SELECT UNNEST(?::VARCHAR[]))
            """,
            [company_numbers],
        ).fetchall()
        return {row[0]: tuple(row[1:]) for row in rows}

    return with_duckdb_connection(db_path, query)


def _directors_targets(
    found: list[str],
    state: dict[str, tuple],
    refresh: bool,
) -> list[str]:
    if refresh:
        return list(found)
    return [n for n in found if n not in state or state[n][4] or state[n][0] is None]


def _financials_targets(
    found: list[str],
    state: dict[str, tuple],
    refresh: bool,
) -> list[str]:
    if refresh:
        return list(found)
    return [
        n
        for n in found
        if n not in state
        or state[n][4]
        or (state[n][2] is None and (state[n][3] or "") != "no_recent_filing")
    ]


def _needs_directors_backfill(state: dict[str, tuple], found: list[str]) -> bool:
    for number in found:
        if number not in state or state[number][1] is None:
            continue
        directors = state[number][1]
        if isinstance(directors, str):
            directors = json.loads(directors)
        if any("date_of_birth" not in entry for entry in directors or []):
            return True
    return False


def run_upload(
    db_path: str | Path,
    data_dir: str | Path | None,
    input_path: str | Path,
    output_path: str | Path,
    *,
    refresh: bool = False,
    cancel_event: threading.Event | None = None,
    progress: Callable[[str], None] | None = None,
) -> UploadSummary:
    def notify(message: str) -> None:
        if progress is not None:
            progress(message)

    notify("Reading file")
    upload = read_company_numbers(input_path)

    raise_if_cancelled(cancel_event, reason=_CANCEL_REASON)
    notify(f"Looking up {len(upload.numbers)} companies")
    load = load_missing_companies(
        db_path,
        data_dir,
        upload.numbers,
        refresh=refresh,
        cancel_event=cancel_event,
        progress=progress,
    )
    not_found = set(load.not_found)
    found = [n for n in upload.numbers if n not in not_found]

    raise_if_cancelled(cancel_event, reason=_CANCEL_REASON)
    # Both target lists come from the state before any fetch: fetching
    # directors stamps last_enriched_at, which would hide stale accounts.
    state = _enrichment_state(db_path, found)
    directors_targets = _directors_targets(found, state, refresh)
    financials_targets = _financials_targets(found, state, refresh)
    if directors_targets:
        notify(f"Fetching directors for {len(directors_targets)}")
        ch_enricher.enrich_directors(
            db_path,
            data_dir,
            company_numbers=directors_targets,
            cancel_event=cancel_event,
        )

    raise_if_cancelled(cancel_event, reason=_CANCEL_REASON)
    if _needs_directors_backfill(_enrichment_state(db_path, found), found):
        ch_enricher.backfill_directors(db_path)

    raise_if_cancelled(cancel_event, reason=_CANCEL_REASON)
    if financials_targets:
        notify(f"Fetching accounts for {len(financials_targets)}")
        financials_enricher.enrich_financials(
            db_path,
            data_dir,
            mode="list",
            ids=financials_targets,
            cancel_event=cancel_event,
        )

    raise_if_cancelled(cancel_event, reason=_CANCEL_REASON)
    notify("Writing workbook")
    rows = build_profile_rows(
        db_path, upload, load, current_year=date.today().year
    )
    written = write_profile_xlsx(rows, output_path)

    return UploadSummary(
        output_path=written,
        requested=len(upload.numbers),
        invalid=len(upload.invalid),
        not_found=len(load.not_found),
        looked_up=len(load.inserted),
        directors_fetched=len(directors_targets),
        financials_fetched=len(financials_targets),
    )
