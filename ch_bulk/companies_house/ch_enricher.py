"""Companies House enrichment helpers for directors and revenue."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import duckdb
import httpx

from ch_bulk.core.logging import FsyncLineLogger
from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.core.paths import DATA_REFERENCE_DIR, DEFAULT_DATA_DIR
from ch_bulk.core.rate_limit import SlidingWindowThrottle
from ch_bulk.companies_house.revenue_model import estimate, load_bands
from ch_bulk.core.settings import load_settings
from ch_bulk.db.staging import (
    LoadedBatch,
    RAW_API_RESPONSE_INSERT_SQL,
    StagedAPIResponse,
    StagingWriter,
    batch_id_from_staging_path,
    isoformat_utc,
    mark_staging_file_loaded,
    pending_staging_files,
    scan_staged_api_responses,
    summarize_loaded_batches,
    sync_batch_progress,
    with_duckdb_connection,
)
from ch_bulk.db.sync_batches import (
    finish_sync_batch,
    insert_sync_batch,
    update_sync_batch_progress,
    utcnow_naive,
)

logger = logging.getLogger(__name__)

CH_API_BASE = "https://api.company-information.service.gov.uk"
RETRYABLE_STATUS_CODES = {429, 502, 503, 504}
DEFAULT_RETRY_AFTER_SECONDS = 60
DIRECTOR_LOAD_CHUNK_SIZE = 1000
COMPANY_ENRICHMENT_COLUMNS = [
    "company_number",
    "avg_director_age",
    "min_director_age",
    "max_director_age",
    "directors_over_60",
    "all_directors_60_plus",
    "directors_dob_years",
    "revenue",
    "revenue_source",
    "employee_count",
    "filing_period_start",
    "filing_period_end",
    "gross_profit",
    "profit_before_tax",
    "profit_after_tax",
    "fixed_assets",
    "current_assets",
    "total_assets",
    "net_assets",
    "net_current_assets",
    "filing_id",
    "filing_format",
    "filing_age_months",
    "last_enriched_at",
]
DEFAULT_BATCH_SIZE = 1000


def _json_text(value: object) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class CompaniesHouseClient:
    def __init__(
        self,
        data_dir: str | Path | None = None,
        *,
        api_key: str | None = None,
    ) -> None:
        data_dir_path = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
        if api_key is None:
            api_key = load_settings(data_dir_path)["api_keys"]["companies_house"]
        if not api_key:
            raise RuntimeError(
                "Companies House API key is not configured in settings.json"
            )

        self._client = httpx.Client(
            base_url=CH_API_BASE,
            auth=httpx.BasicAuth(api_key, ""),
            timeout=60,
            follow_redirects=True,
        )
        self._short_throttle = SlidingWindowThrottle(
            550,
            300,
            label="companies_house_short",
            logger=logger,
        )
        self._long_throttle = SlidingWindowThrottle(
            4500,
            1800,
            label="companies_house_long",
            logger=logger,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "CompaniesHouseClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _get(self, path: str, *, params: dict | None = None) -> dict:
        for attempt in range(5):
            self._short_throttle.wait()
            self._long_throttle.wait()
            response = self._client.get(path, params=params)

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                try:
                    pause = int(retry_after) if retry_after else DEFAULT_RETRY_AFTER_SECONDS
                except ValueError:
                    pause = DEFAULT_RETRY_AFTER_SECONDS
                logger.warning(
                    "Companies House 429 for %s, sleeping %ss",
                    path,
                    pause,
                )
                time.sleep(pause)
                continue

            if response.status_code in {502, 503, 504}:
                pause = 2 ** attempt
                logger.warning(
                    "Companies House %s for %s, retrying in %ss",
                    response.status_code,
                    path,
                    pause,
                )
                time.sleep(pause)
                continue

            response.raise_for_status()
            return response.json()

        raise RuntimeError(f"Companies House API failed after retries: {path}")

    def get_officers(self, company_number: str) -> list[dict]:
        payload = self._get(
            f"/company/{company_number}/officers",
            params={"items_per_page": 100},
        )
        return payload.get("items", [])


def compute_age_fields(officers: list[dict], current_year: int) -> dict[str, object]:
    years: list[int] = []
    ages: list[int] = []
    for officer in officers:
        if officer.get("resigned_on"):
            continue
        role = str(officer.get("officer_role") or "").lower()
        if "director" not in role:
            continue
        dob = officer.get("date_of_birth") or {}
        year = _coerce_int(dob.get("year"))
        if year is None:
            continue
        years.append(year)
        ages.append(current_year - year)

    if not ages:
        return {
            "avg_director_age": None,
            "min_director_age": None,
            "max_director_age": None,
            "directors_over_60": 0,
            "all_directors_60_plus": False,
            "directors_dob_years": [],
        }

    return {
        "avg_director_age": int(sum(ages) / len(ages)),
        "min_director_age": min(ages),
        "max_director_age": max(ages),
        "directors_over_60": sum(1 for age in ages if age >= 60),
        "all_directors_60_plus": all(age >= 60 for age in ages),
        "directors_dob_years": years,
    }


def _base_company_enrichment_row(company_number: str) -> dict[str, object]:
    return {
        "company_number": company_number,
        "avg_director_age": None,
        "min_director_age": None,
        "max_director_age": None,
        "directors_over_60": None,
        "all_directors_60_plus": None,
        "directors_dob_years": None,
        "revenue": None,
        "revenue_source": None,
        "employee_count": None,
        "filing_period_start": None,
        "filing_period_end": None,
        "gross_profit": None,
        "profit_before_tax": None,
        "profit_after_tax": None,
        "fixed_assets": None,
        "current_assets": None,
        "total_assets": None,
        "net_assets": None,
        "net_current_assets": None,
        "filing_id": None,
        "filing_format": None,
        "filing_age_months": None,
        "last_enriched_at": None,
    }


def _load_existing_company_enrichment(
    con: duckdb.DuckDBPyConnection,
    company_number: str,
) -> dict[str, object]:
    row = con.execute(
        """
        SELECT
            company_number,
            avg_director_age,
            min_director_age,
            max_director_age,
            directors_over_60,
            all_directors_60_plus,
            directors_dob_years,
            revenue,
            revenue_source,
            employee_count,
            filing_period_start,
            filing_period_end,
            gross_profit,
            profit_before_tax,
            profit_after_tax,
            fixed_assets,
            current_assets,
            total_assets,
            net_assets,
            net_current_assets,
            filing_id,
            filing_format,
            filing_age_months,
            last_enriched_at
        FROM company_enrichment
        WHERE company_number = ?
        """,
        [company_number],
    ).fetchone()
    if row is None:
        return _base_company_enrichment_row(company_number)
    return dict(zip(COMPANY_ENRICHMENT_COLUMNS, row))


def _load_existing_company_enrichment_rows(
    con: duckdb.DuckDBPyConnection,
    company_numbers: list[str],
) -> dict[str, dict[str, object]]:
    if not company_numbers:
        return {}

    ordered_company_numbers: list[str] = []
    seen: set[str] = set()
    for company_number in company_numbers:
        if company_number in seen:
            continue
        seen.add(company_number)
        ordered_company_numbers.append(company_number)

    placeholders = ", ".join(["?"] * len(ordered_company_numbers))
    rows = con.execute(
        f"""
        SELECT
            company_number,
            avg_director_age,
            min_director_age,
            max_director_age,
            directors_over_60,
            all_directors_60_plus,
            directors_dob_years,
            revenue,
            revenue_source,
            employee_count,
            filing_period_start,
            filing_period_end,
            gross_profit,
            profit_before_tax,
            profit_after_tax,
            fixed_assets,
            current_assets,
            total_assets,
            net_assets,
            net_current_assets,
            filing_id,
            filing_format,
            filing_age_months,
            last_enriched_at
        FROM company_enrichment
        WHERE company_number IN ({placeholders})
        """,
        ordered_company_numbers,
    ).fetchall()

    out = {
        str(row[0]): dict(zip(COMPANY_ENRICHMENT_COLUMNS, row))
        for row in rows
    }
    for company_number in ordered_company_numbers:
        out.setdefault(company_number, _base_company_enrichment_row(company_number))
    return out


def _decode_httpx_response(response: httpx.Response) -> dict | list | str:
    try:
        return response.json()
    except json.JSONDecodeError:
        return response.text


def _insert_or_replace_company_enrichment(
    con: duckdb.DuckDBPyConnection,
    row: dict[str, object],
) -> None:
    placeholders = [
        "CAST(? AS JSON)" if column == "directors_dob_years" else "?"
        for column in COMPANY_ENRICHMENT_COLUMNS
    ]
    con.execute(
        f"""
        INSERT OR REPLACE INTO company_enrichment ({", ".join(COMPANY_ENRICHMENT_COLUMNS)})
        VALUES ({", ".join(placeholders)})
        """,
        [row.get(column) for column in COMPANY_ENRICHMENT_COLUMNS],
    )


def _insert_or_replace_company_enrichment_rows(
    con: duckdb.DuckDBPyConnection,
    rows: list[dict[str, object]],
) -> None:
    if not rows:
        return

    placeholders = [
        "CAST(? AS JSON)" if column == "directors_dob_years" else "?"
        for column in COMPANY_ENRICHMENT_COLUMNS
    ]
    con.executemany(
        f"""
        INSERT OR REPLACE INTO company_enrichment ({", ".join(COMPANY_ENRICHMENT_COLUMNS)})
        VALUES ({", ".join(placeholders)})
        """,
        [[row.get(column) for column in COMPANY_ENRICHMENT_COLUMNS] for row in rows],
    )


def _load_company_enrichment_from_batch(
    con: duckdb.DuckDBPyConnection,
    *,
    batch_id: str,
) -> dict[str, int]:
    current_year = utcnow_naive().year
    now = utcnow_naive()
    records_updated = 0
    enriched = 0
    no_active_directors = 0
    last_response_id = 0

    while True:
        chunk = con.execute(
            """
            SELECT response_id, entity_id, raw_json
            FROM cqc_api_responses
            WHERE batch_id = CAST(? AS UUID)
              AND entity_type = 'ch_directors'
              AND http_status = 200
              AND response_id > ?
            ORDER BY response_id
            LIMIT ?
            """,
            [batch_id, last_response_id, DIRECTOR_LOAD_CHUNK_SIZE],
        ).fetchall()
        if not chunk:
            break

        last_response_id = int(chunk[-1][0])
        company_numbers = [str(row[1]).zfill(8) for row in chunk]
        existing_rows = _load_existing_company_enrichment_rows(con, company_numbers)
        updated_rows: list[dict[str, object]] = []

        for _, entity_id, raw_json_text in chunk:
            payload = json.loads(str(raw_json_text))
            if not isinstance(payload, list):
                raise ValueError(
                    f"Expected officer list payload for company {entity_id}"
                )

            company_number = str(entity_id).zfill(8)
            target_row = dict(
                existing_rows.get(
                    company_number,
                    _base_company_enrichment_row(company_number),
                )
            )
            age_fields = compute_age_fields(payload, current_year)
            target_row.update(
                {
                    "avg_director_age": age_fields["avg_director_age"],
                    "min_director_age": age_fields["min_director_age"],
                    "max_director_age": age_fields["max_director_age"],
                    "directors_over_60": age_fields["directors_over_60"],
                    "all_directors_60_plus": age_fields["all_directors_60_plus"],
                    "directors_dob_years": _json_text(age_fields["directors_dob_years"]),
                    "last_enriched_at": now,
                }
            )
            updated_rows.append(target_row)
            records_updated += 1
            if age_fields["avg_director_age"] is None:
                no_active_directors += 1
            else:
                enriched += 1

        _insert_or_replace_company_enrichment_rows(con, updated_rows)

    return {
        "records_updated": records_updated,
        "enriched": enriched,
        "no_active_directors": no_active_directors,
    }


def _load_director_staging_file(
    db_path: str | Path,
    *,
    path: Path,
) -> LoadedBatch:
    batch_id = batch_id_from_staging_path("ch_directors", path)

    def read_existing(
        con: duckdb.DuckDBPyConnection,
    ) -> tuple[int, int, int]:
        ensure_pipeline_schema(con)
        return sync_batch_progress(con, batch_id)

    (
        existing_records_fetched,
        existing_records_updated,
        existing_error_count,
    ) = with_duckdb_connection(db_path, read_existing)

    staged_stats = scan_staged_api_responses(
        path,
        expected_entity_type="ch_directors",
        success_json_type="ARRAY",
    )
    final_records_fetched = max(
        existing_records_fetched,
        staged_stats.total_rows,
    )
    final_records_updated = existing_records_updated
    final_error_count = existing_error_count
    enriched = 0
    no_active_directors = 0

    try:
        if staged_stats.invalid_entity_rows:
            raise ValueError(
                f"Unexpected entity_type rows in ch_directors staging: {path}"
            )
        if staged_stats.invalid_payload_rows:
            raise ValueError(
                "Expected array payloads for successful ch_directors staging rows"
            )

        def run_load(con: duckdb.DuckDBPyConnection) -> None:
            nonlocal final_records_fetched
            nonlocal final_records_updated
            nonlocal final_error_count
            nonlocal enriched
            nonlocal no_active_directors

            ensure_pipeline_schema(con)
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute(
                    RAW_API_RESPONSE_INSERT_SQL,
                    [batch_id, str(path)],
                )
                load_stats = _load_company_enrichment_from_batch(
                    con,
                    batch_id=batch_id,
                )
                raw_counts = con.execute(
                    """
                    SELECT
                        COUNT(*) AS records_fetched,
                        COUNT(*) FILTER (WHERE http_status = 200) AS success_rows,
                        COUNT(*) FILTER (WHERE http_status != 200) AS non_200_errors
                    FROM cqc_api_responses
                    WHERE batch_id = CAST(? AS UUID)
                      AND entity_type = 'ch_directors'
                    """,
                    [batch_id],
                ).fetchone()
                final_records_fetched = max(
                    existing_records_fetched,
                    int(raw_counts[0]) if raw_counts else 0,
                )
                final_records_updated = max(
                    existing_records_updated,
                    int(raw_counts[1]) if raw_counts else 0,
                )
                enriched = int(load_stats["enriched"])
                no_active_directors = int(load_stats["no_active_directors"])
                final_error_count = max(
                    existing_error_count,
                    int(raw_counts[2]) if raw_counts else 0,
                )
                finish_sync_batch(
                    con,
                    batch_id,
                    status="succeeded",
                    records_fetched=final_records_fetched,
                    records_updated=final_records_updated,
                    error_count=final_error_count,
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

        with_duckdb_connection(db_path, run_load)
    except Exception:
        def mark_failed(con: duckdb.DuckDBPyConnection) -> None:
            finish_sync_batch(
                con,
                batch_id,
                status="failed",
                records_fetched=final_records_fetched,
                records_updated=final_records_updated,
                error_count=final_error_count + 1,
            )

        with_duckdb_connection(db_path, mark_failed)
        raise

    loaded_path = mark_staging_file_loaded(path)
    return LoadedBatch(
        batch_id=batch_id,
        path=str(loaded_path),
        records_fetched=final_records_fetched,
        records_updated=final_records_updated,
        error_count=final_error_count,
        extra={
            "enriched": enriched,
            "no_active_directors": no_active_directors,
        },
    )


def load_director_staging(
    data_dir: str | Path,
    db_path: str | Path,
    *,
    batch_id: str | None = None,
) -> dict[str, object]:
    results: list[LoadedBatch] = []
    for path in pending_staging_files(
        data_dir,
        sync_type="ch_directors",
        batch_id=batch_id,
    ):
        results.append(
            _load_director_staging_file(
                db_path,
                path=path,
            )
        )
    summary = summarize_loaded_batches("ch_directors", results)
    summary["enriched"] = sum(
        result.extra.get("enriched", 0)
        for result in results
    )
    summary["no_active_directors"] = sum(
        result.extra.get("no_active_directors", 0)
        for result in results
    )
    return summary


def _validated_batch_size(batch_size: int) -> int:
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    return batch_size


def _progress_text(done: int, total: int) -> str:
    if total <= 0:
        return "0/0 0.0%"
    percent = (done / total) * 100
    return f"{done}/{total} {percent:.1f}%"


def _duration_text(seconds: float) -> str:
    rounded = max(0, int(round(seconds)))
    hours, remainder = divmod(rounded, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def _sic_company_filter_sql() -> str:
    return """
        sic_code_1 = ?
        OR sic_code_2 = ?
        OR sic_code_3 = ?
        OR sic_code_4 = ?
    """


def _select_director_targets(
    con: duckdb.DuckDBPyConnection,
    *,
    sic: str,
    company_numbers: list[str] | None,
    force: bool,
) -> list[str]:
    if company_numbers:
        cleaned = [value.strip().zfill(8) for value in company_numbers if value.strip()]
        # Preserve caller ordering while deduping.
        out: list[str] = []
        seen: set[str] = set()
        for value in cleaned:
            if value in seen:
                continue
            seen.add(value)
            out.append(value)
        return out

    if force:
        rows = con.execute(
            f"""
            SELECT company_number
            FROM companies
            WHERE {_sic_company_filter_sql()}
            ORDER BY company_number
            """,
            [sic, sic, sic, sic],
        ).fetchall()
    else:
        rows = con.execute(
            f"""
            SELECT c.company_number
            FROM companies c
            LEFT JOIN company_enrichment ce USING (company_number)
            WHERE ({_sic_company_filter_sql()})
              AND ce.directors_dob_years IS NULL
            ORDER BY c.company_number
            """,
            [sic, sic, sic, sic],
        ).fetchall()
    return [row[0] for row in rows]


def enrich_directors(
    db_path: str | Path,
    data_dir: str | Path | None = None,
    *,
    sic: str = "88100",
    company_numbers: list[str] | None = None,
    force: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, int | str]:
    db_path = Path(db_path)
    data_dir = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
    batch_id: str | None = None
    run_log: FsyncLineLogger | None = None
    staging_writer: StagingWriter | None = None
    sync_type = "ch_directors"
    targets: list[str] = []
    total_fetched = 0
    total_errors = 0
    durable_fetched = 0
    durable_errors = 0
    staged_since_sync = 0
    processed = 0
    started_monotonic = time.monotonic()
    try:
        load_director_staging(
            data_dir,
            db_path,
        )

        batch_size_value = _validated_batch_size(batch_size)

        def prepare_batch(
            con: duckdb.DuckDBPyConnection,
        ) -> tuple[list[str], str]:
            ensure_pipeline_schema(con)
            return (
                _select_director_targets(
                    con,
                    sic=sic,
                    company_numbers=company_numbers,
                    force=force,
                ),
                insert_sync_batch(
                    con,
                    sync_type=sync_type,
                    mode="incremental" if not force else "all",
                ),
            )

        targets, batch_id = with_duckdb_connection(
            db_path,
            prepare_batch,
        )

        run_log = FsyncLineLogger(
            data_dir,
            sync_type=sync_type,
            batch_id=batch_id,
        )
        staging_writer = StagingWriter(
            data_dir,
            sync_type=sync_type,
            batch_id=batch_id,
        )
        run_log.write_line(
            f"start sync_type={sync_type} requested={len(targets)} batch_size={batch_size_value} force={force} sic={sic}"
        )
        run_log.flush_and_fsync()
        current_year = utcnow_naive().year

        # We only fsync the staging file/log at batch boundaries. A hard
        # kill in the middle of a batch can lose up to batch_size staged
        # responses/log lines, and the sync-batch row can lag by the same
        # amount. Clean exits and handled exceptions fsync the partial
        # batch before marking the batch row.
        def checkpoint_write_phase() -> None:
            nonlocal durable_fetched
            nonlocal durable_errors
            nonlocal staged_since_sync
            if batch_id is None or run_log is None or staging_writer is None:
                return
            if (
                staged_since_sync == 0
                and total_fetched == durable_fetched
                and total_errors == durable_errors
            ):
                run_log.flush_and_fsync()
                return

            staging_writer.flush_and_fsync()

            def update_progress(con: duckdb.DuckDBPyConnection) -> None:
                update_sync_batch_progress(
                    con,
                    batch_id,
                    records_fetched=total_fetched,
                    records_updated=0,
                    error_count=total_errors,
                )

            with_duckdb_connection(db_path, update_progress)
            elapsed_seconds = time.monotonic() - started_monotonic
            eta_seconds = (
                (elapsed_seconds / processed) * max(len(targets) - processed, 0)
                if processed
                else 0.0
            )
            run_log.write_line(
                "flush "
                f"batch_size={staged_since_sync} "
                f"total_fetched={total_fetched} "
                f"errors={total_errors} "
                f"elapsed={_duration_text(elapsed_seconds)} "
                f"eta={_duration_text(eta_seconds)}"
            )
            run_log.flush_and_fsync()
            durable_fetched = total_fetched
            durable_errors = total_errors
            staged_since_sync = 0

        with CompaniesHouseClient(data_dir) as client:
            for company_number in targets:
                status = "error"
                http_status: int | None = None
                try:
                    officers = client.get_officers(company_number)
                    http_status = 200
                    staging_writer.append(
                        StagedAPIResponse(
                            entity_type="ch_directors",
                            entity_id=company_number,
                            fetched_at=isoformat_utc(),
                            http_status=http_status,
                            raw_json=officers,
                        )
                    )
                    total_fetched += 1
                    staged_since_sync += 1

                    age_fields = compute_age_fields(officers, current_year)
                    if age_fields["avg_director_age"] is None:
                        status = "skip"
                    else:
                        status = "ok"
                except httpx.HTTPStatusError as exc:
                    response = exc.response
                    if response is not None:
                        http_status = response.status_code
                        staging_writer.append(
                            StagedAPIResponse(
                                entity_type="ch_directors",
                                entity_id=company_number,
                                fetched_at=isoformat_utc(),
                                http_status=http_status,
                                raw_json=_decode_httpx_response(response),
                            )
                        )
                        total_fetched += 1
                        staged_since_sync += 1
                    total_errors += 1
                    status = "skip" if http_status == 404 else "error"
                    if http_status != 404:
                        logger.exception(
                            "Failed to enrich directors for company %s",
                            company_number,
                        )
                except Exception:
                    total_errors += 1
                    logger.exception(
                        "Failed to enrich directors for company %s",
                        company_number,
                    )
                finally:
                    processed += 1
                    status_bits = [
                        f"company_number={company_number}",
                        f"status={status}",
                    ]
                    if http_status is not None:
                        status_bits.append(f"http_status={http_status}")
                    status_bits.append(
                        f"progress={_progress_text(processed, len(targets))}"
                    )
                    run_log.write_line(" ".join(status_bits))

                if staged_since_sync >= batch_size_value:
                    checkpoint_write_phase()

        checkpoint_write_phase()
        load_summary = load_director_staging(
            data_dir,
            db_path,
            batch_id=batch_id,
        )
        records_fetched = int(load_summary["records_fetched"])
        records_updated = int(load_summary["records_updated"])
        enriched = int(load_summary["enriched"])
        no_active_directors = int(load_summary["no_active_directors"])
        error_count = int(load_summary["error_count"])
        elapsed_seconds = time.monotonic() - started_monotonic
        run_log.write_line(
            "complete "
            f"requested={len(targets)} "
            f"records_fetched={records_fetched} "
            f"records_updated={records_updated} "
            f"enriched={enriched} "
            f"no_active_directors={no_active_directors} "
            f"errors={error_count} "
            f"elapsed={_duration_text(elapsed_seconds)}"
        )
        run_log.flush_and_fsync()

        logger.info(
            "CH director enrich complete: requested=%d enriched=%d no_active_directors=%d errors=%d batch_id=%s",
            len(targets),
            enriched,
            no_active_directors,
            error_count,
            batch_id,
        )
        return {
            "requested": len(targets),
            "enriched": enriched,
            "no_active_directors": no_active_directors,
            "error_count": error_count,
            "batch_id": batch_id,
            "log_path": str(run_log.path),
        }
    except BaseException:
        if staging_writer is not None:
            staging_writer.flush_and_fsync()
        if batch_id is not None:
            def mark_failed(con: duckdb.DuckDBPyConnection) -> None:
                finish_sync_batch(
                    con,
                    batch_id,
                    status="failed",
                    records_fetched=total_fetched,
                    records_updated=0,
                    error_count=total_errors + 1,
                )

            with_duckdb_connection(db_path, mark_failed)
        if run_log is not None:
            elapsed_seconds = time.monotonic() - started_monotonic
            run_log.write_line(
                "crash "
                f"records_fetched={total_fetched} "
                f"records_updated=0 "
                f"enriched=0 "
                f"no_active_directors=0 "
                f"errors={total_errors + 1} "
                f"elapsed={_duration_text(elapsed_seconds)}"
            )
            run_log.flush_and_fsync()
        raise
    finally:
        if run_log is not None:
            run_log.close()
        if staging_writer is not None:
            staging_writer.close()


def enrich_revenue(
    db_path: str | Path,
    data_dir: str | Path | None = None,
    *,
    sic: str = "88100",
    company_numbers: list[str] | None = None,
) -> dict[str, int]:
    data_dir = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
    del data_dir
    db_path = Path(db_path)
    bands_path = DATA_REFERENCE_DIR / "revenue_bands.csv"
    bands = load_bands(bands_path)

    con = duckdb.connect(str(db_path))
    try:
        ensure_pipeline_schema(con)
        params: list[object]
        if company_numbers:
            cleaned = [value.strip().zfill(8) for value in company_numbers if value.strip()]
            if not cleaned:
                targets: list[tuple[str, int | None]] = []
            else:
                placeholders = ", ".join(["?"] * len(cleaned))
                targets = con.execute(
                    f"""
                    SELECT company_number, employee_count
                    FROM company_enrichment
                    WHERE company_number IN ({placeholders})
                      AND revenue IS NULL
                    ORDER BY company_number
                    """,
                    cleaned,
                ).fetchall()
        else:
            targets = con.execute(
                f"""
                SELECT c.company_number, ce.employee_count
                FROM companies c
                JOIN company_enrichment ce USING (company_number)
                WHERE ({_sic_company_filter_sql()})
                  AND ce.revenue IS NULL
                ORDER BY c.company_number
                """,
                [sic, sic, sic, sic],
            ).fetchall()

        estimated = 0
        skipped = 0
        for company_number, employee_count in targets:
            estimate_value = estimate(_coerce_int(employee_count), bands)
            if estimate_value is None:
                skipped += 1
                continue

            row = _load_existing_company_enrichment(con, company_number)
            row.update(
                {
                    "revenue": estimate_value,
                    "revenue_source": "employee_band_lookup",
                    "last_enriched_at": utcnow_naive(),
                }
            )
            _insert_or_replace_company_enrichment(con, row)
            estimated += 1

        logger.info(
            "Revenue enrich complete: requested=%d estimated=%d skipped=%d",
            len(targets),
            estimated,
            skipped,
        )
        return {
            "requested": len(targets),
            "estimated": estimated,
            "skipped": skipped,
        }
    finally:
        con.close()
