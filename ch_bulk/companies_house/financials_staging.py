"""Staging, replay, and load helpers for financial filing enrichment."""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path

import duckdb

from ch_bulk.companies_house.financials_contracts import (
    FINANCIALS_FETCH_SYNC_TYPE,
    FINANCIALS_SYNC_TYPE,
    FilingCandidate,
    FinancialTarget,
    FinancialsFileStats,
    FetchedFinancialRow,
    ParsedFinancialFacts,
    StagedFinancialRow,
)
from ch_bulk.companies_house.financials_parsers import (
    _empty_parsed_financials,
    _parse_date,
    _parse_ixbrl_bytes,
    _parse_pdf_bytes,
)
from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.db.staging import (
    LoadedBatch,
    StagingWriter,
    batch_id_from_staging_path,
    isoformat_utc,
    mark_staging_file_loaded,
    pending_staging_files,
    staging_path,
    summarize_loaded_batches,
    truncate_incomplete_jsonl_tail,
    with_duckdb_connection,
)
from ch_bulk.db.sync_batches import finish_sync_batch, utcnow_naive

logger = logging.getLogger(__name__)

FINANCIALS_INSERT_SQL = """
WITH staged AS (
    SELECT
        CAST(raw.company_number AS VARCHAR) AS company_number,
        NULLIF(CAST(raw.filing_id AS VARCHAR), '') AS filing_id,
        CAST(raw.filing_date AS DATE) AS filing_date,
        NULLIF(CAST(raw.filing_format AS VARCHAR), '') AS filing_format,
        CAST(raw.filing_period_start AS DATE) AS filing_period_start,
        CAST(raw.filing_period_end AS DATE) AS filing_period_end,
        TRY_CAST(raw.revenue AS DOUBLE) AS revenue,
        TRY_CAST(raw.employee_count AS INTEGER) AS employee_count,
        TRY_CAST(raw.gross_profit AS DOUBLE) AS gross_profit,
        TRY_CAST(raw.profit_before_tax AS DOUBLE) AS profit_before_tax,
        TRY_CAST(raw.profit_after_tax AS DOUBLE) AS profit_after_tax,
        TRY_CAST(raw.fixed_assets AS DOUBLE) AS fixed_assets,
        TRY_CAST(raw.current_assets AS DOUBLE) AS current_assets,
        TRY_CAST(raw.total_assets AS DOUBLE) AS total_assets,
        TRY_CAST(raw.net_assets AS DOUBLE) AS net_assets,
        TRY_CAST(raw.net_current_assets AS DOUBLE) AS net_current_assets,
        TRY_CAST(raw.filing_age_months AS INTEGER) AS filing_age_months,
        NULLIF(CAST(raw.parse_status AS VARCHAR), '') AS parse_status,
        NULLIF(CAST(raw.parse_failure_reason AS VARCHAR), '') AS parse_failure_reason,
        COALESCE(TRY_CAST(raw.profit_loss_exempt AS BOOLEAN), FALSE) AS profit_loss_exempt,
        CAST(raw.fetched_at AS TIMESTAMP) AS fetched_at
    FROM read_json_auto(?, format='newline_delimited') AS raw
),
deduped AS (
    SELECT * EXCLUDE (row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY company_number
                ORDER BY fetched_at DESC
            ) AS row_num
        FROM staged
    )
    WHERE row_num = 1
)
INSERT OR REPLACE INTO company_enrichment (
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
)
SELECT
    d.company_number,
    e.avg_director_age,
    e.min_director_age,
    e.max_director_age,
    e.directors_over_60,
    e.all_directors_60_plus,
    e.directors_dob_years,
    COALESCE(d.revenue, e.revenue) AS revenue,
    CASE
        WHEN d.revenue IS NOT NULL AND lower(coalesce(d.filing_format, '')) = 'ixbrl'
            THEN 'filed_accounts_ixbrl'
        WHEN d.revenue IS NOT NULL AND lower(coalesce(d.filing_format, '')) = 'pdf'
            THEN 'filed_accounts_pdf'
        WHEN d.revenue IS NOT NULL AND lower(coalesce(d.filing_format, '')) = 'ocr_pdf'
            THEN 'filed_accounts_ocr_pdf'
        WHEN d.parse_status = 'pdf_no_text_layer'
             AND e.revenue IS NULL
             AND e.revenue_source IS NULL
            THEN 'pdf_no_text_layer'
        WHEN d.parse_status = 'partial'
             AND coalesce(d.profit_loss_exempt, FALSE)
             AND d.revenue IS NULL
             AND d.gross_profit IS NULL
             AND d.profit_before_tax IS NULL
             AND d.profit_after_tax IS NULL
             AND (
                d.employee_count IS NOT NULL
                OR d.net_assets IS NOT NULL
                OR d.total_assets IS NOT NULL
                OR d.net_current_assets IS NOT NULL
             )
             AND (e.revenue_source IS NULL OR e.revenue_source IN (
                'filed_accounts_ixbrl',
                'filed_accounts_pdf',
                'filed_accounts_ocr_pdf',
                'partial_no_revenue',
                'pdf_no_text_layer'
             ))
            THEN 'partial_no_revenue'
        WHEN d.parse_status = 'partial'
             AND NOT coalesce(d.profit_loss_exempt, FALSE)
             AND lower(coalesce(d.filing_format, '')) = 'ocr_pdf'
             AND d.revenue IS NULL
             AND d.gross_profit IS NULL
             AND d.profit_before_tax IS NULL
             AND d.profit_after_tax IS NULL
             AND (
                d.employee_count IS NOT NULL
                OR d.net_assets IS NOT NULL
                OR d.total_assets IS NOT NULL
                OR d.net_current_assets IS NOT NULL
             )
             AND (e.revenue_source IS NULL OR e.revenue_source IN (
                'filed_accounts_ixbrl',
                'filed_accounts_pdf',
                'filed_accounts_ocr_pdf',
                'partial_no_revenue',
                'partial_ocr_no_pl',
                'pdf_no_text_layer'
             ))
            THEN 'partial_ocr_no_pl'
        WHEN d.parse_status = 'no_filing' AND e.revenue IS NULL AND e.revenue_source IS NULL
            THEN 'no_recent_filing'
        ELSE e.revenue_source
    END AS revenue_source,
    COALESCE(d.employee_count, e.employee_count) AS employee_count,
    COALESCE(d.filing_period_start, e.filing_period_start) AS filing_period_start,
    COALESCE(d.filing_period_end, e.filing_period_end) AS filing_period_end,
    COALESCE(d.gross_profit, e.gross_profit) AS gross_profit,
    COALESCE(d.profit_before_tax, e.profit_before_tax) AS profit_before_tax,
    COALESCE(d.profit_after_tax, e.profit_after_tax) AS profit_after_tax,
    COALESCE(d.fixed_assets, e.fixed_assets) AS fixed_assets,
    COALESCE(d.current_assets, e.current_assets) AS current_assets,
    COALESCE(d.total_assets, e.total_assets) AS total_assets,
    COALESCE(d.net_assets, e.net_assets) AS net_assets,
    COALESCE(d.net_current_assets, e.net_current_assets) AS net_current_assets,
    COALESCE(d.filing_id, e.filing_id) AS filing_id,
    COALESCE(d.filing_format, e.filing_format) AS filing_format,
    COALESCE(d.filing_age_months, e.filing_age_months) AS filing_age_months,
    COALESCE(d.fetched_at, e.last_enriched_at, CURRENT_TIMESTAMP) AS last_enriched_at
FROM deduped d
LEFT JOIN company_enrichment e USING (company_number)
"""

FINANCIALS_SCAN_SUMMARY_SQL = """
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (
        WHERE coalesce(parse_status, '') = 'ok'
    ) AS ok_count,
    COUNT(*) FILTER (
        WHERE coalesce(parse_status, '') IN ('partial', 'pdf_parse_partial')
    ) AS partial_count,
    COUNT(*) FILTER (
        WHERE lower(coalesce(filing_format, '')) = 'ixbrl'
    ) AS ixbrl_count,
    COUNT(*) FILTER (
        WHERE lower(coalesce(filing_format, '')) IN ('pdf', 'ocr_pdf')
    ) AS pdf_count,
    COUNT(*) FILTER (
        WHERE coalesce(parse_status, '') = 'pdf_no_text_layer'
    ) AS pdf_no_text_layer_count,
    COUNT(*) FILTER (
        WHERE coalesce(parse_status, '') = 'no_filing'
    ) AS no_filing_count,
    COUNT(*) FILTER (
        WHERE TRY_CAST(filing_age_months AS INTEGER) > 24
    ) AS stale_count,
    COUNT(*) FILTER (
        WHERE coalesce(parse_status, '') IN (
            'request_error',
            'document_metadata_error',
            'document_download_error',
            'ixbrl_parse_error',
            'pdf_parse_error',
            'no_document_resource'
        )
    ) AS error_count
FROM read_json_auto(?, format='newline_delimited')
"""


def _months_between(value: date | None, today: date) -> int | None:
    if value is None:
        return None
    months = (today.year - value.year) * 12 + (today.month - value.month)
    if today.day < value.day:
        months -= 1
    return max(months, 0)


def _build_row(
    *,
    target: FinancialTarget,
    filing: FilingCandidate | None,
    filing_format: str | None,
    facts: ParsedFinancialFacts,
    fetched_at: str | None = None,
) -> StagedFinancialRow:
    today = utcnow_naive().date()
    age_source = target.accounts_last_made_up or (filing.made_up_date if filing else None)
    return StagedFinancialRow(
        company_number=target.company_number,
        filing_id=filing.filing_id if filing else None,
        filing_date=filing.filing_date.isoformat() if filing and filing.filing_date else None,
        filing_format=filing_format,
        filing_period_start=(
            facts.filing_period_start.isoformat()
            if facts.filing_period_start
            else None
        ),
        filing_period_end=(
            facts.filing_period_end.isoformat()
            if facts.filing_period_end
            else None
        ),
        revenue=facts.revenue,
        employee_count=facts.employee_count,
        gross_profit=facts.gross_profit,
        profit_before_tax=facts.profit_before_tax,
        profit_after_tax=facts.profit_after_tax,
        fixed_assets=facts.fixed_assets,
        current_assets=facts.current_assets,
        total_assets=facts.total_assets,
        net_assets=facts.net_assets,
        net_current_assets=facts.net_current_assets,
        filing_age_months=_months_between(age_source, today),
        parse_status=facts.parse_status,
        parse_failure_reason=facts.parse_failure_reason,
        fetched_at=fetched_at or isoformat_utc(),
        profit_loss_exempt=facts.profit_loss_exempt,
    )


def _fetched_row_to_target(row: FetchedFinancialRow) -> FinancialTarget:
    return FinancialTarget(
        row.company_number,
        _parse_date(row.accounts_last_made_up),
    )


def _fetched_row_to_filing(row: FetchedFinancialRow) -> FilingCandidate | None:
    if row.filing_id is None:
        return None
    return FilingCandidate(
        filing_id=row.filing_id,
        filing_date=_parse_date(row.filing_date),
        made_up_date=_parse_date(row.filing_made_up_date),
        paper_filed=bool(row.paper_filed) if row.paper_filed is not None else False,
        document_metadata_url="",
    )


def _parse_fetched_row(
    row: FetchedFinancialRow,
) -> StagedFinancialRow:
    target = _fetched_row_to_target(row)
    filing = _fetched_row_to_filing(row)
    if row.parse_status is not None:
        facts = _empty_parsed_financials(
            parse_status=row.parse_status,
            parse_failure_reason=row.parse_failure_reason,
        )
    else:
        raw_path = row.raw_path
        if raw_path is None:
            facts = _empty_parsed_financials(
                parse_status="request_error",
                parse_failure_reason="missing_raw_path",
            )
        else:
            try:
                content = Path(raw_path).read_bytes()
            except Exception as exc:
                facts = _empty_parsed_financials(
                    parse_status="request_error",
                    parse_failure_reason=str(exc),
                )
            else:
                if row.filing_format == "ixbrl":
                    facts = _parse_ixbrl_bytes(content)
                elif row.filing_format == "pdf":
                    facts = _parse_pdf_bytes(content)
                else:
                    facts = _empty_parsed_financials(
                        parse_status="request_error",
                        parse_failure_reason="unsupported_filing_format",
                    )
    return _build_row(
        target=target,
        filing=filing,
        filing_format=row.filing_format,
        facts=facts,
        fetched_at=row.fetched_at,
    )


def _salvage_truncated_staging_file(path: str | Path) -> None:
    truncated = truncate_incomplete_jsonl_tail(path)
    if truncated:
        logger.warning(
            "Truncated incomplete trailing JSONL row from %s (%s bytes)",
            path,
            truncated,
        )


def _existing_staged_company_numbers(path: Path) -> set[str]:
    if not path.exists():
        return set()
    _salvage_truncated_staging_file(path)
    company_numbers: set[str] = set()
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            company_number = payload.get("company_number")
            if company_number:
                company_numbers.add(str(company_number))
    return company_numbers


def _replay_financials_fetch_staging_file(
    data_dir: str | Path,
    *,
    path: Path,
) -> int:
    batch_id = batch_id_from_staging_path(FINANCIALS_FETCH_SYNC_TYPE, path)
    staged_path = staging_path(
        data_dir,
        sync_type=FINANCIALS_SYNC_TYPE,
        batch_id=batch_id,
    )
    _salvage_truncated_staging_file(path)
    existing_company_numbers = _existing_staged_company_numbers(staged_path)
    appended = 0
    writer: StagingWriter | None = None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                fetched_row = FetchedFinancialRow.from_json_line(line)
                if fetched_row.company_number in existing_company_numbers:
                    continue
                if writer is None:
                    writer = StagingWriter(
                        data_dir,
                        sync_type=FINANCIALS_SYNC_TYPE,
                        batch_id=batch_id,
                    )
                writer.append(_parse_fetched_row(fetched_row))
                existing_company_numbers.add(fetched_row.company_number)
                appended += 1
        if writer is not None:
            writer.flush_and_fsync()
    finally:
        if writer is not None:
            writer.close()
    mark_staging_file_loaded(path)
    return appended


def replay_financials_fetch_staging(
    data_dir: str | Path,
    *,
    batch_id: str | None = None,
) -> int:
    replayed = 0
    for path in pending_staging_files(
        data_dir,
        sync_type=FINANCIALS_FETCH_SYNC_TYPE,
        batch_id=batch_id,
    ):
        replayed += _replay_financials_fetch_staging_file(
            data_dir,
            path=path,
        )
    return replayed


def _archive_financials_fetch_manifest(
    data_dir: str | Path,
    *,
    batch_id: str,
) -> None:
    path = staging_path(
        data_dir,
        sync_type=FINANCIALS_FETCH_SYNC_TYPE,
        batch_id=batch_id,
    )
    if path.exists():
        mark_staging_file_loaded(path)


def _scan_staged_financials_file(path: str | Path) -> FinancialsFileStats:
    _salvage_truncated_staging_file(path)
    con = duckdb.connect(":memory:")
    try:
        row = con.execute(
            FINANCIALS_SCAN_SUMMARY_SQL,
            [str(path)],
        ).fetchone()
    finally:
        con.close()

    if row is None:
        return FinancialsFileStats(0, 0, 0, 0, 0, 0, 0, 0, 0)
    return FinancialsFileStats(
        total_rows=int(row[0]),
        ok_count=int(row[1]),
        partial_count=int(row[2]),
        ixbrl_count=int(row[3]),
        pdf_count=int(row[4]),
        pdf_no_text_layer_count=int(row[5]),
        no_filing_count=int(row[6]),
        stale_count=int(row[7]),
        error_count=int(row[8]),
    )


def _sync_batch_status(
    con: duckdb.DuckDBPyConnection,
    batch_id: str,
) -> str | None:
    row = con.execute(
        """
        SELECT status
        FROM cqc_sync_batches
        WHERE batch_id = ?
        """,
        [batch_id],
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return str(row[0])


def _require_sync_batch(
    con: duckdb.DuckDBPyConnection,
    batch_id: str,
) -> str:
    status = _sync_batch_status(con, batch_id)
    if status is None:
        raise ValueError(f"Unknown financials sync batch: {batch_id}")
    return status


def _mark_stale_running_batches(
    con: duckdb.DuckDBPyConnection,
) -> int:
    row = con.execute(
        """
        UPDATE cqc_sync_batches
        SET
            finished_at = ?,
            status = 'failed',
            error_count = COALESCE(error_count, 0) + 1
        WHERE sync_type = ?
          AND status = 'running'
        RETURNING batch_id
        """,
        [utcnow_naive(), FINANCIALS_SYNC_TYPE],
    ).fetchall()
    return len(row)


def _load_financials_staging_file(
    db_path: str | Path,
    *,
    path: Path,
    final_status: str,
) -> LoadedBatch:
    batch_id = batch_id_from_staging_path(FINANCIALS_SYNC_TYPE, path)
    staged_stats = _scan_staged_financials_file(path)

    def ensure_batch_exists(con: duckdb.DuckDBPyConnection) -> None:
        ensure_pipeline_schema(con)
        _require_sync_batch(con, batch_id)

    with_duckdb_connection(db_path, ensure_batch_exists)

    def run_load(con: duckdb.DuckDBPyConnection) -> None:
        ensure_pipeline_schema(con)
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(FINANCIALS_INSERT_SQL, [str(path)])
            finish_sync_batch(
                con,
                batch_id,
                status=final_status,
                records_fetched=staged_stats.total_rows,
                records_updated=staged_stats.total_rows,
                error_count=(
                    staged_stats.error_count + 1
                    if final_status == "failed"
                    else staged_stats.error_count
                ),
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    try:
        with_duckdb_connection(db_path, run_load)
    except Exception:
        def mark_failed(con: duckdb.DuckDBPyConnection) -> None:
            ensure_pipeline_schema(con)
            finish_sync_batch(
                con,
                batch_id,
                status="failed",
                records_fetched=staged_stats.total_rows,
                records_updated=0,
                error_count=staged_stats.error_count + 1,
            )

        with_duckdb_connection(db_path, mark_failed)
        raise

    loaded_path = mark_staging_file_loaded(path)
    return LoadedBatch(
        batch_id=batch_id,
        path=str(loaded_path),
        records_fetched=staged_stats.total_rows,
        records_updated=staged_stats.total_rows,
        error_count=(
            staged_stats.error_count + 1
            if final_status == "failed"
            else staged_stats.error_count
        ),
        extra={
            "ok_count": staged_stats.ok_count,
            "partial_count": staged_stats.partial_count,
            "ixbrl_count": staged_stats.ixbrl_count,
            "pdf_count": staged_stats.pdf_count,
            "pdf_no_text_layer_count": staged_stats.pdf_no_text_layer_count,
            "no_filing_count": staged_stats.no_filing_count,
            "stale_count": staged_stats.stale_count,
        },
    )


def load_financials_staging(
    data_dir: str | Path,
    db_path: str | Path,
    *,
    batch_id: str | None = None,
) -> dict[str, object]:
    results: list[LoadedBatch] = []
    with_duckdb_connection(db_path, ensure_pipeline_schema)

    def read_status(
        con: duckdb.DuckDBPyConnection,
        candidate_batch_id: str,
    ) -> str | None:
        return _sync_batch_status(con, candidate_batch_id)

    for path in pending_staging_files(
        data_dir,
        sync_type=FINANCIALS_SYNC_TYPE,
        batch_id=batch_id,
    ):
        candidate_batch_id = batch_id_from_staging_path(FINANCIALS_SYNC_TYPE, path)
        candidate_status = with_duckdb_connection(
            db_path,
            lambda con, batch=candidate_batch_id: read_status(con, batch),
            read_only=True,
        )
        recovered_stale_batch = batch_id is None and candidate_status == "running"
        results.append(
            _load_financials_staging_file(
                db_path,
                path=path,
                final_status="failed" if recovered_stale_batch else "succeeded",
            )
        )

    def finalize_stale(con: duckdb.DuckDBPyConnection) -> int:
        ensure_pipeline_schema(con)
        return _mark_stale_running_batches(con)

    stale_failed = with_duckdb_connection(db_path, finalize_stale)
    summary = summarize_loaded_batches(FINANCIALS_SYNC_TYPE, results)
    summary["ok_count"] = sum(result.extra.get("ok_count", 0) for result in results)
    summary["partial_count"] = sum(
        result.extra.get("partial_count", 0) for result in results
    )
    summary["ixbrl_count"] = sum(result.extra.get("ixbrl_count", 0) for result in results)
    summary["pdf_count"] = sum(result.extra.get("pdf_count", 0) for result in results)
    summary["pdf_no_text_layer_count"] = sum(
        result.extra.get("pdf_no_text_layer_count", 0) for result in results
    )
    summary["no_filing_count"] = sum(
        result.extra.get("no_filing_count", 0) for result in results
    )
    summary["stale_count"] = sum(result.extra.get("stale_count", 0) for result in results)
    summary["stale_failed_batches"] = stale_failed
    return summary
