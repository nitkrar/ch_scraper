"""Compatibility facade for Companies House financial extraction helpers."""

from __future__ import annotations

import logging
import queue
import time
from pathlib import Path

import pdfplumber
from ixbrlparse import IXBRL

import ch_bulk.companies_house.financials_contracts as _contracts
import ch_bulk.companies_house.financials_fetch as _fetch
import ch_bulk.companies_house.financials_parsers as _parsers
import ch_bulk.companies_house.financials_pipeline as _pipeline
import ch_bulk.companies_house.financials_staging as _staging
from ch_bulk.core.settings import load_settings

logger = logging.getLogger(__name__)

_ORIGINAL_STAGING_PARSE_FETCHED_ROW = _staging._parse_fetched_row

Mode = _contracts.Mode

FINANCIALS_SYNC_TYPE = _contracts.FINANCIALS_SYNC_TYPE
FINANCIALS_FETCH_SYNC_TYPE = _contracts.FINANCIALS_FETCH_SYNC_TYPE

FILED_REVENUE_SOURCES = _contracts.FILED_REVENUE_SOURCES
TERMINAL_REVENUE_SOURCES = _contracts.TERMINAL_REVENUE_SOURCES
ERROR_PARSE_STATUSES = _contracts.ERROR_PARSE_STATUSES

IXBRL_RESOURCE = _contracts.IXBRL_RESOURCE
PDF_RESOURCE = _contracts.PDF_RESOURCE
IXBRL_EXTENSION = _contracts.IXBRL_EXTENSION
PDF_EXTENSION = _contracts.PDF_EXTENSION

FinancialTarget = _contracts.FinancialTarget
FilingCandidate = _contracts.FilingCandidate
ParsedFinancialFacts = _contracts.ParsedFinancialFacts
StagedFinancialRow = _contracts.StagedFinancialRow
FetchedFinancialRow = _contracts.FetchedFinancialRow
FetchedFinancialWorkItem = _contracts.FetchedFinancialWorkItem
FinancialsResult = _contracts.FinancialsResult
ParserProcessResponse = _contracts.ParserProcessResponse
ParserProcessHandle = _contracts.ParserProcessHandle
FinancialsFileStats = _contracts.FinancialsFileStats

CompaniesHouseFinancialsClient = _fetch.CompaniesHouseFinancialsClient
_fetch_company_work_item = _fetch._fetch_company_work_item
_select_targets = _fetch._select_targets
_select_latest_annual_accounts = _fetch._select_latest_annual_accounts

DEFAULT_WORKERS = _pipeline.DEFAULT_WORKERS
DEFAULT_PARSER_WORKERS = _pipeline.DEFAULT_PARSER_WORKERS
DEFAULT_BATCH_SIZE = _pipeline.DEFAULT_BATCH_SIZE
DEFAULT_QUEUE_MAXSIZE = _pipeline.DEFAULT_QUEUE_MAXSIZE
DEFAULT_PARSE_INFLIGHT_PER_WORKER = _pipeline.DEFAULT_PARSE_INFLIGHT_PER_WORKER
PIPELINE_HEARTBEAT_INTERVAL_SECONDS = _pipeline.PIPELINE_HEARTBEAT_INTERVAL_SECONDS


def _parse_ixbrl_bytes(content: bytes) -> ParsedFinancialFacts:
    original_ixbrl = _parsers.IXBRL
    try:
        _parsers.IXBRL = IXBRL
        return _parsers._parse_ixbrl_bytes(content)
    finally:
        _parsers.IXBRL = original_ixbrl


def _parse_pdf_bytes(content: bytes) -> ParsedFinancialFacts:
    original_pdfplumber = _parsers.pdfplumber
    try:
        _parsers.pdfplumber = pdfplumber
        return _parsers._parse_pdf_bytes(content)
    finally:
        _parsers.pdfplumber = original_pdfplumber


def _parse_fetched_row(row: FetchedFinancialRow) -> StagedFinancialRow:
    original_parse_ixbrl = _staging._parse_ixbrl_bytes
    original_parse_pdf = _staging._parse_pdf_bytes
    try:
        _staging._parse_ixbrl_bytes = _parse_ixbrl_bytes
        _staging._parse_pdf_bytes = _parse_pdf_bytes
        return _ORIGINAL_STAGING_PARSE_FETCHED_ROW(row)
    finally:
        _staging._parse_ixbrl_bytes = original_parse_ixbrl
        _staging._parse_pdf_bytes = original_parse_pdf


def _process_company(
    client: CompaniesHouseFinancialsClient,
    *,
    target: FinancialTarget,
    data_dir: str | Path,
) -> FinancialsResult:
    work_item = _fetch_company_work_item(
        client,
        target=target,
        data_dir=data_dir,
    )
    return FinancialsResult(
        row=_parse_fetched_row(work_item.row),
        http_status=work_item.http_status,
        latency_seconds=time.monotonic() - work_item.started_monotonic,
    )


def _replay_financials_fetch_staging_file(
    data_dir: str | Path,
    *,
    path: Path,
) -> int:
    original_parse_fetched_row = _staging._parse_fetched_row
    try:
        _staging._parse_fetched_row = _parse_fetched_row
        return _staging._replay_financials_fetch_staging_file(
            data_dir,
            path=path,
        )
    finally:
        _staging._parse_fetched_row = original_parse_fetched_row


def replay_financials_fetch_staging(
    data_dir: str | Path,
    *,
    batch_id: str | None = None,
) -> int:
    original_parse_fetched_row = _staging._parse_fetched_row
    try:
        _staging._parse_fetched_row = _parse_fetched_row
        return _staging.replay_financials_fetch_staging(
            data_dir,
            batch_id=batch_id,
        )
    finally:
        _staging._parse_fetched_row = original_parse_fetched_row


def load_financials_staging(
    data_dir: str | Path,
    db_path: str | Path,
    *,
    batch_id: str | None = None,
) -> dict[str, object]:
    return _staging.load_financials_staging(
        data_dir,
        db_path,
        batch_id=batch_id,
    )


def enrich_financials(
    db_path: str | Path,
    data_dir: str | Path | None = None,
    *,
    mode: str = "incremental",
    ids: list[str] | None = None,
    workers: int = DEFAULT_WORKERS,
    parser_workers: int = DEFAULT_PARSER_WORKERS,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, object]:
    original_load_settings = _pipeline.load_settings
    original_client = _pipeline.CompaniesHouseFinancialsClient
    original_fetch_company_work_item = _pipeline._fetch_company_work_item
    original_heartbeat = _pipeline.PIPELINE_HEARTBEAT_INTERVAL_SECONDS
    original_parse_fetched_row = _pipeline._parse_fetched_row
    try:
        _pipeline.load_settings = load_settings
        _pipeline.CompaniesHouseFinancialsClient = CompaniesHouseFinancialsClient
        _pipeline._fetch_company_work_item = _fetch_company_work_item
        _pipeline.PIPELINE_HEARTBEAT_INTERVAL_SECONDS = (
            PIPELINE_HEARTBEAT_INTERVAL_SECONDS
        )
        _pipeline._parse_fetched_row = _parse_fetched_row
        return _pipeline.enrich_financials(
            db_path,
            data_dir=data_dir,
            mode=mode,
            ids=ids,
            workers=workers,
            parser_workers=parser_workers,
            batch_size=batch_size,
        )
    finally:
        _pipeline.load_settings = original_load_settings
        _pipeline.CompaniesHouseFinancialsClient = original_client
        _pipeline._fetch_company_work_item = original_fetch_company_work_item
        _pipeline.PIPELINE_HEARTBEAT_INTERVAL_SECONDS = original_heartbeat
        _pipeline._parse_fetched_row = original_parse_fetched_row
