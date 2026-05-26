"""Companies House filing-history financial extraction with JSONL staging."""

from __future__ import annotations

import io
import json
import logging
import multiprocessing as mp
import queue
import re
import signal
import threading
import time
from dataclasses import dataclass
from datetime import date
from multiprocessing.connection import Connection, wait as wait_for_connections
from pathlib import Path
from typing import Any, Literal

import duckdb
import pdfplumber
import requests
from ixbrlparse import IXBRL
from requests.adapters import HTTPAdapter

from ch_bulk._logging import FsyncLineLogger
from ch_bulk.bootstrap import ensure_pipeline_schema
from ch_bulk.rate_limit import SlidingWindowThrottle
from ch_bulk.settings import load_settings
from ch_bulk.staging import (
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
from ch_bulk.sync_batches import (
    finish_sync_batch,
    insert_sync_batch,
    update_sync_batch_progress,
    utcnow_naive,
)

logger = logging.getLogger(__name__)

Mode = Literal["incremental", "all", "list"]

CH_API_BASE = "https://api.company-information.service.gov.uk"
DOCUMENT_API_BASE = "https://document-api.company-information.service.gov.uk"
FINANCIALS_SYNC_TYPE = "financials"
FINANCIALS_FETCH_SYNC_TYPE = "financials_fetch"
DEFAULT_WORKERS = 3
DEFAULT_PARSER_WORKERS = 4
DEFAULT_BATCH_SIZE = 100
DEFAULT_RETRY_AFTER_SECONDS = 60
DEFAULT_QUEUE_MAXSIZE = 100
DEFAULT_PARSE_INFLIGHT_PER_WORKER = 2
PIPELINE_HEARTBEAT_INTERVAL_SECONDS = 30.0
# Stay below the published 600/5min ceiling so small bursts and clock skew do
# not trip a 429/ban cycle on a long unattended run.
EFFECTIVE_CH_MAX_REQUESTS = 550
CH_WINDOW_SECONDS = 300
FILED_REVENUE_SOURCES = {"filed_accounts_ixbrl", "filed_accounts_pdf"}
TERMINAL_REVENUE_SOURCES = {
    "no_recent_filing",
    "pdf_no_text_layer",
    "partial_no_revenue",
}
ANNUAL_ACCOUNTS_TYPES = {"AA", "AAMD"}
ANNUAL_ACCOUNTS_DESCRIPTION_PREFIXES = (
    "accounts-with-accounts-type-",
    "accounts-amended-with-accounts-type-",
)
IXBRL_RESOURCE = "application/xhtml+xml"
PDF_RESOURCE = "application/pdf"
IXBRL_EXTENSION = "ixbrl"
PDF_EXTENSION = "pdf"
REVENUE_FACTS = (
    "TurnoverRevenue",
    "Revenue",
    "TurnoverGrossOperatingRevenue",
)
EMPLOYEE_COUNT_FACTS = (
    "AverageNumberEmployeesDuringPeriod",
    "AverageNumberEmployeesDuringYear",
)
GROSS_PROFIT_FACTS = (
    "GrossProfitLoss",
)
PROFIT_BEFORE_TAX_FACTS = (
    "ProfitLossOnOrdinaryActivitiesBeforeTax",
    "ProfitLossBeforeTax",
)
PROFIT_AFTER_TAX_FACTS = (
    "ProfitLossOnOrdinaryActivitiesAfterTax",
)
FIXED_ASSETS_FACTS = (
    "FixedAssets",
    "PropertyPlantEquipment",
)
CURRENT_ASSETS_FACTS = (
    "CurrentAssets",
)
NET_ASSETS_FACTS = (
    "NetAssetsLiabilities",
    "NetAssetsLiabilitiesIncludingPensionAssetLiability",
    "TotalAssetsLessCurrentLiabilities",
)
NET_CURRENT_ASSETS_FACTS = (
    "NetCurrentAssetsLiabilities",
)
PERIOD_FACT_GROUPS = (
    REVENUE_FACTS,
    GROSS_PROFIT_FACTS,
    PROFIT_BEFORE_TAX_FACTS,
    PROFIT_AFTER_TAX_FACTS,
    EMPLOYEE_COUNT_FACTS,
)
ERROR_PARSE_STATUSES = {
    "request_error",
    "document_metadata_error",
    "document_download_error",
    "ixbrl_parse_error",
    "pdf_parse_error",
    "no_document_resource",
}
IXBRL_PROFIT_LOSS_EXEMPTION_PATTERNS = (
    re.compile(
        r"statementthatdirectorshaveelectednottodeliverprofitlossaccount",
        re.IGNORECASE,
    ),
    re.compile(
        r"profit\s*(?:and|&)?\s*loss\s+account\s+has\s+not\s+been\s+delivered",
        re.IGNORECASE,
    ),
    re.compile(
        r"statement\s+of\s+income(?:\s+and\s+retained\s+earnings)?\s+has\s+not\s+been\s+delivered",
        re.IGNORECASE,
    ),
    re.compile(r"small\s+companies\s+regime", re.IGNORECASE),
    re.compile(r"section\s*444(?:5a)?", re.IGNORECASE),
)

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


@dataclass(frozen=True)
class FinancialTarget:
    company_number: str
    accounts_last_made_up: date | None


@dataclass(frozen=True)
class FilingCandidate:
    filing_id: str
    filing_date: date | None
    made_up_date: date | None
    paper_filed: bool
    document_metadata_url: str


@dataclass(frozen=True)
class ParsedFinancialFacts:
    revenue: float | None
    employee_count: int | None
    filing_period_start: date | None
    filing_period_end: date | None
    gross_profit: float | None
    profit_before_tax: float | None
    profit_after_tax: float | None
    fixed_assets: float | None
    current_assets: float | None
    total_assets: float | None
    net_assets: float | None
    net_current_assets: float | None
    parse_status: str
    parse_failure_reason: str | None
    profit_loss_exempt: bool = False


@dataclass(frozen=True)
class StagedFinancialRow:
    company_number: str
    filing_id: str | None
    filing_date: str | None
    filing_format: str | None
    filing_period_start: str | None
    filing_period_end: str | None
    revenue: float | None
    employee_count: int | None
    gross_profit: float | None
    profit_before_tax: float | None
    profit_after_tax: float | None
    fixed_assets: float | None
    current_assets: float | None
    total_assets: float | None
    net_assets: float | None
    net_current_assets: float | None
    filing_age_months: int | None
    parse_status: str
    parse_failure_reason: str | None
    fetched_at: str
    profit_loss_exempt: bool = False

    def to_json_line(self) -> str:
        return json.dumps(
            {
                "company_number": self.company_number,
                "filing_id": self.filing_id,
                "filing_date": self.filing_date,
                "filing_format": self.filing_format,
                "filing_period_start": self.filing_period_start,
                "filing_period_end": self.filing_period_end,
                "revenue": self.revenue,
                "employee_count": self.employee_count,
                "gross_profit": self.gross_profit,
                "profit_before_tax": self.profit_before_tax,
                "profit_after_tax": self.profit_after_tax,
                "fixed_assets": self.fixed_assets,
                "current_assets": self.current_assets,
                "total_assets": self.total_assets,
                "net_assets": self.net_assets,
                "net_current_assets": self.net_current_assets,
                "filing_age_months": self.filing_age_months,
                "parse_status": self.parse_status,
                "parse_failure_reason": self.parse_failure_reason,
                "fetched_at": self.fetched_at,
                "profit_loss_exempt": self.profit_loss_exempt,
            },
            ensure_ascii=True,
            sort_keys=True,
        )


@dataclass(frozen=True)
class FetchedFinancialRow:
    company_number: str
    accounts_last_made_up: str | None
    filing_id: str | None
    filing_date: str | None
    filing_made_up_date: str | None
    paper_filed: bool | None
    filing_format: str | None
    raw_path: str | None
    parse_status: str | None
    parse_failure_reason: str | None
    fetched_at: str

    @classmethod
    def from_json_line(cls, line: str) -> "FetchedFinancialRow":
        payload = json.loads(line)
        return cls(
            company_number=str(payload["company_number"]),
            accounts_last_made_up=payload.get("accounts_last_made_up"),
            filing_id=payload.get("filing_id"),
            filing_date=payload.get("filing_date"),
            filing_made_up_date=payload.get("filing_made_up_date"),
            paper_filed=payload.get("paper_filed"),
            filing_format=payload.get("filing_format"),
            raw_path=payload.get("raw_path"),
            parse_status=payload.get("parse_status"),
            parse_failure_reason=payload.get("parse_failure_reason"),
            fetched_at=str(payload["fetched_at"]),
        )

    def to_json_line(self) -> str:
        return json.dumps(
            {
                "company_number": self.company_number,
                "accounts_last_made_up": self.accounts_last_made_up,
                "filing_id": self.filing_id,
                "filing_date": self.filing_date,
                "filing_made_up_date": self.filing_made_up_date,
                "paper_filed": self.paper_filed,
                "filing_format": self.filing_format,
                "raw_path": self.raw_path,
                "parse_status": self.parse_status,
                "parse_failure_reason": self.parse_failure_reason,
                "fetched_at": self.fetched_at,
            },
            ensure_ascii=True,
            sort_keys=True,
        )


@dataclass(frozen=True)
class FetchedFinancialWorkItem:
    row: FetchedFinancialRow
    http_status: int | None
    started_monotonic: float


@dataclass(frozen=True)
class FinancialsResult:
    row: StagedFinancialRow
    http_status: int | None
    latency_seconds: float


@dataclass(frozen=True)
class ParserProcessResponse:
    row: StagedFinancialRow | None
    error: str | None


@dataclass
class ParserProcessHandle:
    process: mp.Process
    connection: Connection
    current_work_item: FetchedFinancialWorkItem | None = None


@dataclass(frozen=True)
class FinancialsFileStats:
    total_rows: int
    ok_count: int
    partial_count: int
    ixbrl_count: int
    pdf_count: int
    pdf_no_text_layer_count: int
    no_filing_count: int
    stale_count: int
    error_count: int


def _empty_parsed_financials(
    *,
    parse_status: str,
    parse_failure_reason: str | None,
) -> ParsedFinancialFacts:
    return ParsedFinancialFacts(
        revenue=None,
        employee_count=None,
        filing_period_start=None,
        filing_period_end=None,
        gross_profit=None,
        profit_before_tax=None,
        profit_after_tax=None,
        fixed_assets=None,
        current_assets=None,
        total_assets=None,
        net_assets=None,
        net_current_assets=None,
        parse_status=parse_status,
        parse_failure_reason=parse_failure_reason,
        profit_loss_exempt=False,
    )


def _missing_financial_reasons(
    *,
    revenue: float | None,
    employee_count: int | None,
    gross_profit: float | None,
    profit_before_tax: float | None,
    profit_after_tax: float | None,
    fixed_assets: float | None,
    current_assets: float | None,
    total_assets: float | None,
    net_assets: float | None,
    net_current_assets: float | None,
) -> str | None:
    missing: list[str] = []
    if revenue is None:
        missing.append("revenue_missing")
    if employee_count is None:
        missing.append("employee_count_missing")
    if gross_profit is None:
        missing.append("gross_profit_missing")
    if profit_before_tax is None:
        missing.append("profit_before_tax_missing")
    if profit_after_tax is None:
        missing.append("profit_after_tax_missing")
    if fixed_assets is None:
        missing.append("fixed_assets_missing")
    if current_assets is None:
        missing.append("current_assets_missing")
    if total_assets is None:
        missing.append("total_assets_missing")
    if net_assets is None:
        missing.append("net_assets_missing")
    if net_current_assets is None:
        missing.append("net_current_assets_missing")
    return ",".join(missing) if missing else None


def _classify_financial_facts(
    *,
    revenue: float | None,
    employee_count: int | None,
    gross_profit: float | None,
    profit_before_tax: float | None,
    profit_after_tax: float | None,
    fixed_assets: float | None,
    current_assets: float | None,
    total_assets: float | None,
    net_assets: float | None,
    net_current_assets: float | None,
    partial_status: str,
    error_status: str,
) -> tuple[str, str | None]:
    if revenue is not None and employee_count is not None:
        return "ok", None

    if any(
        value is not None
        for value in (
            revenue,
            employee_count,
            gross_profit,
            profit_before_tax,
            profit_after_tax,
            fixed_assets,
            current_assets,
            total_assets,
            net_assets,
            net_current_assets,
        )
    ):
        return (
            partial_status,
            _missing_financial_reasons(
                revenue=revenue,
                employee_count=employee_count,
                gross_profit=gross_profit,
                profit_before_tax=profit_before_tax,
                profit_after_tax=profit_after_tax,
                fixed_assets=fixed_assets,
                current_assets=current_assets,
                total_assets=total_assets,
                net_assets=net_assets,
                net_current_assets=net_current_assets,
            ),
        )

    return error_status, "no_supported_facts_found"


class CompaniesHouseFinancialsClient:
    def __init__(
        self,
        *,
        api_key: str,
        throttle: SlidingWindowThrottle,
    ) -> None:
        self._api_key = api_key
        self._throttle = throttle
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "ch-bulk-financials"})
        adapter = HTTPAdapter(max_retries=0, pool_connections=8, pool_maxsize=8)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "CompaniesHouseFinancialsClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _get(
        self,
        url: str,
        *,
        params: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
        allow_redirects: bool = True,
        auth: bool = True,
    ) -> requests.Response:
        for attempt in range(5):
            self._throttle.wait()
            response = self._session.get(
                url,
                params=params,
                headers=headers,
                timeout=60,
                allow_redirects=allow_redirects,
                auth=(self._api_key, "") if auth else None,
            )
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                try:
                    pause = (
                        int(retry_after)
                        if retry_after is not None
                        else DEFAULT_RETRY_AFTER_SECONDS
                    )
                except ValueError:
                    pause = DEFAULT_RETRY_AFTER_SECONDS
                logger.warning("Companies House 429 for %s, sleeping %ss", url, pause)
                time.sleep(pause)
                continue
            if response.status_code in {502, 503, 504}:
                pause = 2**attempt
                logger.warning(
                    "Companies House %s for %s, retrying in %ss",
                    response.status_code,
                    url,
                    pause,
                )
                time.sleep(pause)
                continue
            response.raise_for_status()
            return response
        raise RuntimeError(f"Companies House request failed after retries: {url}")

    def get_filing_history(
        self,
        company_number: str,
    ) -> dict[str, Any]:
        response = self._get(
            f"{CH_API_BASE}/company/{company_number}/filing-history",
            params={"category": "accounts", "items_per_page": 100},
        )
        return dict(response.json())

    def get_document_metadata(
        self,
        document_metadata_url: str,
    ) -> dict[str, Any]:
        response = self._get(document_metadata_url)
        return dict(response.json())

    def _response_bytes_with_retries(
        self,
        *,
        url: str,
        request_factory: Any,
    ) -> bytes:
        for attempt in range(5):
            try:
                response = request_factory()
                response.raise_for_status()
                return bytes(response.content)
            except requests.HTTPError:
                raise
            except requests.RequestException as exc:
                pause = 2**attempt
                if attempt == 4:
                    raise
                logger.warning(
                    "Document body download failed for %s (%s), retrying in %ss",
                    url,
                    type(exc).__name__,
                    pause,
                )
                time.sleep(pause)
        raise RuntimeError(f"Document download failed after retries: {url}")

    def download_document(
        self,
        *,
        document_url: str,
        accept: str,
    ) -> bytes:
        response = self._get(
            document_url,
            headers={"Accept": accept},
            allow_redirects=False,
        )
        if response.status_code in {301, 302, 303, 307, 308}:
            redirect_url = response.headers.get("Location")
            if not redirect_url:
                raise RuntimeError(f"Document redirect missing Location: {document_url}")
            return self._response_bytes_with_retries(
                url=redirect_url,
                request_factory=lambda: self._session.get(
                    redirect_url,
                    timeout=60,
                ),
            )
        response_holder = [response]
        return self._response_bytes_with_retries(
            url=document_url,
            request_factory=lambda: (
                response_holder.pop()
                if response_holder
                else self._get(
                    document_url,
                    headers={"Accept": accept},
                    allow_redirects=False,
                )
            ),
        )

    def download_document_content(
        self,
        *,
        document_metadata_url: str,
        accept: str,
    ) -> bytes:
        return self.download_document(
            document_url=f"{document_metadata_url.rstrip('/')}/content",
            accept=accept,
        )


def _validated_mode(mode: str) -> Mode:
    normalized = mode.strip().lower()
    if normalized not in {"incremental", "all", "list"}:
        raise ValueError(f"Unsupported mode: {mode}")
    return normalized  # type: ignore[return-value]


def _validated_batch_size(batch_size: int) -> int:
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    return batch_size


def _validated_workers(workers: int) -> int:
    if workers <= 0:
        raise ValueError("workers must be > 0")
    return workers


def _validated_parser_workers(parser_workers: int) -> int:
    if parser_workers <= 0:
        raise ValueError("parser_workers must be > 0")
    return parser_workers


def _progress_text(done: int, total: int) -> str:
    if total <= 0:
        return "0/0 0.0%"
    return f"{done}/{total} {(done / total) * 100:.1f}%"


def _duration_text(seconds: float) -> str:
    rounded = max(0, int(round(seconds)))
    hours, remainder = divmod(rounded, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def _parse_date(value: object) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _http_status_from_exception(exc: requests.HTTPError) -> int | None:
    response = exc.response
    return response.status_code if response is not None else None


def _months_between(value: date | None, today: date) -> int | None:
    if value is None:
        return None
    months = (today.year - value.year) * 12 + (today.month - value.month)
    if today.day < value.day:
        months -= 1
    return max(months, 0)


def _normalize_company_ids(values: list[str] | None) -> list[str]:
    if not values:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values:
        cleaned = raw.strip().zfill(8)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)
    return normalized


def _filing_extension_for_format(filing_format: str | None) -> str | None:
    normalized = str(filing_format or "").strip().lower()
    if normalized == "ixbrl":
        return IXBRL_EXTENSION
    if normalized == "pdf":
        return PDF_EXTENSION
    return None


def _raw_filing_path(
    *,
    data_dir: str | Path,
    company_number: str,
    filing_id: str | None,
    filing_format: str | None,
) -> Path | None:
    if not filing_id:
        return None
    filings_dir = Path(data_dir) / "staging" / "filings" / company_number
    extension = _filing_extension_for_format(filing_format)
    if extension is not None:
        path = filings_dir / f"{filing_id}.{extension}"
        return path if path.exists() else None

    for candidate_extension in (IXBRL_EXTENSION, PDF_EXTENSION):
        path = filings_dir / f"{filing_id}.{candidate_extension}"
        if path.exists():
            return path
    return None


def _has_ixbrl_profit_loss_exemption_marker(content: bytes) -> bool:
    text = content.decode("utf-8", errors="ignore")
    return any(pattern.search(text) for pattern in IXBRL_PROFIT_LOSS_EXEMPTION_PATTERNS)


def _select_targets(
    con: duckdb.DuckDBPyConnection,
    *,
    mode: Mode,
    ids: list[str] | None,
    data_dir: str | Path | None = None,
) -> list[FinancialTarget]:
    if mode == "list":
        cleaned = _normalize_company_ids(ids)
        if not cleaned:
            raise ValueError("mode=list requires one or more ids")
        placeholders = ", ".join(["?"] * len(cleaned))
        rows = con.execute(
            f"""
            SELECT company_number, accounts_last_made_up
            FROM companies
            WHERE company_number IN ({placeholders})
            ORDER BY company_number
            """,
            cleaned,
        ).fetchall()
        return [FinancialTarget(row[0], row[1]) for row in rows]

    joins = ""
    filters = ""
    select_extra = "NULL AS filing_id, NULL AS filing_format"
    if mode == "incremental":
        joins = "LEFT JOIN company_enrichment ce USING (company_number)"
        select_extra = """
            NULLIF(CAST(ce.filing_id AS VARCHAR), '') AS filing_id,
            NULLIF(CAST(ce.filing_format AS VARCHAR), '') AS filing_format
        """
        filters = """
          AND COALESCE(ce.revenue_source, '') NOT IN ('no_recent_filing', 'pdf_no_text_layer', 'partial_no_revenue')
          AND NOT (
                ce.revenue IS NOT NULL
            AND ce.revenue_source IN ('filed_accounts_ixbrl', 'filed_accounts_pdf')
          )
        """

    rows = con.execute(
        f"""
        SELECT DISTINCT
            c.company_number,
            c.accounts_last_made_up,
            {select_extra}
        FROM current_company_match m
        JOIN companies c USING (company_number)
        {joins}
        WHERE m.status IN ('user_confirmed', 'auto_confirmed', 'needs_review')
        {filters}
        ORDER BY c.company_number
        """
    ).fetchall()
    targets: list[FinancialTarget] = []
    for company_number, accounts_last_made_up, filing_id, filing_format in rows:
        if mode == "incremental" and data_dir is not None:
            raw_path = _raw_filing_path(
                data_dir=data_dir,
                company_number=str(company_number),
                filing_id=str(filing_id) if filing_id is not None else None,
                filing_format=str(filing_format) if filing_format is not None else None,
            )
            if raw_path is not None:
                continue
        targets.append(FinancialTarget(company_number, accounts_last_made_up))
    return targets


def _select_latest_annual_accounts(
    filing_history: dict[str, Any],
) -> FilingCandidate | None:
    for item in filing_history.get("items", []):
        links = item.get("links") or {}
        document_metadata_url = str(links.get("document_metadata") or "").strip()
        if not document_metadata_url:
            continue
        filing_type = str(item.get("type") or "").upper()
        description = str(item.get("description") or "")
        if (
            filing_type not in ANNUAL_ACCOUNTS_TYPES
            and not description.startswith(ANNUAL_ACCOUNTS_DESCRIPTION_PREFIXES)
        ):
            continue
        filing_id = str(item.get("transaction_id") or "").strip()
        if not filing_id:
            continue
        return FilingCandidate(
            filing_id=filing_id,
            filing_date=_parse_date(item.get("date")),
            made_up_date=_parse_date(
                (item.get("description_values") or {}).get("made_up_date")
            ),
            paper_filed=bool(item.get("paper_filed")),
            document_metadata_url=document_metadata_url,
        )
    return None


def _row_has_segments(row: dict[str, Any]) -> bool:
    for key, value in row.items():
        if not key.startswith("segment:"):
            continue
        if str(value or "").strip():
            return True
    return False


def _pick_ixbrl_row(
    rows: list[dict[str, Any]],
    names: tuple[str, ...],
    *,
    instant: bool,
) -> dict[str, Any] | None:
    candidates = [row for row in rows if row.get("name") in names and row.get("value") is not None]
    if not candidates:
        return None

    def sort_key(row: dict[str, Any]) -> tuple[int, int, int, int]:
        try:
            name_rank = names.index(str(row.get("name")))
        except ValueError:
            name_rank = len(names)
        date_value = row.get("instant") if instant else (row.get("enddate") or row.get("instant"))
        ordinal = _parse_date(date_value).toordinal() if date_value else -1
        period_days = -1
        if row.get("startdate") and row.get("enddate"):
            start = _parse_date(row.get("startdate"))
            end = _parse_date(row.get("enddate"))
            if start is not None and end is not None:
                period_days = (end - start).days
        return (
            name_rank,
            1 if _row_has_segments(row) else 0,
            -ordinal,
            -period_days,
        )

    candidates.sort(key=sort_key)
    return candidates[0]


def _pick_ixbrl_value(
    rows: list[dict[str, Any]],
    names: tuple[str, ...],
    *,
    instant: bool,
) -> float | int | None:
    row = _pick_ixbrl_row(rows, names, instant=instant)
    if row is None:
        return None
    return row.get("value")


def _row_period_start(row: dict[str, Any]) -> date | None:
    return _parse_date(row.get("startdate"))


def _row_period_end(row: dict[str, Any]) -> date | None:
    return _parse_date(row.get("enddate") or row.get("instant"))


def _duration_row_sort_key(row: dict[str, Any]) -> tuple[int, int, int]:
    end_date = _row_period_end(row)
    start_date = _row_period_start(row)
    period_days = -1
    if start_date is not None and end_date is not None:
        period_days = (end_date - start_date).days
    return (
        1 if _row_has_segments(row) else 0,
        -(end_date.toordinal() if end_date is not None else -1),
        -period_days,
    )


def _pick_ixbrl_period_bounds(
    rows: list[dict[str, Any]],
) -> tuple[date | None, date | None]:
    revenue_row = _pick_ixbrl_row(rows, REVENUE_FACTS, instant=False)
    if revenue_row is not None:
        start = _row_period_start(revenue_row)
        end = _row_period_end(revenue_row)
        if start is not None or end is not None:
            return start, end

    net_assets_row = _pick_ixbrl_row(rows, NET_ASSETS_FACTS, instant=True)
    if net_assets_row is not None:
        balance_sheet_end = _row_period_end(net_assets_row)
        if balance_sheet_end is not None:
            for names in PERIOD_FACT_GROUPS:
                row = _pick_ixbrl_row(
                    [
                        candidate
                        for candidate in rows
                        if _row_period_end(candidate) == balance_sheet_end
                    ],
                    names,
                    instant=False,
                )
                if row is None:
                    continue
                start = _row_period_start(row)
                end = _row_period_end(row)
                if start is not None or end is not None:
                    return start, end

            matching_duration_rows = [
                row
                for row in rows
                if row.get("value") is not None
                and (row.get("startdate") or row.get("enddate"))
                and _row_period_end(row) == balance_sheet_end
            ]
            if matching_duration_rows:
                matching_duration_rows.sort(key=_duration_row_sort_key)
                return (
                    _row_period_start(matching_duration_rows[0]),
                    _row_period_end(matching_duration_rows[0]),
                )
            return None, balance_sheet_end

    for names in PERIOD_FACT_GROUPS:
        row = _pick_ixbrl_row(rows, names, instant=False)
        if row is None:
            continue
        start = _row_period_start(row)
        end = _row_period_end(row)
        if start is not None or end is not None:
            return start, end

    duration_rows = [
        row
        for row in rows
        if row.get("value") is not None and (row.get("startdate") or row.get("enddate"))
    ]
    if not duration_rows:
        return None, None

    duration_rows.sort(key=_duration_row_sort_key)
    start = _row_period_start(duration_rows[0])
    end = _row_period_end(duration_rows[0])
    return start, end


def _derived_total_assets(
    *,
    fixed_assets: float | None,
    current_assets: float | None,
) -> float | None:
    if fixed_assets is None and current_assets is None:
        return None
    return float((fixed_assets or 0.0) + (current_assets or 0.0))


def _parse_ixbrl_bytes(
    content: bytes,
) -> ParsedFinancialFacts:
    profit_loss_exempt = _has_ixbrl_profit_loss_exemption_marker(content)
    try:
        ixbrl = IXBRL(io.BytesIO(content), raise_on_error=False)
        rows = ixbrl.to_table(fields="numeric")
    except Exception as exc:
        return _empty_parsed_financials(
            parse_status="ixbrl_parse_error",
            parse_failure_reason=str(exc),
        )

    revenue = _pick_ixbrl_value(rows, REVENUE_FACTS, instant=False)
    employee_count_value = _pick_ixbrl_value(
        rows,
        EMPLOYEE_COUNT_FACTS,
        instant=False,
    )
    gross_profit = _pick_ixbrl_value(rows, GROSS_PROFIT_FACTS, instant=False)
    profit_before_tax = _pick_ixbrl_value(
        rows,
        PROFIT_BEFORE_TAX_FACTS,
        instant=False,
    )
    profit_after_tax = _pick_ixbrl_value(
        rows,
        PROFIT_AFTER_TAX_FACTS,
        instant=False,
    )
    fixed_assets = _pick_ixbrl_value(rows, FIXED_ASSETS_FACTS, instant=True)
    current_assets = _pick_ixbrl_value(
        rows,
        CURRENT_ASSETS_FACTS,
        instant=True,
    )
    net_assets = _pick_ixbrl_value(rows, NET_ASSETS_FACTS, instant=True)
    net_current_assets = _pick_ixbrl_value(
        rows,
        NET_CURRENT_ASSETS_FACTS,
        instant=True,
    )

    employee_count = (
        int(employee_count_value) if employee_count_value is not None else None
    )
    filing_period_start, filing_period_end = _pick_ixbrl_period_bounds(rows)
    revenue_value = float(revenue) if revenue is not None else None
    gross_profit_value = (
        float(gross_profit) if gross_profit is not None else None
    )
    profit_before_tax_value = (
        float(profit_before_tax) if profit_before_tax is not None else None
    )
    profit_after_tax_value = (
        float(profit_after_tax) if profit_after_tax is not None else None
    )
    fixed_assets_value = (
        float(fixed_assets) if fixed_assets is not None else None
    )
    current_assets_value = (
        float(current_assets) if current_assets is not None else None
    )
    total_assets_value = _derived_total_assets(
        fixed_assets=fixed_assets_value,
        current_assets=current_assets_value,
    )
    net_assets_value = float(net_assets) if net_assets is not None else None
    net_current_assets_value = (
        float(net_current_assets) if net_current_assets is not None else None
    )
    status, reason = _classify_financial_facts(
        revenue=revenue_value,
        employee_count=employee_count,
        gross_profit=gross_profit_value,
        profit_before_tax=profit_before_tax_value,
        profit_after_tax=profit_after_tax_value,
        fixed_assets=fixed_assets_value,
        current_assets=current_assets_value,
        total_assets=total_assets_value,
        net_assets=net_assets_value,
        net_current_assets=net_current_assets_value,
        partial_status="partial",
        error_status="ixbrl_parse_error",
    )

    return ParsedFinancialFacts(
        revenue=revenue_value,
        employee_count=employee_count,
        filing_period_start=filing_period_start,
        filing_period_end=filing_period_end,
        gross_profit=gross_profit_value,
        profit_before_tax=profit_before_tax_value,
        profit_after_tax=profit_after_tax_value,
        fixed_assets=fixed_assets_value,
        current_assets=current_assets_value,
        total_assets=total_assets_value,
        net_assets=net_assets_value,
        net_current_assets=net_current_assets_value,
        parse_status=status,
        parse_failure_reason=reason,
        profit_loss_exempt=profit_loss_exempt,
    )


def _parse_numeric_text(value: str) -> float | None:
    cleaned = value.strip().replace(",", "").replace(" ", "")
    if not cleaned:
        return None
    negative = False
    if cleaned.startswith("(") and cleaned.endswith(")"):
        negative = True
        cleaned = cleaned[1:-1]
    try:
        parsed = float(cleaned)
    except ValueError:
        return None
    return -parsed if negative else parsed


def _regex_number(text: str, pattern: re.Pattern[str]) -> float | None:
    match = pattern.search(text)
    if not match:
        return None
    value = _parse_numeric_text(match.group("value"))
    return value


REVENUE_REGEXES = (
    re.compile(r"turnover(?:\s+and\s+other\s+income)?\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)", re.IGNORECASE),
    re.compile(r"revenue\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)", re.IGNORECASE),
)
EMPLOYEE_COUNT_REGEXES = (
    re.compile(r"average\s+number\s+of\s+employees(?:\s+during\s+the\s+period|\s+during\s+the\s+year)?\s*[:\-]?\s*(?P<value>\d{1,6})", re.IGNORECASE),
    re.compile(r"employees?\s*[:\-]?\s*(?P<value>\d{1,6})", re.IGNORECASE),
)
GROSS_PROFIT_REGEXES = (
    re.compile(r"gross\s+profit(?:\s+or\s+loss)?\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)", re.IGNORECASE),
)
PROFIT_BEFORE_TAX_REGEXES = (
    re.compile(r"profit\s+before\s+tax(?:ation)?\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)", re.IGNORECASE),
    re.compile(r"profit\s+on\s+ordinary\s+activities\s+before\s+tax(?:ation)?\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)", re.IGNORECASE),
)
PROFIT_AFTER_TAX_REGEXES = (
    re.compile(r"profit(?:\s+or\s+loss)?\s+after\s+tax(?:ation)?\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)", re.IGNORECASE),
    re.compile(r"(?:profit|loss)\s+for\s+the\s+(?:financial\s+)?year\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)", re.IGNORECASE),
)
FIXED_ASSETS_REGEXES = (
    re.compile(r"fixed\s+assets\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)", re.IGNORECASE),
)
CURRENT_ASSETS_REGEXES = (
    re.compile(r"current\s+assets\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)", re.IGNORECASE),
)
NET_ASSETS_REGEXES = (
    re.compile(r"net\s+assets\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)", re.IGNORECASE),
    re.compile(r"shareholders'?[\s\-]+funds\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)", re.IGNORECASE),
)
NET_CURRENT_ASSETS_REGEXES = (
    re.compile(r"net\s+current\s+assets(?:\s*/\s*\(liabilities\))?\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)", re.IGNORECASE),
    re.compile(r"net\s+current\s+assets(?:\s+or\s+liabilities)?\s*[:\-]?\s*[£$]?\s*(?P<value>\(?[\d,\s]+\)?)", re.IGNORECASE),
)


def _first_regex_value(text: str, patterns: tuple[re.Pattern[str], ...]) -> float | None:
    for pattern in patterns:
        value = _regex_number(text, pattern)
        if value is not None:
            return value
    return None


def _parse_pdf_bytes(
    content: bytes,
) -> ParsedFinancialFacts:
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            page_texts = [(page.extract_text() or "") for page in pdf.pages]
    except Exception as exc:
        return _empty_parsed_financials(
            parse_status="pdf_parse_error",
            parse_failure_reason=str(exc),
        )

    page_count = len(page_texts)
    text_len = sum(len(text.strip()) for text in page_texts)
    if page_count > 0 and text_len < page_count * 100:
        return _empty_parsed_financials(
            parse_status="pdf_no_text_layer",
            parse_failure_reason="pdf_no_text_layer",
        )

    text = "\n".join(page_texts)
    revenue = _first_regex_value(text, REVENUE_REGEXES)
    employee_count_value = _first_regex_value(text, EMPLOYEE_COUNT_REGEXES)
    employee_count = (
        int(employee_count_value) if employee_count_value is not None else None
    )
    gross_profit = _first_regex_value(text, GROSS_PROFIT_REGEXES)
    profit_before_tax = _first_regex_value(
        text,
        PROFIT_BEFORE_TAX_REGEXES,
    )
    profit_after_tax = _first_regex_value(
        text,
        PROFIT_AFTER_TAX_REGEXES,
    )
    fixed_assets = _first_regex_value(text, FIXED_ASSETS_REGEXES)
    current_assets = _first_regex_value(text, CURRENT_ASSETS_REGEXES)
    total_assets = _derived_total_assets(
        fixed_assets=fixed_assets,
        current_assets=current_assets,
    )
    net_assets = _first_regex_value(text, NET_ASSETS_REGEXES)
    net_current_assets = _first_regex_value(text, NET_CURRENT_ASSETS_REGEXES)
    status, reason = _classify_financial_facts(
        revenue=revenue,
        employee_count=employee_count,
        gross_profit=gross_profit,
        profit_before_tax=profit_before_tax,
        profit_after_tax=profit_after_tax,
        fixed_assets=fixed_assets,
        current_assets=current_assets,
        total_assets=total_assets,
        net_assets=net_assets,
        net_current_assets=net_current_assets,
        partial_status="pdf_parse_partial",
        error_status="pdf_parse_error",
    )

    return ParsedFinancialFacts(
        revenue=revenue,
        employee_count=employee_count,
        filing_period_start=None,
        filing_period_end=None,
        gross_profit=gross_profit,
        profit_before_tax=profit_before_tax,
        profit_after_tax=profit_after_tax,
        fixed_assets=fixed_assets,
        current_assets=current_assets,
        total_assets=total_assets,
        net_assets=net_assets,
        net_current_assets=net_current_assets,
        parse_status=status,
        parse_failure_reason=reason,
    )


def _save_raw_filing(
    *,
    data_dir: str | Path,
    company_number: str,
    filing_id: str,
    extension: str,
    content: bytes,
) -> Path:
    filings_dir = Path(data_dir) / "staging" / "filings" / company_number
    filings_dir.mkdir(parents=True, exist_ok=True)
    target_path = filings_dir / f"{filing_id}.{extension}"
    with open(target_path, "wb") as handle:
        handle.write(content)
    return target_path


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


def _build_fetched_row(
    *,
    target: FinancialTarget,
    filing: FilingCandidate | None,
    paper_filed: bool | None,
    filing_format: str | None,
    raw_path: str | None,
    parse_status: str | None,
    parse_failure_reason: str | None,
) -> FetchedFinancialRow:
    return FetchedFinancialRow(
        company_number=target.company_number,
        accounts_last_made_up=(
            target.accounts_last_made_up.isoformat()
            if target.accounts_last_made_up
            else None
        ),
        filing_id=filing.filing_id if filing else None,
        filing_date=(
            filing.filing_date.isoformat()
            if filing and filing.filing_date
            else None
        ),
        filing_made_up_date=(
            filing.made_up_date.isoformat()
            if filing and filing.made_up_date
            else None
        ),
        paper_filed=paper_filed,
        filing_format=filing_format,
        raw_path=raw_path,
        parse_status=parse_status,
        parse_failure_reason=parse_failure_reason,
        fetched_at=isoformat_utc(),
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


def _parser_process_main(connection: Connection) -> None:
    try:
        while True:
            row = connection.recv()
            if row is None:
                break
            try:
                parsed_row = _parse_fetched_row(row)
            except BaseException as exc:
                connection.send(
                    ParserProcessResponse(
                        row=None,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )
            else:
                connection.send(
                    ParserProcessResponse(
                        row=parsed_row,
                        error=None,
                    )
                )
    finally:
        connection.close()


def _fetch_company_work_item(
    client: CompaniesHouseFinancialsClient,
    *,
    target: FinancialTarget,
    data_dir: str | Path,
) -> FetchedFinancialWorkItem:
    started = time.monotonic()
    http_status: int | None = None
    try:
        filing_history = client.get_filing_history(target.company_number)
        http_status = 200
        filing = _select_latest_annual_accounts(filing_history)
        if filing is None:
            return FetchedFinancialWorkItem(
                row=_build_fetched_row(
                    target=target,
                    filing=None,
                    paper_filed=None,
                    filing_format=None,
                    raw_path=None,
                    parse_status="no_filing",
                    parse_failure_reason="no_annual_accounts_filing",
                ),
                http_status=http_status,
                started_monotonic=started,
            )

        if filing.paper_filed:
            try:
                content = client.download_document_content(
                    document_metadata_url=filing.document_metadata_url,
                    accept=PDF_RESOURCE,
                )
            except requests.HTTPError as exc:
                http_status = _http_status_from_exception(exc) or http_status
                return FetchedFinancialWorkItem(
                    row=_build_fetched_row(
                        target=target,
                        filing=filing,
                        paper_filed=filing.paper_filed,
                        filing_format="pdf",
                        raw_path=None,
                        parse_status="document_download_error",
                        parse_failure_reason=(
                            f"http_status_{http_status}"
                            if http_status is not None
                            else "http_error"
                        ),
                    ),
                    http_status=http_status,
                    started_monotonic=started,
                )
            raw_path = _save_raw_filing(
                data_dir=data_dir,
                company_number=target.company_number,
                filing_id=filing.filing_id,
                extension=PDF_EXTENSION,
                content=content,
            )
            return FetchedFinancialWorkItem(
                row=_build_fetched_row(
                    target=target,
                    filing=filing,
                    paper_filed=filing.paper_filed,
                    filing_format="pdf",
                    raw_path=str(raw_path),
                    parse_status="pdf_no_text_layer",
                    parse_failure_reason="pdf_no_text_layer",
                ),
                http_status=http_status,
                started_monotonic=started,
            )

        try:
            content = client.download_document_content(
                document_metadata_url=filing.document_metadata_url,
                accept=IXBRL_RESOURCE,
            )
        except requests.HTTPError as exc:
            http_status = _http_status_from_exception(exc) or http_status
            if http_status == 406:
                try:
                    content = client.download_document_content(
                        document_metadata_url=filing.document_metadata_url,
                        accept=PDF_RESOURCE,
                    )
                except requests.HTTPError as pdf_exc:
                    http_status = _http_status_from_exception(pdf_exc) or http_status
                    return FetchedFinancialWorkItem(
                        row=_build_fetched_row(
                            target=target,
                            filing=filing,
                            paper_filed=filing.paper_filed,
                            filing_format="pdf",
                            raw_path=None,
                            parse_status="document_download_error",
                            parse_failure_reason=(
                                f"http_status_{http_status}"
                                if http_status is not None
                                else "http_error"
                            ),
                        ),
                        http_status=http_status,
                        started_monotonic=started,
                    )
                raw_path = _save_raw_filing(
                    data_dir=data_dir,
                    company_number=target.company_number,
                    filing_id=filing.filing_id,
                    extension=PDF_EXTENSION,
                    content=content,
                )
                return FetchedFinancialWorkItem(
                    row=_build_fetched_row(
                        target=target,
                        filing=filing,
                        paper_filed=filing.paper_filed,
                        filing_format="pdf",
                        raw_path=str(raw_path),
                        parse_status="pdf_no_text_layer",
                        parse_failure_reason="pdf_no_text_layer",
                    ),
                    http_status=http_status,
                    started_monotonic=started,
                )

            return FetchedFinancialWorkItem(
                row=_build_fetched_row(
                    target=target,
                    filing=filing,
                    paper_filed=filing.paper_filed,
                    filing_format="ixbrl",
                    raw_path=None,
                    parse_status="document_download_error",
                    parse_failure_reason=(
                        f"http_status_{http_status}"
                        if http_status is not None
                        else "http_error"
                    ),
                ),
                http_status=http_status,
                started_monotonic=started,
            )
        raw_path = _save_raw_filing(
            data_dir=data_dir,
            company_number=target.company_number,
            filing_id=filing.filing_id,
            extension=IXBRL_EXTENSION,
            content=content,
        )
        return FetchedFinancialWorkItem(
            row=_build_fetched_row(
                target=target,
                filing=filing,
                paper_filed=filing.paper_filed,
                filing_format="ixbrl",
                raw_path=str(raw_path),
                parse_status=None,
                parse_failure_reason=None,
            ),
            http_status=http_status,
            started_monotonic=started,
        )
    except requests.HTTPError as exc:
        http_status = _http_status_from_exception(exc) or http_status
        return FetchedFinancialWorkItem(
            row=_build_fetched_row(
                target=target,
                filing=None,
                paper_filed=None,
                filing_format=None,
                raw_path=None,
                parse_status="request_error",
                parse_failure_reason=(
                    f"http_status_{http_status}"
                    if http_status is not None
                    else "http_error"
                ),
            ),
            http_status=http_status,
            started_monotonic=started,
        )
    except Exception as exc:
        logger.exception("Financials enrich failed for %s", target.company_number)
        return FetchedFinancialWorkItem(
            row=_build_fetched_row(
                target=target,
                filing=None,
                paper_filed=None,
                filing_format=None,
                raw_path=None,
                parse_status="request_error",
                parse_failure_reason=str(exc),
            ),
            http_status=http_status,
            started_monotonic=started,
        )


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


def enrich_financials(
    db_path: str | Path,
    data_dir: str | Path = "./data",
    *,
    mode: str = "incremental",
    ids: list[str] | None = None,
    workers: int = DEFAULT_WORKERS,
    parser_workers: int = DEFAULT_PARSER_WORKERS,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, object]:
    validated_mode = _validated_mode(mode)
    validated_workers = _validated_workers(workers)
    validated_parser_workers = _validated_parser_workers(parser_workers)
    validated_batch_size = _validated_batch_size(batch_size)
    db_path = Path(db_path)
    data_dir = Path(data_dir)
    settings = load_settings(data_dir)
    api_key = str(settings["api_keys"]["companies_house"])
    if not api_key:
        raise RuntimeError("Companies House API key is not configured in settings.json")

    batch_id: str | None = None
    run_log: FsyncLineLogger | None = None
    staging_writer: StagingWriter | None = None
    fetch_staging_writer: StagingWriter | None = None
    parser_processes: list[ParserProcessHandle] = []
    started_monotonic = time.monotonic()
    targets: list[FinancialTarget] = []
    total_errors = 0
    processed = 0
    staged_since_sync = 0
    replayed_fetch_rows = 0
    fetched_count = 0
    pdf_no_text_layer_count = 0
    shutdown_logged = False
    worker_failures: list[BaseException] = []
    result_queue: queue.Queue[FinancialsResult | None] | None = None
    input_queue: queue.Queue[FinancialTarget | None] | None = None
    fetched_queue: queue.Queue[FetchedFinancialWorkItem | None] | None = None
    fetch_manifest_lock = threading.Lock()
    counters_lock = threading.Lock()
    fetch_workers_remaining = 0
    fetch_workers_remaining_lock = threading.Lock()
    parser_controller_finished = threading.Event()
    shutdown_event = threading.Event()
    shutdown_reason: dict[str, str | None] = {"value": None}
    original_signal_handlers: dict[int, Any] = {}
    last_pipeline_heartbeat_monotonic = started_monotonic
    queue_maxsize = 0
    parser_worker_count = 0
    parser_future_limit = 0

    replayed_fetch_rows = replay_financials_fetch_staging(data_dir)
    load_financials_staging(data_dir, db_path)

    def prepare_batch(
        con: duckdb.DuckDBPyConnection,
    ) -> tuple[list[FinancialTarget], str]:
        ensure_pipeline_schema(con)
        _mark_stale_running_batches(con)
        selected = _select_targets(
            con,
            mode=validated_mode,
            ids=ids,
            data_dir=data_dir,
        )
        batch = insert_sync_batch(
            con,
            sync_type=FINANCIALS_SYNC_TYPE,
            mode=validated_mode,
        )
        return selected, batch

    targets, batch_id = with_duckdb_connection(db_path, prepare_batch)
    total_requested = len(targets)
    worker_count = min(validated_workers, total_requested) if total_requested else 0
    parser_worker_count = (
        min(validated_parser_workers, total_requested) if total_requested else 0
    )
    parser_future_limit = parser_worker_count
    queue_maxsize = max(
        10,
        min(
            DEFAULT_QUEUE_MAXSIZE,
            max(validated_batch_size, parser_future_limit),
        ),
    )
    input_queue = queue.Queue(maxsize=queue_maxsize)
    fetched_queue = queue.Queue(maxsize=queue_maxsize)
    result_queue = queue.Queue(maxsize=queue_maxsize)

    run_log = FsyncLineLogger(
        data_dir,
        sync_type=FINANCIALS_SYNC_TYPE,
        batch_id=batch_id,
        filename_prefix="financials",
    )
    staging_writer = StagingWriter(
        data_dir,
        sync_type=FINANCIALS_SYNC_TYPE,
        batch_id=batch_id,
    )
    fetch_staging_writer = StagingWriter(
        data_dir,
        sync_type=FINANCIALS_FETCH_SYNC_TYPE,
        batch_id=batch_id,
    )
    run_log.write_line(
        "start "
        f"sync_type={FINANCIALS_SYNC_TYPE} "
        f"requested={total_requested} "
        f"mode={validated_mode} "
        f"fetch_workers={worker_count} "
        f"parser_workers={parser_worker_count} "
        f"queue_maxsize={queue_maxsize} "
        f"parser_inflight_limit={parser_future_limit} "
        f"batch_size={validated_batch_size} "
        f"replayed_fetch_rows={replayed_fetch_rows}"
    )
    run_log.flush_and_fsync()

    if not targets:
        def finish_empty(con: duckdb.DuckDBPyConnection) -> None:
            ensure_pipeline_schema(con)
            finish_sync_batch(
                con,
                batch_id,
                status="succeeded",
                records_fetched=0,
                records_updated=0,
                error_count=0,
            )

        with_duckdb_connection(db_path, finish_empty)
        return {
            "batch_id": batch_id,
            "requested": 0,
            "records_fetched": 0,
            "records_updated": 0,
            "ok_count": 0,
            "partial_count": 0,
            "ixbrl_count": 0,
            "pdf_count": 0,
            "pdf_no_text_layer_count": 0,
            "no_filing_count": 0,
            "stale_count": 0,
            "error_count": 0,
            "log_path": str(run_log.path),
            "mode": validated_mode,
        }

    throttle = SlidingWindowThrottle(
        EFFECTIVE_CH_MAX_REQUESTS,
        CH_WINDOW_SECONDS,
        label="companies_house_financials",
        logger=logger,
    )

    def queue_depth(q: queue.Queue[Any] | None) -> int:
        if q is None:
            return 0
        try:
            return q.qsize()
        except NotImplementedError:
            return -1

    def request_shutdown(reason: str) -> None:
        shutdown_event.set()
        if shutdown_reason["value"] is None:
            shutdown_reason["value"] = reason

    def note_shutdown_if_needed() -> None:
        nonlocal shutdown_logged
        if shutdown_event.is_set() and not shutdown_logged and run_log is not None:
            run_log.write_line(
                f"shutdown_requested reason={shutdown_reason['value'] or 'external'}"
            )
            run_log.flush_and_fsync()
            shutdown_logged = True

    def persist_batch_progress() -> None:
        if batch_id is None:
            return

        def update_progress(con: duckdb.DuckDBPyConnection) -> None:
            ensure_pipeline_schema(con)
            update_sync_batch_progress(
                con,
                batch_id,
                records_fetched=fetched_count,
                records_updated=processed,
                error_count=total_errors,
            )

        with_duckdb_connection(db_path, update_progress)

    def checkpoint_write_phase() -> None:
        nonlocal staged_since_sync
        if run_log is None or staging_writer is None or fetch_staging_writer is None:
            return
        with fetch_manifest_lock:
            fetch_staging_writer.flush_and_fsync()
        if staged_since_sync > 0:
            staging_writer.flush_and_fsync()
            elapsed = time.monotonic() - started_monotonic
            eta_seconds = (
                ((elapsed / processed) * max(total_requested - processed, 0))
                if processed
                else 0.0
            )
            run_log.write_line(
                "flush "
                f"batch_size={staged_since_sync} "
                f"processed={processed} "
                f"errors={total_errors} "
                f"elapsed={_duration_text(elapsed)} "
                f"eta={_duration_text(eta_seconds)}"
            )
            staged_since_sync = 0
        run_log.flush_and_fsync()
        persist_batch_progress()

    def maybe_log_pipeline_heartbeat(*, force: bool = False) -> None:
        nonlocal last_pipeline_heartbeat_monotonic
        if run_log is None:
            return
        now = time.monotonic()
        if (
            not force
            and now - last_pipeline_heartbeat_monotonic
            < PIPELINE_HEARTBEAT_INTERVAL_SECONDS
        ):
            return
        last_pipeline_heartbeat_monotonic = now
        run_log.write_line(
                "heartbeat "
                f"fetched={fetched_count} "
                f"parsed={processed} "
                f"input_queue={queue_depth(input_queue)} "
                f"fetched_queue={queue_depth(fetched_queue)} "
                f"result_queue={queue_depth(result_queue)} "
                f"pdf_no_text_layer={pdf_no_text_layer_count} "
                f"errors={total_errors} "
                f"progress={_progress_text(processed, total_requested)}"
        )
        run_log.flush_and_fsync()

    def record_fetched_row(row: FetchedFinancialRow) -> None:
        nonlocal fetched_count
        if fetch_staging_writer is None:
            return
        with fetch_manifest_lock:
            fetch_staging_writer.append(row)
            fetch_staging_writer.flush_and_fsync()
        with counters_lock:
            fetched_count += 1

    def record_worker_failure(exc: BaseException) -> None:
        worker_failures.append(exc)
        request_shutdown(f"worker_failure:{type(exc).__name__}")

    def enqueue_input_item(item: FinancialTarget | None) -> bool:
        assert input_queue is not None
        while True:
            if shutdown_event.is_set():
                return False
            try:
                input_queue.put(item, timeout=0.25)
                return True
            except queue.Full:
                note_shutdown_if_needed()
                maybe_log_pipeline_heartbeat(force=False)
                continue

    def handle_signal(signum: int, _frame: object) -> None:
        try:
            signal_name = signal.Signals(signum).name
        except ValueError:
            signal_name = str(signum)
        request_shutdown(f"signal:{signal_name}")

    if threading.current_thread() is threading.main_thread():
        for signum in (signal.SIGINT, signal.SIGTERM):
            original_signal_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, handle_signal)

    parser_context = mp.get_context("fork")

    def fetch_worker() -> None:
        nonlocal fetch_workers_remaining
        assert input_queue is not None
        assert fetched_queue is not None
        try:
            with CompaniesHouseFinancialsClient(
                api_key=api_key,
                throttle=throttle,
            ) as client:
                while True:
                    if shutdown_event.is_set():
                        break
                    try:
                        item = input_queue.get(timeout=0.25)
                    except queue.Empty:
                        continue
                    if item is None:
                        break
                    if shutdown_event.is_set():
                        break
                    work_item = _fetch_company_work_item(
                        client,
                        target=item,
                        data_dir=data_dir,
                    )
                    record_fetched_row(work_item.row)
                    while True:
                        try:
                            fetched_queue.put(work_item, timeout=0.25)
                            break
                        except queue.Full:
                            if shutdown_event.is_set():
                                break
        except BaseException as exc:
            record_worker_failure(exc)
        finally:
            with fetch_workers_remaining_lock:
                fetch_workers_remaining -= 1
                is_last_fetch_worker = fetch_workers_remaining == 0
            if is_last_fetch_worker:
                assert fetched_queue is not None
                while True:
                    try:
                        fetched_queue.put(None, timeout=0.25)
                        break
                    except queue.Full:
                        if parser_controller_finished.is_set():
                            break

    def input_feeder() -> None:
        try:
            for target in targets:
                if not enqueue_input_item(target):
                    return
            for _ in range(worker_count):
                if not enqueue_input_item(None):
                    return
        except BaseException as exc:
            record_worker_failure(exc)

    def parser_controller() -> None:
        assert fetched_queue is not None
        assert result_queue is not None
        connection_to_handle: dict[Connection, ParserProcessHandle] = {}
        fetch_complete = False
        try:
            for handle in parser_processes:
                connection_to_handle[handle.connection] = handle

            while True:
                idle_handles = [
                    handle
                    for handle in parser_processes
                    if handle.current_work_item is None
                ]
                while idle_handles and not fetch_complete:
                    try:
                        work_item = fetched_queue.get(timeout=0.25)
                    except queue.Empty:
                        break
                    if work_item is None:
                        fetch_complete = True
                        break
                    handle = idle_handles.pop()
                    handle.connection.send(work_item.row)
                    handle.current_work_item = work_item

                busy_connections = [
                    handle.connection
                    for handle in parser_processes
                    if handle.current_work_item is not None
                ]
                if busy_connections:
                    ready_connections = wait_for_connections(
                        busy_connections,
                        timeout=0.25,
                    )
                    for connection in ready_connections:
                        handle = connection_to_handle[connection]
                        work_item = handle.current_work_item
                        if work_item is None:
                            continue
                        response = connection.recv()
                        handle.current_work_item = None
                        if response.error is not None or response.row is None:
                            raise RuntimeError(
                                response.error or "parser worker returned no row"
                            )
                        result = FinancialsResult(
                            row=response.row,
                            http_status=work_item.http_status,
                            latency_seconds=time.monotonic()
                            - work_item.started_monotonic,
                        )
                        while True:
                            try:
                                result_queue.put(result, timeout=0.25)
                                break
                            except queue.Full:
                                continue

                if fetch_complete and not any(
                    handle.current_work_item is not None
                    for handle in parser_processes
                ):
                    break
        except BaseException as exc:
            record_worker_failure(exc)
        finally:
            for handle in parser_processes:
                try:
                    handle.connection.send(None)
                except (BrokenPipeError, EOFError, OSError):
                    pass
            for handle in parser_processes:
                try:
                    handle.process.join()
                finally:
                    handle.connection.close()
            parser_controller_finished.set()
            while True:
                try:
                    result_queue.put(None, timeout=0.25)
                    break
                except queue.Full:
                    continue

    threads: list[threading.Thread] = []
    try:
        for index in range(parser_worker_count):
            parent_connection, child_connection = parser_context.Pipe()
            process = parser_context.Process(
                target=_parser_process_main,
                args=(child_connection,),
                name=f"financials-parse-{batch_id}-{index + 1}",
                daemon=False,
            )
            process.start()
            child_connection.close()
            parser_processes.append(
                ParserProcessHandle(
                    process=process,
                    connection=parent_connection,
                )
            )

        fetch_workers_remaining = worker_count
        for _ in range(worker_count):
            thread = threading.Thread(
                target=fetch_worker,
                name=f"financials-fetch-{batch_id}-{len(threads) + 1}",
            )
            threads.append(thread)
            thread.start()

        parser_thread = threading.Thread(
            target=parser_controller,
            name=f"financials-parse-{batch_id}-1",
        )
        threads.append(parser_thread)
        parser_thread.start()

        feeder_thread = threading.Thread(
            target=input_feeder,
            name=f"financials-feed-{batch_id}-1",
        )
        threads.append(feeder_thread)
        feeder_thread.start()

        pending_failure: BaseException | None = None
        parser_finished = False
        assert result_queue is not None
        while not parser_finished:
            if worker_failures and pending_failure is None:
                pending_failure = worker_failures[0]
            try:
                result = result_queue.get(timeout=0.25)
            except queue.Empty:
                note_shutdown_if_needed()
                maybe_log_pipeline_heartbeat(force=False)
                continue
            note_shutdown_if_needed()
            maybe_log_pipeline_heartbeat(force=False)
            if result is None:
                parser_finished = True
                continue
            staging_writer.append(result.row)
            processed += 1
            staged_since_sync += 1
            if result.row.parse_status == "pdf_no_text_layer":
                pdf_no_text_layer_count += 1
            if result.row.parse_status in ERROR_PARSE_STATUSES:
                total_errors += 1
            run_log.write_line(
                "company_number="
                f"{result.row.company_number} "
                f"http_status={result.http_status if result.http_status is not None else 'none'} "
                f"parse_status={result.row.parse_status} "
                f"filing_format={result.row.filing_format or 'none'} "
                f"latency={result.latency_seconds:.2f}s "
                f"progress={_progress_text(processed, total_requested)}"
            )
            if staged_since_sync >= validated_batch_size:
                checkpoint_write_phase()

        for thread in threads:
            thread.join()
        if pending_failure is not None:
            raise pending_failure
        if worker_failures:
            raise worker_failures[0]

        maybe_log_pipeline_heartbeat(force=True)
        if shutdown_event.is_set():
            raise KeyboardInterrupt(shutdown_reason["value"] or "shutdown requested")
        checkpoint_write_phase()
        load_summary = load_financials_staging(
            data_dir,
            db_path,
            batch_id=batch_id,
        )
        _archive_financials_fetch_manifest(
            data_dir,
            batch_id=batch_id,
        )
        elapsed_seconds = time.monotonic() - started_monotonic
        run_log.write_line(
            "complete "
            f"requested={total_requested} "
            f"records_fetched={load_summary['records_fetched']} "
            f"records_updated={load_summary['records_updated']} "
            f"ok={load_summary['ok_count']} "
            f"partial={load_summary['partial_count']} "
            f"ixbrl={load_summary['ixbrl_count']} "
            f"pdf={load_summary['pdf_count']} "
            f"pdf_no_text_layer={load_summary['pdf_no_text_layer_count']} "
            f"no_filing={load_summary['no_filing_count']} "
            f"stale={load_summary['stale_count']} "
            f"errors={load_summary['error_count']} "
            f"elapsed={_duration_text(elapsed_seconds)}"
        )
        run_log.flush_and_fsync()
        return {
            "batch_id": batch_id,
            "requested": total_requested,
            "records_fetched": load_summary["records_fetched"],
            "records_updated": load_summary["records_updated"],
            "ok_count": load_summary["ok_count"],
            "partial_count": load_summary["partial_count"],
            "ixbrl_count": load_summary["ixbrl_count"],
            "pdf_count": load_summary["pdf_count"],
            "pdf_no_text_layer_count": load_summary["pdf_no_text_layer_count"],
            "no_filing_count": load_summary["no_filing_count"],
            "stale_count": load_summary["stale_count"],
            "error_count": load_summary["error_count"],
            "loaded_paths": load_summary["loaded_paths"],
            "log_path": str(run_log.path),
            "mode": validated_mode,
        }
    except BaseException:
        request_shutdown(shutdown_reason["value"] or "exception")
        note_shutdown_if_needed()
        for thread in threads:
            thread.join()
        if run_log is not None:
            checkpoint_write_phase()
            maybe_log_pipeline_heartbeat(force=True)
        if batch_id is not None:
            def mark_failed(con: duckdb.DuckDBPyConnection) -> None:
                ensure_pipeline_schema(con)
                finish_sync_batch(
                    con,
                    batch_id,
                    status="failed",
                    records_fetched=processed,
                    records_updated=0,
                    error_count=total_errors + 1,
                )

            with_duckdb_connection(db_path, mark_failed)
        if run_log is not None:
            run_log.write_line(
                "crash "
                f"processed={processed} "
                f"fetched={fetched_count} "
                f"errors={total_errors + 1} "
                f"reason={shutdown_reason['value'] or 'exception'}"
            )
            run_log.flush_and_fsync()
        raise
    finally:
        for handle in parser_processes:
            if not handle.connection.closed:
                try:
                    handle.connection.send(None)
                except (BrokenPipeError, EOFError, OSError):
                    pass
                handle.connection.close()
            if handle.process.is_alive():
                handle.process.join(timeout=1.0)
                if handle.process.is_alive():
                    handle.process.terminate()
                    handle.process.join(timeout=1.0)
        for signum, handler in original_signal_handlers.items():
            signal.signal(signum, handler)
        if run_log is not None:
            run_log.close()
        if staging_writer is not None:
            staging_writer.close()
        if fetch_staging_writer is not None:
            fetch_staging_writer.close()
