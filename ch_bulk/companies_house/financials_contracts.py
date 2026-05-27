"""Shared contracts and policy constants for financial filing enrichment."""

from __future__ import annotations

import json
import multiprocessing as mp
from dataclasses import dataclass
from datetime import date
from multiprocessing.connection import Connection
from typing import Literal

Mode = Literal["incremental", "all", "list"]

FINANCIALS_SYNC_TYPE = "financials"
FINANCIALS_FETCH_SYNC_TYPE = "financials_fetch"

FILED_REVENUE_SOURCES = {"filed_accounts_ixbrl", "filed_accounts_pdf"}
TERMINAL_REVENUE_SOURCES = {
    "no_recent_filing",
    "pdf_no_text_layer",
    "partial_no_revenue",
}
ERROR_PARSE_STATUSES = {
    "request_error",
    "document_metadata_error",
    "document_download_error",
    "ixbrl_parse_error",
    "pdf_parse_error",
    "no_document_resource",
}

IXBRL_RESOURCE = "application/xhtml+xml"
PDF_RESOURCE = "application/pdf"
IXBRL_EXTENSION = "ixbrl"
PDF_EXTENSION = "pdf"


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
