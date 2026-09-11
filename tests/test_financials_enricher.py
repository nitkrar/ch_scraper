"""Focused tests for financial filing enrichment staging and parsing."""

from __future__ import annotations

import json
import os
import queue as std_queue
import signal
import shutil
import tempfile
import threading
import time
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import duckdb
import requests
from typer.testing import CliRunner

from ch_bulk.core.paths import raw_filings_dir, run_stage_file, runs_dir
from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.cli import app as cli_app
from ch_bulk.companies_house.financials_enricher import (
    IXBRL_RESOURCE,
    PDF_RESOURCE,
    CompaniesHouseFinancialsClient,
    FinancialTarget,
    FetchedFinancialRow,
    FetchedFinancialWorkItem,
    ParsedFinancialFacts,
    StagedFinancialRow,
    _parse_ixbrl_bytes,
    _parse_pdf_bytes,
    _process_company,
    _replay_financials_fetch_staging_file,
    _select_latest_annual_accounts,
    _select_targets,
    enrich_financials,
    load_financials_staging,
)
from ch_bulk.db.staging import StagingWriter
from ch_bulk.db.sync_batches import insert_sync_batch
from tests.support.paths import fixture_path, make_test_workspace

CLI_RUNNER = CliRunner()


def _create_companies_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE companies (
            company_number TEXT PRIMARY KEY,
            company_name TEXT,
            postcode TEXT,
            address_post_town TEXT,
            sic_code_1 TEXT,
            sic_code_2 TEXT,
            sic_code_3 TEXT,
            sic_code_4 TEXT,
            accounts_category TEXT,
            accounts_last_made_up DATE
        )
        """
    )


def _insert_company(
    con: duckdb.DuckDBPyConnection,
    *,
    company_number: str,
    company_name: str = "Example Care Ltd",
    accounts_last_made_up: date | None = None,
    accounts_category: str = "TOTAL EXEMPTION FULL",
) -> None:
    con.execute(
        """
        INSERT INTO companies (
            company_number,
            company_name,
            postcode,
            address_post_town,
            sic_code_1,
            sic_code_2,
            sic_code_3,
            sic_code_4,
            accounts_category,
            accounts_last_made_up
        )
        VALUES (?, ?, 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL, ?, ?)
        """,
        [
            company_number,
            company_name,
            accounts_category,
            accounts_last_made_up,
        ],
    )


def _insert_match(
    con: duckdb.DuckDBPyConnection,
    *,
    company_number: str,
    provider_id: str,
    status: str = "user_confirmed",
) -> None:
    con.execute(
        """
        INSERT INTO ch_cqc_matches (
            company_number,
            cqc_provider_id,
            total_score,
            match_signals,
            status,
            matched_at
        )
        VALUES (
            ?,
            ?,
            100,
            CAST('["fixture"]' AS JSON),
            ?,
            TIMESTAMP '2026-05-25 12:00:00'
        )
        """,
        [company_number, provider_id, status],
    )


def _copy_sample_ixbrl(
    data_dir: str | Path,
    *,
    company_number: str,
    filename: str,
) -> Path:
    target_dir = raw_filings_dir(data_dir, company_number)
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / filename
    shutil.copy(
        fixture_path("financials", "ixbrl", "07545840", "sample.ixbrl"),
        target_path,
    )
    return target_path


class _FakeIXBRLDocument:
    def __init__(self, rows):
        self._rows = rows

    def to_table(self, *, fields: str):
        if fields != "numeric":
            raise AssertionError(f"Unexpected IXBRL field selection: {fields}")
        return list(self._rows)


class _FakePDFPage:
    def __init__(self, text: str):
        self._text = text

    def extract_text(self) -> str:
        return self._text


class _FakePDFDocument:
    def __init__(self, texts: list[str]):
        self.pages = [_FakePDFPage(text) for text in texts]

    def __enter__(self) -> "_FakePDFDocument":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeHTTPResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        content: bytes = b"",
    ) -> None:
        self.status_code = status_code
        self.headers = headers or {}
        self.content = content

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)


class _NoopThrottle:
    def wait(self) -> None:
        return None


class FinancialParserTests(unittest.TestCase):
    def test_parse_ixbrl_bytes_drops_implausibly_large_employee_count(self):
        """A staff-costs-sized figure must not survive as an employee count.

        Observed on CH 11357496, where the OCR path read the staff-costs total
        (1,579,539 + 158,550 + 22,163 = 1,760,252) out of the "Employees and
        directors" note. Unbounded, that passes every downstream size filter
        instead of reading as missing data.
        """
        rows = [
            {
                "name": "TurnoverRevenue",
                "value": 10151720.0,
                "startdate": "2025-01-01",
                "enddate": "2025-12-31",
            },
            {
                "name": "AverageNumberEmployeesDuringPeriod",
                "value": 1760252,
                "startdate": "2025-01-01",
                "enddate": "2025-12-31",
            },
        ]

        with patch(
            "ch_bulk.companies_house.financials_enricher.IXBRL",
            return_value=_FakeIXBRLDocument(rows),
        ):
            facts = _parse_ixbrl_bytes(b"<html></html>")

        self.assertEqual(facts.revenue, 10151720.0)
        self.assertIsNone(facts.employee_count)

    def test_parse_ixbrl_bytes_drops_negative_employee_count(self):
        """A headcount tagged sign="-" must not be stored as a negative count.

        Observed on CH 07384125, whose micro-entity filing tags
        AverageNumberEmployeesDuringPeriod with sign="-" on 9.00. The iXBRL
        spec says to negate, so the parser correctly yields -9; a headcount
        cannot be negative, so it is dropped rather than stored.
        """
        rows = [
            {
                "name": "AverageNumberEmployeesDuringPeriod",
                "value": -9,
                "startdate": "2024-01-01",
                "enddate": "2024-12-31",
            },
            {
                "name": "NetAssetsLiabilities",
                "value": 228421.0,
                "instant": "2024-12-31",
            },
        ]

        with patch(
            "ch_bulk.companies_house.financials_enricher.IXBRL",
            return_value=_FakeIXBRLDocument(rows),
        ):
            facts = _parse_ixbrl_bytes(b"<html></html>")

        self.assertIsNone(facts.employee_count)
        self.assertEqual(facts.net_assets, 228421.0)

    def test_parse_ixbrl_bytes_keeps_plausible_employee_count(self):
        rows = [
            {
                "name": "TurnoverRevenue",
                "value": 10151720.0,
                "startdate": "2025-01-01",
                "enddate": "2025-12-31",
            },
            {
                "name": "AverageNumberEmployeesDuringPeriod",
                "value": 240,
                "startdate": "2025-01-01",
                "enddate": "2025-12-31",
            },
        ]

        with patch(
            "ch_bulk.companies_house.financials_enricher.IXBRL",
            return_value=_FakeIXBRLDocument(rows),
        ):
            facts = _parse_ixbrl_bytes(b"<html></html>")

        self.assertEqual(facts.employee_count, 240)

    def test_parse_ixbrl_bytes_prefers_latest_non_segmented_values(self):
        rows = [
            {
                "name": "TurnoverRevenue",
                "value": 1200.0,
                "startdate": "2024-01-01",
                "enddate": "2024-12-31",
                "segment:business": "",
            },
            {
                "name": "TurnoverRevenue",
                "value": 9999.0,
                "startdate": "2025-01-01",
                "enddate": "2025-12-31",
                "segment:business": "retail",
            },
            {
                "name": "TurnoverRevenue",
                "value": 2400.0,
                "startdate": "2025-01-01",
                "enddate": "2025-12-31",
                "segment:business": "",
            },
            {
                "name": "AverageNumberEmployeesDuringPeriod",
                "value": 31,
                "startdate": "2025-01-01",
                "enddate": "2025-12-31",
            },
            {
                "name": "GrossProfitLoss",
                "value": 640.0,
                "startdate": "2025-01-01",
                "enddate": "2025-12-31",
            },
            {
                "name": "ProfitLossOnOrdinaryActivitiesBeforeTax",
                "value": 75.0,
                "startdate": "2025-01-01",
                "enddate": "2025-12-31",
            },
            {
                "name": "ProfitLossOnOrdinaryActivitiesAfterTax",
                "value": 55.0,
                "startdate": "2025-01-01",
                "enddate": "2025-12-31",
            },
            {
                "name": "FixedAssets",
                "value": 700.0,
                "instant": "2025-12-31",
            },
            {
                "name": "CurrentAssets",
                "value": 500.0,
                "instant": "2025-12-31",
            },
            {
                "name": "NetAssetsLiabilities",
                "value": 810.0,
                "instant": "2025-12-31",
            },
            {
                "name": "NetCurrentAssetsLiabilities",
                "value": 230.0,
                "instant": "2025-12-31",
            },
        ]

        with patch(
            "ch_bulk.companies_house.financials_enricher.IXBRL",
            return_value=_FakeIXBRLDocument(rows),
        ):
            facts = _parse_ixbrl_bytes(b"<html></html>")

        self.assertEqual(facts.parse_status, "ok")
        self.assertIsNone(facts.parse_failure_reason)
        self.assertEqual(facts.revenue, 2400.0)
        self.assertEqual(facts.employee_count, 31)
        self.assertEqual(facts.filing_period_start, date(2025, 1, 1))
        self.assertEqual(facts.filing_period_end, date(2025, 12, 31))
        self.assertEqual(facts.gross_profit, 640.0)
        self.assertEqual(facts.profit_before_tax, 75.0)
        self.assertEqual(facts.profit_after_tax, 55.0)
        self.assertEqual(facts.fixed_assets, 700.0)
        self.assertEqual(facts.current_assets, 500.0)
        self.assertEqual(facts.total_assets, 1200.0)
        self.assertEqual(facts.net_assets, 810.0)
        self.assertEqual(facts.net_current_assets, 230.0)

    def test_parse_ixbrl_bytes_uses_property_plant_equipment_when_fixed_assets_missing(self):
        rows = [
            {
                "name": "AverageNumberEmployeesDuringPeriod",
                "value": 12,
                "startdate": "2025-01-01",
                "enddate": "2025-12-31",
            },
            {
                "name": "PropertyPlantEquipment",
                "value": 700.0,
                "instant": "2025-12-31",
            },
            {
                "name": "CurrentAssets",
                "value": 500.0,
                "instant": "2025-12-31",
            },
            {
                "name": "NetAssetsLiabilities",
                "value": 820.0,
                "instant": "2025-12-31",
            },
        ]

        with patch(
            "ch_bulk.companies_house.financials_enricher.IXBRL",
            return_value=_FakeIXBRLDocument(rows),
        ):
            facts = _parse_ixbrl_bytes(b"<html></html>")

        self.assertEqual(facts.parse_status, "partial")
        self.assertEqual(facts.fixed_assets, 700.0)
        self.assertEqual(facts.current_assets, 500.0)
        self.assertEqual(facts.total_assets, 1200.0)

    def test_parse_ixbrl_bytes_prefers_fixed_assets_over_property_plant_equipment(self):
        rows = [
            {
                "name": "AverageNumberEmployeesDuringPeriod",
                "value": 12,
                "startdate": "2025-01-01",
                "enddate": "2025-12-31",
            },
            {
                "name": "FixedAssets",
                "value": 900.0,
                "instant": "2025-12-31",
            },
            {
                "name": "PropertyPlantEquipment",
                "value": 700.0,
                "instant": "2025-12-31",
            },
            {
                "name": "CurrentAssets",
                "value": 500.0,
                "instant": "2025-12-31",
            },
            {
                "name": "NetAssetsLiabilities",
                "value": 820.0,
                "instant": "2025-12-31",
            },
        ]

        with patch(
            "ch_bulk.companies_house.financials_enricher.IXBRL",
            return_value=_FakeIXBRLDocument(rows),
        ):
            facts = _parse_ixbrl_bytes(b"<html></html>")

        self.assertEqual(facts.fixed_assets, 900.0)
        self.assertEqual(facts.total_assets, 1400.0)

    def test_parse_ixbrl_bytes_marks_profit_loss_exempt_partials(self):
        rows = [
            {
                "name": "AverageNumberEmployeesDuringPeriod",
                "value": 27,
                "startdate": "2025-01-01",
                "enddate": "2025-12-31",
            },
            {
                "name": "NetAssetsLiabilities",
                "value": 820.0,
                "instant": "2025-12-31",
            },
            {
                "name": "CurrentAssets",
                "value": 500.0,
                "instant": "2025-12-31",
            },
        ]

        with patch(
            "ch_bulk.companies_house.financials_enricher.IXBRL",
            return_value=_FakeIXBRLDocument(rows),
        ):
            facts = _parse_ixbrl_bytes(
                b"<html>StatementThatDirectorsHaveElectedNotToDeliverProfitLossAccountUnderSection4445ACompaniesAct2006</html>"
            )

        self.assertEqual(facts.parse_status, "partial")
        self.assertTrue(facts.profit_loss_exempt)
        self.assertIsNone(facts.revenue)
        self.assertEqual(facts.employee_count, 27)

    def test_parse_ixbrl_bytes_from_sample_file_matches_real_exact_facts(self):
        sample_path = fixture_path(
            "financials",
            "ixbrl",
            "07545840",
            "sample.ixbrl",
        )
        self.assertTrue(sample_path.exists(), f"Missing sample IXBRL file: {sample_path}")

        facts = _parse_ixbrl_bytes(sample_path.read_bytes())

        self.assertEqual(facts.parse_status, "ok")
        self.assertIsNone(facts.parse_failure_reason)
        self.assertIsNotNone(facts.revenue)
        self.assertIsNotNone(facts.employee_count)
        self.assertEqual(facts.filing_period_start, date(2025, 3, 1))
        self.assertEqual(facts.filing_period_end, date(2026, 2, 28))
        self.assertIsNotNone(facts.gross_profit)
        self.assertIsNotNone(facts.profit_before_tax)
        self.assertIsNotNone(facts.profit_after_tax)
        self.assertIsNotNone(facts.fixed_assets)
        self.assertIsNotNone(facts.current_assets)
        self.assertIsNotNone(facts.total_assets)
        self.assertIsNotNone(facts.net_assets)
        self.assertIsNotNone(facts.net_current_assets)

    def test_parse_ixbrl_bytes_uses_balance_sheet_end_when_revenue_missing(self):
        rows = [
            {
                "name": "AverageNumberEmployeesDuringPeriod",
                "value": 14,
                "startdate": "2025-02-01",
                "enddate": "2026-01-31",
            },
            {
                "name": "AverageNumberEmployeesDuringPeriod",
                "value": 9,
                "startdate": "2024-02-01",
                "enddate": "2025-01-31",
            },
            {
                "name": "NetAssetsLiabilities",
                "value": 820.0,
                "instant": "2026-01-31",
            },
            {
                "name": "CurrentAssets",
                "value": 500.0,
                "instant": "2026-01-31",
            },
        ]

        with patch(
            "ch_bulk.companies_house.financials_enricher.IXBRL",
            return_value=_FakeIXBRLDocument(rows),
        ):
            facts = _parse_ixbrl_bytes(b"<html></html>")

        self.assertEqual(facts.employee_count, 14)
        self.assertEqual(facts.filing_period_start, date(2025, 2, 1))
        self.assertEqual(facts.filing_period_end, date(2026, 1, 31))
        self.assertEqual(facts.net_assets, 820.0)

    def test_parse_pdf_bytes_marks_partial_when_revenue_missing(self):
        fake_pdf = _FakePDFDocument(
            [
                (
                    "Average number of employees during the year 17. "
                    "These notes provide narrative context and enough surrounding text "
                    "to clear the no-text-layer threshold for a genuine text PDF."
                ),
                (
                    "Current assets 120,000\nProfit before taxation (12,500)\nNet assets 80,000\n"
                    "Additional directors report wording keeps this fixture above the sparse-scan "
                    "cutoff so the regex parser actually runs."
                ),
            ]
        )

        with patch("ch_bulk.companies_house.financials_enricher.pdfplumber.open", return_value=fake_pdf):
            facts = _parse_pdf_bytes(b"%PDF-1.4 fixture")

        self.assertEqual(facts.parse_status, "pdf_parse_partial")
        self.assertEqual(
            facts.parse_failure_reason,
            "revenue_missing,gross_profit_missing,profit_after_tax_missing,fixed_assets_missing,net_current_assets_missing",
        )
        self.assertIsNone(facts.revenue)
        self.assertEqual(facts.employee_count, 17)
        self.assertIsNone(facts.filing_period_start)
        self.assertIsNone(facts.filing_period_end)
        self.assertIsNone(facts.gross_profit)
        self.assertEqual(facts.profit_before_tax, -12500.0)
        self.assertIsNone(facts.profit_after_tax)
        self.assertIsNone(facts.fixed_assets)
        self.assertEqual(facts.current_assets, 120000.0)
        self.assertEqual(facts.total_assets, 120000.0)
        self.assertEqual(facts.net_assets, 80000.0)
        self.assertIsNone(facts.net_current_assets)

    def test_parse_pdf_bytes_marks_sparse_scans_as_no_text_layer(self):
        fake_pdf = _FakePDFDocument(["", "", ""])

        with patch("ch_bulk.companies_house.financials_enricher.pdfplumber.open", return_value=fake_pdf):
            facts = _parse_pdf_bytes(b"%PDF-1.4 fixture")

        self.assertEqual(facts.parse_status, "pdf_no_text_layer")
        self.assertEqual(facts.parse_failure_reason, "pdf_no_text_layer")
        self.assertIsNone(facts.revenue)
        self.assertIsNone(facts.employee_count)
        self.assertIsNone(facts.filing_period_start)
        self.assertIsNone(facts.filing_period_end)
        self.assertIsNone(facts.gross_profit)
        self.assertIsNone(facts.profit_before_tax)
        self.assertIsNone(facts.profit_after_tax)
        self.assertIsNone(facts.fixed_assets)
        self.assertIsNone(facts.current_assets)
        self.assertIsNone(facts.total_assets)
        self.assertIsNone(facts.net_assets)
        self.assertIsNone(facts.net_current_assets)

    def test_select_latest_annual_accounts_skips_aa01_change_reference_date(self):
        filing = _select_latest_annual_accounts(
            {
                "items": [
                    {
                        "date": "2025-11-03",
                        "type": "AA01",
                        "description": "change-account-reference-date-company-previous-extended",
                        "transaction_id": "wrong-one",
                        "links": {"document_metadata": "https://example.test/aa01"},
                    },
                    {
                        "date": "2025-03-28",
                        "type": "AA",
                        "description": "accounts-with-accounts-type-total-exemption-full",
                        "transaction_id": "right-one",
                        "links": {"document_metadata": "https://example.test/aa"},
                        "paper_filed": False,
                        "description_values": {"made_up_date": "2024-10-31"},
                    },
                ]
            }
        )

        self.assertIsNotNone(filing)
        self.assertEqual(filing.filing_id, "right-one")
        self.assertEqual(filing.filing_date, date(2025, 3, 28))
        self.assertEqual(filing.made_up_date, date(2024, 10, 31))
        self.assertFalse(filing.paper_filed)

    def test_download_document_retries_redirect_body_failures(self):
        client = CompaniesHouseFinancialsClient(
            api_key="fixture-key",
            throttle=_NoopThrottle(),
        )
        self.addCleanup(client.close)

        redirect_response = _FakeHTTPResponse(
            status_code=302,
            headers={"Location": "https://example.test/document.ixbrl"},
        )
        success_response = _FakeHTTPResponse(content=b"<html>ok</html>")

        with (
            patch.object(client, "_get", return_value=redirect_response),
            patch.object(
                client._session,
                "get",
                side_effect=[
                    requests.exceptions.ChunkedEncodingError("boom"),
                    success_response,
                ],
            ) as public_get,
            patch("ch_bulk.companies_house.financials_enricher.time.sleep"),
        ):
            content = client.download_document(
                document_url="https://document-api.company-information.service.gov.uk/document/id/content",
                accept=IXBRL_RESOURCE,
            )

        self.assertEqual(content, b"<html>ok</html>")
        self.assertEqual(public_get.call_count, 2)

    def test_download_document_content_uses_document_metadata_base(self):
        client = CompaniesHouseFinancialsClient(
            api_key="fixture-key",
            throttle=_NoopThrottle(),
        )
        self.addCleanup(client.close)

        with patch.object(
            client,
            "download_document",
            return_value=b"%PDF-1.1 fixture",
        ) as download_document:
            content = client.download_document_content(
                document_metadata_url="https://document-api.company-information.service.gov.uk/document/abc123",
                accept=PDF_RESOURCE,
            )

        self.assertEqual(content, b"%PDF-1.1 fixture")
        download_document.assert_called_once_with(
            document_url="https://document-api.company-information.service.gov.uk/document/abc123/content",
            accept=PDF_RESOURCE,
        )


class FinancialProcessTests(unittest.TestCase):
    def test_process_company_downloads_ixbrl_direct_from_content_url(self):
        client = Mock()
        client.get_filing_history.return_value = {
            "items": [
                {
                    "date": "2025-03-28",
                    "type": "AA",
                    "description": "accounts-with-accounts-type-total-exemption-full",
                    "transaction_id": "right-one",
                    "paper_filed": False,
                    "links": {
                        "document_metadata": "https://document-api.company-information.service.gov.uk/document/right-one"
                    },
                    "description_values": {"made_up_date": "2024-10-31"},
                }
            ]
        }
        client.download_document_content.return_value = b"<?xml version='1.0'?><html></html>"

        parsed_facts = ParsedFinancialFacts(
            revenue=2400.0,
            employee_count=31,
            filing_period_start=date(2025, 1, 1),
            filing_period_end=date(2025, 12, 31),
            gross_profit=640.0,
            profit_before_tax=75.0,
            profit_after_tax=55.0,
            fixed_assets=700.0,
            current_assets=500.0,
            total_assets=1200.0,
            net_assets=810.0,
            net_current_assets=230.0,
            parse_status="ok",
            parse_failure_reason=None,
        )

        with (
            tempfile.TemporaryDirectory() as tmpdir,
            patch(
                "ch_bulk.companies_house.financials_enricher._parse_ixbrl_bytes",
                return_value=parsed_facts,
            ) as parse_ixbrl,
            patch("ch_bulk.companies_house.financials_enricher._parse_pdf_bytes") as parse_pdf,
        ):
            result = _process_company(
                client,
                target=FinancialTarget("12345678", date(2024, 10, 31)),
                data_dir=tmpdir,
            )
            raw_path = raw_filings_dir(tmpdir, "12345678") / "right-one.ixbrl"
            self.assertTrue(raw_path.exists())
            self.assertEqual(raw_path.read_bytes(), b"<?xml version='1.0'?><html></html>")

        client.download_document_content.assert_called_once_with(
            document_metadata_url="https://document-api.company-information.service.gov.uk/document/right-one",
            accept=IXBRL_RESOURCE,
        )
        parse_ixbrl.assert_called_once_with(b"<?xml version='1.0'?><html></html>")
        parse_pdf.assert_not_called()
        self.assertEqual(result.row.filing_format, "ixbrl")
        self.assertEqual(result.row.parse_status, "ok")
        self.assertEqual(result.row.filing_id, "right-one")

    def test_process_company_marks_paper_filed_as_pdf_no_text_layer_without_parsing(self):
        client = Mock()
        client.get_filing_history.return_value = {
            "items": [
                {
                    "date": "2025-03-28",
                    "type": "AA",
                    "description": "accounts-with-accounts-type-total-exemption-full",
                    "transaction_id": "paper-one",
                    "paper_filed": True,
                    "links": {
                        "document_metadata": "https://document-api.company-information.service.gov.uk/document/paper-one"
                    },
                    "description_values": {"made_up_date": "2024-10-31"},
                }
            ]
        }
        client.download_document_content.return_value = b"%PDF-1.1 fixture"

        with (
            tempfile.TemporaryDirectory() as tmpdir,
            patch("ch_bulk.companies_house.financials_enricher._parse_ixbrl_bytes") as parse_ixbrl,
            patch("ch_bulk.companies_house.financials_enricher._parse_pdf_bytes") as parse_pdf,
        ):
            result = _process_company(
                client,
                target=FinancialTarget("12345678", date(2024, 10, 31)),
                data_dir=tmpdir,
            )
            raw_path = raw_filings_dir(tmpdir, "12345678") / "paper-one.pdf"
            self.assertTrue(raw_path.exists())
            self.assertEqual(raw_path.read_bytes(), b"%PDF-1.1 fixture")

        client.download_document_content.assert_called_once_with(
            document_metadata_url="https://document-api.company-information.service.gov.uk/document/paper-one",
            accept=PDF_RESOURCE,
        )
        parse_ixbrl.assert_not_called()
        parse_pdf.assert_not_called()
        self.assertEqual(result.row.filing_format, "pdf")
        self.assertEqual(result.row.parse_status, "pdf_no_text_layer")
        self.assertEqual(result.row.parse_failure_reason, "pdf_no_text_layer")

    def _filing_history(self, transaction_id: str, *, paper_filed: bool) -> dict:
        return {
            "items": [
                {
                    "date": "2025-03-28",
                    "type": "AA",
                    "description": "accounts-with-accounts-type-total-exemption-full",
                    "transaction_id": transaction_id,
                    "paper_filed": paper_filed,
                    "links": {
                        "document_metadata": (
                            "https://document-api.company-information.service.gov.uk"
                            f"/document/{transaction_id}"
                        )
                    },
                    "description_values": {"made_up_date": "2024-10-31"},
                }
            ]
        }

    def test_process_company_reuses_saved_ixbrl_instead_of_redownloading(self):
        """A filing already on disk must not be fetched again.

        Documents are immutable for a given filing_id, so re-running any mode
        over companies we have already fetched should cost no API calls.
        """
        client = Mock()
        client.get_filing_history.return_value = self._filing_history(
            "cached-one", paper_filed=False
        )
        client.download_document_content.side_effect = AssertionError(
            "download_document_content must not be called when a saved copy exists"
        )

        parsed_ok = ParsedFinancialFacts(
            revenue=100.0,
            employee_count=2,
            filing_period_start=date(2025, 1, 1),
            filing_period_end=date(2025, 12, 31),
            gross_profit=None,
            profit_before_tax=None,
            profit_after_tax=None,
            fixed_assets=None,
            current_assets=None,
            total_assets=None,
            net_assets=None,
            net_current_assets=None,
            parse_status="ok",
            parse_failure_reason=None,
        )

        with (
            tempfile.TemporaryDirectory() as tmpdir,
            patch(
                "ch_bulk.companies_house.financials_enricher._parse_ixbrl_bytes",
                return_value=parsed_ok,
            ) as parse_ixbrl,
        ):
            cached = raw_filings_dir(tmpdir, "12345678") / "cached-one.ixbrl"
            cached.parent.mkdir(parents=True, exist_ok=True)
            cached.write_bytes(b"<html>cached</html>")

            result = _process_company(
                client,
                target=FinancialTarget("12345678", date(2024, 10, 31)),
                data_dir=tmpdir,
            )

        client.download_document_content.assert_not_called()
        parse_ixbrl.assert_called_once_with(b"<html>cached</html>")
        self.assertEqual(result.row.filing_format, "ixbrl")
        self.assertEqual(result.row.filing_id, "cached-one")

    def test_process_company_reuses_saved_pdf_instead_of_redownloading(self):
        """Same guarantee for paper-filed PDFs, which are the largest downloads."""
        client = Mock()
        client.get_filing_history.return_value = self._filing_history(
            "cached-pdf", paper_filed=True
        )
        client.download_document_content.side_effect = AssertionError(
            "download_document_content must not be called when a saved copy exists"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            cached = raw_filings_dir(tmpdir, "12345678") / "cached-pdf.pdf"
            cached.parent.mkdir(parents=True, exist_ok=True)
            cached.write_bytes(b"%PDF-1.1 cached")

            result = _process_company(
                client,
                target=FinancialTarget("12345678", date(2024, 10, 31)),
                data_dir=tmpdir,
            )

            # The saved copy must be served as-is, not re-fetched or rewritten.
            self.assertEqual(cached.read_bytes(), b"%PDF-1.1 cached")

        client.download_document_content.assert_not_called()
        self.assertEqual(result.row.filing_format, "pdf")
        self.assertEqual(result.row.filing_id, "cached-pdf")

    def test_process_company_retries_406_ixbrl_as_pdf_no_text_layer(self):
        client = Mock()
        client.get_filing_history.return_value = {
            "items": [
                {
                    "date": "2025-03-28",
                    "type": "AA",
                    "description": "accounts-with-accounts-type-total-exemption-full",
                    "transaction_id": "fallback-one",
                    "paper_filed": False,
                    "links": {
                        "document_metadata": "https://document-api.company-information.service.gov.uk/document/fallback-one"
                    },
                    "description_values": {"made_up_date": "2024-10-31"},
                }
            ]
        }
        client.download_document_content.side_effect = [
            requests.HTTPError(response=_FakeHTTPResponse(status_code=406)),
            b"%PDF-1.1 fallback",
        ]

        with (
            tempfile.TemporaryDirectory() as tmpdir,
            patch("ch_bulk.companies_house.financials_enricher._parse_ixbrl_bytes") as parse_ixbrl,
            patch("ch_bulk.companies_house.financials_enricher._parse_pdf_bytes") as parse_pdf,
        ):
            result = _process_company(
                client,
                target=FinancialTarget("12345678", date(2024, 10, 31)),
                data_dir=tmpdir,
            )
            raw_path = raw_filings_dir(tmpdir, "12345678") / "fallback-one.pdf"
            self.assertTrue(raw_path.exists())
            self.assertEqual(raw_path.read_bytes(), b"%PDF-1.1 fallback")

        self.assertEqual(
            client.download_document_content.call_args_list,
            [
                unittest.mock.call(
                    document_metadata_url="https://document-api.company-information.service.gov.uk/document/fallback-one",
                    accept=IXBRL_RESOURCE,
                ),
                unittest.mock.call(
                    document_metadata_url="https://document-api.company-information.service.gov.uk/document/fallback-one",
                    accept=PDF_RESOURCE,
                ),
            ],
        )
        parse_ixbrl.assert_not_called()
        parse_pdf.assert_not_called()
        self.assertEqual(result.row.filing_format, "pdf")
        self.assertEqual(result.row.parse_status, "pdf_no_text_layer")
        self.assertEqual(result.row.parse_failure_reason, "pdf_no_text_layer")


class FinancialTargetSelectionTests(unittest.TestCase):
    def test_select_targets_incremental_skips_terminal_financial_states(self):
        con = duckdb.connect()
        try:
            _create_companies_table(con)
            ensure_pipeline_schema(con)

            _insert_company(
                con,
                company_number="10000001",
                company_name="Filed Revenue Ltd",
                accounts_last_made_up=date(2026, 1, 31),
            )
            _insert_company(
                con,
                company_number="10000002",
                company_name="Estimated Revenue Ltd",
                accounts_last_made_up=date(2026, 2, 28),
            )
            _insert_company(
                con,
                company_number="10000003",
                company_name="Pending Revenue Ltd",
                accounts_last_made_up=date(2026, 3, 31),
            )
            _insert_company(
                con,
                company_number="10000004",
                company_name="Null Filed Revenue Ltd",
                accounts_last_made_up=date(2026, 4, 30),
            )
            _insert_company(
                con,
                company_number="10000005",
                company_name="No Filing Yet Ltd",
                accounts_last_made_up=date(2026, 5, 31),
            )
            _insert_company(
                con,
                company_number="10000006",
                company_name="Filed PDF Revenue Ltd",
                accounts_last_made_up=date(2026, 6, 30),
            )
            _insert_company(
                con,
                company_number="10000007",
                company_name="No Text PDF Ltd",
                accounts_last_made_up=date(2026, 7, 31),
            )
            _insert_company(
                con,
                company_number="10000008",
                company_name="Filleted Accounts Ltd",
                accounts_last_made_up=date(2026, 8, 31),
            )

            _insert_match(con, company_number="10000001", provider_id="prov-1")
            _insert_match(con, company_number="10000002", provider_id="prov-2")
            _insert_match(con, company_number="10000003", provider_id="prov-3")
            _insert_match(con, company_number="10000004", provider_id="prov-4")
            _insert_match(con, company_number="10000005", provider_id="prov-5")
            _insert_match(con, company_number="10000006", provider_id="prov-6")
            _insert_match(con, company_number="10000007", provider_id="prov-7")
            _insert_match(con, company_number="10000008", provider_id="prov-8")

            con.execute(
                """
                INSERT INTO company_enrichment (
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
                    last_enriched_at
                )
                VALUES
                    (
                        '10000001',
                        64, 60, 70, 2, TRUE,
                        CAST('[1956,1960]' AS JSON),
                        1250000,
                        'filed_accounts_ixbrl',
                        18,
                        TIMESTAMP '2026-05-25 12:00:00'
                    ),
                    (
                        '10000002',
                        58, 55, 61, 1, FALSE,
                        CAST('[1965,1968]' AS JSON),
                        900000,
                        'employee_band_lookup',
                        14,
                        TIMESTAMP '2026-05-25 12:00:00'
                    ),
                    (
                        '10000004',
                        57, 54, 60, 1, FALSE,
                        CAST('[1966,1969]' AS JSON),
                        NULL,
                        'filed_accounts_pdf',
                        12,
                        TIMESTAMP '2026-05-25 12:00:00'
                    ),
                    (
                        '10000005',
                        59, 56, 63, 1, FALSE,
                        CAST('[1963,1967]' AS JSON),
                        NULL,
                        'no_recent_filing',
                        NULL,
                        TIMESTAMP '2026-05-25 12:00:00'
                    ),
                    (
                        '10000006',
                        61, 58, 64, 2, FALSE,
                        CAST('[1962,1965]' AS JSON),
                        1400000,
                        'filed_accounts_pdf',
                        24,
                        TIMESTAMP '2026-05-25 12:00:00'
                    ),
                    (
                        '10000007',
                        56, 53, 59, 0, FALSE,
                        CAST('[1967,1970]' AS JSON),
                        NULL,
                        'pdf_no_text_layer',
                        NULL,
                        TIMESTAMP '2026-05-25 12:00:00'
                    ),
                    (
                        '10000008',
                        56, 53, 59, 0, FALSE,
                        CAST('[1967,1970]' AS JSON),
                        NULL,
                        'partial_no_revenue',
                        27,
                        TIMESTAMP '2026-05-25 12:00:00'
                    )
                """
            )

            targets = _select_targets(con, mode="incremental", ids=None)
        finally:
            con.close()

        self.assertEqual(
            [target.company_number for target in targets],
            ["10000002", "10000003", "10000004"],
        )
        self.assertEqual(targets[0].accounts_last_made_up, date(2026, 2, 28))

    def test_select_targets_incremental_skips_ocr_extracted_rows(self):
        """OCR-extracted financials must be terminal.

        A scanned PDF stays scanned, so re-fetching one can only overwrite the
        OCR-derived figures with pdf_no_text_layer again. Without this the
        next incremental run silently destroys the extraction.
        """
        con = duckdb.connect()
        try:
            _create_companies_table(con)
            ensure_pipeline_schema(con)
            _insert_company(
                con,
                company_number="10000009",
                company_name="OCR Extracted Ltd",
                accounts_last_made_up=date(2026, 6, 30),
            )
            _insert_match(con, company_number="10000009", provider_id="prov-9")
            con.execute(
                """
                INSERT INTO company_enrichment (
                    company_number, revenue, revenue_source, employee_count, last_enriched_at
                ) VALUES (
                    '10000009', 11481000, 'filed_accounts_pdf_ocr', 98,
                    TIMESTAMP '2026-05-25 12:00:00'
                )
                """
            )
            targets = _select_targets(con, mode="incremental", ids=None)
        finally:
            con.close()

        self.assertEqual([t.company_number for t in targets], [])

    def test_select_targets_incremental_skips_saved_raw_files_on_disk(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_path = raw_filings_dir(tmpdir, "10000009") / "saved-one.ixbrl"
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_bytes(b"<html>fixture</html>")

            con = duckdb.connect()
            try:
                _create_companies_table(con)
                ensure_pipeline_schema(con)
                _insert_company(
                    con,
                    company_number="10000009",
                    company_name="Saved Raw Ltd",
                    accounts_last_made_up=date(2026, 9, 30),
                )
                _insert_match(con, company_number="10000009", provider_id="prov-9")
                con.execute(
                    """
                    INSERT INTO company_enrichment (
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
                        filing_id,
                        filing_format,
                        last_enriched_at
                    )
                    VALUES (
                        '10000009',
                        60, 57, 63, 1, FALSE,
                        CAST('[1964,1968]' AS JSON),
                        NULL,
                        NULL,
                        11,
                        'saved-one',
                        'ixbrl',
                        TIMESTAMP '2026-05-25 12:00:00'
                    )
                    """
                )
                targets = _select_targets(
                    con,
                    mode="incremental",
                    ids=None,
                    data_dir=tmpdir,
                )
            finally:
                con.close()

        self.assertEqual(targets, [])


class FinancialStagingTests(unittest.TestCase):
    def _create_db(self) -> tuple[tempfile.TemporaryDirectory[str], Path]:
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        db_path = Path(tmpdir.name) / "financials.duckdb"
        con = duckdb.connect(str(db_path))
        try:
            _create_companies_table(con)
            ensure_pipeline_schema(con)
        finally:
            con.close()
        return tmpdir, db_path

    def test_load_financials_staging_updates_company_enrichment_and_preserves_director_fields(self):
        tmpdir, db_path = self._create_db()

        con = duckdb.connect(str(db_path))
        try:
            _insert_company(
                con,
                company_number="20000001",
                company_name="Loader Fixture Ltd",
                accounts_last_made_up=date(2026, 1, 31),
            )
            batch_id = insert_sync_batch(
                con,
                sync_type="financials",
                mode="incremental",
            )
            con.execute(
                """
                INSERT INTO company_enrichment (
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
                VALUES (
                    '20000001',
                    63,
                    60,
                    66,
                    2,
                    TRUE,
                    CAST('[1960,1963]' AS JSON),
                    500000,
                    'employee_band_lookup',
                    11,
                    DATE '2025-01-01',
                    DATE '2025-12-31',
                    150000,
                    110000,
                    90000,
                    400000,
                    550000,
                    320000,
                    180000,
                    50000,
                    'old-file',
                    'ixbrl',
                    12,
                    TIMESTAMP '2026-05-25 11:00:00'
                )
                """
            )
        finally:
            con.close()

        writer = StagingWriter(
            tmpdir.name,
            sync_type="financials",
            batch_id=batch_id,
        )
        try:
            writer.append(
                StagedFinancialRow(
                    company_number="20000001",
                    filing_id="abc123",
                    filing_date="2026-03-12",
                    filing_format="ixbrl",
                    filing_period_start="2025-01-01",
                    filing_period_end="2025-12-31",
                    revenue=1750000.0,
                    employee_count=42,
                    gross_profit=600000.0,
                    profit_before_tax=120000.0,
                    profit_after_tax=100000.0,
                    fixed_assets=400000.0,
                    current_assets=550000.0,
                    total_assets=950000.0,
                    net_assets=700000.0,
                    net_current_assets=250000.0,
                    filing_age_months=3,
                    parse_status="ok",
                    parse_failure_reason=None,
                    fetched_at="2026-05-25T12:30:00Z",
                )
            )
            writer.flush_and_fsync()
        finally:
            writer.close()

        summary = load_financials_staging(
            tmpdir.name,
            db_path,
            batch_id=batch_id,
        )
        self.assertEqual(summary["loaded_batches"], 1)
        self.assertEqual(summary["records_fetched"], 1)
        self.assertEqual(summary["records_updated"], 1)
        self.assertEqual(summary["ok_count"], 1)
        self.assertEqual(summary["partial_count"], 0)
        self.assertEqual(summary["ixbrl_count"], 1)
        self.assertEqual(summary["pdf_count"], 0)
        self.assertEqual(summary["pdf_no_text_layer_count"], 0)
        self.assertEqual(summary["error_count"], 0)

        pending_path = run_stage_file(tmpdir.name, "financials", batch_id)
        loaded_path = Path(f"{pending_path}.loaded")
        self.assertFalse(pending_path.exists())
        self.assertTrue(loaded_path.exists())

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            enrichment_row = con.execute(
                """
                SELECT
                    avg_director_age,
                    min_director_age,
                    max_director_age,
                    directors_over_60,
                    all_directors_60_plus,
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
                    filing_age_months
                FROM company_enrichment
                WHERE company_number = '20000001'
                """
            ).fetchone()
            self.assertEqual(
                enrichment_row,
                (
                    63,
                    60,
                    66,
                    2,
                    True,
                    1750000.0,
                    "filed_accounts_ixbrl",
                    42,
                    date(2025, 1, 1),
                    date(2025, 12, 31),
                    600000.0,
                    120000.0,
                    100000.0,
                    400000.0,
                    550000.0,
                    950000.0,
                    700000.0,
                    250000.0,
                    "abc123",
                    "ixbrl",
                    3,
                ),
            )

            batch_row = con.execute(
                """
                SELECT status, records_fetched, records_updated, error_count
                FROM cqc_sync_batches
                WHERE batch_id = ?
                """,
                [batch_id],
            ).fetchone()
            self.assertEqual(batch_row, ("succeeded", 1, 1, 0))
        finally:
            con.close()

    def test_replay_financials_fetch_staging_file_rebuilds_final_staging(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_path = raw_filings_dir(tmpdir, "30000001") / "replay-one.ixbrl"
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_bytes(b"<html>fixture</html>")

            batch_id = "fetch-replay-batch"
            fetch_writer = StagingWriter(
                tmpdir,
                sync_type="financials_fetch",
                batch_id=batch_id,
            )
            try:
                fetch_writer.append(
                    FetchedFinancialRow(
                        company_number="30000001",
                        accounts_last_made_up="2026-01-31",
                        filing_id="replay-one",
                        filing_date="2026-03-12",
                        filing_made_up_date="2026-01-31",
                        paper_filed=False,
                        filing_format="ixbrl",
                        raw_path=str(raw_path),
                        parse_status=None,
                        parse_failure_reason=None,
                        fetched_at="2026-05-25T14:00:00Z",
                    )
                )
                fetch_writer.flush_and_fsync()
                manifest_path = fetch_writer.path
            finally:
                fetch_writer.close()

            parsed_facts = ParsedFinancialFacts(
                revenue=2400.0,
                employee_count=31,
                filing_period_start=date(2025, 1, 1),
                filing_period_end=date(2025, 12, 31),
                gross_profit=640.0,
                profit_before_tax=75.0,
                profit_after_tax=55.0,
                fixed_assets=700.0,
                current_assets=500.0,
                total_assets=1200.0,
                net_assets=810.0,
                net_current_assets=230.0,
                parse_status="ok",
                parse_failure_reason=None,
            )

            with patch(
                "ch_bulk.companies_house.financials_enricher._parse_ixbrl_bytes",
                return_value=parsed_facts,
            ) as parse_ixbrl:
                appended = _replay_financials_fetch_staging_file(
                    tmpdir,
                    path=manifest_path,
                )

            self.assertEqual(appended, 1)
            parse_ixbrl.assert_called_once_with(b"<html>fixture</html>")
            self.assertFalse(manifest_path.exists())
            self.assertTrue(Path(f"{manifest_path}.loaded").exists())

            staged_path = run_stage_file(tmpdir, "financials", batch_id)
            self.assertTrue(staged_path.exists())
            lines = staged_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 1)
            payload = json.loads(lines[0])
            self.assertEqual(payload["company_number"], "30000001")
            self.assertEqual(payload["filing_id"], "replay-one")
            self.assertEqual(payload["filing_format"], "ixbrl")
            self.assertEqual(payload["revenue"], 2400.0)
            self.assertEqual(payload["employee_count"], 31)
            self.assertEqual(payload["total_assets"], 1200.0)
            self.assertEqual(payload["parse_status"], "ok")

    def test_enrich_financials_runs_fetch_and_parse_pipeline_and_archives_manifest(self):
        tmpdir, db_path = self._create_db()

        con = duckdb.connect(str(db_path))
        try:
            _insert_company(
                con,
                company_number="30000011",
                company_name="Pipeline IXBRL Ltd",
                accounts_last_made_up=date(2026, 1, 31),
            )
            _insert_company(
                con,
                company_number="30000012",
                company_name="Pipeline PDF Ltd",
                accounts_last_made_up=date(2026, 2, 28),
            )
            _insert_match(con, company_number="30000011", provider_id="prov-11")
            _insert_match(con, company_number="30000012", provider_id="prov-12")
        finally:
            con.close()

        fake_client = MagicMock()
        fake_client.__enter__.return_value = fake_client
        fake_client.__exit__.return_value = False

        ixbrl_raw_path = _copy_sample_ixbrl(
            tmpdir.name,
            company_number="30000011",
            filename="ixbrl-one.ixbrl",
        )

        pdf_raw_path = raw_filings_dir(tmpdir.name, "30000012") / "pdf-one.pdf"
        pdf_raw_path.parent.mkdir(parents=True, exist_ok=True)
        pdf_raw_path.write_bytes(b"%PDF-1.1 fixture")

        def fake_fetch(
            client,
            *,
            target: FinancialTarget,
            data_dir: str | Path,
        ) -> FetchedFinancialWorkItem:
            if target.company_number == "30000011":
                return FetchedFinancialWorkItem(
                    row=FetchedFinancialRow(
                        company_number="30000011",
                        accounts_last_made_up="2026-01-31",
                        filing_id="ixbrl-one",
                        filing_date="2026-03-12",
                        filing_made_up_date="2026-01-31",
                        paper_filed=False,
                        filing_format="ixbrl",
                        raw_path=str(ixbrl_raw_path),
                        parse_status=None,
                        parse_failure_reason=None,
                        fetched_at="2026-05-25T14:15:00Z",
                    ),
                    http_status=200,
                    started_monotonic=0.0,
                )
            return FetchedFinancialWorkItem(
                row=FetchedFinancialRow(
                    company_number="30000012",
                    accounts_last_made_up="2026-02-28",
                    filing_id="pdf-one",
                    filing_date="2026-04-02",
                    filing_made_up_date="2026-02-28",
                    paper_filed=True,
                    filing_format="pdf",
                    raw_path=str(pdf_raw_path),
                    parse_status="pdf_no_text_layer",
                    parse_failure_reason="pdf_no_text_layer",
                    fetched_at="2026-05-25T14:16:00Z",
                ),
                http_status=200,
                started_monotonic=0.0,
            )

        with (
            patch(
                "ch_bulk.companies_house.financials_enricher.load_settings",
                return_value={"api_keys": {"companies_house": "fixture-key"}},
            ),
            patch(
                "ch_bulk.companies_house.financials_enricher.CompaniesHouseFinancialsClient",
                return_value=fake_client,
            ),
            patch(
                "ch_bulk.companies_house.financials_enricher._fetch_company_work_item",
                side_effect=fake_fetch,
            ),
            patch(
                "ch_bulk.companies_house.financials_enricher.PIPELINE_HEARTBEAT_INTERVAL_SECONDS",
                0.0,
            ),
        ):
            summary = enrich_financials(
                db_path,
                data_dir=tmpdir.name,
                mode="incremental",
                workers=2,
                batch_size=1,
            )

        self.assertEqual(summary["requested"], 2)
        self.assertEqual(summary["records_fetched"], 2)
        self.assertEqual(summary["records_updated"], 2)
        self.assertEqual(summary["ok_count"], 1)
        self.assertEqual(summary["pdf_no_text_layer_count"], 1)
        self.assertEqual(summary["error_count"], 0)

        batch_id = str(summary["batch_id"])
        fetch_manifest_path = run_stage_file(
            tmpdir.name,
            "financials_fetch",
            batch_id,
        )
        self.assertFalse(fetch_manifest_path.exists())
        loaded_fetch_manifest_path = Path(f"{fetch_manifest_path}.loaded")
        self.assertTrue(loaded_fetch_manifest_path.exists())

        manifest_rows = [
            json.loads(line)
            for line in loaded_fetch_manifest_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.assertEqual(
            [(row["company_number"], row["paper_filed"]) for row in manifest_rows],
            [("30000011", False), ("30000012", True)],
        )

        log_text = Path(summary["log_path"]).read_text(encoding="utf-8")
        self.assertIn("heartbeat fetched=", log_text)
        self.assertIn("input_queue=", log_text)
        self.assertIn("fetched_queue=", log_text)
        self.assertIn("result_queue=", log_text)

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            rows = con.execute(
                """
                SELECT
                    company_number,
                    revenue_source,
                    revenue,
                    employee_count,
                    filing_id,
                    filing_format
                FROM company_enrichment
                WHERE company_number IN ('30000011', '30000012')
                ORDER BY company_number
                """
            ).fetchall()
        finally:
            con.close()

        self.assertEqual(rows[0][0], "30000011")
        self.assertEqual(rows[0][1], "filed_accounts_ixbrl")
        self.assertIsNotNone(rows[0][2])
        self.assertIsNotNone(rows[0][3])
        self.assertEqual(rows[0][4], "ixbrl-one")
        self.assertEqual(rows[0][5], "ixbrl")
        self.assertEqual(
            rows[1],
            ("30000012", "pdf_no_text_layer", None, None, "pdf-one", "pdf"),
        )

    def test_enrich_financials_honours_parser_worker_setting_and_binds_all_queues(self):
        tmpdir, db_path = self._create_db()

        con = duckdb.connect(str(db_path))
        try:
            for index in range(12):
                company_number = f"3000002{index + 1}"
                _insert_company(
                    con,
                    company_number=company_number,
                    company_name=f"Queue Bound {index + 1} Ltd",
                    accounts_last_made_up=date(2026, 1, 31),
                )
                _insert_match(
                    con,
                    company_number=company_number,
                    provider_id=f"prov-queue-{index + 1}",
                )
        finally:
            con.close()

        fake_client = MagicMock()
        fake_client.__enter__.return_value = fake_client
        fake_client.__exit__.return_value = False

        def fake_fetch(
            client,
            *,
            target: FinancialTarget,
            data_dir: str | Path,
        ) -> FetchedFinancialWorkItem:
            return FetchedFinancialWorkItem(
                row=FetchedFinancialRow(
                    company_number=target.company_number,
                    accounts_last_made_up="2026-01-31",
                    filing_id=f"{target.company_number}-pdf",
                    filing_date="2026-03-12",
                    filing_made_up_date="2026-01-31",
                    paper_filed=True,
                    filing_format="pdf",
                    raw_path=None,
                    parse_status="pdf_no_text_layer",
                    parse_failure_reason="pdf_no_text_layer",
                    fetched_at="2026-05-25T15:45:00Z",
                ),
                http_status=200,
                started_monotonic=0.0,
            )

        created_queue_maxsizes: list[int] = []
        real_queue = std_queue.Queue

        def recording_queue(*args, **kwargs):
            created_queue_maxsizes.append(kwargs.get("maxsize", 0))
            return real_queue(*args, **kwargs)

        with (
            patch(
                "ch_bulk.companies_house.financials_enricher.load_settings",
                return_value={"api_keys": {"companies_house": "fixture-key"}},
            ),
            patch(
                "ch_bulk.companies_house.financials_enricher.CompaniesHouseFinancialsClient",
                return_value=fake_client,
            ),
            patch(
                "ch_bulk.companies_house.financials_enricher._fetch_company_work_item",
                side_effect=fake_fetch,
            ),
            patch(
                "ch_bulk.companies_house.financials_enricher.PIPELINE_HEARTBEAT_INTERVAL_SECONDS",
                0.0,
            ),
            patch(
                "ch_bulk.companies_house.financials_enricher.queue.Queue",
                side_effect=recording_queue,
            ),
        ):
            summary = enrich_financials(
                db_path,
                data_dir=tmpdir.name,
                mode="incremental",
                workers=1,
                parser_workers=3,
                batch_size=1,
            )

        self.assertEqual(summary["requested"], 12)
        self.assertEqual(summary["records_fetched"], 12)
        self.assertEqual(summary["records_updated"], 12)
        self.assertGreaterEqual(len(created_queue_maxsizes), 3)
        self.assertEqual(created_queue_maxsizes[:3], [10, 10, 10])

        log_text = Path(summary["log_path"]).read_text(encoding="utf-8")
        self.assertIn("parser_workers=3", log_text)
        self.assertIn("input_queue=", log_text)

    def test_enrich_financials_cli_passes_parser_worker_option(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir, db_path = make_test_workspace(tmpdir, db_name="fixture.duckdb")
            with patch(
                "ch_bulk.cli.ChBulk.enrich_financials",
                return_value={
                    "batch_id": "cli-batch",
                    "requested": 1,
                    "records_updated": 1,
                    "ok_count": 1,
                    "partial_count": 0,
                    "pdf_no_text_layer_count": 0,
                    "error_count": 0,
                },
            ) as mock_enrich:
                result = CLI_RUNNER.invoke(
                    cli_app,
                    [
                        "enrich-financials",
                        "--mode",
                        "list",
                        "--ids",
                        "30000011",
                        "--workers",
                        "2",
                        "--parser-workers",
                        "5",
                        "--db-path",
                        str(db_path),
                        "--data-dir",
                        str(data_dir),
                    ],
                )

        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        mock_enrich.assert_called_once_with(
            mode="list",
            ids=["30000011"],
            workers=2,
            parser_workers=5,
            batch_size=100,
        )

    def test_fetched_financial_row_defaults_missing_paper_filed_for_old_manifests(self):
        row = FetchedFinancialRow.from_json_line(
            json.dumps(
                {
                    "company_number": "30000100",
                    "accounts_last_made_up": "2026-01-31",
                    "filing_id": "legacy-one",
                    "filing_date": "2026-03-12",
                    "filing_made_up_date": "2026-01-31",
                    "filing_format": "pdf",
                    "raw_path": "/tmp/legacy.pdf",
                    "parse_status": "pdf_no_text_layer",
                    "parse_failure_reason": "pdf_no_text_layer",
                    "fetched_at": "2026-05-25T15:00:00Z",
                },
                sort_keys=True,
            )
        )

        self.assertIsNone(row.paper_filed)
        self.assertEqual(row.filing_format, "pdf")

    def test_replay_financials_fetch_staging_file_tolerates_truncated_existing_staged_tail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            batch_id = "torn-existing-batch"
            staged_writer = StagingWriter(
                tmpdir,
                sync_type="financials",
                batch_id=batch_id,
            )
            try:
                staged_writer.append(
                    StagedFinancialRow(
                        company_number="30000001",
                        filing_id="existing-one",
                        filing_date="2026-03-12",
                        filing_format="ixbrl",
                        filing_period_start="2025-01-01",
                        filing_period_end="2025-12-31",
                        revenue=10.0,
                        employee_count=2,
                        gross_profit=3.0,
                        profit_before_tax=1.0,
                        profit_after_tax=1.0,
                        fixed_assets=4.0,
                        current_assets=5.0,
                        total_assets=9.0,
                        net_assets=6.0,
                        net_current_assets=1.0,
                        filing_age_months=3,
                        parse_status="ok",
                        parse_failure_reason=None,
                        fetched_at="2026-05-25T15:05:00Z",
                    )
                )
                staged_writer.flush_and_fsync()
                staged_path = staged_writer.path
            finally:
                staged_writer.close()

            with open(staged_path, "a", encoding="utf-8") as handle:
                handle.write('{"company_number":"broken-tail"')

            raw_path = raw_filings_dir(tmpdir, "30000002") / "replay-two.ixbrl"
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_bytes(b"<html>fixture</html>")

            fetch_writer = StagingWriter(
                tmpdir,
                sync_type="financials_fetch",
                batch_id=batch_id,
            )
            try:
                fetch_writer.append(
                    FetchedFinancialRow(
                        company_number="30000002",
                        accounts_last_made_up="2026-01-31",
                        filing_id="replay-two",
                        filing_date="2026-03-12",
                        filing_made_up_date="2026-01-31",
                        paper_filed=False,
                        filing_format="ixbrl",
                        raw_path=str(raw_path),
                        parse_status=None,
                        parse_failure_reason=None,
                        fetched_at="2026-05-25T15:06:00Z",
                    )
                )
                fetch_writer.flush_and_fsync()
                manifest_path = fetch_writer.path
            finally:
                fetch_writer.close()

            parsed_facts = ParsedFinancialFacts(
                revenue=2400.0,
                employee_count=31,
                filing_period_start=date(2025, 1, 1),
                filing_period_end=date(2025, 12, 31),
                gross_profit=640.0,
                profit_before_tax=75.0,
                profit_after_tax=55.0,
                fixed_assets=700.0,
                current_assets=500.0,
                total_assets=1200.0,
                net_assets=810.0,
                net_current_assets=230.0,
                parse_status="ok",
                parse_failure_reason=None,
            )

            with patch(
                "ch_bulk.companies_house.financials_enricher._parse_ixbrl_bytes",
                return_value=parsed_facts,
            ):
                appended = _replay_financials_fetch_staging_file(
                    tmpdir,
                    path=manifest_path,
                )

            self.assertEqual(appended, 1)
            lines = staged_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 2)
            payloads = [json.loads(line) for line in lines]
            self.assertEqual(
                [payload["company_number"] for payload in payloads],
                ["30000001", "30000002"],
            )

    def test_load_financials_staging_tolerates_truncated_trailing_line(self):
        tmpdir, db_path = self._create_db()

        con = duckdb.connect(str(db_path))
        try:
            _insert_company(
                con,
                company_number="30000021",
                company_name="Truncated Tail Ltd",
                accounts_last_made_up=date(2026, 1, 31),
            )
            batch_id = insert_sync_batch(
                con,
                sync_type="financials",
                mode="incremental",
            )
        finally:
            con.close()

        writer = StagingWriter(
            tmpdir.name,
            sync_type="financials",
            batch_id=batch_id,
        )
        try:
            writer.append(
                StagedFinancialRow(
                    company_number="30000021",
                    filing_id="tail-one",
                    filing_date="2026-03-12",
                    filing_format="ixbrl",
                    filing_period_start="2025-01-01",
                    filing_period_end="2025-12-31",
                    revenue=100.0,
                    employee_count=9,
                    gross_profit=50.0,
                    profit_before_tax=20.0,
                    profit_after_tax=18.0,
                    fixed_assets=40.0,
                    current_assets=60.0,
                    total_assets=100.0,
                    net_assets=70.0,
                    net_current_assets=20.0,
                    filing_age_months=3,
                    parse_status="ok",
                    parse_failure_reason=None,
                    fetched_at="2026-05-25T15:10:00Z",
                )
            )
            writer.flush_and_fsync()
            pending_path = writer.path
        finally:
            writer.close()

        with open(pending_path, "a", encoding="utf-8") as handle:
            handle.write('{"company_number":"broken-tail"')

        summary = load_financials_staging(
            tmpdir.name,
            db_path,
            batch_id=batch_id,
        )
        self.assertEqual(summary["loaded_batches"], 1)
        self.assertEqual(summary["records_fetched"], 1)
        self.assertFalse(pending_path.exists())
        self.assertTrue(Path(f"{pending_path}.loaded").exists())

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            row = con.execute(
                """
                SELECT revenue, revenue_source, employee_count
                FROM company_enrichment
                WHERE company_number = '30000021'
                """
            ).fetchone()
        finally:
            con.close()
        self.assertEqual(row, (100.0, "filed_accounts_ixbrl", 9))

    def test_enrich_financials_fsyncs_fetch_manifest_per_fetched_row(self):
        tmpdir, db_path = self._create_db()

        con = duckdb.connect(str(db_path))
        try:
            _insert_company(
                con,
                company_number="30000031",
                company_name="Fetch Sync One Ltd",
                accounts_last_made_up=date(2026, 1, 31),
            )
            _insert_company(
                con,
                company_number="30000032",
                company_name="Fetch Sync Two Ltd",
                accounts_last_made_up=date(2026, 2, 28),
            )
            _insert_match(con, company_number="30000031", provider_id="prov-31")
            _insert_match(con, company_number="30000032", provider_id="prov-32")
        finally:
            con.close()

        fake_client = MagicMock()
        fake_client.__enter__.return_value = fake_client
        fake_client.__exit__.return_value = False

        def fake_fetch(
            client,
            *,
            target: FinancialTarget,
            data_dir: str | Path,
        ) -> FetchedFinancialWorkItem:
            return FetchedFinancialWorkItem(
                row=FetchedFinancialRow(
                    company_number=target.company_number,
                    accounts_last_made_up="2026-01-31",
                    filing_id=f"{target.company_number}-pdf",
                    filing_date="2026-03-12",
                    filing_made_up_date="2026-01-31",
                    paper_filed=True,
                    filing_format="pdf",
                    raw_path=None,
                    parse_status="pdf_no_text_layer",
                    parse_failure_reason="pdf_no_text_layer",
                    fetched_at="2026-05-25T15:20:00Z",
                ),
                http_status=200,
                started_monotonic=0.0,
            )

        fetch_flush_calls: list[str] = []
        original_flush_and_fsync = StagingWriter.flush_and_fsync

        def counting_flush_and_fsync(writer_self: StagingWriter) -> None:
            if writer_self.path.parent.name == "financials_fetch":
                fetch_flush_calls.append(str(writer_self.path))
            original_flush_and_fsync(writer_self)

        with (
            patch(
                "ch_bulk.companies_house.financials_enricher.load_settings",
                return_value={"api_keys": {"companies_house": "fixture-key"}},
            ),
            patch(
                "ch_bulk.companies_house.financials_enricher.CompaniesHouseFinancialsClient",
                return_value=fake_client,
            ),
            patch(
                "ch_bulk.companies_house.financials_enricher._fetch_company_work_item",
                side_effect=fake_fetch,
            ),
            patch.object(
                StagingWriter,
                "flush_and_fsync",
                new=counting_flush_and_fsync,
            ),
        ):
            summary = enrich_financials(
                db_path,
                data_dir=tmpdir.name,
                mode="incremental",
                workers=2,
                batch_size=100,
            )

        self.assertEqual(summary["records_fetched"], 2)
        self.assertGreaterEqual(len(fetch_flush_calls), 3)

    def test_enrich_financials_sigterm_drains_and_checkpoints_pending_work(self):
        tmpdir, db_path = self._create_db()

        con = duckdb.connect(str(db_path))
        try:
            _insert_company(
                con,
                company_number="30000041",
                company_name="Signal Drain One Ltd",
                accounts_last_made_up=date(2026, 1, 31),
            )
            _insert_company(
                con,
                company_number="30000042",
                company_name="Signal Drain Two Ltd",
                accounts_last_made_up=date(2026, 2, 28),
            )
            _insert_match(con, company_number="30000041", provider_id="prov-41")
            _insert_match(con, company_number="30000042", provider_id="prov-42")
        finally:
            con.close()

        ixbrl_raw_path = _copy_sample_ixbrl(
            tmpdir.name,
            company_number="30000041",
            filename="signal-one.ixbrl",
        )

        fake_client = MagicMock()
        fake_client.__enter__.return_value = fake_client
        fake_client.__exit__.return_value = False
        fetch_calls: list[str] = []

        def fake_fetch(
            client,
            *,
            target: FinancialTarget,
            data_dir: str | Path,
        ) -> FetchedFinancialWorkItem:
            fetch_calls.append(target.company_number)
            if target.company_number == "30000041":
                time.sleep(1.0)
                return FetchedFinancialWorkItem(
                    row=FetchedFinancialRow(
                        company_number="30000041",
                        accounts_last_made_up="2026-01-31",
                        filing_id="signal-one",
                        filing_date="2026-03-12",
                        filing_made_up_date="2026-01-31",
                        paper_filed=False,
                        filing_format="ixbrl",
                        raw_path=str(ixbrl_raw_path),
                        parse_status=None,
                        parse_failure_reason=None,
                        fetched_at="2026-05-25T15:30:00Z",
                    ),
                    http_status=200,
                    started_monotonic=0.0,
                )
            return FetchedFinancialWorkItem(
                row=FetchedFinancialRow(
                    company_number="30000042",
                    accounts_last_made_up="2026-02-28",
                    filing_id="signal-two",
                    filing_date="2026-04-02",
                    filing_made_up_date="2026-02-28",
                    paper_filed=True,
                    filing_format="pdf",
                    raw_path=None,
                    parse_status="pdf_no_text_layer",
                    parse_failure_reason="pdf_no_text_layer",
                    fetched_at="2026-05-25T15:31:00Z",
                ),
                http_status=200,
                started_monotonic=0.0,
            )

        timer = threading.Timer(0.5, lambda: os.kill(os.getpid(), signal.SIGTERM))
        timer.start()
        try:
            with (
                patch(
                    "ch_bulk.companies_house.financials_enricher.load_settings",
                    return_value={"api_keys": {"companies_house": "fixture-key"}},
                ),
                patch(
                    "ch_bulk.companies_house.financials_enricher.CompaniesHouseFinancialsClient",
                    return_value=fake_client,
                ),
                patch(
                    "ch_bulk.companies_house.financials_enricher._fetch_company_work_item",
                    side_effect=fake_fetch,
                ),
            ):
                with self.assertRaises(KeyboardInterrupt):
                    enrich_financials(
                        db_path,
                        data_dir=tmpdir.name,
                        mode="incremental",
                        workers=1,
                        batch_size=100,
                    )
        finally:
            timer.cancel()
            timer.join(timeout=1.0)

        self.assertEqual(fetch_calls, ["30000041"])
        pending_fetch_files = sorted(runs_dir(tmpdir.name, "financials_fetch").glob("*.jsonl"))
        pending_final_files = sorted(runs_dir(tmpdir.name, "financials").glob("*.jsonl"))
        self.assertEqual(len(pending_fetch_files), 1)
        self.assertEqual(len(pending_final_files), 1)
        self.assertEqual(
            len(
                pending_fetch_files[0].read_text(encoding="utf-8").splitlines()
            ),
            1,
        )
        self.assertEqual(
            len(
                pending_final_files[0].read_text(encoding="utf-8").splitlines()
            ),
            1,
        )

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            batch_row = con.execute(
                """
                SELECT status, records_fetched, records_updated
                FROM cqc_sync_batches
                WHERE sync_type = 'financials'
                ORDER BY started_at DESC
                LIMIT 1
                """
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(batch_row, ("failed", 1, 0))

    def test_load_financials_staging_replays_running_batch_and_marks_it_failed(self):
        tmpdir, db_path = self._create_db()

        con = duckdb.connect(str(db_path))
        try:
            _insert_company(
                con,
                company_number="20000002",
                company_name="Recovered Batch Ltd",
                accounts_last_made_up=date(2026, 2, 28),
            )
            batch_id = insert_sync_batch(
                con,
                sync_type="financials",
                mode="incremental",
            )
        finally:
            con.close()

        writer = StagingWriter(
            tmpdir.name,
            sync_type="financials",
            batch_id=batch_id,
        )
        try:
            writer.append(
                StagedFinancialRow(
                    company_number="20000002",
                    filing_id="def456",
                    filing_date="2026-04-01",
                    filing_format="pdf",
                    filing_period_start=None,
                    filing_period_end=None,
                    revenue=None,
                    employee_count=9,
                    gross_profit=None,
                    profit_before_tax=None,
                    profit_after_tax=None,
                    fixed_assets=None,
                    current_assets=None,
                    total_assets=None,
                    net_assets=None,
                    net_current_assets=None,
                    filing_age_months=4,
                    parse_status="pdf_parse_partial",
                    parse_failure_reason=(
                        "revenue_missing,gross_profit_missing,"
                        "profit_before_tax_missing,profit_after_tax_missing,"
                        "fixed_assets_missing,current_assets_missing,"
                        "total_assets_missing,net_assets_missing,"
                        "net_current_assets_missing"
                    ),
                    fetched_at="2026-05-25T13:00:00Z",
                )
            )
            writer.flush_and_fsync()
        finally:
            writer.close()

        summary = load_financials_staging(tmpdir.name, db_path)
        self.assertEqual(summary["loaded_batches"], 1)
        self.assertEqual(summary["records_fetched"], 1)
        self.assertEqual(summary["records_updated"], 1)
        self.assertEqual(summary["partial_count"], 1)
        self.assertEqual(summary["ixbrl_count"], 0)
        self.assertEqual(summary["pdf_count"], 1)
        self.assertEqual(summary["pdf_no_text_layer_count"], 0)
        self.assertEqual(summary["error_count"], 1)
        self.assertEqual(summary["stale_failed_batches"], 0)

        pending_path = run_stage_file(tmpdir.name, "financials", batch_id)
        self.assertFalse(pending_path.exists())
        self.assertTrue(Path(f"{pending_path}.loaded").exists())

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            enrichment_row = con.execute(
                """
                SELECT revenue, revenue_source, employee_count, filing_id, filing_format
                FROM company_enrichment
                WHERE company_number = '20000002'
                """
            ).fetchone()
            self.assertEqual(
                enrichment_row,
                (None, None, 9, "def456", "pdf"),
            )

            batch_row = con.execute(
                """
                SELECT status, records_fetched, records_updated, error_count
                FROM cqc_sync_batches
                WHERE batch_id = ?
                """,
                [batch_id],
            ).fetchone()
            self.assertEqual(batch_row, ("failed", 1, 1, 1))
        finally:
            con.close()

    def test_load_financials_staging_counts_pdf_no_text_layer_as_terminal_no_data(self):
        tmpdir, db_path = self._create_db()

        con = duckdb.connect(str(db_path))
        try:
            _insert_company(
                con,
                company_number="20000003",
                company_name="Image PDF Ltd",
                accounts_last_made_up=date(2026, 2, 28),
            )
            batch_id = insert_sync_batch(
                con,
                sync_type="financials",
                mode="incremental",
            )
        finally:
            con.close()

        writer = StagingWriter(
            tmpdir.name,
            sync_type="financials",
            batch_id=batch_id,
        )
        try:
            writer.append(
                StagedFinancialRow(
                    company_number="20000003",
                    filing_id="ghi789",
                    filing_date="2026-04-30",
                    filing_format="pdf",
                    filing_period_start=None,
                    filing_period_end=None,
                    revenue=None,
                    employee_count=None,
                    gross_profit=None,
                    profit_before_tax=None,
                    profit_after_tax=None,
                    fixed_assets=None,
                    current_assets=None,
                    total_assets=None,
                    net_assets=None,
                    net_current_assets=None,
                    filing_age_months=1,
                    parse_status="pdf_no_text_layer",
                    parse_failure_reason="pdf_no_text_layer",
                    fetched_at="2026-05-25T13:30:00Z",
                )
            )
            writer.flush_and_fsync()
        finally:
            writer.close()

        summary = load_financials_staging(
            tmpdir.name,
            db_path,
            batch_id=batch_id,
        )
        self.assertEqual(summary["loaded_batches"], 1)
        self.assertEqual(summary["records_fetched"], 1)
        self.assertEqual(summary["records_updated"], 1)
        self.assertEqual(summary["pdf_count"], 1)
        self.assertEqual(summary["pdf_no_text_layer_count"], 1)
        self.assertEqual(summary["error_count"], 0)

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            enrichment_row = con.execute(
                """
                SELECT revenue, revenue_source, filing_id, filing_format
                FROM company_enrichment
                WHERE company_number = '20000003'
                """
            ).fetchone()
            self.assertEqual(
                enrichment_row,
                (None, "pdf_no_text_layer", "ghi789", "pdf"),
            )
        finally:
            con.close()

    def test_load_financials_staging_marks_partial_no_revenue_as_terminal(self):
        tmpdir, db_path = self._create_db()

        con = duckdb.connect(str(db_path))
        try:
            _insert_company(
                con,
                company_number="20000004",
                company_name="Filleted Accounts Ltd",
                accounts_last_made_up=date(2026, 2, 28),
            )
            batch_id = insert_sync_batch(
                con,
                sync_type="financials",
                mode="incremental",
            )
        finally:
            con.close()

        writer = StagingWriter(
            tmpdir.name,
            sync_type="financials",
            batch_id=batch_id,
        )
        try:
            writer.append(
                StagedFinancialRow(
                    company_number="20000004",
                    filing_id="partial-one",
                    filing_date="2026-04-30",
                    filing_format="ixbrl",
                    filing_period_start="2025-03-01",
                    filing_period_end="2026-02-28",
                    revenue=None,
                    employee_count=27,
                    gross_profit=None,
                    profit_before_tax=None,
                    profit_after_tax=None,
                    fixed_assets=1705232.0,
                    current_assets=953243.0,
                    total_assets=2658475.0,
                    net_assets=1382731.0,
                    net_current_assets=640538.0,
                    filing_age_months=1,
                    parse_status="partial",
                    parse_failure_reason=(
                        "revenue_missing,gross_profit_missing,"
                        "profit_before_tax_missing,profit_after_tax_missing"
                    ),
                    fetched_at="2026-05-25T13:45:00Z",
                    profit_loss_exempt=True,
                )
            )
            writer.flush_and_fsync()
        finally:
            writer.close()

        summary = load_financials_staging(
            tmpdir.name,
            db_path,
            batch_id=batch_id,
        )
        self.assertEqual(summary["loaded_batches"], 1)
        self.assertEqual(summary["records_fetched"], 1)
        self.assertEqual(summary["records_updated"], 1)
        self.assertEqual(summary["partial_count"], 1)
        self.assertEqual(summary["error_count"], 0)

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            enrichment_row = con.execute(
                """
                SELECT revenue, revenue_source, employee_count, filing_id, filing_format
                FROM company_enrichment
                WHERE company_number = '20000004'
                """
            ).fetchone()
            self.assertEqual(
                enrichment_row,
                (None, "partial_no_revenue", 27, "partial-one", "ixbrl"),
            )
        finally:
            con.close()

if __name__ == "__main__":
    unittest.main()
