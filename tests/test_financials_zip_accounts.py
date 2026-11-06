"""Accounts that Companies House serves only as a zip package (e.g. CIC filings)."""

from __future__ import annotations

import io
import tempfile
import unittest
import zipfile
from datetime import date
from unittest.mock import Mock, call

import requests

from ch_bulk.companies_house.financials_contracts import (
    IXBRL_RESOURCE,
    PDF_RESOURCE,
    FinancialTarget,
)
from ch_bulk.companies_house.financials_enricher import _process_company
from ch_bulk.companies_house.financials_parsers import _parse_ixbrl_bytes
from ch_bulk.core.paths import raw_filings_dir
from tests.support.paths import fixture_path

METADATA_URL = "https://document-api.company-information.service.gov.uk/document/zip-one"
ACCOUNTS_XHTML = fixture_path("financials", "ixbrl", "07545840", "sample.ixbrl").read_bytes()
CIC_REPORT_XHTML = b"<html><body>CIC34 community interest report</body></html>"


class _HTTPResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code


def _not_acceptable() -> requests.HTTPError:
    return requests.HTTPError(response=_HTTPResponse(406))


def _cic_zip(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, content in members.items():
            archive.writestr(name, content)
    return buffer.getvalue()


def _client(zip_response) -> Mock:
    client = Mock()
    client.get_filing_history.return_value = {
        "items": [
            {
                "date": "2026-02-23",
                "type": "AA",
                "description": "accounts-with-accounts-type-micro-entity",
                "transaction_id": "zip-one",
                "paper_filed": False,
                "links": {"document_metadata": METADATA_URL},
                "description_values": {"made_up_date": "2025-05-31"},
            }
        ]
    }
    client.download_document_content.side_effect = [
        _not_acceptable(),
        _not_acceptable(),
        zip_response,
    ]
    return client


class ZipOnlyAccountsTests(unittest.TestCase):
    def _process(self, client, tmpdir):
        return _process_company(
            client,
            target=FinancialTarget("15117422", date(2025, 5, 31)),
            data_dir=tmpdir,
        )

    def test_accounts_inside_zip_are_parsed_as_ixbrl(self):
        client = _client(_cic_zip({
            "CIC-15117422/accounts/": b"",
            "CIC-15117422/accounts/financialStatement.xhtml": ACCOUNTS_XHTML,
            "CIC-15117422/cic34/": b"",
            "CIC-15117422/cic34/cicReport.xhtml": CIC_REPORT_XHTML,
        }))
        expected = _parse_ixbrl_bytes(ACCOUNTS_XHTML)

        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._process(client, tmpdir)
            saved = raw_filings_dir(tmpdir, "15117422") / "zip-one.ixbrl"
            self.assertEqual(saved.read_bytes(), ACCOUNTS_XHTML)

        self.assertEqual(
            client.download_document_content.call_args_list,
            [
                call(document_metadata_url=METADATA_URL, accept=IXBRL_RESOURCE),
                call(document_metadata_url=METADATA_URL, accept=PDF_RESOURCE),
                call(document_metadata_url=METADATA_URL, accept="application/zip"),
            ],
        )
        self.assertEqual(result.row.filing_format, "ixbrl")
        self.assertEqual(result.row.filing_id, "zip-one")
        self.assertEqual(result.row.parse_status, expected.parse_status)
        self.assertEqual(result.row.filing_period_end, expected.filing_period_end.isoformat())
        self.assertEqual(result.row.net_assets, expected.net_assets)
        self.assertEqual(result.row.employee_count, expected.employee_count)

    def test_saved_zip_accounts_are_reused_without_downloading(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            first = _client(_cic_zip({"CIC-1/accounts/financialStatement.xhtml": ACCOUNTS_XHTML}))
            self._process(first, tmpdir)

            again = _client(AssertionError("must not download"))
            again.download_document_content.side_effect = AssertionError("must not download")
            result = self._process(again, tmpdir)

        again.download_document_content.assert_not_called()
        self.assertEqual(result.row.filing_format, "ixbrl")

    def test_zip_without_accounts_is_a_download_error(self):
        client = _client(_cic_zip({"CIC-1/cic34/cicReport.xhtml": CIC_REPORT_XHTML}))
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._process(client, tmpdir)
            self.assertEqual(list(raw_filings_dir(tmpdir, "15117422").glob("*")), [])

        self.assertEqual(result.row.parse_status, "document_download_error")
        self.assertEqual(result.row.parse_failure_reason, "zip_without_accounts")

    def test_zip_also_refused_is_a_download_error(self):
        client = _client(_not_acceptable())
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._process(client, tmpdir)

        self.assertEqual(result.row.parse_status, "document_download_error")
        self.assertEqual(result.row.parse_failure_reason, "http_status_406")


if __name__ == "__main__":
    unittest.main()
