"""Regression tests for AUDIT-05.

Retry-After must accept HTTP-date values and be capped, and a transient
transport failure must be retried instead of escaping the client.
"""

from __future__ import annotations

import email.utils
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import httpx
import requests

from ch_bulk.companies_house.ch_enricher import CompaniesHouseClient
from ch_bulk.companies_house.financials_fetch import CompaniesHouseFinancialsClient
from ch_bulk.core.http_retry import parse_retry_after
from ch_bulk.cqc.api_client import CQCAPIClient


class ParseRetryAfterTests(unittest.TestCase):
    def test_integer_seconds(self) -> None:
        self.assertEqual(parse_retry_after("120"), 120.0)
        self.assertEqual(parse_retry_after({"Retry-After": "30"}), 30.0)

    def test_http_date_value(self) -> None:
        now = datetime.now(timezone.utc)
        future = email.utils.format_datetime(now + timedelta(seconds=90))
        parsed = parse_retry_after(future, now=now)
        self.assertAlmostEqual(parsed, 90.0, delta=1.0)

    def test_past_http_date_means_no_wait(self) -> None:
        now = datetime.now(timezone.utc)
        past = email.utils.format_datetime(now - timedelta(seconds=60))
        self.assertEqual(parse_retry_after(past, now=now), 0.0)

    def test_garbage_and_missing_fall_back_to_default(self) -> None:
        self.assertEqual(parse_retry_after("not-a-date"), 60.0)
        self.assertEqual(parse_retry_after(None), 60.0)
        self.assertEqual(parse_retry_after(""), 60.0)

    def test_large_values_are_capped(self) -> None:
        self.assertEqual(parse_retry_after("3600"), 300.0)
        self.assertEqual(parse_retry_after("-5"), 0.0)


class _NoopThrottle:
    def wait(self, **_kwargs: object) -> None:
        return None


def _httpx_client(
    handler, *, base_url: str = "https://example.test"
) -> httpx.Client:
    return httpx.Client(base_url=base_url, transport=httpx.MockTransport(handler))


class CqcTransportRetryTests(unittest.TestCase):
    def test_transport_error_is_retried(self) -> None:
        calls: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.url.path)
            if len(calls) == 1:
                raise httpx.ConnectError("boom", request=request)
            return httpx.Response(200, json={"providerId": "abc"})

        with tempfile.TemporaryDirectory() as tmpdir:
            client = CQCAPIClient(tmpdir, api_key="test-key")
            client.close()
            client._client = _httpx_client(handler)
            self.addCleanup(client.close)
            with patch("ch_bulk.cqc.api_client.cancellable_sleep"):
                result = client.get_provider("abc")
            self.assertEqual(result.status_code, 200)
            self.assertEqual(len(calls), 2)

    def test_persistent_transport_error_raises_after_five_attempts(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("boom", request=request)

        with tempfile.TemporaryDirectory() as tmpdir:
            client = CQCAPIClient(tmpdir, api_key="test-key")
            client.close()
            client._client = _httpx_client(handler)
            self.addCleanup(client.close)
            with patch("ch_bulk.cqc.api_client.cancellable_sleep") as sleep_mock:
                with self.assertRaises(RuntimeError):
                    client.get_provider("abc")
            self.assertEqual(sleep_mock.call_count, 5)

    def test_http_date_retry_after_is_parsed(self) -> None:
        past = email.utils.format_datetime(
            datetime.now(timezone.utc) - timedelta(seconds=60)
        )
        calls: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.url.path)
            if len(calls) == 1:
                return httpx.Response(429, headers={"Retry-After": past}, json={})
            return httpx.Response(200, json={"providerId": "abc"})

        with tempfile.TemporaryDirectory() as tmpdir:
            client = CQCAPIClient(tmpdir, api_key="test-key")
            client.close()
            client._client = _httpx_client(handler)
            self.addCleanup(client.close)
            with patch("ch_bulk.cqc.api_client.cancellable_sleep") as sleep_mock:
                result = client.get_provider("abc")
            self.assertEqual(result.status_code, 200)
            self.assertEqual(sleep_mock.call_args_list[0].args[1], 0.0)


class CompaniesHouseTransportRetryTests(unittest.TestCase):
    def test_transport_error_is_retried(self) -> None:
        calls: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.url.path)
            if len(calls) == 1:
                raise httpx.ConnectError("boom", request=request)
            return httpx.Response(200, json={"items": [], "total_results": 0})

        client = CompaniesHouseClient(api_key="test-key")
        client.close()
        client._client = _httpx_client(handler)
        self.addCleanup(client.close)
        with patch("ch_bulk.companies_house.ch_enricher.cancellable_sleep"):
            officers = client.get_officers("12345678")
        self.assertEqual(officers, [])
        self.assertEqual(len(calls), 2)


class OfficersPaginationTests(unittest.TestCase):
    def test_officers_are_paginated_until_total_results(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            start = int(request.url.params["start_index"])
            total = 150
            items = [
                {"officer_role": "director", "name": f"Director {start + i}"}
                for i in range(min(100, total - start))
            ]
            return httpx.Response(
                200, json={"items": items, "total_results": total}
            )

        client = CompaniesHouseClient(api_key="test-key")
        client.close()
        client._client = _httpx_client(handler)
        self.addCleanup(client.close)
        with patch("ch_bulk.companies_house.ch_enricher.cancellable_sleep"):
            officers = client.get_officers("12345678")
        self.assertEqual(len(officers), 150)
        self.assertEqual(officers[-1]["name"], "Director 149")


class FinancialsTransportRetryTests(unittest.TestCase):
    def test_transport_error_is_retried(self) -> None:
        response = Mock()
        response.status_code = 200
        response.headers = {}
        response.json.return_value = {"items": [], "total_results": 0}
        response.raise_for_status.return_value = None
        session = Mock()
        session.get.side_effect = [
            requests.exceptions.ConnectionError("boom"),
            response,
        ]
        client = CompaniesHouseFinancialsClient(
            api_key="test-key", throttle=_NoopThrottle()  # type: ignore[arg-type]
        )
        client._session = session
        self.addCleanup(client.close)
        with patch(
            "ch_bulk.companies_house.financials_fetch.cancellable_sleep"
        ):
            history = client.get_filing_history("12345678")
        self.assertEqual(history, {"items": [], "total_results": 0})
        self.assertEqual(session.get.call_count, 2)


class FilingHistoryPaginationTests(unittest.TestCase):
    def test_latest_annual_accounts_found_on_second_page(self) -> None:
        from ch_bulk.companies_house.financials_contracts import FinancialTarget
        from ch_bulk.companies_house.financials_fetch import _fetch_company_work_item

        non_annual = {
            "date": "2025-03-28",
            "type": "CS01",
            "description": "confirmation-statement",
            "transaction_id": "not-accounts",
            "paper_filed": False,
            "links": {"document_metadata": "https://example.test/doc/1"},
        }
        annual = {
            "date": "2025-03-28",
            "type": "AA",
            "description": "accounts-with-accounts-type-total-exemption-full",
            "transaction_id": "accounts-page-2",
            "paper_filed": False,
            "links": {"document_metadata": "https://example.test/doc/2"},
            "description_values": {"made_up_date": "2024-10-31"},
        }
        client = Mock()
        client.get_filing_history.side_effect = [
            # Filing history reports its size as total_count (checked against
            # the live API); officers use total_results.
            {"items": [non_annual], "total_count": 101},
            {"items": [annual], "total_count": 101},
        ]
        client.download_document_content.return_value = b"<html></html>"
        with tempfile.TemporaryDirectory() as tmpdir:
            work_item = _fetch_company_work_item(
                client,
                target=FinancialTarget("12345678", None),
                data_dir=tmpdir,
            )
        self.assertEqual(work_item.row.filing_id, "accounts-page-2")
        starts = [
            call.kwargs["start_index"]
            for call in client.get_filing_history.call_args_list
        ]
        self.assertEqual(starts, [0, 1])

    def test_single_page_without_total_count_is_not_refetched(self) -> None:
        from ch_bulk.companies_house.financials_contracts import FinancialTarget
        from ch_bulk.companies_house.financials_fetch import _fetch_company_work_item

        client = Mock()
        client.get_filing_history.return_value = {"items": []}
        with tempfile.TemporaryDirectory() as tmpdir:
            work_item = _fetch_company_work_item(
                client,
                target=FinancialTarget("12345678", None),
                data_dir=tmpdir,
            )
        self.assertEqual(work_item.row.parse_status, "no_filing")
        self.assertEqual(client.get_filing_history.call_count, 1)


if __name__ == "__main__":
    unittest.main()
