"""Focused tests for website classification staging and parsing."""

from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb
import requests

from ch_bulk.bootstrap import ensure_pipeline_schema
from ch_bulk.classifier import (
    StagedClassification,
    WebsiteClassifier,
    _extract_json_object,
    insert_classification_batch,
    load_classification_staging,
)

try:
    import resource
except ImportError:  # pragma: no cover - unavailable on Windows
    resource = None


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
            sic_code_4 TEXT
        )
        """
    )


def _insert_company_and_site(
    con: duckdb.DuckDBPyConnection,
    *,
    company_number: str,
    company_name: str,
    url: str,
) -> None:
    con.execute(
        """
        INSERT INTO companies VALUES
            (?, ?, 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL)
        """,
        [company_number, company_name],
    )
    con.execute(
        """
        INSERT INTO company_websites (
            company_number,
            discovered_via,
            url,
            is_primary,
            discovered_at
        )
        VALUES (
            ?,
            'manual',
            ?,
            TRUE,
            TIMESTAMP '2026-05-24 12:00:00'
        )
        """,
        [company_number, url],
    )


class DummyResponse:
    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text
        self.ok = 200 <= status_code < 400


class DummyLLMResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


def _peak_rss_mb() -> float | None:
    if resource is None:
        return None
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return rss / (1024 * 1024)
    return rss / 1024


class WebsiteClassifierTests(unittest.TestCase):
    def _create_db(self) -> tuple[Path, tempfile.TemporaryDirectory[str]]:
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        db_path = Path(tmpdir.name) / "classifier.duckdb"
        con = duckdb.connect(str(db_path))
        try:
            _create_companies_table(con)
            ensure_pipeline_schema(con)
            _insert_company_and_site(
                con,
                company_number="12345678",
                company_name="Example Homecare Ltd",
                url="https://www.example.com",
            )
        finally:
            con.close()
        return db_path, tmpdir

    def _create_loader_db(
        self,
    ) -> tuple[Path, tempfile.TemporaryDirectory[str], str]:
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        db_path = Path(tmpdir.name) / "classification-load.duckdb"
        con = duckdb.connect(str(db_path))
        try:
            ensure_pipeline_schema(con)
            batch_id = insert_classification_batch(
                con,
                classifier="llm:test",
                source_type="website",
                input_count=0,
                model_version="fixture-model",
            )
        finally:
            con.close()
        return db_path, tmpdir, batch_id

    def _write_staged_rows(
        self,
        tmpdir: str,
        *,
        batch_id: str,
        rows: list[dict[str, object]],
    ) -> Path:
        stage_dir = Path(tmpdir) / "staging"
        stage_dir.mkdir(parents=True, exist_ok=True)
        path = stage_dir / f"classifications_{batch_id}.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, sort_keys=True) + "\n")
        return path

    def test_classify_handles_multi_page_www_fallback_and_json_parse(self):
        db_path, tmpdir = self._create_db()
        html = """
        <html><body>
        <h1>Home care services</h1>
        <p>We provide visiting care and live-in care in clients' own homes across the region.</p>
        <p>Our carers support people at home every day.</p>
        <p>Families choose us for medication support, companionship, personal care, meal preparation, overnight support, and tailored care plans that keep people safe and independent at home.</p>
        </body></html>
        """

        def fake_get(url, headers=None, timeout=None):
            if url == "https://www.example.com/":
                raise requests.exceptions.ConnectTimeout("timed out")
            if url in {
                "https://example.com/",
                "https://www.example.com/about",
                "https://example.com/about",
            }:
                return DummyResponse(200, html)
            return DummyResponse(404, "<html><body>missing</body></html>")

        llm_payload = {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "verdict": "Majority domiciliary",
                                "evidence": "visiting care and live-in care in clients' own homes",
                            }
                        )
                    }
                }
            ]
        }

        with patch("ch_bulk.classifier.requests.Session.get", side_effect=fake_get):
            with patch(
                "ch_bulk.classifier.httpx.Client.post",
                return_value=DummyLLMResponse(llm_payload),
            ):
                summary = WebsiteClassifier(tmpdir.name, db_path).classify(
                    mode="list",
                    ids=["12345678"],
                    batch_size=1,
                )

        self.assertEqual(summary["records_updated"], 1)
        self.assertEqual(summary["unable_count"], 0)

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            row = con.execute(
                """
                SELECT verdict, verdict_reason, evidence_quote, source_url, classifier
                FROM classifications
                WHERE company_number = '12345678'
                """
            ).fetchone()
            self.assertEqual(row[0], "Majority domiciliary")
            self.assertEqual(row[1], None)
            self.assertIn("clients' own homes", row[2])
            self.assertEqual(row[3], "https://www.example.com")
            self.assertEqual(row[4], "llm:Qwen2.5-14B-Instruct-Q4_K_M.gguf")

            batch = con.execute(
                """
                SELECT status, input_count, classified_count, unable_count, error_count
                FROM classification_batches
                ORDER BY started_at DESC
                LIMIT 1
                """
            ).fetchone()
            self.assertEqual(batch, ("succeeded", 1, 1, 0, 0))
        finally:
            con.close()

    def test_classify_uses_playwright_retry_for_tiny_bodies(self):
        db_path, tmpdir = self._create_db()
        tiny_html = "<html><body><p>Homecare</p></body></html>"
        rich_html = """
        <html><body>
        <p>Supported living services help adults with learning disabilities in their own tenancy.</p>
        <p>Our supported living teams provide on-site and visiting support.</p>
        <p>We work with people who have autism, learning disabilities, and complex needs, helping them manage daily routines, community access, budgeting, and tenancy responsibilities in supported living settings.</p>
        </body></html>
        """

        def fake_get(url, headers=None, timeout=None):
            if url == "https://www.example.com/":
                return DummyResponse(200, tiny_html)
            return DummyResponse(403, "")

        llm_payload = {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "verdict": "Majority Supported living",
                                "evidence": "supported living services help adults with learning disabilities in their own tenancy",
                            }
                        )
                    }
                }
            ]
        }

        with patch("ch_bulk.classifier.requests.Session.get", side_effect=fake_get):
            with patch(
                "ch_bulk.classifier.browser.is_playwright_available",
                return_value=True,
            ):
                with patch(
                    "ch_bulk.classifier.browser.fetch_rendered",
                    return_value=rich_html,
                ):
                    with patch(
                        "ch_bulk.classifier.httpx.Client.post",
                        return_value=DummyLLMResponse(llm_payload),
                    ):
                        summary = WebsiteClassifier(tmpdir.name, db_path).classify(
                            mode="list",
                            ids=["12345678"],
                            batch_size=1,
                        )

        self.assertEqual(summary["records_updated"], 1)
        self.assertEqual(summary["unable_count"], 0)
        log_text = Path(str(summary["log_path"])).read_text(encoding="utf-8")
        self.assertIn("used_playwright=true", log_text)

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            row = con.execute(
                """
                SELECT verdict, evidence_quote
                FROM classifications
                WHERE company_number = '12345678'
                """
            ).fetchone()
            self.assertEqual(row[0], "Majority Supported living")
            self.assertIn("learning disabilities", row[1])
        finally:
            con.close()

    def test_classify_marks_parse_failure_unable(self):
        db_path, tmpdir = self._create_db()
        html = """
        <html><body>
        <p>Residential care home services with nursing and long-term accommodation.</p>
        <p>Residents live with us full time.</p>
        <p>Our home provides twenty-four hour support, personal care, nursing oversight, meals, activities, and long-stay accommodation for older adults who live with us on a permanent basis.</p>
        </body></html>
        """

        with patch(
            "ch_bulk.classifier.requests.Session.get",
            return_value=DummyResponse(200, html),
        ):
            with patch(
                "ch_bulk.classifier.httpx.Client.post",
                return_value=DummyLLMResponse(
                    {"choices": [{"message": {"content": "not valid json"}}]}
                ),
            ):
                summary = WebsiteClassifier(tmpdir.name, db_path).classify(
                    mode="list",
                    ids=["12345678"],
                    batch_size=1,
                )

        self.assertEqual(summary["records_updated"], 1)
        self.assertEqual(summary["unable_count"], 1)
        self.assertEqual(summary["error_count"], 1)

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            row = con.execute(
                """
                SELECT verdict, verdict_reason
                FROM classifications
                WHERE company_number = '12345678'
                """
            ).fetchone()
            self.assertEqual(row, ("Unable to classify", "parse_error"))
        finally:
            con.close()

    def test_extract_json_object_accepts_strict_and_python_dict_syntax(self):
        cases = [
            (
                '{"verdict": "Majority residential", "evidence": "full-time residential care"}',
                "Majority residential",
                "full-time residential care",
            ),
            (
                "{'verdict': 'Majority residential', 'evidence': 'full-time residential care'}",
                "Majority residential",
                "full-time residential care",
            ),
            (
                """{'verdict': 'Majority residential', 'evidence': "home's residents"}""",
                "Majority residential",
                "home's residents",
            ),
        ]

        for raw, verdict, evidence in cases:
            with self.subTest(raw=raw):
                parsed = _extract_json_object(raw)
                self.assertIsNotNone(parsed)
                self.assertEqual(parsed["verdict"], verdict)
                self.assertEqual(parsed["evidence"], evidence)

        self.assertIsNone(_extract_json_object("not valid json"))

    def test_select_company_inputs_incremental_requires_primary_matched_url(self):
        db_path, tmpdir = self._create_db()
        con = duckdb.connect(str(db_path))
        try:
            _insert_company_and_site(
                con,
                company_number="20000001",
                company_name="Matched Primary Ltd",
                url="https://matched-primary.example",
            )
            con.execute(
                """
                INSERT INTO companies VALUES
                    ('20000002', 'Matched Non Primary Ltd', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL)
                """
            )
            con.execute(
                """
                INSERT INTO company_websites (
                    company_number,
                    discovered_via,
                    url,
                    is_primary,
                    discovered_at
                )
                VALUES (
                    '20000002',
                    'manual',
                    'https://matched-secondary.example',
                    FALSE,
                    TIMESTAMP '2026-05-24 12:00:00'
                )
                """
            )
            _insert_company_and_site(
                con,
                company_number="20000003",
                company_name="Unmatched Primary Ltd",
                url="https://unmatched-primary.example",
            )
            _insert_company_and_site(
                con,
                company_number="20000004",
                company_name="Matched Classified Ltd",
                url="https://matched-classified.example",
            )
            _insert_company_and_site(
                con,
                company_number="20000005",
                company_name="Matched Parse Error Ltd",
                url="https://matched-parse-error.example",
            )
            for company_number in ("20000001", "20000002", "20000004", "20000005"):
                con.execute(
                    """
                    INSERT INTO ch_cqc_matches VALUES
                        (?, '1-101', 10, '[]', 'auto_confirmed', TIMESTAMP '2026-05-24 12:00:00')
                    """,
                    [company_number],
                )
            batch_id = insert_classification_batch(
                con,
                classifier="llm:test",
                source_type="website",
                input_count=1,
                model_version="fixture-model",
            )
            con.execute(
                """
                INSERT INTO classifications (
                    company_number,
                    source_type,
                    verdict,
                    verdict_reason,
                    evidence_quote,
                    source_url,
                    classifier,
                    classified_at,
                    batch_id
                )
                VALUES (
                    '20000004',
                    'website',
                    'Majority domiciliary',
                    NULL,
                    'fixture',
                    'https://matched-classified.example',
                    'llm:test',
                    TIMESTAMP '2026-05-24 12:00:00',
                    ?
                )
                """,
                [batch_id],
            )
            con.execute(
                """
                INSERT INTO classifications (
                    company_number,
                    source_type,
                    verdict,
                    verdict_reason,
                    evidence_quote,
                    source_url,
                    classifier,
                    classified_at,
                    batch_id
                )
                VALUES (
                    '20000005',
                    'website',
                    'Unable to classify',
                    'parse_error',
                    '',
                    'https://matched-parse-error.example',
                    'llm:test',
                    TIMESTAMP '2026-05-24 12:00:00',
                    ?
                )
                """,
                [batch_id],
            )
        finally:
            con.close()

        with WebsiteClassifier(tmpdir.name, db_path) as classifier:
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                incremental = classifier._select_company_inputs(
                    con,
                    mode="incremental",
                    ids=None,
                )
                all_rows = classifier._select_company_inputs(
                    con,
                    mode="all",
                    ids=None,
                )
            finally:
                con.close()

        self.assertEqual(
            incremental,
            [
                ("20000001", ["https://matched-primary.example"]),
                ("20000005", ["https://matched-parse-error.example"]),
            ],
        )
        self.assertEqual(
            all_rows,
            [
                ("20000001", ["https://matched-primary.example"]),
                ("20000004", ["https://matched-classified.example"]),
                ("20000005", ["https://matched-parse-error.example"]),
            ],
        )

    def test_classify_ignores_unrelated_staging_files_on_startup(self):
        db_path, tmpdir = self._create_db()
        stage_dir = Path(tmpdir.name) / "staging"
        stage_dir.mkdir(parents=True, exist_ok=True)
        (stage_dir / "ch_directors_fixture.jsonl").write_text(
            '{"entity_type":"director"}\n',
            encoding="utf-8",
        )
        html = """
        <html><body>
        <p>We provide visiting care, medication support, companionship, meal preparation, and personal care for adults living in their own homes.</p>
        <p>Families rely on our home care teams for flexible support that helps people stay independent in their own homes.</p>
        </body></html>
        """
        llm_payload = {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "verdict": "Majority domiciliary",
                                "evidence": "adults living in their own homes",
                            }
                        )
                    }
                }
            ]
        }

        with patch(
            "ch_bulk.classifier.requests.Session.get",
            return_value=DummyResponse(200, html),
        ):
            with patch(
                "ch_bulk.classifier.httpx.Client.post",
                return_value=DummyLLMResponse(llm_payload),
            ):
                summary = WebsiteClassifier(tmpdir.name, db_path).classify(
                    mode="list",
                    ids=["12345678"],
                    batch_size=1,
                )

        self.assertEqual(summary["records_updated"], 1)
        self.assertEqual(summary["unable_count"], 0)

    def test_classify_does_not_use_playwright_for_llm_unable_on_real_text(self):
        db_path, tmpdir = self._create_db()
        html = """
        <html><body>
        <p>We provide home care, companionship, medication support, shopping assistance, meal preparation, personal care, and overnight support for adults living in their own homes.</p>
        <p>Our care teams work across the community and tailor support plans to each client, family, and referral partner.</p>
        </body></html>
        """

        with patch(
            "ch_bulk.classifier.requests.Session.get",
            return_value=DummyResponse(200, html),
        ):
            with patch(
                "ch_bulk.classifier.browser.fetch_rendered",
            ) as fetch_rendered:
                with patch(
                    "ch_bulk.classifier.httpx.Client.post",
                    return_value=DummyLLMResponse(
                        {
                            "choices": [
                                {
                                    "message": {
                                        "content": json.dumps(
                                            {
                                                "verdict": "Unable to classify",
                                                "evidence": "not enough specificity",
                                            }
                                        )
                                    }
                                }
                            ]
                        }
                    ),
                ):
                    summary = WebsiteClassifier(tmpdir.name, db_path).classify(
                        mode="list",
                        ids=["12345678"],
                        batch_size=1,
                    )

        self.assertEqual(summary["records_updated"], 1)
        self.assertEqual(summary["unable_count"], 1)
        self.assertEqual(summary["error_count"], 0)
        fetch_rendered.assert_not_called()

    def test_classify_does_not_use_playwright_for_dead_site_timeouts(self):
        db_path, tmpdir = self._create_db()

        def always_timeout(url, headers=None, timeout=None):
            raise requests.exceptions.ConnectTimeout("timed out")

        with patch(
            "ch_bulk.classifier.requests.Session.get",
            side_effect=always_timeout,
        ):
            with patch(
                "ch_bulk.classifier.browser.fetch_rendered",
            ) as fetch_rendered:
                summary = WebsiteClassifier(tmpdir.name, db_path).classify(
                    mode="list",
                    ids=["12345678"],
                    batch_size=1,
                )

        self.assertEqual(summary["records_updated"], 1)
        self.assertEqual(summary["unable_count"], 1)
        fetch_rendered.assert_not_called()

    def test_classify_reuses_playwright_session_for_multiple_handoffs(self):
        db_path, tmpdir = self._create_db()
        con = duckdb.connect(str(db_path))
        try:
            _insert_company_and_site(
                con,
                company_number="87654321",
                company_name="Second Example Homecare Ltd",
                url="https://www.second-example.com",
            )
        finally:
            con.close()

        tiny_html = "<html><body><p>Care</p></body></html>"
        session_holder: list[object] = []

        class DummySession:
            def __enter__(self):
                session_holder.append(self)
                return self

            def __exit__(self, exc_type, exc, tb):
                return None

        def fake_get(url, headers=None, timeout=None):
            return DummyResponse(200, tiny_html)

        def fake_fetch_rendered(url, *, timeout_seconds=30.0, session=None):
            self.assertIs(session, session_holder[0])
            return f"""
            <html><body>
            <p>Supported living services help adults with learning disabilities in their own tenancy and offer structured daily support.</p>
            <p>Teams help with budgeting, routines, medication prompts, travel training, and community participation in supported living services.</p>
            <p>We also support people with autism and complex needs, coordinating person-centred plans, supported living staffing, and community-based independence goals across multiple supported living homes.</p>
            <p>Rendered source marker: {url}</p>
            </body></html>
            """

        llm_payload = {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "verdict": "Majority Supported living",
                                "evidence": "supported living services help adults with learning disabilities in their own tenancy",
                            }
                        )
                    }
                }
            ]
        }

        with patch("ch_bulk.classifier.requests.Session.get", side_effect=fake_get):
            with patch(
                "ch_bulk.classifier.browser.PlaywrightSession",
                return_value=DummySession(),
            ) as session_ctor:
                with patch(
                    "ch_bulk.classifier.browser.fetch_rendered",
                    side_effect=fake_fetch_rendered,
                ) as fetch_rendered:
                    with patch(
                        "ch_bulk.classifier.httpx.Client.post",
                        return_value=DummyLLMResponse(llm_payload),
                    ):
                        summary = WebsiteClassifier(tmpdir.name, db_path).classify(
                            mode="list",
                            ids=["12345678", "87654321"],
                            batch_size=1,
                        )

        self.assertEqual(summary["records_updated"], 2)
        self.assertEqual(summary["unable_count"], 0)
        self.assertEqual(session_ctor.call_count, 1)
        self.assertGreaterEqual(fetch_rendered.call_count, 2)

    def test_classify_parallel_workers_preserve_complete_jsonl_rows(self):
        db_path, tmpdir = self._create_db()
        company_ids = [f"{20000000 + idx:08d}" for idx in range(30)]
        con = duckdb.connect(str(db_path))
        try:
            for idx, company_number in enumerate(company_ids):
                _insert_company_and_site(
                    con,
                    company_number=company_number,
                    company_name=f"Parallel Care {idx}",
                    url=f"https://parallel-{idx}.example",
                )
        finally:
            con.close()

        def fake_classify_company(company_number, urls):
            time.sleep(0.005)
            return (
                StagedClassification(
                    entity_id=company_number,
                    entity_type="classification",
                    fetched_at="2026-05-25T00:00:00Z",
                    http_status=200,
                    raw_json={
                        "verdict": "Majority domiciliary",
                        "failure_reason": None,
                        "evidence": "adults living in their own homes",
                        "source_url": urls[0],
                        "classifier": "llm:test",
                        "error": None,
                    },
                ),
                {
                    "n_pages": 1,
                    "text_len": 500,
                    "verdict": "Majority domiciliary",
                    "used_playwright": False,
                    "fetch_latency": 0.01,
                    "llm_latency": 0.02,
                },
            )

        classifier = WebsiteClassifier(tmpdir.name, db_path)
        with patch.object(
            classifier,
            "_classify_company",
            side_effect=fake_classify_company,
        ):
            summary = classifier.classify(
                mode="list",
                ids=["12345678", *company_ids],
                batch_size=5,
            )

        self.assertEqual(summary["records_updated"], 31)
        self.assertEqual(summary["unable_count"], 0)

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            counts = con.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT company_number)
                FROM classifications
                WHERE batch_id = ?
                """,
                [summary["batch_id"]],
            ).fetchone()
            self.assertEqual(counts, (31, 31))
        finally:
            con.close()

        stage_dir = Path(tmpdir.name) / "staging"
        loaded_paths = sorted(
            stage_dir.glob(f"classifications_{summary['batch_id']}*.jsonl.loaded")
        )
        self.assertGreaterEqual(len(loaded_paths), 6)

        staged_company_numbers: set[str] = set()
        staged_rows = 0
        for path in loaded_paths:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    payload = json.loads(line)
                    staged_rows += 1
                    staged_company_numbers.add(str(payload["entity_id"]))
        self.assertEqual(staged_rows, 31)
        self.assertEqual(len(staged_company_numbers), 31)

    def test_load_classification_staging_sql_round_trip_five_row_fixture(self):
        db_path, tmpdir, batch_id = self._create_loader_db()
        self._write_staged_rows(
            tmpdir.name,
            batch_id=batch_id,
            rows=[
                {
                    "entity_id": "10000001",
                    "entity_type": "classification",
                    "fetched_at": "2026-05-25T00:00:00Z",
                    "http_status": 200,
                    "raw_json": {
                        "verdict": "Majority domiciliary care",
                        "failure_reason": None,
                        "evidence": "Provides care in clients' own homes",
                        "source_url": "https://a.example",
                        "classifier": "llm:test",
                        "error": None,
                    },
                },
                {
                    "entity_id": "10000002",
                    "entity_type": "classification",
                    "fetched_at": "2026-05-25T00:00:01Z",
                    "http_status": 200,
                    "raw_json": {
                        "verdict": "",
                        "failure_reason": "no_content",
                        "evidence": "",
                        "source_url": "https://b.example",
                        "classifier": None,
                        "error": None,
                    },
                },
                {
                    "entity_id": "10000003",
                    "entity_type": "classification",
                    "fetched_at": "2026-05-25T00:00:02Z",
                    "http_status": 200,
                    "raw_json": {
                        "verdict": "Majority Supported living",
                        "failure_reason": None,
                        "evidence": "Supported living in their own tenancy",
                        "source_url": "https://c.example",
                        "classifier": "llm:test",
                        "error": None,
                    },
                },
                {
                    "entity_id": "10000004",
                    "entity_type": "classification",
                    "fetched_at": "2026-05-25T00:00:03Z",
                    "http_status": None,
                    "raw_json": {
                        "verdict": "Unable to classify",
                        "failure_reason": "parse_error",
                        "evidence": "",
                        "source_url": "https://d.example",
                        "classifier": "llm:test",
                        "error": "boom",
                    },
                },
                {
                    "entity_id": "10000005",
                    "entity_type": "classification",
                    "fetched_at": "2026-05-25T00:00:04Z",
                    "http_status": None,
                    "raw_json": {
                        "verdict": "Unable to classify",
                        "failure_reason": "all_pages_unreachable",
                        "evidence": "",
                        "source_url": "https://e.example",
                        "classifier": "llm:test",
                        "error": None,
                    },
                },
            ],
        )

        summary = load_classification_staging(
            tmpdir.name,
            db_path,
            batch_id=batch_id,
        )
        self.assertEqual(summary["loaded_batches"], 1)
        self.assertEqual(summary["records_fetched"], 5)
        self.assertEqual(summary["records_updated"], 5)
        self.assertEqual(summary["unable_count"], 3)
        self.assertEqual(summary["error_count"], 2)

        pending_path = (
            Path(tmpdir.name) / "staging" / f"classifications_{batch_id}.jsonl"
        )
        self.assertFalse(pending_path.exists())
        self.assertTrue(Path(f"{pending_path}.loaded").exists())

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            rows = con.execute(
                """
                SELECT
                    company_number,
                    verdict,
                    verdict_reason,
                    evidence_quote,
                    source_url,
                    classifier
                FROM classifications
                WHERE batch_id = ?
                ORDER BY company_number
                """,
                [batch_id],
            ).fetchall()
            self.assertEqual(
                rows,
                [
                    (
                        "10000001",
                        "Majority domiciliary",
                        None,
                        "Provides care in clients' own homes",
                        "https://a.example",
                        "llm:test",
                    ),
                    (
                        "10000002",
                        "Unable to classify",
                        "no_content",
                        "",
                        "https://b.example",
                        "llm:unknown",
                    ),
                    (
                        "10000003",
                        "Majority Supported living",
                        None,
                        "Supported living in their own tenancy",
                        "https://c.example",
                        "llm:test",
                    ),
                    (
                        "10000004",
                        "Unable to classify",
                        "parse_error",
                        "",
                        "https://d.example",
                        "llm:test",
                    ),
                    (
                        "10000005",
                        "Unable to classify",
                        "all_pages_unreachable",
                        "",
                        "https://e.example",
                        "llm:test",
                    ),
                ],
            )
            batch = con.execute(
                """
                SELECT status, classified_count, unable_count, error_count
                FROM classification_batches
                WHERE batch_id = ?
                """,
                [batch_id],
            ).fetchone()
            self.assertEqual(batch, ("succeeded", 5, 3, 2))
        finally:
            con.close()

    def test_load_classification_staging_large_batch_is_memory_bounded(self):
        if resource is None:
            self.skipTest("resource module unavailable")

        db_path, tmpdir, batch_id = self._create_loader_db()
        path = (
            Path(tmpdir.name) / "staging" / f"classifications_{batch_id}.jsonl"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for idx in range(10_000):
                verdict = (
                    "Unable to classify"
                    if idx % 5 == 0
                    else "Majority domiciliary"
                )
                failure_reason = "all_pages_unreachable" if idx % 10 == 0 else None
                handle.write(
                    json.dumps(
                        {
                            "entity_id": f"{idx:08d}",
                            "entity_type": "classification",
                            "fetched_at": "2026-05-25T00:00:00Z",
                            "http_status": 200,
                            "raw_json": {
                                "verdict": verdict,
                                "failure_reason": failure_reason,
                                "evidence": "fixture evidence",
                                "source_url": f"https://{idx}.example",
                                "classifier": "llm:test",
                                "error": None,
                            },
                        },
                        sort_keys=True,
                    )
                    + "\n"
                )

        started = time.monotonic()
        summary = load_classification_staging(
            tmpdir.name,
            db_path,
            batch_id=batch_id,
        )
        elapsed = time.monotonic() - started
        peak_rss_mb = _peak_rss_mb()

        self.assertEqual(summary["records_updated"], 10_000)
        self.assertEqual(summary["unable_count"], 2_000)
        self.assertEqual(summary["error_count"], 1_000)
        self.assertLess(elapsed, 10.0)
        self.assertIsNotNone(peak_rss_mb)
        self.assertLess(float(peak_rss_mb), 500.0)


if __name__ == "__main__":
    unittest.main()
