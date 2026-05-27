"""Focused tests for DDG website discovery and staging loads."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from ch_bulk.core.paths import run_stage_file
from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.db.sync_batches import insert_sync_batch
from ch_bulk.web.search import DuckDuckGoSearcher, URLCheckResult
from ch_bulk.web.website_finder import (
    StagedWebsiteSearch,
    load_website_finder_staging,
)


class DummyResponse:
    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text
        self.url = "https://html.duckduckgo.com/html/"

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class DuckDuckGoSearchTests(unittest.TestCase):
    def test_search_company_parses_results_and_picks_high_scoring_match(self):
        html = """
        <html><body>
            <div class="result">
                <a class="result__a" href="https://www.homecare.co.uk/provider">Directory listing</a>
                <a class="result__snippet">Directory entry</a>
            </div>
            <div class="result">
                <a class="result__a" href="https://www.examplehomecare.co.uk/">Example Homecare</a>
                <a class="result__snippet">Home care services in London</a>
            </div>
            <div class="result">
                <a class="result__a" href="https://www.example.com/about">About Example</a>
                <a class="result__snippet">General company site</a>
            </div>
        </body></html>
        """

        searcher = DuckDuckGoSearcher()
        self.addCleanup(searcher.close)

        with patch.object(
            searcher._session,
            "post",
            return_value=DummyResponse(200, html),
        ):
            with patch(
                "ch_bulk.web.search._check_results_in_parallel",
                return_value={
                    1: URLCheckResult(
                        reachable=True,
                        status_code=200,
                        final_url="https://www.homecare.co.uk/provider",
                        error=None,
                        checked_via="head",
                    ),
                    2: URLCheckResult(
                        reachable=True,
                        status_code=200,
                        final_url="https://www.examplehomecare.co.uk/",
                        error=None,
                        checked_via="head",
                    ),
                    3: URLCheckResult(
                        reachable=True,
                        status_code=200,
                        final_url="https://www.example.com/about",
                        error=None,
                        checked_via="head",
                    ),
                },
            ):
                outcome = searcher.search_company(
                    "Example Homecare Ltd",
                    "SW1A 1AA",
                )

        self.assertEqual(
            outcome.query,
            "Example Homecare Ltd homecare SW1A 1AA",
        )
        self.assertEqual(outcome.picked_url, "https://www.examplehomecare.co.uk/")
        self.assertEqual(outcome.picked_reason, "top_scored_with_uk_tld")
        self.assertEqual(outcome.picked_score, 13)
        self.assertEqual(len(outcome.search_results), 3)
        self.assertTrue(outcome.search_results[0].denylisted)
        self.assertEqual(outcome.search_results[1].score, 13)
        self.assertIn("uk_tld", outcome.search_results[1].score_signals)
        self.assertIn("root_url", outcome.search_results[1].score_signals)

    def test_search_company_normalizes_picked_deep_link_to_root(self):
        html = """
        <html><body>
            <div class="result">
                <a class="result__a" href="https://www.abbeyfieldengland.com/find-a-home/york?ref=ddg">Abbeyfield York</a>
                <a class="result__snippet">Find a home in York</a>
            </div>
        </body></html>
        """

        searcher = DuckDuckGoSearcher()
        self.addCleanup(searcher.close)

        with patch.object(
            searcher._session,
            "post",
            return_value=DummyResponse(200, html),
        ):
            with patch(
                "ch_bulk.web.search._check_results_in_parallel",
                return_value={
                    1: URLCheckResult(
                        reachable=True,
                        status_code=200,
                        final_url="https://www.abbeyfieldengland.com/find-a-home/york?ref=ddg",
                        error=None,
                        checked_via="head",
                    ),
                },
            ):
                outcome = searcher.search_company(
                    "Abbeyfield York Society Ltd",
                    "YO1 1AA",
                )

        self.assertEqual(
            outcome.picked_url,
            "https://www.abbeyfieldengland.com/",
        )


class WebsiteFinderLoaderTests(unittest.TestCase):
    def _create_loader_db(
        self,
    ) -> tuple[Path, tempfile.TemporaryDirectory[str], str]:
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        db_path = Path(tmpdir.name) / "website-finder.duckdb"
        con = duckdb.connect(str(db_path))
        try:
            ensure_pipeline_schema(con)
            batch_id = insert_sync_batch(
                con,
                sync_type="website_finder",
                mode="incremental",
            )
        finally:
            con.close()
        return db_path, tmpdir, batch_id

    def _write_staged_rows(
        self,
        tmpdir: str,
        *,
        batch_id: str,
        rows: list[StagedWebsiteSearch],
    ) -> Path:
        path = run_stage_file(tmpdir, "website_finder", batch_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(row.to_json_line() + "\n")
        return path

    def test_load_staging_inserts_primary_row_and_counts_no_match(self):
        db_path, tmpdir, batch_id = self._create_loader_db()
        path = self._write_staged_rows(
            tmpdir.name,
            batch_id=batch_id,
            rows=[
                StagedWebsiteSearch(
                    company_number="12345678",
                    name="Example Homecare Ltd",
                    postcode="SW1A 1AA",
                    query="Example Homecare Ltd homecare SW1A 1AA",
                    fetched_at="2026-05-25T12:00:00Z",
                    search_results=[
                        {
                            "rank": 1,
                            "url": "https://www.examplehomecare.co.uk/",
                            "score": 13,
                        }
                    ],
                    picked_url="https://www.examplehomecare.co.uk/",
                    picked_reason="top_scored_with_uk_tld",
                    picked_score=13,
                    picked_rank=1,
                    picked_reachable=True,
                    min_score=4,
                ),
                StagedWebsiteSearch(
                    company_number="87654321",
                    name="No Match Care Ltd",
                    postcode="EC1A 1BB",
                    query="No Match Care Ltd homecare EC1A 1BB",
                    fetched_at="2026-05-25T12:01:00Z",
                    search_results=[],
                    picked_url=None,
                    picked_reason="no_match_above_threshold",
                    picked_score=None,
                    picked_rank=None,
                    picked_reachable=None,
                    min_score=4,
                ),
            ],
        )

        summary = load_website_finder_staging(
            tmpdir.name,
            db_path,
            batch_id=batch_id,
        )
        self.assertEqual(summary["records_fetched"], 2)
        self.assertEqual(summary["records_updated"], 1)
        self.assertEqual(summary["error_count"], 0)
        self.assertEqual(summary["no_match_count"], 1)

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            row = con.execute(
                """
                SELECT
                    company_number,
                    discovered_via,
                    url,
                    is_primary,
                    last_seen_reachable_at IS NOT NULL,
                    discovered_by_batch
                FROM company_websites
                """
            ).fetchone()
            normalized_row = row[:5] + (str(row[5]),)
            self.assertEqual(
                normalized_row,
                (
                    "12345678",
                    "web_search",
                    "https://www.examplehomecare.co.uk/",
                    True,
                    True,
                    batch_id,
                ),
            )
        finally:
            con.close()

        self.assertFalse(path.exists())
        self.assertTrue(Path(str(path) + ".loaded").exists())

    def test_load_staging_inserts_secondary_row_when_primary_already_exists(self):
        db_path, tmpdir, batch_id = self._create_loader_db()
        con = duckdb.connect(str(db_path))
        try:
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
                    '12345678',
                    'manual',
                    'https://www.existing-primary.example/',
                    TRUE,
                    TIMESTAMP '2026-05-24 12:00:00'
                )
                """
            )
        finally:
            con.close()

        self._write_staged_rows(
            tmpdir.name,
            batch_id=batch_id,
            rows=[
                StagedWebsiteSearch(
                    company_number="12345678",
                    name="Example Homecare Ltd",
                    postcode="SW1A 1AA",
                    query="Example Homecare Ltd homecare SW1A 1AA",
                    fetched_at="2026-05-25T12:00:00Z",
                    search_results=[
                        {
                            "rank": 1,
                            "url": "https://www.examplehomecare.co.uk/",
                            "score": 13,
                        }
                    ],
                    picked_url="https://www.examplehomecare.co.uk/",
                    picked_reason="top_scored_with_uk_tld",
                    picked_score=13,
                    picked_rank=1,
                    picked_reachable=True,
                    min_score=4,
                )
            ],
        )

        summary = load_website_finder_staging(
            tmpdir.name,
            db_path,
            batch_id=batch_id,
        )
        self.assertEqual(summary["records_updated"], 1)

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            rows = con.execute(
                """
                SELECT url, is_primary, discovered_via
                FROM company_websites
                WHERE company_number = '12345678'
                ORDER BY discovered_at, website_id
                """
            ).fetchall()
            self.assertEqual(
                rows,
                [
                    (
                        "https://www.existing-primary.example/",
                        True,
                        "manual",
                    ),
                    (
                        "https://www.examplehomecare.co.uk/",
                        False,
                        "web_search",
                    ),
                ],
            )
        finally:
            con.close()


if __name__ == "__main__":
    unittest.main()
