"""`ch-bulk query` shows a page of matches and the total without loading every row."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb
from typer.testing import CliRunner

from ch_bulk.cli import app
from ch_bulk.companies_house import query as ch_query
from ch_bulk.companies_house.query import count_by_sic


def _make_db(path: Path, *, active: int, dissolved: int) -> Path:
    con = duckdb.connect(str(path))
    try:
        con.execute(
            """
            CREATE TABLE companies (
                company_number VARCHAR, company_name VARCHAR, company_status VARCHAR,
                postcode VARCHAR, sic_code_1 VARCHAR, sic_code_2 VARCHAR,
                sic_code_3 VARCHAR, sic_code_4 VARCHAR
            )
            """
        )
        rows = [
            (f"{i:08d}", f"CARE {i:03d} LTD", "Active", "SW1A 1AA", "88100", None, None, None)
            for i in range(active)
        ] + [
            (f"{9000 + i:08d}", f"OLD {i:03d} LTD", "Dissolved", None, None, "88100", None, None)
            for i in range(dissolved)
        ] + [("77777777", "OTHER LTD", "Active", None, "62020", None, None, None)]
        con.executemany("INSERT INTO companies VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    finally:
        con.close()
    return path


class CountBySicTests(unittest.TestCase):
    def test_counts_matches_across_sic_columns_with_status_and_limit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = _make_db(Path(tmpdir) / "t.duckdb", active=70, dissolved=5)
            self.assertEqual(count_by_sic(db, "88100"), 70)
            self.assertEqual(count_by_sic(db, "88100", status=None), 75)
            self.assertEqual(count_by_sic(db, ["88100", "62020"]), 71)
            self.assertEqual(count_by_sic(db, "88100", limit=10), 10)
            self.assertEqual(count_by_sic(db, "88100", limit=500), 70)

    def test_missing_database(self):
        with self.assertRaises(FileNotFoundError):
            count_by_sic(Path("/nonexistent/x.duckdb"), "88100")


class QueryCommandTests(unittest.TestCase):
    def test_shows_first_50_and_the_full_total_without_fetching_every_row(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = _make_db(Path(tmpdir) / "t.duckdb", active=70, dissolved=0)
            fetched: list[int] = []
            real_query = ch_query.query_by_sic

            def spy(*args, **kwargs):
                rows = real_query(*args, **kwargs)
                fetched.append(len(rows))
                return rows

            with patch("ch_bulk.api.query_by_sic", side_effect=spy):
                result = CliRunner().invoke(
                    app, ["query", "88100", "--db-path", str(db)], terminal_width=200
                )

        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("70 total results", result.output)
        self.assertIn("CARE 049 LTD", result.output)
        self.assertNotIn("CARE 050 LTD", result.output)
        self.assertEqual(fetched, [50])

    def test_limit_caps_the_total(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = _make_db(Path(tmpdir) / "t.duckdb", active=70, dissolved=0)
            result = CliRunner().invoke(
                app, ["query", "88100", "--db-path", str(db), "--limit", "20"],
                terminal_width=200,
            )
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("20 total results", result.output)
        self.assertNotIn("CARE 020 LTD", result.output)

    def test_no_matches(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db = _make_db(Path(tmpdir) / "t.duckdb", active=0, dissolved=0)
            result = CliRunner().invoke(app, ["query", "88100", "--db-path", str(db)])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("No companies found", result.output)


if __name__ == "__main__":
    unittest.main()
