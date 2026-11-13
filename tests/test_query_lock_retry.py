"""Tests for AUDIT-07: read queries retry lock conflicts.

Each query module must route its reads through the lock-retry
helper, so a transient "database is locked" failure on the first
connection attempt is retried instead of surfacing to the caller.
Non-lock errors must not be retried.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from ch_bulk.companies_house.query import export_query_csv, query_by_sic
from ch_bulk.cqc.query import query_cqc_locations
from ch_bulk.matching.query import query_tiered_targets


def _make_ch_db(path: Path) -> Path:
    con = duckdb.connect(str(path))
    try:
        con.execute(
            """
            CREATE TABLE companies (
                company_number VARCHAR, company_name VARCHAR,
                company_status VARCHAR, sic_code_1 VARCHAR,
                sic_code_2 VARCHAR, sic_code_3 VARCHAR, sic_code_4 VARCHAR
            )
            """
        )
        con.execute(
            "INSERT INTO companies VALUES "
            "('12345678', 'ACME LTD', 'Active', '88100', NULL, NULL, NULL)"
        )
    finally:
        con.close()
    return path


def _make_cqc_db(path: Path) -> Path:
    con = duckdb.connect(str(path))
    try:
        con.execute(
            """
            CREATE TABLE cqc_locations (
                name VARCHAR, postcode VARCHAR, service_types VARCHAR,
                provider_name VARCHAR, local_authority VARCHAR,
                region VARCHAR, is_active BOOLEAN, last_scrape_date DATE
            )
            """
        )
        con.execute(
            "INSERT INTO cqc_locations VALUES "
            "('Acme Care', 'SW1A 1AA', 'Domiciliary', 'Acme', "
            "'Westminster', 'London', TRUE, '2026-01-01')"
        )
    finally:
        con.close()
    return path


def _make_targets_db(path: Path) -> Path:
    con = duckdb.connect(str(path))
    try:
        con.execute(
            """
            CREATE TABLE tiered_targets (
                company_number VARCHAR, company_name VARCHAR, tier VARCHAR,
                total_score DOUBLE, revenue DOUBLE, employee_count INTEGER,
                directors_over_60 INTEGER, total_active_directors INTEGER,
                match_status VARCHAR, address_post_town VARCHAR,
                cqc_provider_id VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE cqc_providers (
                provider_id VARCHAR, provider_name VARCHAR
            )
            """
        )
        con.execute(
            "INSERT INTO tiered_targets VALUES "
            "('12345678', 'ACME LTD', 'Tier 1', 90.0, 1000000.0, 10, 1, 2, "
            "'auto_confirmed', 'London', 'P1')"
        )
        con.execute("INSERT INTO cqc_providers VALUES ('P1', 'Acme')")
    finally:
        con.close()
    return path


class LockRetryQueryTests(unittest.TestCase):
    def _run_with_locked_first_connect(self, fn):
        original_connect = duckdb.connect
        calls = {"count": 0}

        def flaky_connect(*args, **kwargs):
            calls["count"] += 1
            if calls["count"] == 1:
                raise IOError(
                    "IO Error: Could not set lock on file - database is locked"
                )
            return original_connect(*args, **kwargs)

        with (
            patch("ch_bulk.db.staging.duckdb.connect", side_effect=flaky_connect),
            patch("ch_bulk.db.staging.time.sleep"),
        ):
            result = fn()
        self.assertEqual(calls["count"], 2)
        return result

    def test_ch_query_retries_lock_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _make_ch_db(Path(tmpdir) / "test.duckdb")
            rows = self._run_with_locked_first_connect(
                lambda: query_by_sic(db_path, "88100")
            )
            self.assertEqual(len(rows), 1)

    def test_ch_export_retries_lock_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _make_ch_db(Path(tmpdir) / "test.duckdb")
            out = Path(tmpdir) / "out.csv"
            count = self._run_with_locked_first_connect(
                lambda: export_query_csv(db_path, "88100", out)
            )
            self.assertEqual(count, 1)
            self.assertTrue(out.exists())

    def test_cqc_query_retries_lock_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _make_cqc_db(Path(tmpdir) / "test.duckdb")
            rows, total = self._run_with_locked_first_connect(
                lambda: query_cqc_locations(db_path)
            )
            self.assertEqual((total, len(rows)), (1, 1))

    def test_tiered_targets_query_retries_lock_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _make_targets_db(Path(tmpdir) / "test.duckdb")
            rows, total = self._run_with_locked_first_connect(
                lambda: query_tiered_targets(db_path)
            )
            self.assertEqual((total, len(rows)), (1, 1))

    def test_non_lock_error_is_not_retried(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _make_ch_db(Path(tmpdir) / "test.duckdb")
            calls = {"count": 0}

            def failing_connect(*args, **kwargs):
                calls["count"] += 1
                raise RuntimeError("boom")

            with (
                patch(
                    "ch_bulk.db.staging.duckdb.connect",
                    side_effect=failing_connect,
                ),
                patch("ch_bulk.db.staging.time.sleep"),
            ):
                with self.assertRaises(RuntimeError):
                    query_by_sic(db_path, "88100")
            self.assertEqual(calls["count"], 1)


class ReadsAlongsideInProcessWriterTests(unittest.TestCase):
    """Enrichment and uploads write from a worker thread in the GUI process.

    DuckDB refuses a read-only connection to a file this process already
    has open read-write (and the reverse), so a read-only search would
    fail, or make the job's next write fail, whenever the two overlap.
    """

    def _assert_reads_work_while_writer_open(self, db_path: Path, read) -> None:
        writer = duckdb.connect(str(db_path))
        try:
            read()
            writer.execute("SELECT 1")
        finally:
            writer.close()

    def test_ch_reads(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _make_ch_db(Path(tmpdir) / "test.duckdb")
            out = Path(tmpdir) / "out.csv"
            self._assert_reads_work_while_writer_open(
                db_path, lambda: query_by_sic(db_path, "88100")
            )
            self._assert_reads_work_while_writer_open(
                db_path, lambda: export_query_csv(db_path, "88100", out)
            )

    def test_cqc_reads(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _make_cqc_db(Path(tmpdir) / "test.duckdb")
            self._assert_reads_work_while_writer_open(
                db_path, lambda: query_cqc_locations(db_path)
            )

    def test_tiered_targets_reads(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = _make_targets_db(Path(tmpdir) / "test.duckdb")
            self._assert_reads_work_while_writer_open(
                db_path, lambda: query_tiered_targets(db_path)
            )


if __name__ == "__main__":
    unittest.main()
