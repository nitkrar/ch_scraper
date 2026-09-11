"""Tests for staged JSONL replay and explicit log fsync checkpoints."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from ch_bulk.core.paths import logs_dir, run_stage_file, runs_dir
from ch_bulk.core.logging import FsyncLineLogger
from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.cqc.api_enricher import load_cqc_staging
from ch_bulk.db.staging import StagedAPIResponse, StagingWriter
from ch_bulk.db.sync_batches import insert_sync_batch


REPO_ROOT = Path(__file__).resolve().parent.parent


class FsyncLineLoggerTests(unittest.TestCase):
    def test_line_logger_flush_and_fsync_makes_lines_observable(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            buffering_values: list[int] = []
            real_open = open

            def tracking_open(*args, **kwargs):
                if str(args[0]).endswith(".log"):
                    buffering_values.append(kwargs.get("buffering"))
                return real_open(*args, **kwargs)

            with patch("ch_bulk.core.logging.open", side_effect=tracking_open):
                logger = FsyncLineLogger(
                    tmpdir,
                    sync_type="demo",
                    batch_id="batch-1",
                )
                try:
                    logger.write_line("first line")
                    logger.flush_and_fsync()
                finally:
                    logger.close()

            self.assertIn(1, buffering_values)
            log_text = (logs_dir(tmpdir) / "enrich_demo_batch-1.log").read_text(
                encoding="utf-8"
            )
            self.assertIn("first line", log_text)


class StagingReplayTests(unittest.TestCase):
    def test_load_cqc_staging_replays_pending_file_and_marks_it_loaded(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "replay.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                ensure_pipeline_schema(con)
                batch_id = insert_sync_batch(
                    con,
                    sync_type="api_providers",
                    mode="all",
                )
            finally:
                con.close()

            writer = StagingWriter(
                tmpdir,
                sync_type="api_providers",
                batch_id=batch_id,
            )
            try:
                writer.append(
                    StagedAPIResponse(
                        entity_type="provider",
                        entity_id="prov-1",
                        fetched_at="2026-05-24T21:00:00Z",
                        http_status=200,
                        raw_json={
                            "providerId": "prov-1",
                            "name": "Provider 1",
                            "registrationStatus": "Registered",
                            "registrationDate": "2020-12-09",
                            "regulatedActivities": [],
                            "relationships": [],
                            "locationIds": ["loc-1"],
                        },
                    )
                )
                writer.flush_and_fsync()
            finally:
                writer.close()

            summary = load_cqc_staging(
                tmpdir,
                db_path,
                sync_type="api_providers",
                batch_id=batch_id,
            )
            self.assertEqual(summary["loaded_batches"], 1)
            self.assertEqual(summary["records_fetched"], 1)
            self.assertEqual(summary["records_updated"], 1)

            pending_path = run_stage_file(tmpdir, "api_providers", batch_id)
            loaded_path = Path(f"{pending_path}.loaded")
            self.assertFalse(pending_path.exists())
            self.assertTrue(loaded_path.exists())

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                provider_row = con.execute(
                    """
                    SELECT provider_id, company_name, registration_status
                    FROM cqc_providers_enriched
                    """
                ).fetchone()
                self.assertEqual(
                    provider_row,
                    ("prov-1", "Provider 1", "Registered"),
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

    def test_load_cqc_staging_keeps_jsonl_when_loader_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "failure.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                ensure_pipeline_schema(con)
                batch_id = insert_sync_batch(
                    con,
                    sync_type="api_providers",
                    mode="all",
                )
            finally:
                con.close()

            writer = StagingWriter(
                tmpdir,
                sync_type="api_providers",
                batch_id=batch_id,
            )
            try:
                writer.append(
                    StagedAPIResponse(
                        entity_type="provider",
                        entity_id="prov-1",
                        fetched_at="2026-05-24T21:00:00Z",
                        http_status=200,
                        raw_json="not-a-provider-object",
                    )
                )
                writer.flush_and_fsync()
            finally:
                writer.close()

            pending_path = run_stage_file(tmpdir, "api_providers", batch_id)
            loaded_path = Path(f"{pending_path}.loaded")

            with self.assertRaises(ValueError):
                load_cqc_staging(
                    tmpdir,
                    db_path,
                    sync_type="api_providers",
                    batch_id=batch_id,
                )

            self.assertTrue(pending_path.exists())
            self.assertFalse(loaded_path.exists())

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                batch_row = con.execute(
                    """
                    SELECT status, records_fetched, records_updated, error_count
                    FROM cqc_sync_batches
                    WHERE batch_id = ?
                    """,
                    [batch_id],
                ).fetchone()
                self.assertEqual(batch_row, ("failed", 1, 0, 1))
            finally:
                con.close()


class ParallelEnricherTests(unittest.TestCase):
    def test_parallel_processes_complete_without_duckdb_lock_errors(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "parallel.duckdb"
            self._bootstrap_parallel_db(db_path)

            env = dict(os.environ)
            env["PYTHONPATH"] = str(REPO_ROOT)

            cqc_code = textwrap.dedent(
                """
                import sys
                import time
                from unittest.mock import patch

                from ch_bulk.cqc.api_client import APIResult
                from ch_bulk.cqc.api_enricher import CQCAPIEnricher

                data_dir, db_path = sys.argv[1], sys.argv[2]
                payloads = {
                    "loc-1": {
                        "locationId": "loc-1",
                        "providerId": "prov-1",
                        "registrationStatus": "Registered",
                        "registrationDate": "2020-12-09",
                        "regulatedActivities": [],
                        "relationships": [],
                    },
                    "loc-2": {
                        "locationId": "loc-2",
                        "providerId": "prov-2",
                        "registrationStatus": "Registered",
                        "registrationDate": "2020-12-09",
                        "regulatedActivities": [],
                        "relationships": [],
                    },
                }

                def get_location(location_id):
                    time.sleep(0.05)
                    return APIResult(200, payloads[location_id])

                with patch("ch_bulk.cqc.api_enricher.CQCAPIClient") as client_cls:
                    client = client_cls.return_value.__enter__.return_value
                    client.get_location.side_effect = get_location
                    CQCAPIEnricher(data_dir, db_path).enrich_locations(
                        mode="all",
                        batch_size=1,
                    )
                """
            )
            ch_code = textwrap.dedent(
                """
                import sys
                import time
                from unittest.mock import patch

                from ch_bulk.companies_house.ch_enricher import enrich_directors

                data_dir, db_path = sys.argv[1], sys.argv[2]
                officers = {
                    "11111111": [
                        {"officer_role": "director", "date_of_birth": {"year": 1960}}
                    ],
                    "22222222": [
                        {"officer_role": "director", "date_of_birth": {"year": 1970}}
                    ],
                }

                def get_officers(company_number):
                    time.sleep(0.05)
                    return officers[company_number]

                with patch("ch_bulk.companies_house.ch_enricher.CompaniesHouseClient") as client_cls:
                    client = client_cls.return_value.__enter__.return_value
                    client.get_officers.side_effect = get_officers
                    enrich_directors(
                        db_path,
                        data_dir,
                        batch_size=1,
                    )
                """
            )

            cqc_proc = subprocess.Popen(
                [sys.executable, "-c", cqc_code, tmpdir, str(db_path)],
                cwd=REPO_ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            ch_proc = subprocess.Popen(
                [sys.executable, "-c", ch_code, tmpdir, str(db_path)],
                cwd=REPO_ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            cqc_stdout, cqc_stderr = cqc_proc.communicate(timeout=60)
            ch_stdout, ch_stderr = ch_proc.communicate(timeout=60)

            self.assertEqual(
                cqc_proc.returncode,
                0,
                msg=f"CQC process failed\nstdout:\n{cqc_stdout}\nstderr:\n{cqc_stderr}",
            )
            self.assertEqual(
                ch_proc.returncode,
                0,
                msg=f"CH process failed\nstdout:\n{ch_stdout}\nstderr:\n{ch_stderr}",
            )

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                succeeded_rows = con.execute(
                    """
                    SELECT sync_type, status, records_fetched, records_updated
                    FROM cqc_sync_batches
                    ORDER BY sync_type
                    """
                ).fetchall()
                self.assertEqual(
                    succeeded_rows,
                    [
                        ("api_locations", "succeeded", 2, 2),
                        ("ch_directors", "succeeded", 2, 2),
                    ],
                )
            finally:
                con.close()

            pending_files = list(runs_dir(tmpdir, "api_locations").glob("*.jsonl"))
            pending_files.extend(runs_dir(tmpdir, "ch_directors").glob("*.jsonl"))
            loaded_files = list(
                runs_dir(tmpdir, "api_locations").glob("*.jsonl.loaded")
            )
            loaded_files.extend(
                runs_dir(tmpdir, "ch_directors").glob("*.jsonl.loaded")
            )
            self.assertEqual(pending_files, [])
            self.assertEqual(len(loaded_files), 2)

    def _bootstrap_parallel_db(self, db_path: Path) -> None:
        con = duckdb.connect(str(db_path))
        try:
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
            ensure_pipeline_schema(con)
            con.execute(
                """
                INSERT INTO companies VALUES
                    ('11111111', 'Alpha', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL),
                    ('22222222', 'Beta', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL)
                """
            )
            con.execute(
                """
                INSERT INTO cqc_locations (location_id)
                VALUES ('loc-1'), ('loc-2')
                """
            )
        finally:
            con.close()


if __name__ == "__main__":
    unittest.main()
