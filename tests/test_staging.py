"""Tests for staged JSONL replay and explicit log fsync checkpoints."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from ch_bulk._logging import FsyncLineLogger
from ch_bulk.bootstrap import ensure_pipeline_schema
from ch_bulk.cqc_api_enricher import load_cqc_staging
from ch_bulk.staging import StagedAPIResponse, StagingWriter
from ch_bulk.sync_batches import insert_sync_batch


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

            with patch("ch_bulk._logging.open", side_effect=tracking_open):
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
            log_text = (
                Path(tmpdir) / "logs" / "enrich_demo_batch-1.log"
            ).read_text(encoding="utf-8")
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

            pending_path = Path(tmpdir) / "staging" / f"api_providers_{batch_id}.jsonl"
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

    def test_load_cqc_staging_large_batch_keeps_memory_bounded(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "large-batch.duckdb"
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
                for index in range(5000):
                    writer.append(
                        StagedAPIResponse(
                            entity_type="provider",
                            entity_id=f"prov-{index:05d}",
                            fetched_at="2026-05-24T21:00:00Z",
                            http_status=200,
                            raw_json={
                                "providerId": f"prov-{index:05d}",
                                "name": f"Provider {index}",
                                "registrationStatus": "Registered",
                                "registrationDate": "2020-12-09",
                                "regulatedActivities": [],
                                "relationships": [
                                    {
                                        "type": "parent",
                                        "detail": "x" * 64,
                                    }
                                ],
                                "locationIds": [f"loc-{index:05d}"],
                            },
                        )
                    )
                writer.flush_and_fsync()
            finally:
                writer.close()

            env = dict(os.environ)
            env["PYTHONPATH"] = str(REPO_ROOT)
            load_code = textwrap.dedent(
                """
                import json
                import resource
                import sys

                from ch_bulk.cqc_api_enricher import load_cqc_staging

                def rss_mb():
                    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                    if sys.platform == "darwin":
                        return value / (1024 * 1024)
                    return value / 1024

                data_dir, db_path, batch_id = sys.argv[1], sys.argv[2], sys.argv[3]
                before = rss_mb()
                summary = load_cqc_staging(
                    data_dir,
                    db_path,
                    sync_type="api_providers",
                    batch_id=batch_id,
                )
                after = rss_mb()
                print(
                    json.dumps(
                        {
                            "summary": summary,
                            "rss_before_mb": before,
                            "rss_after_mb": after,
                            "rss_delta_mb": after - before,
                        }
                    )
                )
                """
            )
            proc = subprocess.run(
                [sys.executable, "-c", load_code, tmpdir, str(db_path), batch_id],
                cwd=REPO_ROOT,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(
                proc.returncode,
                0,
                msg=f"loader subprocess failed\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}",
            )
            result = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertEqual(result["summary"]["records_fetched"], 5000)
            self.assertEqual(result["summary"]["records_updated"], 5000)
            self.assertLess(result["rss_delta_mb"], 256.0)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                counts = con.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM cqc_api_responses WHERE batch_id = CAST(? AS UUID)),
                        (SELECT COUNT(*) FROM cqc_providers_enriched)
                    """,
                    [batch_id],
                ).fetchone()
                self.assertEqual(counts, (5000, 5000))
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

            pending_path = Path(tmpdir) / "staging" / f"api_providers_{batch_id}.jsonl"
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

                from ch_bulk.cqc_api_client import APIResult
                from ch_bulk.cqc_api_enricher import CQCAPIEnricher

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

                with patch("ch_bulk.cqc_api_enricher.CQCAPIClient") as client_cls:
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

                from ch_bulk.ch_enricher import enrich_directors

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

                with patch("ch_bulk.ch_enricher.CompaniesHouseClient") as client_cls:
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

            pending_files = list((Path(tmpdir) / "staging").glob("*.jsonl"))
            loaded_files = list((Path(tmpdir) / "staging").glob("*.jsonl.loaded"))
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
                CREATE TABLE cqc_locations (
                    location_id TEXT PRIMARY KEY
                )
                """
            )
            con.execute(
                """
                INSERT INTO companies VALUES
                    ('11111111', 'Alpha', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL),
                    ('22222222', 'Beta', 'SW1A 1AA', 'London', '88100', NULL, NULL, NULL)
                """
            )
            con.execute(
                """
                INSERT INTO cqc_locations VALUES
                    ('loc-1'),
                    ('loc-2')
                """
            )
        finally:
            con.close()


if __name__ == "__main__":
    unittest.main()
