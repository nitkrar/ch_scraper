"""Tests for the Upload List orchestrator (run_upload / ChBulk.upload_profile)."""

from __future__ import annotations

import csv
import json
import tempfile
import threading
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import duckdb
import openpyxl

from ch_bulk.api import ChBulk
from ch_bulk.core.cancellation import OperationCancelled
from ch_bulk.companies_house.company_profile import ensure_companies_table
from ch_bulk.companies_house.upload_pipeline import UploadSummary, run_upload
from ch_bulk.db.bootstrap import ensure_pipeline_schema

ENRICH_DIRECTORS = "ch_bulk.companies_house.ch_enricher.enrich_directors"
BACKFILL_DIRECTORS = "ch_bulk.companies_house.ch_enricher.backfill_directors"
ENRICH_FINANCIALS = "ch_bulk.companies_house.financials_enricher.enrich_financials"
PROFILE_CLIENT = "ch_bulk.companies_house.company_profile.CompaniesHouseClient"

# Seeded companies, named by what the reuse rules should do with them.
BOTH = "10000001"          # no enrichment row -> directors + financials
REUSE = "10000002"         # fully enriched -> neither
DIRS_ONLY = "10000003"     # directors never fetched; accounts terminal (no_recent_filing)
FIN_ONLY = "10000004"      # directors fetched; no filing, no revenue_source
FIN_ESTIMATE = "10000005"  # directors fetched; no filing, only an estimate
NEW = "20000001"           # not in DB; API returns a profile
MISSING = "20000002"       # not in DB; API returns 404
SEEDED = [BOTH, REUSE, DIRS_ONLY, FIN_ONLY, FIN_ESTIMATE]
INPUT_NUMBERS = [BOTH, REUSE, DIRS_ONLY, FIN_ONLY, FIN_ESTIMATE, NEW, MISSING]


def _director(name: str, *, legacy: bool = False) -> dict:
    entry = {"name": name, "officer_role": "director", "appointed_on": "2015-01-01"}
    if not legacy:
        entry["date_of_birth"] = {"month": 1, "year": 1970}
    return entry


class FakeProfileClient:
    def __init__(self, profiles: dict[str, dict]):
        self.profiles = profiles
        self.calls: list[str] = []
        self.closed = False

    def get_company_profile(self, company_number, cancel_event=None):
        self.calls.append(company_number)
        return self.profiles.get(company_number)

    def close(self):
        self.closed = True


class _PipelineCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.data_dir = self.tmp / "data"
        self.data_dir.mkdir()
        self.db_path = self.data_dir / "test.duckdb"
        self.output_path = self.tmp / "out.xlsx"
        self.client = FakeProfileClient({
            NEW: {"company_number": NEW, "company_name": "New Api Ltd", "company_status": "active"},
            **{
                n: {"company_number": n, "company_name": f"Refreshed {n}", "company_status": "active"}
                for n in SEEDED
            },
        })

    def tearDown(self):
        self._tmp.cleanup()

    def seed(self, *, legacy_directors: bool = False):
        con = duckdb.connect(str(self.db_path))
        try:
            ensure_companies_table(con)
            ensure_pipeline_schema(con)
            for number in SEEDED:
                # Fresh bulk row (record_source NULL): refresh never re-fetches it.
                con.execute(
                    "INSERT INTO companies (company_number, company_name, company_status, "
                    "last_scrape_date) VALUES (?, ?, 'Active', CURRENT_DATE)",
                    [number, f"Seeded {number}"],
                )
            directors = json.dumps([
                _director("SMITH, John"),
                _director("OLD, Entry", legacy=legacy_directors),
            ])
            enrichment = [
                (REUSE, "[1970, 1970]", 2, directors, "F2", "filed_accounts_ixbrl"),
                (DIRS_ONLY, None, None, None, None, "no_recent_filing"),
                (FIN_ONLY, "[1970]", 1, None, None, None),
                (FIN_ESTIMATE, "[1970]", 1, None, None, "employee_band_lookup"),
            ]
            for number, dob_years, total, dirs, filing_id, source in enrichment:
                con.execute(
                    "INSERT INTO company_enrichment (company_number, directors_dob_years, "
                    "total_active_directors, directors, filing_id, revenue_source, "
                    "last_enriched_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [number, dob_years, total, dirs, filing_id, source, datetime.now()],
                )
        finally:
            con.close()

    def write_input(self, numbers, invalid=()) -> Path:
        path = self.tmp / "input.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["Company Number"])
            for value in [*numbers, *invalid]:
                writer.writerow([value])
        return path

    def run_patched(self, input_path, **kwargs):
        with patch(ENRICH_DIRECTORS, return_value={}) as directors, \
                patch(BACKFILL_DIRECTORS, return_value={}) as backfill, \
                patch(ENRICH_FINANCIALS, return_value={}) as financials, \
                patch(PROFILE_CLIENT, return_value=self.client):
            summary = run_upload(
                self.db_path, self.data_dir, input_path, self.output_path, **kwargs
            )
        return summary, directors, backfill, financials

    @staticmethod
    def directors_targets(mock) -> list[str]:
        mock.assert_called_once()
        return list(mock.call_args.kwargs["company_numbers"])

    @staticmethod
    def financials_targets(mock) -> list[str]:
        mock.assert_called_once()
        assert mock.call_args.kwargs["mode"] == "list"
        return list(mock.call_args.kwargs["ids"])


class RunUploadTests(_PipelineCase):
    def test_reuse_rules_pick_targets_and_summary_counts(self):
        self.seed()
        input_path = self.write_input(INPUT_NUMBERS, invalid=["bad!"])
        summary, directors, backfill, financials = self.run_patched(input_path)

        self.assertCountEqual(self.directors_targets(directors), [BOTH, DIRS_ONLY, NEW])
        self.assertCountEqual(
            self.financials_targets(financials), [BOTH, FIN_ONLY, FIN_ESTIMATE, NEW]
        )
        backfill.assert_not_called()
        self.assertEqual(self.client.calls, [NEW, MISSING])

        args = directors.call_args
        self.assertEqual(Path(args.args[0] if args.args else args.kwargs["db_path"]), self.db_path)

        self.assertIsInstance(summary, UploadSummary)
        self.assertEqual(Path(summary.output_path), self.output_path)
        self.assertEqual(summary.requested, 7)
        self.assertEqual(summary.invalid, 1)
        self.assertEqual(summary.not_found, 1)
        self.assertEqual(summary.looked_up, 1)
        self.assertEqual(summary.directors_fetched, 3)
        self.assertEqual(summary.financials_fetched, 4)

    def test_enrichment_over_a_month_old_is_fetched_again(self):
        self.seed()
        con = duckdb.connect(str(self.db_path))
        con.execute(
            "UPDATE company_enrichment "
            "SET last_enriched_at = CURRENT_DATE - INTERVAL 1 MONTH - INTERVAL 1 DAY "
            "WHERE company_number IN (?, ?)",
            [REUSE, DIRS_ONLY],
        )
        con.close()
        input_path = self.write_input([REUSE, DIRS_ONLY])
        _summary, directors, _backfill, financials = self.run_patched(input_path)

        self.assertCountEqual(self.directors_targets(directors), [REUSE, DIRS_ONLY])
        # A stale "no recent filing" is checked again: a filing may have arrived.
        self.assertCountEqual(self.financials_targets(financials), [REUSE, DIRS_ONLY])
        self.assertEqual(self.client.calls, [])

    def test_stale_accounts_are_fetched_after_directors_update_the_shared_date(self):
        self.seed()
        con = duckdb.connect(str(self.db_path))
        con.execute(
            "UPDATE company_enrichment "
            "SET last_enriched_at = CURRENT_DATE - INTERVAL 2 MONTH WHERE company_number = ?",
            [REUSE],
        )
        con.close()

        def enrich_directors(db_path, data_dir, *, company_numbers, **_kwargs):
            # The real enricher stamps last_enriched_at on every row it writes.
            with duckdb.connect(str(db_path)) as dcon:
                dcon.execute(
                    "UPDATE company_enrichment SET last_enriched_at = now() "
                    "WHERE company_number IN (SELECT UNNEST(?::VARCHAR[]))",
                    [list(company_numbers)],
                )
            return {}

        with patch(ENRICH_DIRECTORS, side_effect=enrich_directors), \
                patch(BACKFILL_DIRECTORS, return_value={}), \
                patch(ENRICH_FINANCIALS, return_value={}) as financials, \
                patch(PROFILE_CLIENT, return_value=self.client):
            run_upload(self.db_path, self.data_dir, self.write_input([REUSE]), self.output_path)

        self.assertEqual(self.financials_targets(financials), [REUSE])

    def test_enrichment_exactly_a_month_old_is_reused(self):
        self.seed()
        con = duckdb.connect(str(self.db_path))
        con.execute(
            "UPDATE company_enrichment SET last_enriched_at = CURRENT_DATE - INTERVAL 1 MONTH "
            "WHERE company_number = ?",
            [REUSE],
        )
        con.close()
        _summary, directors, _backfill, financials = self.run_patched(
            self.write_input([REUSE])
        )
        directors.assert_not_called()
        financials.assert_not_called()

    def test_refresh_targets_every_found_company(self):
        self.seed()
        input_path = self.write_input(INPUT_NUMBERS)
        summary, directors, _backfill, financials = self.run_patched(input_path, refresh=True)

        found = [n for n in INPUT_NUMBERS if n != MISSING]
        self.assertCountEqual(self.directors_targets(directors), found)
        self.assertCountEqual(self.financials_targets(financials), found)
        self.assertEqual(summary.directors_fetched, len(found))
        self.assertEqual(summary.financials_fetched, len(found))
        # Bulk-sourced seeded rows are never re-fetched, even on refresh.
        self.assertEqual(self.client.calls, [NEW, MISSING])

    def test_nothing_to_fetch_skips_enrichers(self):
        self.seed()
        input_path = self.write_input([REUSE])
        summary, directors, backfill, financials = self.run_patched(input_path)
        directors.assert_not_called()
        financials.assert_not_called()
        backfill.assert_not_called()
        self.assertEqual(summary.directors_fetched, 0)
        self.assertEqual(summary.financials_fetched, 0)
        self.assertEqual(summary.looked_up, 0)
        self.assertTrue(self.output_path.exists())

    def test_legacy_directors_json_triggers_one_backfill(self):
        self.seed(legacy_directors=True)
        input_path = self.write_input([REUSE, BOTH])
        _summary, _directors, backfill, _financials = self.run_patched(input_path)
        backfill.assert_called_once()
        args = backfill.call_args
        self.assertEqual(Path(args.args[0] if args.args else args.kwargs["db_path"]), self.db_path)

    def test_legacy_directors_outside_upload_do_not_trigger_backfill(self):
        self.seed(legacy_directors=True)
        input_path = self.write_input([BOTH])
        _summary, _directors, backfill, _financials = self.run_patched(input_path)
        backfill.assert_not_called()

    def test_workbook_lists_companies_in_input_order_with_invalid_last(self):
        self.seed()
        input_path = self.write_input(INPUT_NUMBERS, invalid=["bad!"])
        self.run_patched(input_path)

        ws = openpyxl.load_workbook(self.output_path)["Companies"]
        header = [c.value for c in ws[1]]
        self.assertEqual(header[0], "Company Number")
        col = {name: i for i, name in enumerate(header)}
        rows = {r[0]: r for r in ws.iter_rows(min_row=2, values_only=True)}
        self.assertEqual(
            [r[0] for r in ws.iter_rows(min_row=2, values_only=True)],
            [*INPUT_NUMBERS, "bad!"],
        )
        self.assertEqual(rows[NEW][col["Company Name"]], "New Api Ltd")
        self.assertEqual(rows[REUSE][col["Company Name"]], f"Seeded {REUSE}")
        self.assertEqual(rows[MISSING][col["Notes"]], "Not found at Companies House")
        self.assertEqual(rows["bad!"][col["Notes"]], "Invalid company number")
        self.assertEqual(
            rows[REUSE][col["Directors"]],
            f"SMITH, John ({date.today().year - 1970}); OLD, Entry ({date.today().year - 1970})",
        )

    def test_progress_reports_each_stage_in_order(self):
        self.seed()
        lines: list[str] = []
        self.run_patched(self.write_input(INPUT_NUMBERS), progress=lines.append)
        stages = ["Reading file", "Looking up", "Fetching directors", "Fetching accounts",
                  "Writing workbook"]
        positions = []
        for stage in stages:
            with self.subTest(stage=stage):
                matches = [i for i, line in enumerate(lines) if stage.lower() in line.lower()]
                self.assertTrue(matches, f"no progress line for {stage!r}: {lines}")
                positions.append(matches[0])
        self.assertEqual(positions, sorted(positions))

    def test_per_company_progress_is_reported_with_counts(self):
        self.seed()

        def report_each(_db_path, _data_dir, **kwargs):
            targets = kwargs.get("company_numbers") or kwargs.get("ids")
            for done in range(1, len(targets) + 1):
                kwargs["progress"](done, len(targets))
            return {}

        lines: list[str] = []
        with patch(ENRICH_DIRECTORS, side_effect=report_each), \
                patch(BACKFILL_DIRECTORS, return_value={}), \
                patch(ENRICH_FINANCIALS, side_effect=report_each), \
                patch(PROFILE_CLIENT, return_value=self.client):
            run_upload(self.db_path, self.data_dir, self.write_input([BOTH, DIRS_ONLY]),
                       self.output_path, progress=lines.append)

        self.assertIn("Fetching directors 1/2 (50%)", [l.split(" · ")[0] for l in lines])
        self.assertIn("Fetching directors 2/2 (100%)", [l.split(" · ")[0] for l in lines])
        self.assertIn("Fetching accounts 1/1 (100%)", [l.split(" · ")[0] for l in lines])

    def test_unreadable_input_raises_value_error_without_output(self):
        path = self.tmp / "input.csv"
        path.write_text("Name,City\nAcme Ltd,London\n")
        with self.assertRaises(ValueError):
            self.run_patched(path)
        self.assertFalse(self.output_path.exists())

    def test_cancelled_before_start_raises_without_enrichment_or_output(self):
        self.seed()
        cancel = threading.Event()
        cancel.set()
        with patch(ENRICH_DIRECTORS) as directors, patch(ENRICH_FINANCIALS) as financials, \
                patch(BACKFILL_DIRECTORS) as backfill, patch(PROFILE_CLIENT, return_value=self.client):
            with self.assertRaises(OperationCancelled):
                run_upload(
                    self.db_path, self.data_dir, self.write_input(INPUT_NUMBERS),
                    self.output_path, cancel_event=cancel,
                )
        directors.assert_not_called()
        financials.assert_not_called()
        backfill.assert_not_called()
        self.assertEqual(self.client.calls, [])
        self.assertFalse(self.output_path.exists())

    def test_cancel_event_forwarded_to_enrichers(self):
        self.seed()
        cancel = threading.Event()
        _summary, directors, _backfill, financials = self.run_patched(
            self.write_input([BOTH]), cancel_event=cancel
        )
        self.assertIs(directors.call_args.kwargs["cancel_event"], cancel)
        self.assertIs(financials.call_args.kwargs["cancel_event"], cancel)


class ChBulkUploadProfileTests(_PipelineCase):
    def test_upload_profile_uses_instance_db_and_data_dir(self):
        self.seed()
        ch = ChBulk(data_dir=self.data_dir, db_path=self.db_path)
        with patch(ENRICH_DIRECTORS, return_value={}) as directors, \
                patch(BACKFILL_DIRECTORS, return_value={}), \
                patch(ENRICH_FINANCIALS, return_value={}), \
                patch(PROFILE_CLIENT, return_value=self.client):
            summary = ch.upload_profile(self.write_input([BOTH, NEW]), self.output_path)

        self.assertEqual(summary.requested, 2)
        self.assertEqual(summary.looked_up, 1)
        self.assertTrue(self.output_path.exists())
        args = directors.call_args
        db_arg = args.args[0] if args.args else args.kwargs["db_path"]
        data_arg = args.args[1] if len(args.args) > 1 else args.kwargs["data_dir"]
        self.assertEqual(Path(db_arg), self.db_path)
        self.assertEqual(Path(data_arg), self.data_dir)

    def test_fresh_install_without_db_folder(self):
        # Default layout puts the DB in data/db/, which a fresh install doesn't have yet.
        ch = ChBulk(data_dir=self.data_dir)
        self.assertFalse(ch.db_path.parent.exists())
        with patch(ENRICH_DIRECTORS, return_value={}), \
                patch(BACKFILL_DIRECTORS, return_value={}), \
                patch(ENRICH_FINANCIALS, return_value={}), \
                patch(PROFILE_CLIENT, return_value=self.client):
            summary = ch.upload_profile(self.write_input([NEW]), self.output_path)

        self.assertEqual(summary.looked_up, 1)
        self.assertTrue(self.output_path.exists())


if __name__ == "__main__":
    unittest.main()
