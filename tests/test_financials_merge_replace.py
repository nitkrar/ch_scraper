"""Merge of staged financial rows into company_enrichment, plus the schema and
contract changes for the balance-sheet columns.

Staged rows are written as raw JSONL (the shape both StagedFinancialRow and
scripts/adhoc/load_pdf_extractions.py produce) and loaded through
load_financials_staging, then read back from company_enrichment.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb

from ch_bulk.companies_house.ch_enricher import backfill_directors, enrich_revenue
from ch_bulk.companies_house.financials_contracts import (
    ParsedFinancialFacts,
    StagedFinancialRow,
)
from ch_bulk.companies_house.financials_enricher import load_financials_staging
from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.db.staging import staging_path
from ch_bulk.db.sync_batches import insert_sync_batch

NUMBER = "20000001"
NEW_COLUMNS = (
    "debtors",
    "creditors_within_one_year",
    "creditors_after_one_year",
    "provisions_for_liabilities",
    "deferred_tax",
)
FINANCIAL_COLUMNS = (
    "revenue",
    "employee_count",
    "filing_period_start",
    "filing_period_end",
    "gross_profit",
    "profit_before_tax",
    "profit_after_tax",
    "fixed_assets",
    "current_assets",
    "total_assets",
    "net_assets",
    "net_current_assets",
    *NEW_COLUMNS,
    "filing_id",
    "filing_format",
    "filing_age_months",
)
DIRECTOR_COLUMNS = (
    "avg_director_age",
    "min_director_age",
    "max_director_age",
    "directors_over_60",
    "all_directors_60_plus",
)
ERROR_STATUSES = (
    "request_error",
    "document_metadata_error",
    "document_download_error",
    "no_document_resource",
    "ixbrl_parse_error",
    "pdf_parse_error",
)
OCR_SOURCES = ("filed_accounts_ocr_pdf", "partial_ocr_no_pl", "filed_accounts_pdf_ocr")

# A previously merged filing. Values are distinct from every staged value below
# so a kept value can't be confused with a replaced one.
STORED_FILING = {
    "revenue": 500000.0,
    "revenue_source": "filed_accounts_ixbrl",
    "employee_count": 11,
    "filing_period_start": date(2024, 1, 1),
    "filing_period_end": date(2024, 12, 31),
    "gross_profit": 150000.0,
    "profit_before_tax": 110000.0,
    "profit_after_tax": 90000.0,
    "fixed_assets": 400000.0,
    "current_assets": 550000.0,
    "total_assets": 950000.0,
    "net_assets": 180000.0,
    "net_current_assets": 50000.0,
    "filing_id": "old-file",
    "filing_format": "ixbrl",
    "filing_age_months": 14,
}
STORED_NEW_COLUMNS = {
    "debtors": 61000.0,
    "creditors_within_one_year": 62000.0,
    "creditors_after_one_year": 63000.0,
    "provisions_for_liabilities": 64000.0,
    "deferred_tax": 65000.0,
}
STORED_DIRECTORS = {
    "avg_director_age": 63,
    "min_director_age": 60,
    "max_director_age": 66,
    "directors_over_60": 2,
    "all_directors_60_plus": True,
}


def staged(**overrides) -> dict:
    """A staged row for a newly parsed iXBRL filing with every figure present."""
    row = {
        "company_number": NUMBER,
        "filing_id": "new-file",
        "filing_date": "2026-03-12",
        "filing_format": "ixbrl",
        "filing_period_start": "2025-01-01",
        "filing_period_end": "2025-12-31",
        "revenue": 1750000.0,
        "employee_count": 42,
        "gross_profit": 600000.0,
        "profit_before_tax": 120000.0,
        "profit_after_tax": 100000.0,
        "fixed_assets": 410000.0,
        "current_assets": 560000.0,
        "total_assets": 970000.0,
        "net_assets": 700000.0,
        "net_current_assets": 250000.0,
        "debtors": 71000.0,
        "creditors_within_one_year": 72000.0,
        "creditors_after_one_year": 73000.0,
        "provisions_for_liabilities": 74000.0,
        "deferred_tax": 75000.0,
        "filing_age_months": 3,
        "parse_status": "ok",
        "parse_failure_reason": None,
        "fetched_at": "2026-05-25T12:30:00Z",
        "profit_loss_exempt": False,
    }
    row.update(overrides)
    return row


def blank_figures(**overrides) -> dict:
    """A staged row carrying no figures at all (errors, scans, no filing)."""
    row = staged(**{column: None for column in FINANCIAL_COLUMNS})
    row["filing_date"] = None
    row.update(overrides)
    return row


class _MergeCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.data_dir = Path(self._tmp.name)
        self.db_path = self.data_dir / "financials.duckdb"
        con = duckdb.connect(str(self.db_path))
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
                    sic_code_4 TEXT,
                    accounts_category TEXT,
                    accounts_last_made_up DATE
                )
                """
            )
            ensure_pipeline_schema(con)
            con.execute(
                "INSERT INTO companies (company_number, company_name, sic_code_1) "
                "VALUES (?, 'Merge Fixture Ltd', '88100')",
                [NUMBER],
            )
        finally:
            con.close()

    def store(self, **columns) -> None:
        values = {"company_number": NUMBER, **columns}
        con = duckdb.connect(str(self.db_path))
        try:
            con.execute(
                f"INSERT INTO company_enrichment ({', '.join(values)}, last_enriched_at) "
                f"VALUES ({', '.join('?' for _ in values)}, TIMESTAMP '2026-01-01 00:00:00')",
                list(values.values()),
            )
        finally:
            con.close()

    def load(self, *rows: dict) -> None:
        con = duckdb.connect(str(self.db_path))
        try:
            batch_id = insert_sync_batch(con, sync_type="financials", mode="incremental")
        finally:
            con.close()
        path = staging_path(self.data_dir, sync_type="financials", batch_id=batch_id)
        path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        load_financials_staging(self.data_dir, self.db_path, batch_id=batch_id)

    def stored(self, *columns: str) -> dict:
        con = duckdb.connect(str(self.db_path), read_only=True)
        try:
            row = con.execute(
                f"SELECT {', '.join(columns)} FROM company_enrichment WHERE company_number = ?",
                [NUMBER],
            ).fetchone()
        finally:
            con.close()
        self.assertIsNotNone(row, "company_enrichment row missing")
        return dict(zip(columns, row))


def _expected_from_staged(row: dict) -> dict:
    expected = {column: row[column] for column in FINANCIAL_COLUMNS}
    for column in ("filing_period_start", "filing_period_end"):
        if expected[column] is not None:
            expected[column] = date.fromisoformat(expected[column])
    return expected


class ReplacingRowTests(_MergeCase):
    def test_every_financial_column_takes_the_staged_value(self):
        self.store(**STORED_FILING, **STORED_NEW_COLUMNS)
        row = staged()
        self.load(row)
        self.assertEqual(self.stored(*FINANCIAL_COLUMNS), _expected_from_staged(row))

    def test_values_missing_from_the_new_filing_are_blanked(self):
        self.store(**STORED_FILING)
        row = staged(
            revenue=None,
            gross_profit=None,
            net_assets=None,
            profit_after_tax=None,
            parse_status="partial",
            parse_failure_reason="revenue_missing,gross_profit_missing",
        )
        self.load(row)
        got = self.stored("revenue", "gross_profit", "net_assets", "profit_after_tax",
                          "employee_count", "filing_id", "revenue_source")
        self.assertEqual(got, {
            "revenue": None,
            "gross_profit": None,
            "net_assets": None,
            "profit_after_tax": None,
            "employee_count": 42,
            "filing_id": "new-file",
            "revenue_source": None,
        })

    def test_new_columns_missing_from_the_new_filing_are_blanked(self):
        self.store(**STORED_FILING, **STORED_NEW_COLUMNS)
        row = staged(debtors=None, provisions_for_liabilities=None, deferred_tax=None)
        self.load(row)
        self.assertEqual(self.stored(*NEW_COLUMNS), {
            "debtors": None,
            "creditors_within_one_year": 72000.0,
            "creditors_after_one_year": 73000.0,
            "provisions_for_liabilities": None,
            "deferred_tax": None,
        })

    def test_partial_row_with_only_new_fields_replaces(self):
        self.store(**STORED_FILING, **STORED_NEW_COLUMNS)
        row = blank_figures(
            filing_id="new-file", filing_format="ixbrl", filing_age_months=3,
            debtors=81000.0, creditors_within_one_year=82000.0,
            parse_status="partial", parse_failure_reason="revenue_missing",
        )
        self.load(row)
        self.assertEqual(self.stored("revenue_source", *FINANCIAL_COLUMNS),
                         {"revenue_source": None, **_expected_from_staged(row)})

    def test_director_columns_are_untouched(self):
        self.store(**STORED_FILING, **STORED_DIRECTORS)
        self.load(staged())
        self.assertEqual(self.stored(*DIRECTOR_COLUMNS), STORED_DIRECTORS)


class ReplacingRowRevenueSourceTests(_MergeCase):
    def test_revenue_present_maps_by_filing_format(self):
        cases = {
            "ixbrl": "filed_accounts_ixbrl",
            "pdf": "filed_accounts_pdf",
            "ocr_pdf": "filed_accounts_ocr_pdf",
        }
        for filing_format, expected in cases.items():
            with self.subTest(filing_format=filing_format):
                self.setUp()
                self.store(**{**STORED_FILING, "revenue_source": "employee_band_lookup"})
                self.load(staged(filing_format=filing_format))
                self.assertEqual(self.stored("revenue_source")["revenue_source"], expected)

    def test_stored_estimate_does_not_survive(self):
        self.store(
            revenue=500000.0,
            revenue_source="employee_band_lookup",
            employee_count=11,
            filing_id="old-file",
            filing_format="ixbrl",
        )
        self.load(staged(revenue=None, parse_status="partial",
                         parse_failure_reason="revenue_missing"))
        self.assertEqual(self.stored("revenue", "revenue_source"),
                         {"revenue": None, "revenue_source": None})

    def test_older_filing_source_does_not_survive(self):
        self.store(**{**STORED_FILING, "revenue_source": "filed_accounts_pdf", "filing_format": "pdf"})
        self.load(staged(revenue=None, parse_status="partial",
                         parse_failure_reason="revenue_missing"))
        self.assertEqual(self.stored("revenue", "revenue_source"),
                         {"revenue": None, "revenue_source": None})

    def test_partial_no_revenue_decided_from_staged_row_alone(self):
        self.store(**{**STORED_FILING, "revenue_source": "employee_band_lookup"})
        self.load(staged(
            revenue=None, gross_profit=None, profit_before_tax=None, profit_after_tax=None,
            parse_status="partial", profit_loss_exempt=True,
            parse_failure_reason="revenue_missing,gross_profit_missing",
        ))
        self.assertEqual(self.stored("revenue", "revenue_source"),
                         {"revenue": None, "revenue_source": "partial_no_revenue"})

    def test_partial_ocr_no_pl_decided_from_staged_row_alone(self):
        self.store(**{**STORED_FILING, "revenue_source": "employee_band_lookup"})
        self.load(staged(
            filing_format="ocr_pdf",
            revenue=None, gross_profit=None, profit_before_tax=None, profit_after_tax=None,
            parse_status="partial", profit_loss_exempt=False,
            parse_failure_reason="revenue_missing",
        ))
        self.assertEqual(self.stored("revenue", "revenue_source"),
                         {"revenue": None, "revenue_source": "partial_ocr_no_pl"})

    def test_pdf_no_text_layer_for_a_different_filing_replaces(self):
        for stored_source in ("filed_accounts_ixbrl", *OCR_SOURCES):
            with self.subTest(stored_source=stored_source):
                self.setUp()
                self.store(**{**STORED_FILING, "revenue_source": stored_source, "filing_id": "scan-old"},
                           **STORED_NEW_COLUMNS)
                self.load(blank_figures(filing_id="scan-new", filing_format="pdf",
                                        parse_status="pdf_no_text_layer",
                                        parse_failure_reason="pdf_no_text_layer"))
                got = self.stored("revenue", "revenue_source", "net_assets", "filing_id",
                                  "filing_format", *NEW_COLUMNS)
                self.assertEqual(got, {
                    "revenue": None,
                    "revenue_source": "pdf_no_text_layer",
                    "net_assets": None,
                    "filing_id": "scan-new",
                    "filing_format": "pdf",
                    **{column: None for column in NEW_COLUMNS},
                })

    def test_pdf_no_text_layer_same_filing_without_ocr_source_replaces(self):
        self.store(**{**STORED_FILING, "revenue_source": "employee_band_lookup", "filing_id": "scan-1"})
        self.load(blank_figures(filing_id="scan-1", filing_format="pdf",
                                parse_status="pdf_no_text_layer",
                                parse_failure_reason="pdf_no_text_layer"))
        self.assertEqual(self.stored("revenue", "revenue_source", "net_assets"),
                         {"revenue": None, "revenue_source": "pdf_no_text_layer", "net_assets": None})


class NonReplacingRowTests(_MergeCase):
    KEPT = ("revenue", "revenue_source", "employee_count", "gross_profit", "net_assets",
            "current_assets", "filing_period_end", "filing_format")

    def test_error_rows_keep_stored_values(self):
        for status in ERROR_STATUSES:
            with self.subTest(parse_status=status):
                self.setUp()
                self.store(**STORED_FILING)
                self.load(blank_figures(filing_id="new-file", filing_format="ixbrl",
                                        parse_status=status, parse_failure_reason=status))
                expected = {column: STORED_FILING[column] for column in self.KEPT}
                self.assertEqual(self.stored(*self.KEPT), expected)
                # filing_id is COALESCEd like every other column today.
                self.assertEqual(self.stored("filing_id")["filing_id"], "new-file")

    def test_error_rows_keep_stored_new_columns(self):
        self.store(**STORED_FILING, **STORED_NEW_COLUMNS)
        self.load(blank_figures(filing_id="new-file", filing_format="ixbrl",
                                parse_status="ixbrl_parse_error", parse_failure_reason="boom"))
        self.assertEqual(self.stored(*NEW_COLUMNS), STORED_NEW_COLUMNS)

    def test_error_row_for_a_filing_clears_a_stored_no_recent_filing(self):
        # A filing now exists even though it could not be read.
        for status in ERROR_STATUSES:
            with self.subTest(parse_status=status):
                self.setUp()
                self.store(revenue_source="no_recent_filing")
                self.load(blank_figures(filing_id="new-file", filing_format="ixbrl",
                                        parse_status=status, parse_failure_reason=status))
                self.assertEqual(
                    self.stored("revenue_source", "filing_id"),
                    {"revenue_source": None, "filing_id": "new-file"},
                )

    def test_no_filing_row_keeps_stored_values(self):
        self.store(**STORED_FILING, **STORED_NEW_COLUMNS)
        self.load(blank_figures(filing_id=None, filing_format=None, parse_status="no_filing"))
        expected = {column: STORED_FILING[column] for column in self.KEPT}
        self.assertEqual(self.stored(*self.KEPT), expected)
        self.assertEqual(self.stored("filing_id")["filing_id"], "old-file")
        self.assertEqual(self.stored(*NEW_COLUMNS), STORED_NEW_COLUMNS)

    def test_no_filing_row_without_stored_row_marks_no_recent_filing(self):
        self.load(blank_figures(filing_id=None, filing_format=None, parse_status="no_filing"))
        self.assertEqual(self.stored("revenue", "revenue_source", *NEW_COLUMNS), {
            "revenue": None,
            "revenue_source": "no_recent_filing",
            **{column: None for column in NEW_COLUMNS},
        })

    def test_batch_of_only_no_filing_rows_loads(self):
        # e.g. an upload of one company that has never filed accounts: every
        # row lacks filing_format, so read_json_auto cannot infer its type.
        self.load(blank_figures(filing_id=None, filing_format=None, parse_status="no_filing"))
        self.assertEqual(self.stored("revenue_source")["revenue_source"], "no_recent_filing")

    def test_batch_of_only_error_rows_loads(self):
        self.store(**STORED_FILING)
        self.load(blank_figures(filing_format=None, filing_age_months=None,
                                parse_status="request_error"))
        self.assertEqual(self.stored("revenue")["revenue"], STORED_FILING["revenue"])

    def test_scan_of_the_same_ocr_recovered_filing_keeps_stored_values(self):
        for source in OCR_SOURCES:
            with self.subTest(stored_source=source):
                self.setUp()
                stored = {**STORED_FILING, "revenue_source": source, "filing_id": "scan-1",
                          "filing_format": "ocr_pdf"}
                if source == "partial_ocr_no_pl":
                    stored["revenue"] = None
                self.store(**stored, **STORED_NEW_COLUMNS)
                self.load(blank_figures(filing_id="scan-1", filing_format="pdf",
                                        parse_status="pdf_no_text_layer",
                                        parse_failure_reason="pdf_no_text_layer"))
                columns = ("revenue_source", *FINANCIAL_COLUMNS)
                expected = {column: {**stored, **STORED_NEW_COLUMNS}[column] for column in columns}
                self.assertEqual(self.stored(*columns), expected)


class StagingCompatibilityTests(_MergeCase):
    def test_staging_file_without_new_keys_loads(self):
        row = staged()
        for column in NEW_COLUMNS:
            del row[column]
        self.load(row)
        got = self.stored("revenue", "revenue_source", "net_assets", *NEW_COLUMNS)
        self.assertEqual(got, {
            "revenue": 1750000.0,
            "revenue_source": "filed_accounts_ixbrl",
            "net_assets": 700000.0,
            **{column: None for column in NEW_COLUMNS},
        })

    def test_staged_row_json_carries_new_fields(self):
        row = StagedFinancialRow(
            company_number=NUMBER,
            filing_id="new-file",
            filing_date="2026-03-12",
            filing_format="ixbrl",
            filing_period_start="2025-01-01",
            filing_period_end="2025-12-31",
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
            filing_age_months=3,
            parse_status="partial",
            parse_failure_reason=None,
            fetched_at="2026-05-25T12:30:00Z",
            debtors=71000.0,
            creditors_within_one_year=72000.0,
            creditors_after_one_year=73000.0,
            provisions_for_liabilities=74000.0,
            deferred_tax=75000.0,
        )
        payload = json.loads(row.to_json_line())
        self.assertEqual({column: payload[column] for column in NEW_COLUMNS}, {
            "debtors": 71000.0,
            "creditors_within_one_year": 72000.0,
            "creditors_after_one_year": 73000.0,
            "provisions_for_liabilities": 74000.0,
            "deferred_tax": 75000.0,
        })

        # The JSON line round-trips through the loader into the new columns.
        self.load(payload)
        self.assertEqual(self.stored(*NEW_COLUMNS), {
            "debtors": 71000.0,
            "creditors_within_one_year": 72000.0,
            "creditors_after_one_year": 73000.0,
            "provisions_for_liabilities": 74000.0,
            "deferred_tax": 75000.0,
        })

    def test_existing_constructors_default_new_fields_to_none(self):
        parsed = ParsedFinancialFacts(
            revenue=None, employee_count=None, filing_period_start=None, filing_period_end=None,
            gross_profit=None, profit_before_tax=None, profit_after_tax=None, fixed_assets=None,
            current_assets=None, total_assets=None, net_assets=None, net_current_assets=None,
            parse_status="ok", parse_failure_reason=None,
        )
        staged_row = StagedFinancialRow(
            company_number=NUMBER, filing_id=None, filing_date=None, filing_format=None,
            filing_period_start=None, filing_period_end=None, revenue=None, employee_count=None,
            gross_profit=None, profit_before_tax=None, profit_after_tax=None, fixed_assets=None,
            current_assets=None, total_assets=None, net_assets=None, net_current_assets=None,
            filing_age_months=None, parse_status="no_filing", parse_failure_reason=None,
            fetched_at="2026-05-25T12:30:00Z",
        )
        for column in NEW_COLUMNS:
            with self.subTest(column=column):
                self.assertIsNone(getattr(parsed, column))
                self.assertIsNone(getattr(staged_row, column))


class SchemaTests(unittest.TestCase):
    def _types(self, con) -> dict[str, str]:
        return {row[0]: row[1] for row in con.execute("DESCRIBE company_enrichment").fetchall()}

    def test_new_database_has_nullable_double_columns(self):
        con = duckdb.connect(":memory:")
        try:
            ensure_pipeline_schema(con)
            types = self._types(con)
        finally:
            con.close()
        self.assertEqual({column: types.get(column) for column in NEW_COLUMNS},
                         {column: "DOUBLE" for column in NEW_COLUMNS})

    def test_existing_database_is_migrated(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "old.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                ensure_pipeline_schema(con)
                con.execute(
                    "INSERT INTO company_enrichment (company_number, revenue, last_enriched_at) "
                    "VALUES ('30000001', 10.0, TIMESTAMP '2026-01-01 00:00:00')"
                )
                # Reduce to a pre-change table shape.
                for column in NEW_COLUMNS:
                    con.execute(f"ALTER TABLE company_enrichment DROP COLUMN IF EXISTS {column}")
            finally:
                con.close()

            con = duckdb.connect(str(db_path))
            try:
                ensure_pipeline_schema(con)
                types = self._types(con)
                row = con.execute(
                    f"SELECT revenue, {', '.join(NEW_COLUMNS)} FROM company_enrichment"
                ).fetchone()
            finally:
                con.close()
        self.assertEqual({column: types.get(column) for column in NEW_COLUMNS},
                         {column: "DOUBLE" for column in NEW_COLUMNS})
        self.assertEqual(row, (10.0, None, None, None, None, None))


class RowRebuildingWritersTests(_MergeCase):
    def test_revenue_estimate_keeps_new_columns(self):
        self.store(employee_count=75, filing_id="F1", filing_format="ixbrl", **STORED_NEW_COLUMNS)
        summary = enrich_revenue(self.db_path)
        self.assertEqual(summary["estimated"], 1)
        got = self.stored("revenue_source", *NEW_COLUMNS)
        self.assertEqual(got, {"revenue_source": "employee_band_lookup", **STORED_NEW_COLUMNS})

    def test_directors_backfill_keeps_new_columns(self):
        self.store(**STORED_FILING, **STORED_NEW_COLUMNS)
        batch_id = "00000000-0000-0000-0000-000000000001"
        con = duckdb.connect(str(self.db_path))
        try:
            con.execute(
                """
                INSERT INTO cqc_sync_batches (
                    batch_id, sync_type, mode, started_at, finished_at, status,
                    records_fetched, records_updated, error_count
                )
                VALUES (CAST(? AS UUID), 'ch_directors', 'fixture',
                        TIMESTAMP '2026-05-29 12:00:00', TIMESTAMP '2026-05-29 12:00:00',
                        'succeeded', 0, 0, 0)
                """,
                [batch_id],
            )
            payload = json.dumps([{
                "name": "DIR ONE",
                "officer_role": "director",
                "appointed_on": "2020-01-01",
                "date_of_birth": {"year": 1960},
            }])
            con.execute(
                """
                INSERT INTO cqc_api_responses (
                    batch_id, entity_type, entity_id, fetched_at, scrape_date, http_status, raw_json
                )
                VALUES (CAST(? AS UUID), 'ch_directors', ?, TIMESTAMP '2026-05-29 12:00:00',
                        DATE '2026-05-29', 200, CAST(? AS JSON))
                """,
                [batch_id, NUMBER, payload],
            )
        finally:
            con.close()

        summary = backfill_directors(self.db_path)
        self.assertEqual(summary["records_updated"], 1)
        got = self.stored("total_active_directors", "revenue", *NEW_COLUMNS)
        self.assertEqual(got, {
            "total_active_directors": 1,
            "revenue": STORED_FILING["revenue"],
            **STORED_NEW_COLUMNS,
        })


if __name__ == "__main__":
    unittest.main()
