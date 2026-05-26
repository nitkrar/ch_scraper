"""Focused tests for CH↔CQC matching."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import duckdb

from ch_bulk.bootstrap import ensure_pipeline_schema
from ch_bulk.matcher import match_companies_to_cqc


def _create_companies_table(con: duckdb.DuckDBPyConnection) -> None:
    previous_name_columns = ",\n".join(
        f"prev_name_{idx} TEXT" for idx in range(1, 11)
    )
    con.execute(
        f"""
        CREATE TABLE companies (
            company_number TEXT PRIMARY KEY,
            company_name TEXT,
            company_status TEXT,
            postcode TEXT,
            address_post_town TEXT,
            sic_code_1 TEXT,
            sic_code_2 TEXT,
            sic_code_3 TEXT,
            sic_code_4 TEXT,
            is_active BOOLEAN,
            {previous_name_columns}
        )
        """
    )


def _create_cqc_providers_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE cqc_providers (
            provider_id TEXT PRIMARY KEY,
            provider_name TEXT,
            postcode_prefixes_list VARCHAR[],
            is_active BOOLEAN
        )
        """
    )


def _insert_company(
    con: duckdb.DuckDBPyConnection,
    *,
    company_number: str,
    company_name: str,
    postcode: str,
    previous_names: list[str] | None = None,
    sic_code_1: str = "62012",
) -> None:
    previous_names = previous_names or []
    padded_previous_names = previous_names + [None] * (10 - len(previous_names))
    con.execute(
        """
        INSERT INTO companies VALUES (
            ?, ?, 'Active', ?, 'Townsville',
            ?, NULL, NULL, NULL,
            TRUE,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        [
            company_number,
            company_name,
            postcode,
            sic_code_1,
            *padded_previous_names[:10],
        ],
    )


def _insert_provider(
    con: duckdb.DuckDBPyConnection,
    *,
    provider_id: str,
    provider_name: str,
    postcode_prefixes: list[str],
) -> None:
    placeholders = ", ".join(["?"] * len(postcode_prefixes))
    con.execute(
        f"""
        INSERT INTO cqc_providers VALUES (
            ?, ?, [{placeholders}], TRUE
        )
        """,
        [provider_id, provider_name, *postcode_prefixes],
    )


def _insert_hsca_row(
    con: duckdb.DuckDBPyConnection,
    *,
    provider_id: str,
    location_id: str,
    provider_companies_house_number: str | None = None,
    domiciliary: bool = False,
    supported_living: bool = False,
    care_home_with_nursing: bool = False,
    care_home_without_nursing: bool = False,
    extra_care: bool = False,
    hospice_at_home: bool = False,
) -> None:
    con.execute(
        """
        INSERT INTO cqc_hsca_locations (
            location_id,
            provider_id,
            provider_companies_house_number,
            st_domiciliary_care_service,
            st_supported_living_service,
            st_care_home_with_nursing,
            st_care_home_without_nursing,
            st_extra_care_housing_services,
            st_hospice_services_at_home,
            bulk_imported_at,
            bulk_file_date,
            raw_row
        )
        VALUES (
            ?, ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, DATE '2026-05-24', CAST(? AS JSON)
        )
        """,
        [
            location_id,
            provider_id,
            provider_companies_house_number,
            domiciliary,
            supported_living,
            care_home_with_nursing,
            care_home_without_nursing,
            extra_care,
            hospice_at_home,
            datetime(2026, 5, 24, 12, 0, 0),
            json.dumps({"fixture": location_id}),
        ],
    )


def _insert_api_company_number(
    con: duckdb.DuckDBPyConnection,
    *,
    provider_id: str,
    company_number: str,
) -> None:
    con.execute(
        """
        INSERT INTO cqc_providers_enriched (
            provider_id,
            companies_house_number,
            enriched_at
        )
        VALUES (?, ?, TIMESTAMP '2026-05-24 12:00:00')
        """,
        [provider_id, company_number],
    )


def _load_match_rows(db_path: Path) -> list[tuple]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return con.execute(
            """
            SELECT
                company_number,
                cqc_provider_id,
                total_score,
                match_signals,
                status
            FROM ch_cqc_matches
            ORDER BY company_number, cqc_provider_id
            """
        ).fetchall()
    finally:
        con.close()


def _load_current_company_match_rows(db_path: Path) -> list[tuple]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return con.execute(
            """
            SELECT
                company_number,
                cqc_provider_id,
                total_score,
                status
            FROM current_company_match
            ORDER BY company_number, cqc_provider_id
            """
        ).fetchall()
    finally:
        con.close()


def _parse_signals(value: object) -> list[dict[str, object]]:
    if isinstance(value, str):
        return json.loads(value)
    return list(value)  # type: ignore[arg-type]


class MatcherTests(unittest.TestCase):
    def _create_db(self) -> Path:
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        db_path = Path(tmpdir.name) / "matcher.duckdb"
        con = duckdb.connect(str(db_path))
        try:
            _create_companies_table(con)
            ensure_pipeline_schema(con)
            _create_cqc_providers_table(con)
        finally:
            con.close()
        return db_path

    def test_hsca_direct_match_ignores_company_sic_code(self):
        db_path = self._create_db()
        con = duckdb.connect(str(db_path))
        try:
            _insert_company(
                con,
                company_number="12345678",
                company_name="Orchid Homecare Limited",
                postcode="DN4 9PE",
                sic_code_1="62012",
            )
            _insert_provider(
                con,
                provider_id="prov-1",
                provider_name="Orchid Homecare Ltd",
                postcode_prefixes=["DN4"],
            )
            _insert_hsca_row(
                con,
                provider_id="prov-1",
                location_id="loc-1",
                provider_companies_house_number="12345678",
                domiciliary=True,
            )
        finally:
            con.close()

        summary = match_companies_to_cqc(db_path, mode="all")
        self.assertEqual(summary["auto_confirmed"], 1)
        rows = _load_match_rows(db_path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0:3], ("12345678", "prov-1", 100))
        self.assertEqual(rows[0][4], "auto_confirmed")

        signals = _parse_signals(rows[0][3])
        self.assertEqual(signals[0]["signal"], "ch_number")
        self.assertEqual(signals[0]["score"], 100)
        self.assertTrue(signals[0]["affects_total"])

    def test_api_only_ch_number_match_is_auto_confirmed(self):
        db_path = self._create_db()
        con = duckdb.connect(str(db_path))
        try:
            _insert_company(
                con,
                company_number="76543210",
                company_name="API Match Care Limited",
                postcode="LS1 4AB",
                sic_code_1="86900",
            )
            _insert_provider(
                con,
                provider_id="prov-api",
                provider_name="API Match Care Ltd",
                postcode_prefixes=["LS1"],
            )
            _insert_hsca_row(
                con,
                provider_id="prov-api",
                location_id="loc-api",
                domiciliary=True,
            )
            _insert_api_company_number(
                con,
                provider_id="prov-api",
                company_number="76543210",
            )
        finally:
            con.close()

        summary = match_companies_to_cqc(db_path, mode="all")
        self.assertEqual(summary["auto_confirmed"], 1)
        rows = _load_match_rows(db_path)
        self.assertEqual(rows[0][0:3], ("76543210", "prov-api", 100))
        self.assertEqual(rows[0][4], "auto_confirmed")

        signals = _parse_signals(rows[0][3])
        self.assertEqual(signals[0]["signal"], "ch_number")
        self.assertEqual(
            signals[0]["sources"],
            [{"source": "api", "value": "76543210"}],
        )

    def test_fuzzy_name_plus_shared_outward_postcode_match_is_auto_confirmed(self):
        db_path = self._create_db()
        con = duckdb.connect(str(db_path))
        try:
            _insert_company(
                con,
                company_number="87654321",
                company_name="Bright Support Limited",
                postcode="SW1A 1AA",
                sic_code_1="70229",
            )
            _insert_provider(
                con,
                provider_id="prov-2",
                provider_name="Bright Support Ltd",
                postcode_prefixes=["SW1A"],
            )
            _insert_hsca_row(
                con,
                provider_id="prov-2",
                location_id="loc-2",
                domiciliary=True,
            )
        finally:
            con.close()

        summary = match_companies_to_cqc(db_path, mode="all")
        self.assertEqual(summary["matches_found"], 1)
        rows = _load_match_rows(db_path)
        self.assertEqual(rows[0][0:3], ("87654321", "prov-2", 90))
        self.assertEqual(rows[0][4], "auto_confirmed")

        signals = _parse_signals(rows[0][3])
        self.assertEqual(signals[0]["signal"], "fuzzy_name_outward_pc")
        self.assertEqual(signals[0]["score"], 90)
        self.assertGreaterEqual(signals[0]["ratio"], 90)

    def test_abc_care_vs_abc_cars_is_not_a_match(self):
        db_path = self._create_db()
        con = duckdb.connect(str(db_path))
        try:
            _insert_company(
                con,
                company_number="11111111",
                company_name="ABC Cars Ltd",
                postcode="M1 1AE",
            )
            _insert_provider(
                con,
                provider_id="prov-3",
                provider_name="ABC Care Services Ltd",
                postcode_prefixes=["M1"],
            )
            _insert_hsca_row(
                con,
                provider_id="prov-3",
                location_id="loc-3",
                supported_living=True,
            )
        finally:
            con.close()

        summary = match_companies_to_cqc(db_path, mode="all")
        self.assertEqual(summary["matches_found"], 0)
        self.assertEqual(_load_match_rows(db_path), [])

    def test_previous_name_is_stored_as_annotation_only(self):
        db_path = self._create_db()
        con = duckdb.connect(str(db_path))
        try:
            _insert_company(
                con,
                company_number="22222222",
                company_name="New Name Holdings Limited",
                postcode="BS1 5XX",
                previous_names=["Old Homecare Ltd"],
            )
            _insert_provider(
                con,
                provider_id="prov-4",
                provider_name="Old Homecare Limited",
                postcode_prefixes=["BS1"],
            )
            _insert_hsca_row(
                con,
                provider_id="prov-4",
                location_id="loc-4",
                provider_companies_house_number="22222222",
                domiciliary=True,
            )
        finally:
            con.close()

        match_companies_to_cqc(db_path, mode="all")
        rows = _load_match_rows(db_path)
        signals = _parse_signals(rows[0][3])
        previous_name_signal = next(
            signal
            for signal in signals
            if signal["signal"] == "prev_name_annotation"
        )
        self.assertFalse(previous_name_signal["affects_total"])
        self.assertEqual(previous_name_signal["value"], "Old Homecare Ltd")

    def test_full_rerun_preserves_user_rows_and_removes_stale_auto_rows(self):
        db_path = self._create_db()
        con = duckdb.connect(str(db_path))
        try:
            _insert_company(
                con,
                company_number="33333333",
                company_name="Stable Match Limited",
                postcode="L1 8JQ",
            )
            _insert_provider(
                con,
                provider_id="prov-5",
                provider_name="Stable Match Ltd",
                postcode_prefixes=["L1"],
            )
            _insert_hsca_row(
                con,
                provider_id="prov-5",
                location_id="loc-5",
                provider_companies_house_number="33333333",
                domiciliary=True,
            )
            con.execute(
                """
                INSERT INTO ch_cqc_matches VALUES
                    (
                        '33333333',
                        'prov-5',
                        100,
                        CAST('[{"signal":"manual","affects_total":false}]' AS JSON),
                        'user_confirmed',
                        TIMESTAMP '2026-05-24 12:00:00'
                    ),
                    (
                        '99999999',
                        'prov-stale',
                        90,
                        CAST('[{"signal":"stale","affects_total":true}]' AS JSON),
                        'auto_confirmed',
                        TIMESTAMP '2026-05-24 12:00:00'
                    )
                """
            )
        finally:
            con.close()

        summary = match_companies_to_cqc(db_path, mode="all")
        self.assertEqual(summary["preserved_user_rows"], 1)
        rows = _load_match_rows(db_path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0:2], ("33333333", "prov-5"))
        self.assertEqual(rows[0][4], "user_confirmed")

    def test_conflicting_hsca_and_api_company_numbers_create_needs_review_row(self):
        db_path = self._create_db()
        con = duckdb.connect(str(db_path))
        try:
            _insert_company(
                con,
                company_number="44444444",
                company_name="Conflict Care Limited",
                postcode="EC1A 1BB",
            )
            _insert_provider(
                con,
                provider_id="prov-6",
                provider_name="Conflict Care Ltd",
                postcode_prefixes=["EC1A"],
            )
            _insert_hsca_row(
                con,
                provider_id="prov-6",
                location_id="loc-6",
                provider_companies_house_number="44444444",
                care_home_without_nursing=True,
            )
            _insert_api_company_number(
                con,
                provider_id="prov-6",
                company_number="55555555",
            )
        finally:
            con.close()

        summary = match_companies_to_cqc(db_path, mode="all")
        self.assertEqual(summary["needs_review"], 1)
        rows = _load_match_rows(db_path)
        self.assertEqual(rows[0][0:3], ("44444444", "prov-6", 89))
        self.assertEqual(rows[0][4], "needs_review")

        signals = _parse_signals(rows[0][3])
        scored_signal = next(
            signal for signal in signals if signal["signal"] == "ch_number"
        )
        self.assertEqual(scored_signal["score"], 89)
        self.assertEqual(
            scored_signal["sources"],
            [
                {"source": "hsca", "value": "44444444"},
                {"source": "api", "value": "55555555"},
            ],
        )
        self.assertTrue(
            any(signal["signal"] == "ch_number_conflict" for signal in signals)
        )

    def test_current_company_match_prefers_provider_with_more_hsca_locations(self):
        db_path = self._create_db()
        con = duckdb.connect(str(db_path))
        try:
            _insert_company(
                con,
                company_number="66666666",
                company_name="Chain Care Limited",
                postcode="NE1 1AA",
                sic_code_1="68209",
            )
            _insert_provider(
                con,
                provider_id="prov-7a",
                provider_name="Chain Care Ltd",
                postcode_prefixes=["NE1"],
            )
            _insert_provider(
                con,
                provider_id="prov-7b",
                provider_name="Chain Care Ltd",
                postcode_prefixes=["NE1"],
            )
            _insert_hsca_row(
                con,
                provider_id="prov-7a",
                location_id="loc-7a-1",
                provider_companies_house_number="66666666",
                domiciliary=True,
            )
            _insert_hsca_row(
                con,
                provider_id="prov-7b",
                location_id="loc-7b-1",
                provider_companies_house_number="66666666",
                domiciliary=True,
            )
            _insert_hsca_row(
                con,
                provider_id="prov-7b",
                location_id="loc-7b-2",
                domiciliary=True,
            )
        finally:
            con.close()

        summary = match_companies_to_cqc(db_path, mode="all")
        self.assertEqual(summary["matches_found"], 2)
        rows = _load_match_rows(db_path)
        self.assertEqual(
            [(row[0], row[1], row[2], row[4]) for row in rows],
            [
                ("66666666", "prov-7a", 100, "auto_confirmed"),
                ("66666666", "prov-7b", 100, "auto_confirmed"),
            ],
        )

        current_rows = _load_current_company_match_rows(db_path)
        self.assertEqual(
            current_rows,
            [("66666666", "prov-7b", 100, "auto_confirmed")],
        )


if __name__ == "__main__":
    unittest.main()
