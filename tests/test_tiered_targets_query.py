"""Tests for the tiered_targets screening query helpers."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from ch_bulk.matching import query as tq


class TieredTargetsQueryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.db = self.tmp / "t.duckdb"
        con = duckdb.connect(str(self.db))
        try:
            con.execute(
                """
                CREATE TABLE tiered_targets (
                    company_number TEXT,
                    company_name TEXT,
                    postcode TEXT,
                    address_post_town TEXT,
                    avg_director_age INTEGER,
                    directors_over_60 INTEGER,
                    all_directors_60_plus BOOLEAN,
                    revenue DOUBLE,
                    revenue_source TEXT,
                    employee_count INTEGER,
                    total_active_directors INTEGER,
                    verdict TEXT,
                    classification_source TEXT,
                    evidence_quote TEXT,
                    source_url TEXT,
                    sources_agreement TEXT,
                    cqc_provider_id TEXT,
                    match_status TEXT,
                    match_score INTEGER,
                    class_pts INTEGER,
                    age_pts INTEGER,
                    size_pts INTEGER,
                    total_score INTEGER,
                    tier TEXT
                )
                """
            )
            con.execute(
                "CREATE TABLE cqc_providers (provider_id TEXT, provider_name TEXT)"
            )
            con.execute(
                """
                INSERT INTO cqc_providers (provider_id, provider_name) VALUES
                    ('P1', 'Alpha Group'),
                    ('P2', 'Beta Group')
                """
            )
            con.execute(
                """
                INSERT INTO tiered_targets (
                    company_number, company_name, cqc_provider_id, address_post_town,
                    directors_over_60, revenue, employee_count, total_active_directors,
                    total_score, tier
                ) VALUES
                    ('T1', 'Alpha Care Ltd', 'P1', 'LONDON', 2, 5000000.0, 40, 3, 6, 'Tier 1'),
                    ('T2', 'Beta Homecare Ltd', 'P2', 'LEEDS', 0, 1200000.0, 12, 2, 4, 'Tier 2'),
                    ('T3', 'Gamma Support Ltd', NULL, 'BATH', 1, NULL, NULL, 1, 2, 'Tier 3'),
                    ('T4', 'Delta Dormant Ltd', NULL, 'HULL', 0, NULL, NULL, 1, 0, 'Excluded')
                """
            )
        finally:
            con.close()

    def test_default_hides_excluded(self):
        rows, total = tq.query_tiered_targets(self.db)
        ids = {r["company_number"] for r in rows}
        self.assertEqual(ids, {"T1", "T2", "T3"})
        self.assertEqual(total, 3)

    def test_default_sorted_best_first(self):
        rows, _ = tq.query_tiered_targets(self.db)
        self.assertEqual([r["company_number"] for r in rows], ["T1", "T2", "T3"])

    def test_tier_filter(self):
        rows, total = tq.query_tiered_targets(self.db, tiers=["Tier 1"])
        self.assertEqual(total, 1)
        self.assertEqual(rows[0]["company_number"], "T1")

    def test_tiers_none_includes_excluded(self):
        rows, total = tq.query_tiered_targets(self.db, tiers=None)
        self.assertEqual(total, 4)

    def test_search_matches_provider_name(self):
        rows, _ = tq.query_tiered_targets(self.db, search="beta group")
        self.assertEqual({r["company_number"] for r in rows}, {"T2"})

    def test_search_matches_company_number(self):
        rows, _ = tq.query_tiered_targets(self.db, search="T3")
        self.assertEqual({r["company_number"] for r in rows}, {"T3"})

    def test_any_director_over_60(self):
        rows, _ = tq.query_tiered_targets(self.db, any_director_over_60=True)
        self.assertEqual({r["company_number"] for r in rows}, {"T1", "T3"})

    def test_min_revenue(self):
        rows, _ = tq.query_tiered_targets(self.db, min_revenue=2_000_000)
        self.assertEqual({r["company_number"] for r in rows}, {"T1"})

    def test_min_employees(self):
        rows, _ = tq.query_tiered_targets(self.db, min_employees=20)
        self.assertEqual({r["company_number"] for r in rows}, {"T1"})

    def test_invalid_sort_falls_back(self):
        rows, _ = tq.query_tiered_targets(self.db, sort_by="not_a_col")
        self.assertEqual(rows[0]["company_number"], "T1")  # total_score DESC default

    def test_missing_view_returns_empty(self):
        empty = self.tmp / "empty.duckdb"
        duckdb.connect(str(empty)).close()
        self.assertEqual(tq.query_tiered_targets(empty), ([], 0))

    def test_export_writes_filtered_rows(self):
        out = self.tmp / "targets.csv"
        n = tq.export_tiered_targets_csv(self.db, out, tiers=["Tier 1", "Tier 2"])
        self.assertEqual(n, 2)
        header = out.read_text().splitlines()[0]
        self.assertIn("total_score", header)
        self.assertIn("provider_name", header)


if __name__ == "__main__":
    unittest.main()
