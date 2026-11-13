"""Tests for AUDIT-01: packaged revenue-band defaults.

A data-directory revenue_bands.csv takes precedence; when it is
absent, the bands packaged with the code are used. Band CSV text
round-trips through the parser used by the Settings tab.
"""

from __future__ import annotations

import csv
import io
import tempfile
import unittest
from importlib import resources
from pathlib import Path

from ch_bulk.companies_house.revenue_model import (
    RevenueBand,
    bands_to_csv_text,
    default_revenue_bands,
    delete_custom_revenue_bands,
    load_revenue_bands,
    parse_revenue_bands_text,
    resolve_revenue_bands,
    save_revenue_bands,
)
from ch_bulk.core.paths import revenue_bands_path


class PackagedBandsTests(unittest.TestCase):
    def test_packaged_bands_file_is_shipped_with_the_package(self) -> None:
        packaged = resources.files("ch_bulk").joinpath("data/revenue_bands.csv")
        self.assertTrue(packaged.is_file())

    def test_default_bands_match_the_original_csv(self) -> None:
        bands = default_revenue_bands()
        self.assertEqual(len(bands), 9)
        self.assertEqual(bands[0].min_employees, 1)
        self.assertEqual(bands[-1].min_employees, 1000)
        self.assertEqual(bands[-1].max_employees, 99999)
        self.assertEqual(bands[4].midpoint_gbp, 3000000.0)

    def test_missing_custom_file_falls_back_to_packaged_bands(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bands, is_custom = resolve_revenue_bands(tmpdir)
            self.assertFalse(is_custom)
            self.assertEqual(bands, default_revenue_bands())
            self.assertEqual(load_revenue_bands(tmpdir), default_revenue_bands())

    def test_custom_file_takes_precedence_over_packaged_bands(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            custom = [
                RevenueBand(1, 49, 1000.0, 2000.0, 1500.0),
                RevenueBand(50, 99999, 2000.0, 4000.0, 3000.0),
            ]
            save_revenue_bands(tmpdir, custom)
            bands, is_custom = resolve_revenue_bands(tmpdir)
            self.assertTrue(is_custom)
            self.assertEqual(bands, custom)
            self.assertEqual(load_revenue_bands(tmpdir), custom)

    def test_delete_custom_file_restores_packaged_bands(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            save_revenue_bands(
                tmpdir,
                [RevenueBand(1, 99999, 1000.0, 2000.0, 1500.0)],
            )
            self.assertTrue(delete_custom_revenue_bands(tmpdir))
            self.assertFalse(revenue_bands_path(tmpdir).exists())
            self.assertFalse(delete_custom_revenue_bands(tmpdir))
            bands, is_custom = resolve_revenue_bands(tmpdir)
            self.assertFalse(is_custom)
            self.assertEqual(bands, default_revenue_bands())


class BandCsvTextTests(unittest.TestCase):
    def test_round_trip_preserves_values(self) -> None:
        bands = default_revenue_bands()
        reparsed = parse_revenue_bands_text(bands_to_csv_text(bands))
        self.assertEqual(reparsed, bands)

    def test_open_ended_max_is_blank_and_parses_back(self) -> None:
        import sys

        bands = [RevenueBand(1000, sys.maxsize, 1000.0, 2000.0, 1500.0)]
        text = bands_to_csv_text(bands)
        self.assertIn("1000,,1000,2000,1500", text)
        self.assertEqual(
            parse_revenue_bands_text(text)[0].max_employees, sys.maxsize
        )

    def test_invalid_text_is_rejected(self) -> None:
        bad_texts = [
            "",
            "min_employees,max_employees,low_gbp,high_gbp\n1,4,1,2\n",
            "min_employees,max_employees,low_gbp,high_gbp,midpoint_gbp\n"
            "x,4,1,2,1500\n",
            "min_employees,max_employees,low_gbp,high_gbp,midpoint_gbp\n"
            "10,4,1,2,1500\n",
            "min_employees,max_employees,low_gbp,high_gbp,midpoint_gbp\n"
            "1,4,2000,1000,1500\n",
        ]
        for text in bad_texts:
            with self.subTest(text=text), self.assertRaises(ValueError):
                parse_revenue_bands_text(text)

    def test_save_writes_csv_readable_by_csv_module(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = save_revenue_bands(tmpdir, default_revenue_bands())
            with path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 9)
            self.assertEqual(rows[0]["min_employees"], "1")
            self.assertEqual(rows[-1]["max_employees"], "99999")


if __name__ == "__main__":
    unittest.main()
