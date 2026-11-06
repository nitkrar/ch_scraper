"""Upsert sanity check: compare a snapshot with the companies it should contain."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ch_bulk.companies_house.processor import SanityCheckError
from tests.test_company_profile import _process, _rows, _write_bulk_csv


def _companies(start: int, stop: int) -> list[tuple[str, str]]:
    return [(f"{i:08d}", f"Company {i}") for i in range(start, stop)]


class RowCountComparesActiveCompaniesTests(unittest.TestCase):
    """Snapshots hold live companies only; companies that dropped out of an
    earlier snapshot stay in the table as inactive and must not count."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)
        self.db_path = self.tmp / "db" / "ch.duckdb"

    def _month(self, scrape_date: str, companies) -> int:
        month_dir = self.tmp / scrape_date
        month_dir.mkdir(exist_ok=True)
        return _process(_write_bulk_csv(month_dir, scrape_date, companies), self.db_path)

    def test_inactive_pool_over_threshold_does_not_block_next_month(self):
        self._month("2026-04-01", _companies(1, 101))
        self._month("2026-05-01", _companies(5, 101))   # 4 of 100 drop out
        # 4 of the 96 active drop out (4.2%); 92 rows vs 100 in the table (8%).
        total = self._month("2026-06-01", _companies(9, 101))
        self.assertEqual(total, 100)
        self.assertEqual(
            _rows(self.db_path, "SELECT COUNT(*) FROM companies WHERE is_active"), [(92,)]
        )

    def test_reprocessing_the_loaded_month_passes(self):
        self._month("2026-04-01", _companies(1, 101))
        self._month("2026-05-01", _companies(5, 101))
        self._month("2026-06-01", _companies(9, 101))
        total = self._month("2026-06-01", _companies(9, 101))
        self.assertEqual(total, 100)

    def test_truncated_snapshot_is_still_blocked(self):
        self._month("2026-04-01", _companies(1, 101))
        with self.assertRaises(SanityCheckError):
            self._month("2026-05-01", _companies(1, 81))   # 20% fewer rows


if __name__ == "__main__":
    unittest.main()
