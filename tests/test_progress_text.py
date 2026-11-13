"""Progress wording for long-running fetches."""

from __future__ import annotations

import unittest

from ch_bulk.companies_house.upload_pipeline import progress_message


class ProgressMessageTests(unittest.TestCase):
    def test_counts_and_percentage(self):
        self.assertEqual(
            progress_message("Fetching directors", 0, 855, elapsed_seconds=0),
            "Fetching directors 0/855 (0%)",
        )

    def test_time_left_extrapolates_from_the_rate_so_far(self):
        # 100 companies in 60s -> 700 left at the same rate = 7 minutes.
        self.assertEqual(
            progress_message("Fetching accounts", 100, 800, elapsed_seconds=60),
            "Fetching accounts 100/800 (12%) · about 7 min left",
        )

    def test_short_and_long_estimates(self):
        self.assertEqual(
            progress_message("Fetching directors", 99, 100, elapsed_seconds=99),
            "Fetching directors 99/100 (99%) · under a minute left",
        )
        self.assertEqual(
            progress_message("Fetching directors", 1_000, 20_000, elapsed_seconds=600),
            "Fetching directors 1,000/20,000 (5%) · about 3 h 10 min left",
        )

    def test_finished_stage_has_no_estimate(self):
        self.assertEqual(
            progress_message("Fetching directors", 5, 5, elapsed_seconds=30),
            "Fetching directors 5/5 (100%)",
        )


if __name__ == "__main__":
    unittest.main()
