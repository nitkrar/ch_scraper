"""Regression tests for shared path helpers and CH input continuity."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ch_bulk import ChBulk
from ch_bulk.companies_house.downloader import download_bulk_data
from ch_bulk.core.paths import ch_input_dir, cqc_input_dir, default_db_path


class PathHelperTests(unittest.TestCase):
    def test_ch_input_dir_matches_chbulk_ch_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ch = ChBulk(data_dir=tmpdir)
            self.assertEqual(ch.ch_dir, ch_input_dir(tmpdir))

    def test_cqc_input_dir_matches_chbulk_cqc_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ch = ChBulk(data_dir=tmpdir)
            self.assertEqual(ch.cqc_dir, cqc_input_dir(tmpdir))

    def test_chbulk_db_path_follows_data_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ch = ChBulk(data_dir=tmpdir)
            self.assertEqual(ch.db_path, default_db_path(tmpdir))

    def test_ch_downloader_writes_to_input_ch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            expected_dir = ch_input_dir(tmpdir)
            with patch("ch_bulk.companies_house.downloader._download_file"), patch(
                "ch_bulk.companies_house.downloader._extract_zip",
                return_value=[],
            ) as extract_zip:
                result = download_bulk_data(
                    data_dir=tmpdir,
                    month="2026-05",
                    strict=False,
                )

            self.assertEqual(result, [])
            self.assertTrue(expected_dir.is_dir())
            self.assertEqual(extract_zip.call_count, 7)
            self.assertTrue(
                all(call.args[1] == expected_dir for call in extract_zip.call_args_list)
            )
