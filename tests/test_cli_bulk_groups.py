"""Smoke tests for cqc-bulk and hsca-bulk CLI subgroups."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from typer.testing import CliRunner

from ch_bulk.cli import app as cli_app

CLI_RUNNER = CliRunner()


class BulkCliGroupTests(unittest.TestCase):
    def test_cqc_bulk_group_listed_in_top_help(self) -> None:
        result = CLI_RUNNER.invoke(cli_app, ["--help"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        self.assertIn("cqc-bulk", result.stdout)

    def test_hsca_bulk_group_listed_in_top_help(self) -> None:
        result = CLI_RUNNER.invoke(cli_app, ["--help"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        self.assertIn("hsca-bulk", result.stdout)

    def test_cqc_bulk_subcommands_present(self) -> None:
        result = CLI_RUNNER.invoke(cli_app, ["cqc-bulk", "--help"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        for command_name in ("download", "process", "sync"):
            self.assertIn(command_name, result.stdout)

    def test_hsca_bulk_subcommands_present(self) -> None:
        result = CLI_RUNNER.invoke(cli_app, ["hsca-bulk", "--help"])
        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        for command_name in ("download", "process", "sync"):
            self.assertIn(command_name, result.stdout)

    def test_cqc_bulk_download_invokes_chbulk(self) -> None:
        with patch("ch_bulk.cli.ChBulk") as mock_ch:
            mock_ch.return_value.download_cqc.return_value = "/tmp/dummy.csv"
            result = CLI_RUNNER.invoke(
                cli_app,
                ["cqc-bulk", "download", "--data-dir", "/tmp"],
            )

        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        mock_ch.return_value.download_cqc.assert_called_once_with()

    def test_hsca_bulk_download_passes_target_date(self) -> None:
        with patch("ch_bulk.cli.ChBulk") as mock_ch:
            mock_ch.return_value.download_hsca.return_value = "/tmp/dummy.ods"
            result = CLI_RUNNER.invoke(
                cli_app,
                [
                    "hsca-bulk",
                    "download",
                    "--data-dir",
                    "/tmp",
                    "--target-date",
                    "2026-05-01",
                ],
            )

        self.assertEqual(result.exit_code, 0, msg=result.stdout)
        mock_ch.return_value.download_hsca.assert_called_once()
        _, kwargs = mock_ch.return_value.download_hsca.call_args
        self.assertEqual(kwargs.get("target_date"), "2026-05-01")
