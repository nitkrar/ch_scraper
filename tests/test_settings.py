"""Regression tests for AUDIT-08 (settings file permissions) and
AUDIT-01 default data directory behaviour."""

from __future__ import annotations

import os
import stat
import tempfile
import unittest
from pathlib import Path

from ch_bulk.core.paths import DEFAULT_DATA_DIR, REPO_ROOT, _default_data_dir
from ch_bulk.core.settings import load_settings, save_settings


class DefaultDataDirTests(unittest.TestCase):
    def test_checkout_uses_repo_data_dir(self) -> None:
        self.assertEqual(DEFAULT_DATA_DIR, REPO_ROOT / "data")

    def test_source_tree_uses_package_root_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "pyproject.toml").write_text("", encoding="utf-8")
            self.assertEqual(
                _default_data_dir(package_root=root, home=root / "home"),
                root / "data",
            )

    def test_installed_package_uses_home_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            home = root / "home"
            self.assertEqual(
                _default_data_dir(package_root=root, home=home),
                home / ".ch_bulk" / "data",
            )

    def test_frozen_app_uses_package_root_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.assertEqual(
                _default_data_dir(
                    package_root=root, home=root / "home", frozen=True
                ),
                root / "data",
            )


class SettingsFileTests(unittest.TestCase):
    def test_save_and_load_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = {
                "api_keys": {"companies_house": "ch-key", "cqc": "cqc-key"},
                "theme": "alt",
            }
            save_settings(tmpdir, settings)
            loaded = load_settings(tmpdir)
            self.assertEqual(loaded["api_keys"], settings["api_keys"])
            self.assertEqual(loaded["theme"], "alt")

    def test_missing_and_corrupt_settings_return_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            defaults = load_settings(tmpdir)
            self.assertEqual(defaults["api_keys"]["companies_house"], "")
            path = Path(tmpdir) / "settings.json"
            path.write_text("{not json", encoding="utf-8")
            self.assertEqual(
                load_settings(tmpdir)["api_keys"]["companies_house"], ""
            )

    @unittest.skipIf(os.name != "posix", "POSIX file permissions only")
    def test_saved_settings_are_owner_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = save_settings(tmpdir, {"api_keys": {}, "theme": "clam"})
            mode = stat.S_IMODE(path.stat().st_mode)
            self.assertEqual(mode, 0o600)
            self.assertFalse((Path(tmpdir) / "settings.json.tmp").exists())


if __name__ == "__main__":
    unittest.main()
