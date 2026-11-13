# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for CH Bulk desktop app (Tkinter GUI)."""

import tomllib
from pathlib import Path

from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs

with open(Path(SPECPATH) / "pyproject.toml", "rb") as f:
    VERSION = tomllib.load(f)["project"]["version"]

# Collect DuckDB native libraries — PyInstaller misses these by default
duckdb_datas = collect_data_files("duckdb")
duckdb_binaries = collect_dynamic_libs("duckdb")

# Collect Tcl/Tk data files — required for Tkinter on macOS
tk_datas = collect_data_files("tkinter")
tk_binaries = collect_dynamic_libs("_tkinter")

# Collect the SQL files the pipelines read from ch_bulk/db/sql/ at runtime
ch_bulk_datas = collect_data_files("ch_bulk")

a = Analysis(
    ["ch_bulk/gui.py"],
    pathex=["."],
    datas=duckdb_datas + tk_datas + ch_bulk_datas,
    binaries=duckdb_binaries + tk_binaries,
    hiddenimports=[
        "ch_bulk",
        "ch_bulk.api",
        "ch_bulk.companies_house.query",
        "ch_bulk.companies_house.downloader",
        "ch_bulk.companies_house.processor",
        "ch_bulk.gui",
        "duckdb",
        "httpx",
        "rich",
    ],
    hookspath=[],
    runtime_hooks=[],
    excludes=["unittest", "test"],
)

pyz = PYZ(a.pure, a.zipped_data)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="CH Bulk",
    debug=False,
    strip=False,
    upx=False,
    console=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    name="CH Bulk",
)

app = BUNDLE(
    coll,
    name="CH Bulk.app",
    icon=None,
    bundle_identifier="com.nitkrar.ch-bulk",
    info_plist={
        "CFBundleName": "CH Bulk",
        "CFBundleDisplayName": "Companies House Bulk Data Explorer",
        "CFBundleVersion": VERSION,
        "CFBundleShortVersionString": VERSION,
        "NSHighResolutionCapable": True,
    },
)
