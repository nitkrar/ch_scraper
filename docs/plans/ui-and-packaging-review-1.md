VERDICT: NEEDS_REVISION

## Summary Assessment

The plan is well-structured with correct overall architecture choices (Flask+HTMX, PyInstaller, GitHub Releases), and the dependency groups are sound. However, there are several critical issues around PyInstaller bundling, thread safety, the HTMX long-running operation pattern, and a column name mismatch that will cause runtime failures if not addressed.

## Critical Issues (must fix)

### 1. PyInstaller entry point calls `app.run()` directly -- will not work as a Mac .app

The spec file sets `console=False` and uses `ch_bulk/web/app.py` as the entry point. But `app.py` defines `create_app()` as a factory and `main()` at module level. PyInstaller needs a clear `if __name__ == "__main__"` guard or a dedicated entry script. The current `main()` function in the plan parses `sys.argv` for the port, which is wrong for a bundled `.app` (there are no CLI args). The `.app` should just launch on a fixed port with no argument parsing. Recommend creating a separate `ch_bulk/web/entry.py` that PyInstaller targets, which calls `create_app()` and `app.run()` unconditionally.

### 2. PyInstaller spec is missing the `datas` path for DuckDB native extensions

DuckDB ships native `.so`/`.dylib` extensions (e.g., for SQLite export via `INSTALL sqlite; LOAD sqlite;` in `api.py`). PyInstaller's auto-detection often misses these. The `hiddenimports` list includes `duckdb` but DuckDB's compiled shared libraries need to be explicitly collected. The spec should use `collect_data_files('duckdb')` and/or `collect_dynamic_libs('duckdb')` from `PyInstaller.utils.hooks`:

```python
from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs
duckdb_datas = collect_data_files('duckdb')
duckdb_binaries = collect_dynamic_libs('duckdb')
```

Then add these to `a.datas` and `a.binaries`. Without this, the bundled app will crash on `import duckdb`.

### 3. DuckDB concurrent access / thread safety problem

The plan has Flask routes calling `ch.query_advanced()` (read queries) while `/sync` runs `ch.sync()` in a background thread. DuckDB does not support concurrent writers and readers on the same file from separate connections. If a user triggers a sync and then queries while it is running, the query will either fail or block indefinitely. The plan needs one of:
- A read-only connection pool for queries + a write lock for sync, with queries returning an error/spinner while sync is active.
- Disabling query routes while sync is running (simplest).
- Using DuckDB WAL mode (though DuckDB's concurrency story differs from SQLite -- read-only connections via `read_only=True` can coexist with a single writer, but this needs explicit design).

### 4. HTMX polling for long-running operations is underspecified and has a race condition

The plan shows `/sync` returning an "HTMX polling snippet" and `/status` returning state, using a module-level `_task` dict. Issues:
- **No stop/cancel mechanism**: `sync()` downloads ~1.5GB of data and processes it. There is no way for the user to cancel. If the browser tab is closed, the server-side thread keeps running silently.
- **Race condition**: If the user clicks Sync twice before the first completes, the `_task` dict is overwritten and the first thread's state is lost.
- **No error propagation**: The plan says `_task["error"]` exists but never specifies how exceptions from the background thread are caught and stored.
- **Missing HTMX polling directive**: The plan should specify that the response from `/sync` needs `hx-trigger="every 2s"` or `hx-get="/status" hx-trigger="every 2s"` on the returned div to actually trigger polling. This is not detailed in the template section.

### 5. Flask `send_file()` for CSV export will block on large result sets

The plan calls `export_filtered_csv()` which writes to a temp file, then `send_file()` returns it. For a filtered query returning millions of rows, this will:
- Block the single Flask worker thread during the entire DuckDB COPY.
- Create a potentially multi-GB temp file that is never cleaned up (the plan uses `tempfile` but does not show cleanup logic).

Recommend using `send_file()` with `as_attachment=True` and wrapping the temp file with a context manager or `after_this_request` cleanup. Or, for very large exports, stream directly via DuckDB's `COPY ... TO '/dev/stdout'` piped into a Flask streaming response.

### 6. `CompanyCategory` vs `company_type` column name inconsistency in filter plan

The plan's filter table says `company_type = ?` maps to `company_type` column. The processor creates this column from `"CompanyCategory"` (line 74 of processor.py). The plan's `get_filter_options()` queries `SELECT DISTINCT company_type FROM companies` -- this is correct per the schema. However, the plan's SORTABLE_COLUMNS whitelist includes `"company_type"` for sorting but the template shows a "Company Type" dropdown. The actual Companies House data in `CompanyCategory` contains values like "Private Limited Company", "PRI/LTD BY GUAR/NSC (Private, limited by guarantee, no share capital)" etc. The dropdown could have 40+ values making it unwieldy. Consider truncating or grouping.

### 7. `block_cipher = None` is deprecated in PyInstaller 6.x

The spec uses `block_cipher = None` and passes it to `Analysis(cipher=...)` and `PYZ(cipher=...)`. This parameter was deprecated in PyInstaller 5.x and removed in 6.x. The plan specifies PyInstaller 6.x in the tech stack. This will cause a build failure. Remove all `cipher` and `block_cipher` references.

## Suggestions (nice to have)

### 1. Task time estimates are optimistic

Several "5 min" tasks are more realistically 10-15 minutes:
- **Step 1** (query_companies + get_filter_options + export_filtered_csv): Three new functions with SQL generation, pagination, and a shared WHERE builder. More like 10-15 min.
- **Step 5** (Jinja2 template, ~250 lines HTML): Getting HTMX interactions, pagination controls, sort headers, filter form, and database status panel all working correctly is closer to 15-20 min.
- **Step 8** (PyInstaller spec + build script): PyInstaller debugging alone (missing imports, wrong paths) typically takes 15-30 min on first attempt.

The "41 min total" is probably closer to 2-3 hours for a first implementation.

### 2. No `MANIFEST.in` or `package_data` for templates/static

The plan adds `ch_bulk/web/templates/` and `ch_bulk/web/static/` but does not update `pyproject.toml` with `[tool.setuptools.package-data]` to include `*.html`, `*.js`, `*.css` files. Without this, `pip install` will not include the templates and static files, and the `ch-bulk-ui` entry point will fail after install. Add:

```toml
[tool.setuptools.package-data]
"ch_bulk.web" = ["templates/*.html", "static/*.js", "static/*.css"]
```

### 3. Pico CSS file size inconsistency

Step 3 says Pico CSS is "vendored 10KB" in the tech stack but the verify step says it should be "~80KB". The actual Pico CSS minified file is ~80KB. The 10KB figure is wrong; update the tech stack description.

### 4. HTMX file size inconsistency

Similarly, HTMX is described as "vendored 14KB JS" but the verify step says "~45KB". The actual htmx.min.js 2.0.4 is about 45KB. Update the tech stack.

### 5. Missing `.gitignore` updates

The build process creates `build/`, `dist/`, `.build-venv/`, and `*.dmg` files. While `dist/` and `build/` are already in `.gitignore`, `.build-venv/` and `*.spec` generated output are not. Consider adding `.build-venv/` to `.gitignore`.

### 6. No automated tests

The plan has no unit tests for the new query functions (Steps 1-2) or the Flask routes (Step 4). Even simple smoke tests would catch regressions. At minimum, add a `tests/test_query_advanced.py` that creates a small in-memory DuckDB, inserts sample rows, and verifies `query_companies()` pagination, filtering, and sorting.

### 7. `create_app()` parameter types mismatch

In Step 4, `create_app(db_path="ch_bulk.duckdb", data_dir="./data")` takes strings. But in Step 6, the CLI passes `db_path=str(db_path), data_dir=str(data_dir)` after converting from `Path`. Meanwhile, `ChBulk.__init__()` accepts `str | Path`. This works but is fragile -- consider having `create_app()` accept `Path` objects to stay consistent with the rest of the codebase.

### 8. No macOS code signing

The plan acknowledges Gatekeeper ("Right-click the app -> Open, first time only, to bypass Gatekeeper") but does not mention that unsigned apps will show a scary warning and some macOS versions block them entirely. For distribution beyond personal use, consider adding ad-hoc signing (`codesign --force --deep --sign - "dist/CH Bulk.app"`) to the build script, or note this as a known limitation.

### 9. GitHub Actions `macos-latest` runner architecture

`macos-latest` on GitHub Actions currently maps to macOS 14 on Apple Silicon (arm64). If you need to support Intel Macs, you need a separate `macos-13` job or a universal2 build. The plan does not address this. PyInstaller on arm64 produces arm64-only binaries by default.

### 10. Flask debug mode in packaged app

The plan sets `debug=False` in `main()` which is correct. However, Flask's development server (`app.run()`) is explicitly documented as "not suitable for production." For a desktop app this is acceptable since it only binds to `127.0.0.1`, but adding `threaded=True` to `app.run()` would help prevent the UI from hanging when multiple HTMX requests fire simultaneously (e.g., polling `/status` while also loading filter options).

### 11. Export CSV button implementation unclear

The template section says Export CSV is a regular `<a href="/export?...same_params">` link. But the filter form uses HTMX `hx-get` to submit, so the export link needs to dynamically construct its query string from the current form state. This requires JavaScript to copy form values into the href, or the export button should submit the form to `/export` with a different `hx-target` (or no HTMX at all, using a regular form submission). This interaction is underspecified.

### 12. Database does not exist on first launch

When the app launches for the first time with no database, `ch.info()` (called in the `/` route) will raise `FileNotFoundError` per the current `get_db_info()` implementation (query.py line 179). The plan should handle this gracefully -- showing a "No database found, click Sync to get started" state instead of crashing.

## Verified Claims (things you confirmed are correct)

1. **File paths are accurate**: All existing files referenced (`api.py`, `query.py`, `cli.py`, `pyproject.toml`) exist at the stated paths. New files go into `ch_bulk/web/` which does not exist yet but its parent `ch_bulk/` does.

2. **`_build_sic_where()` exists and can be reused**: The plan correctly identifies this function in `query.py` (line 28) for reuse in the new `query_companies()` function.

3. **Existing API structure is compatible**: The `ChBulk` class pattern (wrapping standalone functions from `query.py`, `downloader.py`, `processor.py`) is consistent -- adding `query_advanced()`, `get_filter_options()`, and `export_filtered_csv()` as thin wrappers is the correct pattern.

4. **`pyproject.toml` structure is correct**: The existing entry point `ch-bulk = "ch_bulk.cli:app"` matches the Typer pattern. Adding `ch-bulk-ui` as a second entry point and `[project.optional-dependencies]` for Flask is syntactically correct.

5. **Dependency groups avoid file conflicts**: Group 1 (Steps 1-3) touches `query.py`, `api.py`, and new `web/` files -- no overlap. Groups 2+ are sequential and properly ordered.

6. **GitHub Releases supports 2GB assets**: Confirmed, this is correct per GitHub documentation.

7. **Column names in filter mappings match the schema**: `company_status`, `company_type`, `postcode`, `incorporation_date`, `country_of_origin`, and `sic_code_1` through `sic_code_4` all match the columns created in `processor.py`.

8. **No existing `.github/workflows/` directory**: Confirmed -- this is a new addition, no conflicts.

9. **Existing `sync()` method in `ChBulk`**: The plan correctly references `ch.sync()` for the background operation -- this method exists at line 116 of `api.py` and calls `download()` then `process()`.

10. **`get_db_info()` exists**: The plan correctly builds on the existing `info()` method and `get_db_info()` function for the database status panel.
