# Plan: Web UI + Mac Desktop App Packaging for ch-bulk (v2)

**Goal**: Add a Flask+HTMX web UI for querying Companies House data, then package it as a standalone Mac .app in a .dmg for distribution via GitHub Releases.

**Architecture**: Flask backend wrapping the existing `ChBulk` API. HTMX for partial-page updates (no SPA framework). Pico CSS for styling (no build step). PyInstaller for Mac .app bundling. GitHub Releases for distribution (DMG files up to 2GB supported).

**Tech Stack**: Python 3.12, Flask 3.x, HTMX 2.x (vendored ~45KB JS), Pico CSS (vendored ~80KB), PyInstaller 6.x, create-dmg (Homebrew tool for DMG creation)

---

## Changes from previous version

This revision addresses all critical issues and incorporates suggestions from [ui-and-packaging-review-1.md](ui-and-packaging-review-1.md).

**Critical fixes:**

1. **PyInstaller entry point** (was: Critical #1) -- Created a separate `ch_bulk/web/entry.py` that PyInstaller targets. It calls `create_app()` and `app.run()` unconditionally with no `sys.argv` parsing. The `main()` in `app.py` is now only used by the `ch-bulk-ui` pip entry point and the CLI command.

2. **DuckDB native libraries** (was: Critical #2) -- Added `collect_data_files('duckdb')` and `collect_dynamic_libs('duckdb')` from `PyInstaller.utils.hooks` to the spec file. Without this, `import duckdb` crashes in the bundled app.

3. **DuckDB thread safety** (was: Critical #3) -- Query and export routes now return a "sync in progress" HTML fragment when a sync is running, preventing concurrent DuckDB access. The `_task` dict includes a `running` flag checked by a `@sync_guard` decorator on read routes.

4. **HTMX polling** (was: Critical #4) -- Fully specified: `/sync` response includes `hx-get="/status" hx-trigger="every 2s"`. Added double-click guard via `hx-disabled-elt="this"` on the sync button. Background thread exceptions are caught and stored in `_task["error"]`. Added a note that cancel is not supported (closing the tab lets the server-side thread finish).

5. **CSV export cleanup** (was: Critical #5) -- Added `@after_this_request` to delete the temp file after sending. Added a note about streaming as a future option for very large exports.

6. **`block_cipher` removed** (was: Critical #7) -- All `block_cipher`, `cipher=block_cipher` references removed from the PyInstaller spec. This parameter was deprecated in PyInstaller 5.x and removed in 6.x.

7. **First-launch crash** (was: Critical #12 / suggestion) -- The index route catches `FileNotFoundError` from `ch.info()` and renders a "no database" welcome state instead of crashing.

**Suggestions incorporated:**

- Added `[tool.setuptools.package-data]` for templates and static files to `pyproject.toml` (was: Suggestion #2).
- Fixed file size descriptions: HTMX ~45KB, Pico CSS ~80KB (was: Suggestions #3, #4).
- Added `.build-venv/` to `.gitignore` (was: Suggestion #5).
- Added `threaded=True` to all `app.run()` calls to prevent the UI from hanging when multiple HTMX requests fire simultaneously (was: Suggestion #10).
- Export CSV button uses a small JavaScript snippet to copy current form params into a download URL (was: Suggestion #11).
- Added ad-hoc code signing (`codesign --force --deep --sign -`) to the build script (was: Suggestion #8).

**Not addressed (deferred):**

- Automated tests (Suggestion #6) -- Deferred to a follow-up. The plan is already large.
- Intel Mac support / universal2 binary (Suggestion #9) -- Deferred. Initial release targets Apple Silicon only since that is the development machine.
- `CompanyCategory` dropdown grouping (Critical #6 partial) -- The column name mapping is confirmed correct. Grouping the ~40 company type values is a UX polish item for later.
- Time estimates -- Acknowledged as optimistic. Realistic total is 2-3 hours for first implementation.

---

## Distribution Answer: GitHub Releases

Yes, you can host the full DMG on GitHub. GitHub Releases supports **binary assets up to 2GB per file**. A ~150MB DMG is well within limits.

**How it works:**
1. Create a GitHub Release (e.g., `v0.1.0`)
2. Attach the `.dmg` file as a release asset
3. Users download from: `https://github.com/nitkrar/ch_scraper/releases/latest`
4. The download link is permanent and versioned

**Alternative distribution channels (future):**
- Homebrew tap: `brew install nitkrar/tap/ch-bulk`
- Direct download from a website
- TestFlight (requires Apple Developer account)

---

## Dependency Table

| Group | Steps | Can Parallelize | Dependencies |
|-------|-------|-----------------|--------------|
| 1 | Steps 1-3 | Yes (independent files) | None |
| 2 | Step 4 | No (depends on Group 1) | Steps 1-3 |
| 3 | Steps 5-6 | Yes (independent) | Step 4 |
| 4 | Step 7 | No (depends on Group 3) | Steps 5-6 |
| 5 | Steps 8-9 | No (sequential) | Step 7 |
| 6 | Step 10 | No (final verification) | Step 9 |

---

## Step 1: Add `query_companies()` to query.py

**File**: `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/query.py`

### 1a. Implementation

Add a new generic query function that supports all filter types and pagination. This function is additive -- existing functions remain unchanged.

```python
def query_companies(
    db_path: str | Path,
    *,
    sic_codes: str | list[str] | None = None,
    status: str | None = None,
    company_type: str | None = None,
    postcode_prefix: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    country: str | None = None,
    sort_by: str = "company_name",
    sort_order: str = "ASC",
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    """Multi-filter paginated query. Returns (rows, total_count)."""
```

**Filter-to-SQL mapping:**

| Filter | SQL clause | Param type |
|---|---|---|
| `sic_codes` | Reuse `_build_sic_where()` | Parameterized list |
| `status` | `company_status = ?` | Single param |
| `company_type` | `company_type = ?` | Single param |
| `postcode_prefix` | `STARTS_WITH(postcode, ?)` | Single param |
| `year_from` | `EXTRACT(YEAR FROM incorporation_date) >= ?` | Integer param |
| `year_to` | `EXTRACT(YEAR FROM incorporation_date) <= ?` | Integer param |
| `country` | `country_of_origin = ?` | Single param |

**Sorting**: Validate `sort_by` against whitelist:
```python
SORTABLE_COLUMNS = {
    "company_number", "company_name", "company_status",
    "company_type", "postcode", "incorporation_date",
    "sic_code_1", "country_of_origin",
}
```

**Pagination**: `LIMIT {page_size} OFFSET {(page - 1) * page_size}`. Separate `COUNT(*)` query with same WHERE for total.

Also add:

```python
def get_filter_options(db_path: str | Path) -> dict:
    """Returns distinct values for dropdown population."""
    # Runs 3 queries:
    # SELECT DISTINCT company_status FROM companies ORDER BY 1
    # SELECT DISTINCT company_type FROM companies ORDER BY 1
    # SELECT DISTINCT country_of_origin FROM companies WHERE ... ORDER BY 1
    # Returns {"statuses": [...], "company_types": [...], "countries": [...]}

def export_filtered_csv(
    db_path, output_path, *,
    sic_codes=None, status=None, company_type=None,
    postcode_prefix=None, year_from=None, year_to=None,
    country=None,
) -> int:
    """Export filtered results via DuckDB COPY. Returns row count."""
    # Same WHERE clause builder as query_companies
    # Uses COPY (SELECT ...) TO 'path' (HEADER, DELIMITER ',')
```

### 1b. Verify

```bash
.venv/bin/python -c "from ch_bulk.query import query_companies, get_filter_options; print('OK')"
```

---

## Step 2: Add UI methods to ChBulk API

**File**: `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/api.py`

### 2a. Implementation

Add 3 new methods to the `ChBulk` class that delegate to the new query.py functions:

```python
def query_advanced(self, **filters) -> tuple[list[dict], int]:
    """Multi-filter paginated query. Returns (rows, total_count)."""
    return query_companies(self.db_path, **filters)

def get_filter_options(self) -> dict:
    """Returns distinct values for filter dropdowns."""
    return get_filter_options(self.db_path)

def export_filtered_csv(self, output_path, **filters) -> int:
    """Export filtered results to CSV. Returns row count."""
    return export_filtered_csv(self.db_path, output_path, **filters)
```

Add import for the new functions. Add `db_file_modified` to `info()` return dict:

```python
import os
from datetime import datetime
# In info():
stats["db_file_modified"] = datetime.fromtimestamp(
    os.path.getmtime(self.db_path)
).isoformat() if self.db_path.exists() else None
```

### 2b. Verify

```bash
.venv/bin/python -c "from ch_bulk import ChBulk; print(dir(ChBulk))" | grep query_advanced
```

---

## Step 3: Vendor static assets

**Files**:
- `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/web/__init__.py`
- `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/web/static/htmx.min.js`
- `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/web/static/pico.min.css`

### 3a. Implementation

```bash
mkdir -p ch_bulk/web/static
touch ch_bulk/web/__init__.py

# Download HTMX (~45KB minified)
curl -sL https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js \
  -o ch_bulk/web/static/htmx.min.js

# Download Pico CSS (~80KB minified)
curl -sL https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css \
  -o ch_bulk/web/static/pico.min.css
```

### 3b. Verify

```bash
ls -la ch_bulk/web/static/
# Should show htmx.min.js (~45KB) and pico.min.css (~80KB)
```

---

## Step 4: Create Flask app with routes

**Files**:
- `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/web/app.py`
- `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/web/entry.py`

### 4a. Implementation: `app.py` (~220 lines)

```python
"""Flask web UI for ch-bulk."""
import os
import tempfile
import threading
from functools import wraps
from pathlib import Path
from flask import Flask, render_template, request, send_file, after_this_request
from ch_bulk.api import ChBulk

def create_app(db_path="ch_bulk.duckdb", data_dir="./data"):
    app = Flask(__name__, template_folder="templates", static_folder="static")
    ch = ChBulk(data_dir=data_dir, db_path=db_path)

    # Task state for long-running operations
    _task = {"running": False, "message": "", "error": None}

    def sync_guard(f):
        """Decorator: returns 'sync in progress' fragment if sync is running."""
        @wraps(f)
        def decorated(*args, **kwargs):
            if _task["running"]:
                return (
                    '<div role="alert">Sync is in progress. '
                    'Query and export are disabled until it completes.</div>'
                ), 409
            return f(*args, **kwargs)
        return decorated

    @app.route("/")
    def index():
        # Load filter options + db info, handle missing DB gracefully
        try:
            stats = ch.info()
            filter_opts = ch.get_filter_options()
        except FileNotFoundError:
            stats = None
            filter_opts = {"statuses": [], "company_types": [], "countries": []}
        return render_template("index.html",
            stats=stats, filter_opts=filter_opts, task=_task)

    @app.route("/query")
    @sync_guard
    def query():
        # Extract filters from request.args
        # Call ch.query_advanced(**filters)
        # Return HTML fragment (HTMX target)
        filters = _extract_filters(request.args)
        rows, total = ch.query_advanced(**filters)
        page = int(request.args.get("page", 1))
        page_size = int(request.args.get("page_size", 50))
        total_pages = max(1, -(-total // page_size))  # ceil division
        return render_template("_results.html",
            rows=rows, total=total, page=page,
            total_pages=total_pages, filters=request.args)

    @app.route("/export")
    @sync_guard
    def export():
        # Same filters as query
        filters = _extract_filters(request.args)
        tmp = tempfile.NamedTemporaryFile(
            suffix=".csv", prefix="ch_export_", delete=False)
        tmp.close()
        ch.export_filtered_csv(tmp.name, **filters)

        @after_this_request
        def cleanup(response):
            try:
                os.unlink(tmp.name)
            except OSError:
                pass
            return response

        return send_file(
            tmp.name,
            mimetype="text/csv",
            as_attachment=True,
            download_name="companies_export.csv",
        )
        # NOTE: For very large exports (millions of rows), a future
        # improvement would be to stream directly via a Flask
        # streaming response instead of writing a temp file.

    @app.post("/sync")
    def sync():
        if _task["running"]:
            return '<div role="alert">Sync already in progress.</div>', 409

        _task["running"] = True
        _task["message"] = "Starting sync..."
        _task["error"] = None

        def run_sync():
            try:
                _task["message"] = "Downloading data..."
                csv_files = ch.download()
                _task["message"] = f"Processing {len(csv_files)} files..."
                ch.process(csv_files=csv_files)
                _task["message"] = "Sync complete!"
            except Exception as exc:
                _task["error"] = str(exc)
                _task["message"] = f"Sync failed: {exc}"
            finally:
                _task["running"] = False

        threading.Thread(target=run_sync, daemon=True).start()

        # Return polling div -- HTMX will poll /status every 2s
        return (
            '<div id="task-status" hx-get="/status" hx-trigger="every 2s"'
            ' hx-swap="outerHTML">'
            '<progress></progress> Starting sync...'
            '</div>'
        )

    @app.route("/status")
    def status():
        if _task["running"]:
            return (
                '<div id="task-status" hx-get="/status" hx-trigger="every 2s"'
                ' hx-swap="outerHTML">'
                f'<progress></progress> {_task["message"]}'
                '</div>'
            )
        elif _task["error"]:
            return (
                f'<div id="task-status" role="alert">'
                f'Error: {_task["error"]}</div>'
            )
        elif _task["message"]:
            return (
                f'<div id="task-status" role="alert">'
                f'{_task["message"]}</div>'
            )
        return '<div id="task-status"></div>'

    def _extract_filters(args):
        """Extract query filters from request args."""
        filters = {}
        if args.get("sic_codes"):
            filters["sic_codes"] = args["sic_codes"]
        if args.get("status"):
            filters["status"] = args["status"]
        if args.get("company_type"):
            filters["company_type"] = args["company_type"]
        if args.get("postcode_prefix"):
            filters["postcode_prefix"] = args["postcode_prefix"]
        if args.get("year_from"):
            filters["year_from"] = int(args["year_from"])
        if args.get("year_to"):
            filters["year_to"] = int(args["year_to"])
        if args.get("country"):
            filters["country"] = args["country"]
        if args.get("sort_by"):
            filters["sort_by"] = args["sort_by"]
        if args.get("sort_order"):
            filters["sort_order"] = args["sort_order"]
        if args.get("page"):
            filters["page"] = int(args["page"])
        if args.get("page_size"):
            filters["page_size"] = int(args["page_size"])
        return filters

    return app

def main():
    """Entry point for ch-bulk-ui pip script (not used by PyInstaller).

    Parses an optional port argument for convenience when running
    from the command line via `ch-bulk-ui` or `python -m ch_bulk.web`.
    """
    import webbrowser, sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8050
    webbrowser.open(f"http://127.0.0.1:{port}")
    app = create_app()
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
```

### 4b. Implementation: `entry.py` (~15 lines)

This is the PyInstaller entry point. It has no `sys.argv` parsing -- the bundled .app always launches on a fixed port with no CLI arguments.

```python
"""PyInstaller entry point for the CH Bulk desktop app.

This file is targeted by the .spec file. It launches the Flask app
on a fixed port and opens the browser. No sys.argv parsing -- the
bundled .app has no CLI arguments.
"""
import webbrowser
from ch_bulk.web.app import create_app

PORT = 8050

if __name__ == "__main__":
    webbrowser.open(f"http://127.0.0.1:{PORT}")
    app = create_app()
    app.run(host="127.0.0.1", port=PORT, debug=False, threaded=True)
```

### 4c. Routes summary

| Route | Method | Purpose | Returns |
|---|---|---|---|
| `/` | GET | Main page | Full HTML |
| `/query` | GET | Filtered search (guarded) | HTML fragment (HTMX swap) |
| `/export` | GET | CSV download (guarded) | File download |
| `/sync` | POST | Download + process | HTMX polling div |
| `/download` | POST | Download only | HTMX polling div |
| `/process` | POST | Process only | HTMX polling div |
| `/status` | GET | Task progress | HTML fragment |

**Thread safety design**: The `/query` and `/export` routes are wrapped with `@sync_guard`, which returns HTTP 409 with a user-friendly message if `_task["running"]` is `True`. This prevents concurrent DuckDB access during sync. The sync thread holds `_task["running"] = True` for the entire download+process cycle and clears it in a `finally` block.

**Cancellation note**: There is no cancel mechanism. If the user closes the browser tab, the server-side sync thread continues to completion. This is acceptable for v1 since (a) closing the tab does not harm anything, and (b) adding cancellation requires cooperative thread interruption which adds significant complexity.

### 4d. Verify

```bash
.venv/bin/python -c "from ch_bulk.web.app import create_app; app = create_app(); print('Flask app OK')"
.venv/bin/python -c "from ch_bulk.web.entry import PORT; print(f'Entry point OK, port={PORT}')"
```

---

## Step 5: Create Jinja2 templates

**Files**:
- `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/web/templates/index.html`
- `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/web/templates/_results.html`

### 5a. Implementation: `index.html` (~250 lines HTML)

Single-page layout with these sections:

```html
<!DOCTYPE html>
<html>
<head>
    <link rel="stylesheet" href="{{ url_for('static', filename='pico.min.css') }}">
    <script src="{{ url_for('static', filename='htmx.min.js') }}"></script>
</head>
<body>
    <main class="container">
        <!-- Header -->
        <h1>Companies House Data Explorer</h1>

        <!-- Database Status Panel -->
        <details open>
            <summary>Database Status</summary>
            {% if stats %}
                Total: {{ stats.total_companies | default(0) }}
                <!-- Status breakdown, top SIC codes -->
            {% else %}
                <p>No database found. Click <strong>Sync</strong> to
                download Companies House data and get started.</p>
            {% endif %}
            <!-- Sync button with double-click guard -->
            <button hx-post="/sync"
                    hx-target="#task-status"
                    hx-swap="outerHTML"
                    hx-disabled-elt="this">
                Sync Data
            </button>
        </details>

        <!-- Task Status (HTMX target for long-running ops) -->
        <div id="task-status"></div>

        <!-- Filter Form -->
        <form id="filter-form" hx-get="/query" hx-target="#results"
              hx-indicator="#spinner">
            <!-- SIC Code input -->
            <!-- Status dropdown (populated from filter_opts) -->
            <!-- Company Type dropdown -->
            <!-- Year From / Year To number inputs -->
            <!-- Postcode Prefix text input -->
            <!-- Country dropdown -->
            <!-- Search + Clear buttons -->
            <button type="submit">Search</button>
            <button type="button" onclick="resetForm()">Clear</button>
            <!-- Export CSV via JavaScript -- copies current form params -->
            <button type="button" onclick="exportCsv()">Export CSV</button>
        </form>

        <div id="spinner" class="htmx-indicator">Searching...</div>

        <!-- Results (HTMX target, replaced on each search) -->
        <div id="results">
            <!-- Populated by HTMX from /query -->
        </div>
    </main>

    <script>
    function exportCsv() {
        // Build query string from current form values
        const form = document.getElementById('filter-form');
        const params = new URLSearchParams(new FormData(form));
        // Remove empty params
        for (const [key, value] of [...params.entries()]) {
            if (!value) params.delete(key);
        }
        // Trigger browser download
        window.location.href = '/export?' + params.toString();
    }

    function resetForm() {
        document.getElementById('filter-form').reset();
        document.getElementById('results').innerHTML = '';
    }
    </script>
</body>
</html>
```

### 5b. Implementation: `_results.html` (~50 lines HTML)

Partial template returned by `/query`:

```html
<div id="results">
    <p>{{ total }} companies found (page {{ page }} of {{ total_pages }})</p>

    <table>
        <thead>
            <tr>
                <!-- Sortable headers: clicking swaps #results -->
                <th><a hx-get="/query?{{ filters | sort_link('company_number') }}"
                       hx-target="#results">Company Number</a></th>
                <th><a hx-get="/query?{{ filters | sort_link('company_name') }}"
                       hx-target="#results">Company Name</a></th>
                <!-- ... more columns ... -->
            </tr>
        </thead>
        <tbody>
            {% for row in rows %}
            <tr>
                <td>{{ row.company_number }}</td>
                <td>{{ row.company_name }}</td>
                <!-- ... more columns ... -->
            </tr>
            {% endfor %}
        </tbody>
    </table>

    <!-- Pagination -->
    <nav>
        {% if page > 1 %}
        <a hx-get="/query?{{ filters | page_link(page - 1) }}"
           hx-target="#results">Previous</a>
        {% endif %}
        <span>Page {{ page }} of {{ total_pages }}</span>
        {% if page < total_pages %}
        <a hx-get="/query?{{ filters | page_link(page + 1) }}"
           hx-target="#results">Next</a>
        {% endif %}
    </nav>
</div>
```

### 5c. HTMX interactions

| Interaction | Attribute | Behavior |
|---|---|---|
| Search button | `hx-get="/query"` with form params | Swaps `#results` |
| Sort header click | `hx-get="/query?sort_by=X&sort_order=Y"` | Swaps `#results` |
| Pagination | `hx-get="/query?page=N"` | Swaps `#results` |
| Sync button | `hx-post="/sync"` | Swaps `#task-status`; response div auto-polls `/status` every 2s via `hx-trigger="every 2s"` |
| Sync button guard | `hx-disabled-elt="this"` | Prevents double-click by disabling the button during the request |
| Export CSV | `onclick="exportCsv()"` (JavaScript) | Copies form params into a `/export?...` URL and triggers browser download |

### 5d. Verify

```bash
ls ch_bulk/web/templates/index.html ch_bulk/web/templates/_results.html
```

---

## Step 6: Add `ui` CLI command + web entry point

**Files**:
- `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/cli.py`
- `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/web/__main__.py`
- `/Users/nitinkum/Projects/nitkrar/ch_scraper/pyproject.toml`

### 6a. Implementation

**cli.py** -- add command:

```python
@app.command()
def ui(
    db_path: Path = typer.Option(Path("ch_bulk.duckdb"), "--db-path"),
    data_dir: Path = typer.Option(Path("./data"), "--data-dir", "-d"),
    port: int = typer.Option(8050, "--port", "-p"),
) -> None:
    """Launch the web UI in your browser."""
    try:
        from ch_bulk.web.app import create_app
    except ImportError:
        console.print("[red]Flask is required for the UI. Install with:[/]")
        console.print("  pip install ch-bulk[web]")
        raise typer.Exit(1)

    import webbrowser
    console.print(f"[bold]Starting UI at http://127.0.0.1:{port}[/]")
    webbrowser.open(f"http://127.0.0.1:{port}")
    flask_app = create_app(db_path=str(db_path), data_dir=str(data_dir))
    flask_app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
```

**web/__main__.py**:

```python
from ch_bulk.web.app import main
main()
```

**pyproject.toml** -- add optional dependency, entry point, and package-data:

```toml
[project.optional-dependencies]
web = ["flask>=3.0"]

[project.scripts]
ch-bulk = "ch_bulk.cli:app"
ch-bulk-ui = "ch_bulk.web.app:main"

[tool.setuptools.package-data]
"ch_bulk.web" = ["templates/*.html", "static/*.js", "static/*.css"]
```

### 6b. .gitignore additions

Add to `/Users/nitinkum/Projects/nitkrar/ch_scraper/.gitignore`:

```
.build-venv/
```

### 6c. Verify

```bash
.venv/bin/pip install -e ".[web]"
.venv/bin/ch-bulk ui --help
# Should show: Launch the web UI in your browser.
```

---

## Step 7: End-to-end UI test

### 7a. Manual verification

```bash
# Start the UI
.venv/bin/ch-bulk ui --port 8050

# In browser at http://127.0.0.1:8050:
# 1. Verify "No database found" message (first launch, no crash)
# 2. Click "Sync" -- verify progress indicator appears, button is disabled
# 3. Try clicking "Sync" again -- verify it is rejected (409)
# 4. Try clicking "Search" during sync -- verify "sync in progress" message
# 5. Wait for sync to complete -- verify success message
# 6. Enter SIC code "62012", click Search -- verify results table
# 7. Change status dropdown to "Dissolved" -- verify filtered results
# 8. Set year range 2020-2025 -- verify filtered results
# 9. Click column header -- verify sorting works
# 10. Click page 2 -- verify pagination
# 11. Click "Export CSV" -- verify file downloads with current filters
# 12. Ctrl+C to stop server
```

---

## Step 8: PyInstaller spec + build script

**Files**:
- `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk.spec`
- `/Users/nitinkum/Projects/nitkrar/ch_scraper/build_app.sh`

### 8a. Implementation

**ch_bulk.spec** -- PyInstaller spec file:

```python
# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path
from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs

pkg_dir = Path("ch_bulk")

# Collect DuckDB native libraries and data files.
# Without this, `import duckdb` will crash in the bundled app because
# PyInstaller's auto-detection misses DuckDB's .so/.dylib extensions.
duckdb_datas = collect_data_files('duckdb')
duckdb_binaries = collect_dynamic_libs('duckdb')

a = Analysis(
    ["ch_bulk/web/entry.py"],
    pathex=["."],
    datas=[
        (str(pkg_dir / "web" / "templates"), "ch_bulk/web/templates"),
        (str(pkg_dir / "web" / "static"), "ch_bulk/web/static"),
    ] + duckdb_datas,
    binaries=duckdb_binaries,
    hiddenimports=[
        "ch_bulk", "ch_bulk.api", "ch_bulk.query",
        "ch_bulk.downloader", "ch_bulk.processor",
        "ch_bulk.web", "ch_bulk.web.app",
        "duckdb", "httpx", "rich", "flask",
    ],
    hookspath=[],
    runtime_hooks=[],
    excludes=["tkinter", "unittest", "test"],
)

pyz = PYZ(a.pure, a.zipped_data)

exe = EXE(
    pyz, a.scripts, [],
    exclude_binaries=True,
    name="CH Bulk",
    debug=False,
    strip=False,
    upx=False,
    console=False,  # No terminal window
)

coll = COLLECT(
    exe, a.binaries, a.datas,
    strip=False,
    upx=False,
    name="CH Bulk",
)

app = BUNDLE(
    coll,
    name="CH Bulk.app",
    icon=None,  # Add icon later: icon="assets/icon.icns"
    bundle_identifier="com.nitkrar.ch-bulk",
    info_plist={
        "CFBundleName": "CH Bulk",
        "CFBundleDisplayName": "Companies House Bulk Data Explorer",
        "CFBundleVersion": "0.1.0",
        "CFBundleShortVersionString": "0.1.0",
        "NSHighResolutionCapable": True,
    },
)
```

**Key changes from v1:**
- Entry point is `ch_bulk/web/entry.py` (not `app.py`) -- no `sys.argv` parsing, works as a bundled `.app`.
- `collect_data_files('duckdb')` and `collect_dynamic_libs('duckdb')` ensure DuckDB native libraries are bundled.
- All `block_cipher` / `cipher=` references removed (deprecated in PyInstaller 5.x, removed in 6.x).

**build_app.sh** -- complete build + DMG creation:

```bash
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

VERSION="0.1.0"
APP_NAME="CH Bulk"
DMG_NAME="CH-Bulk-${VERSION}-mac"

echo "=== Building ${APP_NAME} v${VERSION} ==="

# Step 1: Clean
rm -rf build/ dist/

# Step 2: Create build venv
echo "Creating build environment..."
python3 -m venv .build-venv
.build-venv/bin/pip install --quiet --upgrade pip
.build-venv/bin/pip install --quiet -e ".[web]"
.build-venv/bin/pip install --quiet pyinstaller

# Step 3: Build .app
echo "Building .app bundle..."
.build-venv/bin/pyinstaller ch_bulk.spec --noconfirm

# Step 4: Ad-hoc code signing
# This reduces Gatekeeper friction. Without signing, some macOS
# versions block the app entirely. Ad-hoc signing (no Apple Developer
# account needed) makes it openable via right-click -> Open on first launch.
echo "Signing .app (ad-hoc)..."
codesign --force --deep --sign - "dist/${APP_NAME}.app"

# Step 5: Verify .app
echo "Verifying .app..."
if [ ! -d "dist/${APP_NAME}.app" ]; then
    echo "ERROR: .app not found in dist/"
    exit 1
fi

# Step 6: Create DMG
echo "Creating DMG..."
if command -v create-dmg &>/dev/null; then
    create-dmg \
        --volname "${APP_NAME}" \
        --window-pos 200 120 \
        --window-size 600 400 \
        --icon-size 100 \
        --icon "${APP_NAME}.app" 175 190 \
        --app-drop-link 425 190 \
        "dist/${DMG_NAME}.dmg" \
        "dist/${APP_NAME}.app"
else
    echo "create-dmg not found, creating simple DMG..."
    hdiutil create -volname "${APP_NAME}" \
        -srcfolder "dist/${APP_NAME}.app" \
        -ov -format UDZO \
        "dist/${DMG_NAME}.dmg"
fi

# Step 7: Show result
echo ""
echo "========================================="
echo "  Build complete!"
echo "========================================="
echo ""
echo "  .app:  dist/${APP_NAME}.app"
echo "  .dmg:  dist/${DMG_NAME}.dmg"
echo "  Size:  $(du -sh "dist/${DMG_NAME}.dmg" | cut -f1)"
echo ""
echo "To install create-dmg for prettier DMGs:"
echo "  brew install create-dmg"
echo ""

# Cleanup build venv
rm -rf .build-venv
```

**Key changes from v1:**
- Added ad-hoc code signing step (`codesign --force --deep --sign -`).

### 8b. Verify

```bash
chmod +x build_app.sh
./build_app.sh
# Should produce: dist/CH-Bulk-0.1.0-mac.dmg
```

---

## Step 9: GitHub Release workflow

**File**: `/Users/nitinkum/Projects/nitkrar/ch_scraper/.github/workflows/release.yml`

### 9a. Implementation

```yaml
name: Build and Release

on:
  push:
    tags:
      - 'v*'

jobs:
  build-mac:
    runs-on: macos-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: |
          pip install -e ".[web]"
          pip install pyinstaller

      - name: Build .app
        run: pyinstaller ch_bulk.spec --noconfirm

      - name: Sign .app (ad-hoc)
        run: codesign --force --deep --sign - "dist/CH Bulk.app"

      - name: Create DMG
        run: |
          hdiutil create -volname "CH Bulk" \
            -srcfolder "dist/CH Bulk.app" \
            -ov -format UDZO \
            "dist/CH-Bulk-${{ github.ref_name }}-mac.dmg"

      - name: Upload to Release
        uses: softprops/action-gh-release@v2
        with:
          files: dist/CH-Bulk-*.dmg
```

### 9b. How to create a release

```bash
# Tag and push
git tag v0.1.0
git push origin v0.1.0

# Or manually via GitHub:
# 1. Go to repo -> Releases -> Draft a new release
# 2. Create tag "v0.1.0"
# 3. Upload dist/CH-Bulk-0.1.0-mac.dmg
# 4. Write release notes
# 5. Publish
```

### 9c. What users see

Users go to `https://github.com/nitkrar/ch_scraper/releases/latest` and see:

```
CH Bulk v0.1.0

Download:
  CH-Bulk-0.1.0-mac.dmg (150 MB)

Installation:
  1. Download the DMG file
  2. Open the DMG
  3. Drag "CH Bulk" to Applications
  4. Right-click the app -> Open (first time only, to bypass Gatekeeper)
  5. The app opens in your browser at http://127.0.0.1:8050
```

---

## Step 10: End-to-end verification

### 10a. Test the full flow

```bash
# 1. Build the DMG
./build_app.sh

# 2. Mount the DMG
open "dist/CH-Bulk-0.1.0-mac.dmg"

# 3. Run the app from the mounted DMG
open "/Volumes/CH Bulk/CH Bulk.app"

# 4. In browser:
#    - Verify the UI loads at http://127.0.0.1:8050
#    - Verify "No database found" message (first-launch, no crash)
#    - Click Sync to download Companies House data
#    - Verify sync button is disabled during sync
#    - Verify query routes return "sync in progress" during sync
#    - Query SIC code 62012
#    - Export to CSV (verify file downloads, temp file cleaned up)
#    - Verify all filters work

# 5. Test pip install path still works
pip install git+https://github.com/nitkrar/ch_scraper.git
ch-bulk --help
ch-bulk ui --help

# 6. Test Python API still works
python -c "
from ch_bulk import ChBulk
ch = ChBulk()
print(type(ch))
print('API OK')
"
```

---

## Summary

| Step | Files | Lines | Notes |
|------|-------|-------|-------|
| 1. query_companies() | query.py | ~120 | 3 new functions |
| 2. API methods | api.py | ~40 | 3 thin wrappers |
| 3. Vendor static assets | web/static/* | 0 (downloads) | ~45KB + ~80KB |
| 4. Flask app + routes | web/app.py, web/entry.py | ~235 | sync_guard, after_this_request |
| 5. Jinja2 templates | web/templates/*.html | ~300 | index.html + _results.html |
| 6. CLI command + entry point | cli.py, pyproject.toml | ~30 | package-data, .gitignore |
| 7. E2E UI test | -- | 0 | Manual verification |
| 8. PyInstaller + build script | ch_bulk.spec, build_app.sh | ~110 | DuckDB libs, ad-hoc signing |
| 9. GitHub Release workflow | .github/workflows/release.yml | ~40 | Includes signing step |
| 10. Full verification | -- | 0 | First-launch + sync + query |
| **Total** | | **~875 lines** | **~2-3 hours realistic** |

## Three ways to distribute:

| Channel | User experience | Effort |
|---|---|---|
| **GitHub Releases (DMG)** | Download -> drag to Applications -> open | Build + upload once per version |
| **pip install** | `pip install git+https://github.com/...` | Zero -- already works |
| **Homebrew tap** (future) | `brew install nitkrar/tap/ch-bulk` | Create a tap repo with a formula |
