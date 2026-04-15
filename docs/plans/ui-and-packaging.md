# Plan: Web UI + Mac Desktop App Packaging for ch-bulk

**Goal**: Add a Flask+HTMX web UI for querying Companies House data, then package it as a standalone Mac .app in a .dmg for distribution via GitHub Releases.

**Architecture**: Flask backend wrapping the existing `ChBulk` API. HTMX for partial-page updates (no SPA framework). Pico CSS for styling (no build step). PyInstaller for Mac .app bundling. GitHub Releases for distribution (DMG files up to 2GB supported).

**Tech Stack**: Python 3.12, Flask 3.x, HTMX 2.x (vendored 14KB JS), Pico CSS (vendored 10KB), PyInstaller 6.x, create-dmg (Homebrew tool for DMG creation)

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
**Time**: 5 min

### 1a. Implementation

Add a new generic query function that supports all filter types and pagination. This function is additive — existing functions remain unchanged.

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
**Time**: 3 min

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

**Time**: 2 min

### 3a. Implementation

```bash
mkdir -p ch_bulk/web/static
touch ch_bulk/web/__init__.py

# Download HTMX (14KB)
curl -sL https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js \
  -o ch_bulk/web/static/htmx.min.js

# Download Pico CSS (10KB)
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

**File**: `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/web/app.py`
**Time**: 5 min

### 4a. Implementation (~200 lines)

```python
"""Flask web UI for ch-bulk."""
import tempfile
import threading
from pathlib import Path
from flask import Flask, render_template, request, send_file, jsonify
from ch_bulk.api import ChBulk

def create_app(db_path="ch_bulk.duckdb", data_dir="./data"):
    app = Flask(__name__, template_folder="templates", static_folder="static")
    ch = ChBulk(data_dir=data_dir, db_path=db_path)

    # Task state for long-running operations
    _task = {"running": False, "message": "", "error": None}

    @app.route("/")
    def index():
        # Load filter options + db info
        ...
        return render_template("index.html", ...)

    @app.route("/query")
    def query():
        # Extract filters from request.args
        # Call ch.query_advanced(**filters)
        # Return HTML fragment (HTMX target)
        ...

    @app.route("/export")
    def export():
        # Same filters as query
        # Write to temp CSV via ch.export_filtered_csv()
        # Return send_file() with Content-Disposition: attachment
        ...

    @app.post("/sync")
    def sync():
        # Run ch.sync() in background thread
        # Return HTMX polling snippet
        ...

    @app.route("/status")
    def status():
        # Return current task state as HTML fragment
        ...

    return app

def main():
    """Entry point for ch-bulk-ui and ch-bulk ui command."""
    import webbrowser, sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8050
    webbrowser.open(f"http://127.0.0.1:{port}")
    app = create_app()
    app.run(host="127.0.0.1", port=port, debug=False)
```

**Routes summary:**

| Route | Method | Purpose | Returns |
|---|---|---|---|
| `/` | GET | Main page | Full HTML |
| `/query` | GET | Filtered search | HTML fragment (HTMX swap) |
| `/export` | GET | CSV download | File download |
| `/sync` | POST | Download + process | HTMX polling div |
| `/download` | POST | Download only | HTMX polling div |
| `/process` | POST | Process only | HTMX polling div |
| `/status` | GET | Task progress | HTML fragment |

### 4b. Verify

```bash
.venv/bin/python -c "from ch_bulk.web.app import create_app; app = create_app(); print('Flask app OK')"
```

---

## Step 5: Create Jinja2 template

**File**: `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/web/templates/index.html`
**Time**: 5 min

### 5a. Implementation (~250 lines HTML)

Single-page layout with these sections:

```
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
            Total: {{ stats.total_companies | default(0) }}
            <!-- Status breakdown, top SIC codes -->
            <!-- Sync / Download / Process buttons with hx-post -->
        </details>

        <!-- Task Status (HTMX target for long-running ops) -->
        <div id="task-status"></div>

        <!-- Filter Form -->
        <form hx-get="/query" hx-target="#results" hx-indicator="#spinner">
            <!-- SIC Code input -->
            <!-- Status dropdown (populated from get_filter_options) -->
            <!-- Company Type dropdown -->
            <!-- Year From / Year To number inputs -->
            <!-- Postcode Prefix text input -->
            <!-- Country dropdown -->
            <!-- Search + Clear + Export CSV buttons -->
        </form>

        <div id="spinner" class="htmx-indicator">Searching...</div>

        <!-- Results (HTMX target, replaced on each search) -->
        <div id="results">
            <!-- Table with sortable headers (hx-get with sort params) -->
            <!-- Pagination: Prev / Page N of M / Next -->
        </div>
    </main>
</body>
</html>
```

**HTMX interactions:**
- Search button: `hx-get="/query"` with form params → swaps `#results`
- Sort header click: `hx-get="/query?sort_by=company_name&sort_order=DESC"` → swaps `#results`
- Pagination: `hx-get="/query?page=2"` → swaps `#results`
- Sync button: `hx-post="/sync"` → swaps `#task-status`, auto-polls `/status` every 2s
- Export CSV: Regular `<a href="/export?...same_params">` link (browser download)

### 5b. Verify

```bash
ls ch_bulk/web/templates/index.html
```

---

## Step 6: Add `ui` CLI command + web entry point

**File**: `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/cli.py`
**File**: `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/web/__main__.py`
**File**: `/Users/nitinkum/Projects/nitkrar/ch_scraper/pyproject.toml`
**Time**: 3 min

### 6a. Implementation

**cli.py** — add command:

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
    flask_app.run(host="127.0.0.1", port=port, debug=False)
```

**web/__main__.py**:

```python
from ch_bulk.web.app import main
main()
```

**pyproject.toml** — add optional dependency + entry point:

```toml
[project.optional-dependencies]
web = ["flask>=3.0"]

[project.scripts]
ch-bulk = "ch_bulk.cli:app"
ch-bulk-ui = "ch_bulk.web.app:main"
```

### 6b. Verify

```bash
.venv/bin/pip install -e ".[web]"
.venv/bin/ch-bulk ui --help
# Should show: Launch the web UI in your browser.
```

---

## Step 7: End-to-end UI test

**Time**: 5 min

### 7a. Manual verification

```bash
# Start the UI
.venv/bin/ch-bulk ui --port 8050

# In browser at http://127.0.0.1:8050:
# 1. Verify database status panel shows stats (or "no database" message)
# 2. Click "Sync" — verify progress indicator, data downloads
# 3. Enter SIC code "62012", click Search — verify results table
# 4. Change status dropdown to "Dissolved" — verify filtered results
# 5. Set year range 2020-2025 — verify filtered results
# 6. Click column header — verify sorting works
# 7. Click page 2 — verify pagination
# 8. Click "Export CSV" — verify file downloads
# 9. Ctrl+C to stop server
```

---

## Step 8: PyInstaller spec + build script

**Files**:
- `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk.spec`
- `/Users/nitinkum/Projects/nitkrar/ch_scraper/build_app.sh`

**Time**: 5 min

### 8a. Implementation

**ch_bulk.spec** — PyInstaller spec file:

```python
# -*- mode: python ; coding: utf-8 -*-
import os
from pathlib import Path

block_cipher = None
pkg_dir = Path("ch_bulk")

a = Analysis(
    ["ch_bulk/web/app.py"],
    pathex=["."],
    datas=[
        (str(pkg_dir / "web" / "templates"), "ch_bulk/web/templates"),
        (str(pkg_dir / "web" / "static"), "ch_bulk/web/static"),
    ],
    hiddenimports=[
        "ch_bulk", "ch_bulk.api", "ch_bulk.query",
        "ch_bulk.downloader", "ch_bulk.processor",
        "ch_bulk.web", "ch_bulk.web.app",
        "duckdb", "httpx", "rich", "flask",
    ],
    hookspath=[],
    runtime_hooks=[],
    excludes=["tkinter", "unittest", "test"],
    cipher=block_cipher,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

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

**build_app.sh** — complete build + DMG creation:

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

# Step 4: Verify .app
echo "Verifying .app..."
if [ ! -d "dist/CH Bulk.app" ]; then
    echo "ERROR: .app not found in dist/"
    exit 1
fi

# Step 5: Create DMG
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

# Step 6: Show result
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

### 8b. Verify

```bash
chmod +x build_app.sh
./build_app.sh
# Should produce: dist/CH-Bulk-0.1.0-mac.dmg
```

---

## Step 9: GitHub Release workflow

**File**: `/Users/nitinkum/Projects/nitkrar/ch_scraper/.github/workflows/release.yml`
**Time**: 3 min

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
# 1. Go to repo → Releases → Draft a new release
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
  4. Right-click the app → Open (first time only, to bypass Gatekeeper)
  5. The app opens in your browser at http://127.0.0.1:8050
```

---

## Step 10: End-to-end verification

**Time**: 5 min

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
#    - Click Sync to download Companies House data
#    - Query SIC code 62012
#    - Export to CSV
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

| Step | Files | Lines | Time |
|------|-------|-------|------|
| 1. query_companies() | query.py | ~120 | 5 min |
| 2. API methods | api.py | ~40 | 3 min |
| 3. Vendor static assets | web/static/* | 0 (downloads) | 2 min |
| 4. Flask app + routes | web/app.py | ~200 | 5 min |
| 5. Jinja2 template | web/templates/index.html | ~250 | 5 min |
| 6. CLI command + entry point | cli.py, pyproject.toml | ~25 | 3 min |
| 7. E2E UI test | — | 0 | 5 min |
| 8. PyInstaller + build script | ch_bulk.spec, build_app.sh | ~100 | 5 min |
| 9. GitHub Release workflow | .github/workflows/release.yml | ~40 | 3 min |
| 10. Full verification | — | 0 | 5 min |
| **Total** | | **~775 lines** | **~41 min** |

## Three ways to distribute:

| Channel | User experience | Effort |
|---|---|---|
| **GitHub Releases (DMG)** | Download → drag to Applications → open | Build + upload once per version |
| **pip install** | `pip install git+https://github.com/...` | Zero — already works |
| **Homebrew tap** (future) | `brew install nitkrar/tap/ch-bulk` | Create a tap repo with a formula |
