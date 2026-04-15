# Flask/HTMX UI Feature List — Tkinter Rewrite Specification

Extracted from the Flask web UI source files:

- `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/web/app.py`
- `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/web/entry.py`
- `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/web/templates/index.html`
- `/Users/nitinkum/Projects/nitkrar/ch_scraper/ch_bulk/web/templates/_results.html`

---

## Application Lifecycle

1. **App startup and browser launch**
   - `entry.py` / `app.main()` creates the Flask app via `create_app(db_path, data_dir)` with defaults `"ch_bulk.duckdb"` and `"./data"`.
   - Accepts an optional CLI port argument (`sys.argv[1]`, default `8050`).
   - Calls `webbrowser.open()` to open `http://127.0.0.1:<port>` automatically.
   - Flask runs with `debug=False, threaded=True` on `127.0.0.1`.
   - **Tkinter equivalent:** The app window should open directly; no browser launch needed. Accept optional `--port` or `--db-path` / `--data-dir` args if a CLI wrapper is retained.

2. **ChBulk API instance**
   - A single `ChBulk(data_dir=data_dir, db_path=db_path)` instance is created at app startup and shared across all operations.
   - **Tkinter equivalent:** Create one `ChBulk` instance at startup; all UI callbacks reference it.

3. **Shared task state (`_task` dict)**
   - Global mutable dict: `{"running": False, "message": "", "error": None}`.
   - Guards long-running operations (sync, download, process) from concurrent execution.
   - Guards query and export routes from executing while a task is running (`sync_guard` decorator).
   - **Tkinter equivalent:** A threading lock or a simple boolean flag plus status variables, with UI polling via `root.after()`.

---

## Section 1: Database Status Panel

4. **Database status display (collapsible, open by default)**
   - User sees a `<details open>` panel titled "Database Status".
   - **API call:** `ch.info()` — returns `stats` object.
   - **API call:** `ch.get_filter_options()` — returns `filter_opts` dict with keys `statuses`, `company_types`, `countries`.
   - **Error handling:** If `ch.info()` raises `FileNotFoundError`, `stats` is `None` and `filter_opts` returns empty lists.

5. **Stats grid (when database exists)**
   - Displays `stats.total_companies` formatted with commas (e.g., `5,432,100`).
   - Displays `stats.status_breakdown` — a dict of `{status_name: count}` pairs, each shown as a formatted count with the status name below.
   - Unknown/empty status names display as "Unknown".
   - **Tkinter equivalent:** A grid or set of labels showing these counts.

6. **Last updated timestamp**
   - If `stats.db_file_modified` is truthy, displays "Last updated: {timestamp}" in muted small text.

7. **Top SIC Codes (nested collapsible, closed by default)**
   - If `stats.top_sic_codes` is truthy, shows a nested `<details>` (closed by default) titled "Top SIC Codes".
   - Contains a two-column table: SIC Code | Count (formatted with commas).
   - Each entry has `.sic_code` and `.count` attributes.
   - **Tkinter equivalent:** An expandable section or a button that reveals a small table/treeview.

8. **No database state**
   - When `stats` is `None`, displays: "No database found. Click **Sync Data** to download Companies House data and get started."
   - The search/filter section is hidden entirely (wrapped in `{% if stats %}`).

---

## Section 2: Data Management Buttons

9. **"Sync Data" button (primary)**
   - Label: "Sync Data"
   - Style: Primary button (default Pico CSS styling).
   - **HTMX behavior:** `hx-post="/sync"`, targets `#task-status`, swap `outerHTML`. Self-disables during request (`hx-disabled-elt="this"`).
   - **API calls:**
     1. `ch.download()` — returns list of `csv_files`.
     2. `ch.process(csv_files=csv_files)` — processes the downloaded files.
   - **Async behavior:** Runs in a background `threading.Thread(daemon=True)`.
   - **Progress messages:**
     - "Starting sync..." (initial)
     - "Downloading data..."
     - "Processing {N} files..."
     - "Sync complete! Refresh the page to query."
   - **Error handling:** On exception, sets `_task["error"] = str(exc)`, message becomes "Sync failed: {exc}". Logged via `logger.exception`.
   - **Guard:** If `_task["running"]` is True, returns 409 with "Sync already in progress."
   - **Tkinter equivalent:** A button that starts a background thread, disables itself and sibling buttons, and updates a progress label via `root.after()` polling.

10. **"Download Only" button (secondary)**
    - Label: "Download Only"
    - Style: Secondary button (`class="secondary"`).
    - **HTMX behavior:** `hx-post="/download-only"`, same target/swap/disable pattern as Sync.
    - **API call:** `ch.download()` only.
    - **Async behavior:** Background thread, same pattern.
    - **Progress messages:**
      - "Downloading..."
      - "Download complete! {N} files ready. Click Process to build the database."
    - **Error handling:** Same pattern — sets error, message "Download failed: {exc}".
    - **Guard:** Same 409 if already running: "A task is already running."

11. **"Process Only" button (secondary)**
    - Label: "Process Only"
    - Style: Secondary button (`class="secondary"`).
    - **HTMX behavior:** `hx-post="/process-only"`, same pattern.
    - **API call:** `ch.process()` (no `csv_files` argument — processes whatever is already downloaded).
    - **Async behavior:** Background thread, same pattern.
    - **Progress messages:**
      - "Processing..."
      - "Processing complete! {N:,} companies loaded."
    - **Error handling:** Same pattern — sets error, message "Processing failed: {exc}".
    - **Guard:** Same 409 if already running.

---

## Section 3: Task Status Display

12. **Task status area (`#task-status`)**
    - A `<div id="task-status">` region that shows the current background task status.
    - **Three states on initial page load:**
      1. **Running:** Shows an indeterminate `<progress>` bar and `task.message` text. Has `hx-get="/status"` with `hx-trigger="every 2s"` for auto-polling.
      2. **Error (not running):** Shows an alert div with "Error: {task.error}".
      3. **Message (not running, no error):** Shows an alert div with `task.message` (e.g., completion message).
      4. **Idle (no message):** Empty div.

13. **HTMX polling for status updates (`GET /status`)**
    - While `_task["running"]` is True: returns HTML with `<progress>` and current message, with `hx-get="/status" hx-trigger="every 2s"` to continue polling.
    - When done with error: returns a static alert div with "Error: {error}" (no further polling).
    - When done with message: returns a static alert div with the completion message (no further polling).
    - When idle with no message: returns empty `<div id="task-status"></div>`.
    - **Tkinter equivalent:** Use `root.after(2000, poll_status)` to periodically check the shared task state and update a Label or Progressbar widget.

---

## Section 4: Search / Filter Form

14. **Search Companies panel (collapsible, open by default)**
    - Only visible when `stats` is not `None` (database exists).
    - `<details open>` titled "Search Companies".

15. **Filter form**
    - `<form id="filter-form">` with `hx-get="/query"`, targets `#results`, shows `#spinner` indicator.
    - Submitting the form triggers `GET /query` with all filter params.

16. **SIC Code(s) text input**
    - Label: "SIC Code(s)"
    - Input: `<input type="text" name="sic_codes">`
    - Placeholder: "e.g. 62012 or 62012,69201"
    - Accepts comma-separated SIC codes.
    - **API param:** `sic_codes` (string, passed as-is).

17. **Company Status dropdown**
    - Label: "Company Status"
    - `<select name="status">` with "All" as default (`value=""`).
    - Options populated from `filter_opts.statuses`.
    - "Active" is pre-selected by default (`'selected' if s == 'Active'`).
    - **API param:** `status` (string).

18. **Company Type dropdown**
    - Label: "Company Type"
    - `<select name="company_type">` with "All" as default (`value=""`).
    - Options populated from `filter_opts.company_types`.
    - No pre-selection.
    - **API param:** `company_type` (string).

19. **Postcode Prefix text input**
    - Label: "Postcode Prefix"
    - Input: `<input type="text" name="postcode_prefix">`
    - Placeholder: "e.g. EC1, SW1A"
    - **API param:** `postcode_prefix` (string).

20. **Incorporation Year From number input**
    - Label: "Incorporation Year From"
    - Input: `<input type="number" name="year_from" min="1800" max="2030">`
    - Placeholder: "e.g. 2020"
    - **API param:** `year_from` (int; ValueError silently ignored if non-numeric).

21. **Incorporation Year To number input**
    - Label: "Incorporation Year To"
    - Input: `<input type="number" name="year_to" min="1800" max="2030">`
    - Placeholder: "e.g. 2026"
    - **API param:** `year_to` (int; ValueError silently ignored if non-numeric).

22. **Country of Origin dropdown**
    - Label: "Country of Origin"
    - `<select name="country">` with "All" as default (`value=""`).
    - Options populated from `filter_opts.countries`.
    - No pre-selection.
    - **API param:** `country` (string).

23. **"Search" button (primary, form submit)**
    - Label: "Search"
    - `<button type="submit">` — triggers the HTMX form submission (`GET /query`).
    - **API call:** `ch.query_advanced(**filters)` via the `/query` route.

24. **"Clear Filters" button (secondary)**
    - Label: "Clear Filters"
    - `<button type="button" class="secondary">` with `onclick="resetForm()"`.
    - **JavaScript `resetForm()` behavior:**
      1. Resets the form to its initial state (`form.reset()`).
      2. Clears the results area (`#results` innerHTML set to empty string).
    - Does NOT trigger a new query.

25. **"Export CSV" button (contrast)**
    - Label: "Export CSV"
    - `<button type="button" class="contrast">` with `onclick="exportCsv()"`.
    - **JavaScript `exportCsv()` behavior:**
      1. Reads all current form data via `new FormData(form)`.
      2. Builds `URLSearchParams`, removing entries with empty values.
      3. Navigates the browser to `/export?{params}` (triggers a file download).
    - **API call:** `ch.export_filtered_csv(tmp_path, **filters)` via the `/export` route.
    - **File handling:** Server writes to a temp file, sends it as `companies_export.csv` attachment, then deletes the temp file after the response.
    - **Error handling:** If `FileNotFoundError`, returns HTML alert "No database found. Click Sync first."
    - **Sync guard:** Returns 409 "Data sync is in progress..." if a task is running.
    - **Tkinter equivalent:** Open a file-save dialog (`filedialog.asksaveasfilename`), then call `ch.export_filtered_csv()` directly.

26. **Search spinner / loading indicator**
    - `<span id="spinner" class="htmx-indicator" aria-busy="true">Searching...</span>`
    - Hidden by default via CSS (`.htmx-indicator { display: none }`).
    - Shown during HTMX request (`.htmx-request .htmx-indicator { display: inline-block }`).
    - **Tkinter equivalent:** A "Searching..." label or a spinning indicator widget, shown/hidden during query execution.

---

## Section 5: Query Execution and Results

27. **Query execution (`GET /query`)**
    - **Filter extraction** (`_extract_filters`):**
      - `sic_codes`: string (raw, optional)
      - `status`: string (optional)
      - `company_type`: string (optional)
      - `postcode_prefix`: string (optional)
      - `year_from`: int (optional, ValueError silently ignored)
      - `year_to`: int (optional, ValueError silently ignored)
      - `country`: string (optional)
      - `sort_by`: string (optional)
      - `sort_order`: string (optional)
      - `page`: int (default 1, ValueError silently ignored)
      - `page_size`: int (default 50, ValueError silently ignored)
    - **API call:** `ch.query_advanced(**filters)` — returns `(rows, total)`.
    - Calculates `total_pages = max(1, ceil(total / page_size))` using `ceil = -(-total // page_size)`.
    - **Error handling:** If `FileNotFoundError`, returns "No database found. Click Sync first."
    - **Sync guard:** Returns 409 "Data sync is in progress..." if a task is running.

28. **Results summary line**
    - Displays: "**{total:,}** companies found (page {page} of {total_pages})"

29. **Results table**
    - Shown only when `rows` is truthy (non-empty).
    - Wrapped in `<figure>` for Pico CSS styling.
    - **Eight columns with sortable headers:**

      | Column Key | Header Label | Sortable |
      |---|---|---|
      | `company_number` | Company No | Yes |
      | `company_name` | Company Name | Yes |
      | `company_status` | Status | Yes |
      | `company_type` | Type | Yes |
      | `sic_code_1` | SIC 1 | Yes |
      | `postcode` | Postcode | Yes |
      | `incorporation_date` | Inc. Date | Yes |
      | `country_of_origin` | Country | Yes |

    - Each cell displays the row attribute or empty string if `None`.

30. **Column sorting**
    - Each column header is a clickable link (`<a>`) that triggers an HTMX `GET /query` with updated `sort_by` and `sort_order` params.
    - **Toggle logic:** If the current `sort_by` matches the column AND `sort_order` is `ASC`, clicking toggles to `DESC`. Otherwise defaults to `ASC`.
    - Clicking a sort header resets to `page=1`.
    - Active sort column shows an arrow indicator: `▲` for ASC, `▼` for DESC.
    - The link preserves all existing filter params (except `sort_by`, `sort_order`, `page`).
    - **Tkinter equivalent:** Treeview column header click bindings with re-query.

31. **Pagination controls**
    - `<nav>` with Previous / Page indicator / Next.
    - **Previous button:**
      - If `page > 1`: clickable link via HTMX `GET /query` with `page={page-1}`, preserving all other filters.
      - If `page == 1`: disabled (rendered as `<span class="secondary">`).
    - **Page indicator:** "Page {page} of {total_pages}" (static text).
    - **Next button:**
      - If `page < total_pages`: clickable link via HTMX `GET /query` with `page={page+1}`, preserving all other filters.
      - If `page >= total_pages`: disabled (rendered as `<span class="secondary">`).
    - **Tkinter equivalent:** Previous/Next buttons with disabled state, plus a label showing current page.

32. **No results state**
    - When `rows` is empty/falsy: displays "No results found. Try adjusting your filters." instead of the table.

---

## Section 6: Error States and Guards (Cross-Cutting)

33. **Sync guard on query**
    - `GET /query` is wrapped with `@sync_guard`.
    - If `_task["running"]` is True, returns HTTP 409 with: "Data sync is in progress. Query and export are disabled until it completes."
    - **Tkinter equivalent:** Disable the Search and Export buttons while a background task is running.

34. **Sync guard on export**
    - `GET /export` is wrapped with `@sync_guard`.
    - Same 409 behavior as query.

35. **Database not found on query**
    - `ch.query_advanced()` raises `FileNotFoundError` → returns: "No database found. Click Sync first."

36. **Database not found on export**
    - `ch.export_filtered_csv()` raises `FileNotFoundError` → returns: "No database found. Click Sync first." (temp file is cleaned up).

37. **Concurrent task prevention**
    - All three task endpoints (`/sync`, `/download-only`, `/process-only`) check `_task["running"]` before starting.
    - If already running, returns 409 with an alert message.
    - Only one background task can run at a time.

38. **Background thread exception handling**
    - All background threads catch `Exception`, log via `logger.exception()`, set `_task["error"]` and `_task["message"]`, then set `_task["running"] = False` in the `finally` block.
    - The status polling endpoint will then show the error to the user.

---

## Section 7: Layout and Styling

39. **Page title:** "Companies House Data Explorer"
40. **CSS framework:** Pico CSS (`pico.min.css`) with `data-theme="light"`.
41. **JavaScript library:** HTMX (`htmx.min.js`).
42. **Layout:** Single `<main class="container">` — centered, responsive container.
43. **Filter grid:** 2-column CSS grid (`grid-template-columns: 1fr 1fr`) with `1rem` gap.
44. **Stats grid:** Auto-fit responsive grid (`repeat(auto-fit, minmax(150px, 1fr))`) with `0.5rem` gap.
45. **Action buttons:** Flex container (`display: flex; gap: 0.5rem; flex-wrap: wrap`).
46. **Muted text:** `color: var(--pico-muted-color)` for secondary information.
47. **Tkinter equivalent:** Use `ttk` themed widgets. Grid/pack geometry managers. No CSS needed but colors and spacing should feel similar.

---

## Summary: ChBulk API Methods Used

| API Method | Called By | Purpose |
|---|---|---|
| `ChBulk(data_dir, db_path)` | `create_app()` | Constructor |
| `ch.info()` | `GET /` (index) | Database stats |
| `ch.get_filter_options()` | `GET /` (index) | Dropdown values |
| `ch.query_advanced(**filters)` | `GET /query` | Filtered paginated search |
| `ch.export_filtered_csv(path, **filters)` | `GET /export` | CSV export |
| `ch.download()` | `POST /sync`, `POST /download-only` | Download data files |
| `ch.process(csv_files=...)` | `POST /sync` | Process with explicit files |
| `ch.process()` | `POST /process-only` | Process already-downloaded files |

---

## Summary: Filter Parameters for `query_advanced` / `export_filtered_csv`

| Parameter | Type | UI Widget | Default |
|---|---|---|---|
| `sic_codes` | `str` | Text input | empty |
| `status` | `str` | Dropdown (pre-selects "Active") | empty (All) |
| `company_type` | `str` | Dropdown | empty (All) |
| `postcode_prefix` | `str` | Text input | empty |
| `year_from` | `int` | Number input (1800-2030) | not set |
| `year_to` | `int` | Number input (1800-2030) | not set |
| `country` | `str` | Dropdown | empty (All) |
| `sort_by` | `str` | Column header click | not set |
| `sort_order` | `str` (`ASC`/`DESC`) | Column header click toggle | `ASC` |
| `page` | `int` | Pagination buttons | `1` |
| `page_size` | `int` | Not exposed in UI (hardcoded `50`) | `50` |
