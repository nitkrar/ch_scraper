VERDICT: NEEDS_REVISION

## Summary Assessment

The design covers all the right functional areas, but its choice of asyncio+httpx for the enrichment layer creates a fundamental impedance mismatch with the entirely synchronous, threading-based codebase. The GUI layout proposal (right panel, icons in Treeview cells) underestimates Tkinter's limitations and will require significant design rework before implementation can proceed cleanly.

## Critical Issues (must fix)

### 1. Async/sync mismatch is the wrong trade-off for this codebase

The design proposes `asyncio.Semaphore`, `async def` throughout CHClient and the enrichment module, then wraps it all in `asyncio.run()` at the API layer. This is architecturally wrong for this project.

**Why it will hurt:**
- The existing codebase is 100% synchronous: `gui.py` uses `threading.Thread` for background work, `downloader.py` uses synchronous `httpx.Client` (not `AsyncClient`), `query.py` opens and closes `duckdb.connect()` synchronously, `api.py` is a plain synchronous class.
- Calling `asyncio.run()` from a background thread works, but it creates a second concurrency model that nobody working on this codebase will expect. Every future developer (including you in 6 months) will have to reason about threading AND asyncio simultaneously.
- The GUI already uses `threading.Thread` + `root.after()` for background tasks (see `_run_task`, `_poll_task`). Running `asyncio.run()` inside those threads means an event loop nested inside a thread managed by Tkinter's main loop. This is three layers of concurrency for an app that downloads data sequentially from one API.
- `asyncio.Semaphore` only works within a single event loop. Since each `asyncio.run()` call creates a fresh loop, you cannot share a semaphore across calls, which makes it useless for rate-limiting across multiple GUI-triggered enrichments.

**Recommendation:** Use `threading.Thread` + `requests` (or synchronous `httpx.Client`, which is already a dependency) + `threading.Semaphore` for concurrency control, exactly like the downloader does today. A `concurrent.futures.ThreadPoolExecutor` with `max_workers=5` gives the same concurrency as the proposed `asyncio.Semaphore(5)` without the conceptual overhead. The rate limiter should be a thread-safe class using `threading.Lock`, not asyncio primitives.

### 2. Clickable icons in ttk.Treeview cells are not a real Tkinter feature

The design proposes "enrich/eye icons" as clickable elements in Treeview columns. This does not work in ttk.Treeview the way the design implies.

**What Treeview actually supports:**
- `image` parameter on `insert()` -- displays a static image in the *tree column* (the `#0` column with expand/collapse arrows), not in data columns.
- You cannot put clickable widgets (buttons, links) inside Treeview cells. There is no cell-level widget embedding in ttk.Treeview.
- You cannot reliably put images into data columns (the `columns=()` ones). The `image` kwarg on `insert()` only applies to the tree column `#0`.

**Workable alternatives:**
- Use a right-click context menu (bind `<Button-3>`) with "Enrich" and "View Details" options.
- Use double-click (`<Double-1>`) to open the detail panel, and add an "Enrich Selected" button in the toolbar (which the design already proposes).
- Use a separate column with text indicators like "[E]" for enriched companies, styled via tags for color. Bind `<ButtonRelease-1>` on the Treeview and use `identify_region`/`identify_column` to detect which column was clicked.

The third option can simulate clickable cells but requires significant event-handling code. The design should pick one of these concrete approaches rather than assuming icon buttons exist.

### 3. DuckDB concurrent access will cause crashes

The design does not address DuckDB's single-writer constraint. This is a hard correctness issue.

**Current pattern in the codebase:** Every function in `query.py` opens its own `duckdb.connect()`, runs a query, and closes the connection. The GUI runs queries in background threads. This works today because all queries are read-only (`read_only=True` in `query_companies` and `get_db_info`).

**What enrichment changes:** Enrichment writes to DuckDB (INSERT into officers, pscs, charges, etc.) from a background thread, while the GUI may simultaneously read from the same database file for search results.

**DuckDB's threading model:**
- A single DuckDB *connection object* supports multiple threads reading, but only one writer at a time.
- Multiple *separate* `duckdb.connect()` calls to the same file can conflict: DuckDB uses file-level locking, and a write connection will block or fail if another connection is active, depending on the platform and DuckDB version.
- On macOS (which this project runs on, per the environment), file locking behavior is different from Linux, and DuckDB has had reported issues with concurrent connections on macOS.

**Recommendation:** Use a single shared `duckdb.Connection` object, protected by a `threading.Lock` for write operations. Reads can use the same connection or a separate `read_only=True` connection. The `ChBulk` class should own this connection (currently it opens and closes connections per-call, which is fine for read-only but will not work for concurrent read+write). Alternatively, serialize all DuckDB access through a single background thread with a queue, but that is more complex than needed.

### 4. Re-enrichment strategy (DELETE+INSERT vs UPSERT) and partial failure

The design says users can re-enrich but does not specify the data handling strategy. This matters because:

**Partial failure scenario:** Enriching company X calls 6 endpoints (profile, officers, PSCs, charges, filing history, insolvency). If officers and PSCs succeed but charges fails with a 500 error:
- Are the officers and PSCs from this run written to DB? If so, the enrichment_log should say "partial" not "success" or "error".
- If the user re-enriches, should existing officers be replaced or left alone?
- The design's `enrichment_log` has only 'success' or 'error' for status. It needs a 'partial' status and should record which endpoints succeeded.

**DELETE+INSERT vs UPSERT:**
- DELETE all rows for that company_number then INSERT is simpler and correct for re-enrichment. Officers can be added or removed between enrichments, so UPSERT by officer_id would leave stale resigned officers in the DB.
- This DELETE+INSERT must happen in a transaction. If the INSERT fails after DELETE, data is lost.
- DuckDB does not have native UPSERT (`INSERT ... ON CONFLICT`) for all table types, so DELETE+INSERT in a transaction is the right approach anyway.

**Recommendation:** Use `BEGIN TRANSACTION; DELETE FROM officers WHERE company_number = ?; INSERT INTO officers ...; COMMIT;` per table per company. Add 'partial' as a valid enrichment_log status. Track per-endpoint success in a JSON column or separate log table.

### 5. CLI uses Typer, not Click -- but the design's architecture diagram is misleading

The design's architecture diagram labels the CLI box as "CLI (Typer)" but then all the prose and CLI examples are fine. The real issue: the design document says "CLI (Typer)" in the diagram, but the actual `cli.py` imports `typer` directly.

**Wait -- this is actually correct.** The CLI does use Typer (`import typer` on line 9 of cli.py). The user asked me to check "CLI uses Click, not Typer" but Typer is what is actually used. The design is correct on this point.

However, `pyproject.toml` lists `typer>=0.12` as a dependency, and Typer is built on Click internally. There is no mismatch to fix here.

### 6. PanedWindow layout will fight with the existing pack geometry

The GUI currently uses `pack()` exclusively. The results table is inside `self.results_frame` which is packed with `fill="both", expand=True`. Adding a right panel means splitting this area horizontally.

**The problem:** You cannot mix `pack()` and `grid()` inside the same parent frame. If you try to use `grid()` for the results table and detail panel inside `results_frame`, everything packed inside `results_frame` (the count label, tree_frame, pagination) must also switch to `grid()`.

**ttk.PanedWindow works here, but with caveats:**
- You can create a `ttk.PanedWindow` with `orient="horizontal"` inside `results_frame`, and pack the existing tree + pagination into its left pane, and the detail panel into its right pane.
- PanedWindow is packed into `results_frame` with `pack(fill="both", expand=True)`, which is compatible with the existing layout.
- The right pane can be shown/hidden by adding/removing it from the PanedWindow, or by setting its weight to 0.
- But: `ttk.PanedWindow.remove()` is supported; `ttk.PanedWindow.add()` will append the pane back, but the sash position will not be remembered. You need to track and restore it manually.
- The results Treeview will shrink horizontally when the right panel opens. On a 1050px-wide window (the current default), if the detail panel takes 350px, the results table gets only ~650px, which is tight for 8 columns totaling 810px of configured width. Consider making the window wider by default or making the detail panel a separate Toplevel window.

**Collapsible/expandable sections in Tkinter:** The design shows sections with triangle arrows. Tkinter does not have a native collapsible panel widget. The existing codebase implements this manually with `pack()`/`pack_forget()` and a toggle button (see `_toggle_details` at line 302 of gui.py). This same pattern can be reused for the enrichment detail panel sections. It works but is tedious -- each section needs its own frame, toggle button, and state variable.

## Suggestions (nice to have)

### A. Rate limiter should be a standalone, testable class

The sliding-window rate limiter described in the design should be its own class (e.g., `RateLimiter`) rather than being embedded in `CHClient`. This makes it unit-testable without HTTP and reusable if other rate-limited APIs are added later. It should use `time.monotonic()` for the timestamp tracking (not `time.time()`, which can jump on NTP adjustments).

### B. Consider a Toplevel window instead of a right panel for the detail viewer

A `tk.Toplevel` window for company details would avoid all the layout complexity of the right panel. It can be opened from a double-click on a row. It keeps the main window's table at full width. Multiple detail windows could be open simultaneously for comparison. This is a simpler implementation that may actually be a better UX for a data exploration tool.

### C. The `company_profile_enriched` table duplicates data already in `companies`

The existing `companies` table (from processor.py) already contains `accounts_next_due`, `accounts_last_made_up`, `conf_stmt_next_due`, `conf_stmt_last_made_up`, `num_mort_charges`, etc. The proposed `company_profile_enriched` table duplicates some of this. Consider whether you actually need this table, or whether the enrichment should only add data that is not already in the bulk CSV (like `can_file`, `has_insolvency_history`, which are API-only fields).

### D. Add a `python-dotenv` dependency to pyproject.toml

The design mentions using `python-dotenv` for API key loading but it is not currently in the dependencies list. It needs to be added to `pyproject.toml`. Note that `httpx` is already a dependency, so adding `httpx` again (as the design's dependency section suggests) is unnecessary. Only `python-dotenv` is a genuinely new dependency.

### E. Company number formatting deserves explicit attention

Companies House company numbers have these formats:
- 8-digit zero-padded for England/Wales companies: `"00012345"`
- Prefix `SC` for Scottish companies: `"SC123456"`
- Prefix `NI` for Northern Ireland: `"NI012345"`
- Prefix `OC` for LLPs: `"OC301234"`
- Prefix `SO` for Scottish LLPs: `"SO300123"`
- Prefix `IP`, `SP`, `IC`, `SI`, `NP`, `NO`, `R0`, `CE`, `GE`, `FE`, `LP`, `SL`, `NA`, `FC`, `SF`, `GS`, `SE` for various other entity types

The existing bulk data CSV stores these as strings (via `all_varchar = true` in the CSV reader), so `company_number` is VARCHAR in the companies table. The design's schema also uses VARCHAR, which is correct.

However, the CH REST API is strict about formatting: company number `12345` will return 404; you must send `00012345`. The CHClient should either left-pad numeric-only company numbers to 8 digits before calling the API, or the design should document that callers must provide correctly formatted numbers. Since the bulk data already has correctly formatted numbers, this is mainly a concern for the CLI where a user might type `ch-bulk enrich 12345` instead of `ch-bulk enrich 00012345`.

### F. Filing history can be enormous

Some companies have hundreds or thousands of filings. The design does not specify pagination handling for the filing history endpoint. The CH API paginates at 25 items per page by default (max 100). Without pagination handling, you will only get the first page. The design mentions "handles pagination automatically" for officers but does not clarify whether all endpoints do this. Filing history for old companies can be 500+ items, which means 5-20 API calls just for filings. Consider capping at recent filings (e.g., last 2 years) or making it opt-in.

## Verified Claims (things confirmed correct)

1. **CLI uses Typer, not Click.** Despite the review prompt suggesting otherwise, `cli.py` does `import typer` and uses `typer.Typer()`, `typer.Option()`, and `typer.Argument()`. The design's reference to Typer is correct.

2. **httpx is already a dependency.** `pyproject.toml` lists `httpx>=0.27` and `downloader.py` uses it. The design does not need to add it as a new dependency. However, it uses the synchronous `httpx.Client`, not `httpx.AsyncClient`, which reinforces point 1 about not introducing async.

3. **The existing GUI uses threading for background work.** `gui.py` uses `threading.Thread(target=worker, daemon=True).start()` with `root.after()` for polling, exactly as described. The `_run_task`/`_poll_task` pattern is well-established and should be reused for enrichment.

4. **DuckDB schema uses VARCHAR for company_number.** The processor reads all CSV columns as `all_varchar = true` and maps `CompanyNumber` to `company_number`. The design's enrichment tables also use VARCHAR for company_number, which is correct.

5. **The design's progress bar reuse is feasible.** The existing progress bar (`self.progress`) can switch between indeterminate (current) and determinate modes by setting `mode="determinate"` and updating `value`. This is a standard ttk.Progressbar feature.

6. **Export CSV enrichment column appending is a clean approach.** The existing `export_filtered_csv` function in query.py uses `SELECT * FROM companies` with filters. Adding LEFT JOINs to enrichment summary tables at export time is straightforward and does not affect the non-enrichment path.
