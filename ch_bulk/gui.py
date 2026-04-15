"""Tkinter GUI for Companies House bulk data explorer."""

from __future__ import annotations

import logging
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, ttk
from typing import Any

from ch_bulk.api import ChBulk

logger = logging.getLogger(__name__)

COLUMNS = [
    ("company_number", "Company No", 90),
    ("company_name", "Company Name", 250),
    ("company_status", "Status", 80),
    ("company_type", "Type", 60),
    ("sic_code_1", "SIC 1", 60),
    ("postcode", "Postcode", 80),
    ("incorporation_date", "Inc. Date", 90),
    ("country_of_origin", "Country", 100),
]
PAGE_SIZE = 50


class ChBulkApp:
    """Single-window Tkinter application for Companies House data."""

    def __init__(self, db_path: str = "ch_bulk.duckdb", data_dir: str = "./data") -> None:
        self.ch = ChBulk(data_dir=data_dir, db_path=db_path)
        self._task_running = False
        self._task_message = ""
        self._task_error: str | None = None
        self._lock = threading.Lock()
        self.page = 1
        self.total_pages = 1
        self.total_count = 0
        self.sort_by = "company_name"
        self.sort_order = "ASC"
        self._build_ui()

    # ── UI construction ──────────────────────────────────────────────

    def _build_ui(self) -> None:
        self.root = tk.Tk()
        self.root.title("Companies House Data Explorer")
        self.root.geometry("1050x700")
        self.root.minsize(800, 500)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        # Title
        ttk.Label(self.root, text="Companies House Data Explorer",
                  font=("TkDefaultFont", 16, "bold")).pack(pady=(10, 4))

        # Status frame
        self.status_frame = ttk.LabelFrame(self.root, text="Database")
        self.status_frame.pack(fill="x", padx=10, pady=4)

        # Top row: status indicator + action buttons
        top_row = ttk.Frame(self.status_frame)
        top_row.pack(fill="x", padx=8, pady=6)

        self.status_indicator = ttk.Label(top_row, text="\u25cf Not Setup",
                                          foreground="red",
                                          font=("TkDefaultFont", 11, "bold"))
        self.status_indicator.pack(side="left")

        self.status_detail = ttk.Label(top_row, text="", foreground="gray")
        self.status_detail.pack(side="left", padx=(8, 0))

        # Action buttons (right-aligned in the status row)
        self.btn_process = ttk.Button(top_row, text="Process", command=self._on_process)
        self.btn_process.pack(side="right", padx=2)
        self.btn_download = ttk.Button(top_row, text="Download", command=self._on_download)
        self.btn_download.pack(side="right", padx=2)
        self.btn_sync = ttk.Button(top_row, text="Sync Data", command=self._on_sync)
        self.btn_sync.pack(side="right", padx=2)

        # Details section (collapsible, hidden by default)
        self.details_toggle = ttk.Button(
            self.status_frame, text="\u25b6 Show Details",
            command=self._toggle_details,
        )
        self._details_visible = False
        self._top_sic_codes: list[dict] = []

        self.details_frame = ttk.Frame(self.status_frame)

        # Status breakdown labels
        self.breakdown_label = ttk.Label(self.details_frame, text="", wraplength=950)
        self.breakdown_label.pack(anchor="w", padx=8, pady=(4, 2))

        # SIC codes treeview inside details
        self.sic_label = ttk.Label(self.details_frame, text="Top SIC Codes:",
                                    font=("TkDefaultFont", 9, "bold"))
        self.sic_label.pack(anchor="w", padx=8, pady=(4, 0))
        sic_tree_frame = ttk.Frame(self.details_frame)
        sic_tree_frame.pack(fill="x", padx=8, pady=(0, 6))
        self.sic_tree = ttk.Treeview(
            sic_tree_frame, columns=("sic_code", "count"),
            show="headings", height=6,
        )
        self.sic_tree.heading("sic_code", text="SIC Code")
        self.sic_tree.heading("count", text="Count")
        self.sic_tree.column("sic_code", width=120)
        self.sic_tree.column("count", width=120)
        self.sic_tree.pack(fill="x")

        # Progress bar + task status
        self.progress = ttk.Progressbar(self.root, mode="indeterminate")
        self.task_label = ttk.Label(self.root, text="", foreground="gray")
        self.task_label.pack(fill="x", padx=14, pady=2)

        # Filter frame
        self.filter_frame = ttk.LabelFrame(self.root, text="Search Companies")
        self.filter_frame.pack(fill="x", padx=10, pady=4)
        self._build_filters()

        # Results frame
        self.results_frame = ttk.Frame(self.root)
        self.results_frame.pack(fill="both", expand=True, padx=10, pady=4)
        self._build_results()

        self._refresh_db_info()

    def _build_filters(self) -> None:
        f = self.filter_frame
        # Row 0
        ttk.Label(f, text="SIC Code(s):").grid(row=0, column=0, sticky="e", padx=4, pady=2)
        self.sic_var = tk.StringVar()
        sic_entry = ttk.Entry(f, textvariable=self.sic_var, width=24)
        sic_entry.grid(row=0, column=1, sticky="w", padx=4)
        sic_entry.insert(0, "e.g. 62012,69201")
        sic_entry.configure(foreground="gray")
        sic_entry.bind("<FocusIn>", lambda e: self._clear_placeholder(e, self.sic_var, "e.g. 62012,69201"))
        sic_entry.bind("<FocusOut>", lambda e: self._restore_placeholder(e, self.sic_var, "e.g. 62012,69201"))

        ttk.Label(f, text="Company Status:").grid(row=0, column=2, sticky="e", padx=4)
        self.status_var = tk.StringVar(value="Active")
        self.status_combo = ttk.Combobox(f, textvariable=self.status_var, state="readonly", width=20)
        self.status_combo.grid(row=0, column=3, sticky="w", padx=4)

        ttk.Label(f, text="Company Type:").grid(row=0, column=4, sticky="e", padx=4)
        self.type_var = tk.StringVar()
        self.type_combo = ttk.Combobox(f, textvariable=self.type_var, state="readonly", width=20)
        self.type_combo.grid(row=0, column=5, sticky="w", padx=4)

        # Row 1
        ttk.Label(f, text="Postcode Prefix:").grid(row=1, column=0, sticky="e", padx=4, pady=2)
        self.postcode_var = tk.StringVar()
        ttk.Entry(f, textvariable=self.postcode_var, width=12).grid(row=1, column=1, sticky="w", padx=4)

        ttk.Label(f, text="Year From:").grid(row=1, column=2, sticky="e", padx=4)
        self.year_from_var = tk.StringVar()
        ttk.Spinbox(f, textvariable=self.year_from_var, from_=1800, to=2030,
                     width=8, validate="key",
                     validatecommand=(f.register(self._validate_year), "%P"),
                     ).grid(row=1, column=3, sticky="w", padx=4)

        ttk.Label(f, text="Year To:").grid(row=1, column=4, sticky="e", padx=4)
        self.year_to_var = tk.StringVar()
        ttk.Spinbox(f, textvariable=self.year_to_var, from_=1800, to=2030,
                     width=8, validate="key",
                     validatecommand=(f.register(self._validate_year), "%P"),
                     ).grid(row=1, column=5, sticky="w", padx=4)

        # Row 2
        ttk.Label(f, text="Country:").grid(row=2, column=0, sticky="e", padx=4, pady=2)
        self.country_var = tk.StringVar()
        self.country_combo = ttk.Combobox(f, textvariable=self.country_var, state="readonly", width=24)
        self.country_combo.grid(row=2, column=1, sticky="w", padx=4)

        btn_row = ttk.Frame(f)
        btn_row.grid(row=2, column=2, columnspan=4, sticky="e", padx=4, pady=4)
        self.btn_search = ttk.Button(btn_row, text="Search", command=self._on_search)
        self.btn_search.pack(side="left", padx=2)
        ttk.Button(btn_row, text="Clear", command=self._on_clear).pack(side="left", padx=2)
        self.btn_export = ttk.Button(btn_row, text="Export CSV", command=self._on_export)
        self.btn_export.pack(side="left", padx=2)

    @staticmethod
    def _validate_year(value: str) -> bool:
        """Only allow digits or empty in year spinboxes."""
        return value == "" or value.isdigit()

    @staticmethod
    def _clear_placeholder(event, var: tk.StringVar, placeholder: str) -> None:
        if var.get() == placeholder:
            var.set("")
            event.widget.configure(foreground="black")

    @staticmethod
    def _restore_placeholder(event, var: tk.StringVar, placeholder: str) -> None:
        if not var.get():
            var.set(placeholder)
            event.widget.configure(foreground="gray")

    def _build_results(self) -> None:
        rf = self.results_frame
        self.count_label = ttk.Label(rf, text="")
        self.count_label.pack(anchor="w")

        tree_frame = ttk.Frame(rf)
        tree_frame.pack(fill="both", expand=True)

        cols = [c[0] for c in COLUMNS]
        self.tree = ttk.Treeview(tree_frame, columns=cols, show="headings", selectmode="browse")
        for key, heading, width in COLUMNS:
            self.tree.heading(key, text=heading, command=lambda k=key: self._on_sort(k))
            self.tree.column(key, width=width, minwidth=40)
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        # Pagination
        pag = ttk.Frame(rf)
        pag.pack(fill="x", pady=4)
        self.btn_prev = ttk.Button(pag, text="<< Prev", command=self._on_prev)
        self.btn_prev.pack(side="left")
        self.page_label = ttk.Label(pag, text="Page 1 of 1")
        self.page_label.pack(side="left", padx=10)
        self.btn_next = ttk.Button(pag, text="Next >>", command=self._on_next)
        self.btn_next.pack(side="left")

    # ── Database info ────────────────────────────────────────────────

    def _refresh_db_info(self) -> None:
        """Load database status and populate filter dropdowns."""
        has_csvs = any(self.ch.data_dir.glob("BasicCompanyData*.csv"))

        try:
            stats = self.ch.info()
            opts = self.ch.get_filter_options()
        except FileNotFoundError:
            stats = None
            opts = {"statuses": [], "company_types": [], "countries": []}

        if stats:
            # Ready state
            total = stats.get("total_companies", 0)
            modified = stats.get("db_file_modified", "")
            self.status_indicator.configure(
                text=f"\u25cf Ready ({total:,} companies)", foreground="green"
            )
            detail = f"Last updated: {modified}" if modified else ""
            self.status_detail.configure(text=detail)

            # Populate details
            breakdown = stats.get("status_breakdown", {})
            parts = [f"{name or 'Unknown'}: {cnt:,}" for name, cnt in breakdown.items()]
            self.breakdown_label.configure(text="  |  ".join(parts))

            self._top_sic_codes = stats.get("top_sic_codes", [])
            self.sic_tree.delete(*self.sic_tree.get_children())
            for entry in self._top_sic_codes:
                self.sic_tree.insert("", "end", values=(
                    entry["sic_code"], f"{entry['count']:,}",
                ))

            # Show details toggle
            self.details_toggle.pack(anchor="w", padx=8, pady=(0, 4))

            # Show filter / results frames
            if not self.filter_frame.winfo_manager():
                self.filter_frame.pack(fill="x", padx=10, pady=4, after=self.task_label)
            if not self.results_frame.winfo_manager():
                self.results_frame.pack(fill="both", expand=True, padx=10, pady=4, after=self.filter_frame)

            # Populate combos
            self.status_combo["values"] = [""] + opts.get("statuses", [])
            self.type_combo["values"] = [""] + opts.get("company_types", [])
            self.country_combo["values"] = [""] + opts.get("countries", [])

        elif has_csvs:
            # Downloaded but not processed
            self.status_indicator.configure(
                text="\u25cf Downloaded (not processed)", foreground="#cc8800"
            )
            self.status_detail.configure(text="Click Process to build the database")
            self.details_toggle.pack_forget()
            self.details_frame.pack_forget()
            self._details_visible = False
            self.filter_frame.pack_forget()
            self.results_frame.pack_forget()

        else:
            # Not setup
            self.status_indicator.configure(
                text="\u25cf Not Setup", foreground="red"
            )
            self.status_detail.configure(text="Click Sync to download and build")
            self.details_toggle.pack_forget()
            self.details_frame.pack_forget()
            self._details_visible = False
            self.filter_frame.pack_forget()
            self.results_frame.pack_forget()

    def _toggle_details(self) -> None:
        """Show/hide the database details section."""
        if self._details_visible:
            self.details_frame.pack_forget()
            self.details_toggle.configure(text="\u25b6 Show Details")
            self._details_visible = False
        else:
            self.details_frame.pack(fill="x", padx=4, pady=(0, 6))
            self.details_toggle.configure(text="\u25bc Hide Details")
            self._details_visible = True

    # ── Filter helpers ───────────────────────────────────────────────

    def _get_filters(self) -> dict[str, Any]:
        filters: dict[str, Any] = {}
        sic_val = self.sic_var.get().strip()
        if sic_val and sic_val != "e.g. 62012,69201":
            filters["sic_codes"] = sic_val
        if self.status_var.get():
            filters["status"] = self.status_var.get()
        if self.type_var.get():
            filters["company_type"] = self.type_var.get()
        if self.postcode_var.get().strip():
            filters["postcode_prefix"] = self.postcode_var.get().strip()
        try:
            yf = int(self.year_from_var.get())
            filters["year_from"] = yf
        except (ValueError, TypeError):
            pass
        try:
            yt = int(self.year_to_var.get())
            filters["year_to"] = yt
        except (ValueError, TypeError):
            pass
        if self.country_var.get():
            filters["country"] = self.country_var.get()
        filters["sort_by"] = self.sort_by
        filters["sort_order"] = self.sort_order
        filters["page"] = self.page
        filters["page_size"] = PAGE_SIZE
        return filters

    # ── Search / sort / pagination ───────────────────────────────────

    def _do_search(self) -> None:
        """Run search in a background thread to avoid blocking the UI."""
        if self._task_running:
            self._set_status("A task is already running.", error=True)
            return

        filters = self._get_filters()
        self._set_status("Searching...")
        self.btn_search.configure(state="disabled")
        self.btn_export.configure(state="disabled")

        def worker():
            try:
                rows, total = self.ch.query_advanced(**filters)
                self.root.after(0, lambda: self._display_results(rows, total))
            except FileNotFoundError:
                self.root.after(0, lambda: self._set_status("No database found. Click Sync first."))
            except Exception as exc:
                logger.exception("Query error")
                err_msg = f"Query error: {exc}"
                self.root.after(0, lambda msg=err_msg: self._set_status(msg, error=True))
            finally:
                self.root.after(0, lambda: self.btn_search.configure(state="normal"))
                self.root.after(0, lambda: self.btn_export.configure(state="normal"))

        threading.Thread(target=worker, daemon=True).start()

    def _display_results(self, rows: list[dict], total: int) -> None:
        """Update the treeview with query results (called on main thread)."""
        self.total_count = total
        self.total_pages = max(1, -(-total // PAGE_SIZE))

        self.tree.delete(*self.tree.get_children())
        for row in rows:
            vals = [row.get(c[0], "") or "" for c in COLUMNS]
            self.tree.insert("", "end", values=vals)

        self.count_label.configure(text=f"{total:,} companies found")
        self.page_label.configure(text=f"Page {self.page} of {self.total_pages}")
        self.btn_prev.configure(state="normal" if self.page > 1 else "disabled")
        self.btn_next.configure(state="normal" if self.page < self.total_pages else "disabled")

        if rows:
            self._set_status("")
        else:
            self._set_status("No results found. Try adjusting your filters.")

    def _on_search(self) -> None:
        self.page = 1
        self._do_search()

    def _on_sort(self, col: str) -> None:
        if self.sort_by == col and self.sort_order == "ASC":
            self.sort_order = "DESC"
        else:
            self.sort_by = col
            self.sort_order = "ASC"
        for key, heading, _ in COLUMNS:
            arrow = ""
            if key == self.sort_by:
                arrow = " \u25B2" if self.sort_order == "ASC" else " \u25BC"
            self.tree.heading(key, text=heading + arrow)
        self.page = 1
        self._do_search()

    def _on_prev(self) -> None:
        if self.page > 1:
            self.page -= 1
            self._do_search()

    def _on_next(self) -> None:
        if self.page < self.total_pages:
            self.page += 1
            self._do_search()

    def _on_clear(self) -> None:
        self.sic_var.set("")
        self.status_var.set("Active")
        self.type_var.set("")
        self.postcode_var.set("")
        self.year_from_var.set("")
        self.year_to_var.set("")
        self.country_var.set("")
        self.sort_by = "company_name"
        self.sort_order = "ASC"
        self.page = 1
        self.tree.delete(*self.tree.get_children())
        self.count_label.configure(text="")
        self.page_label.configure(text="Page 1 of 1")
        self._set_status("")
        for key, heading, _ in COLUMNS:
            self.tree.heading(key, text=heading)

    # ── Export ───────────────────────────────────────────────────────

    def _on_export(self) -> None:
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile="companies_export.csv",
        )
        if not path:
            return

        filters = self._get_filters()
        filters.pop("page", None)
        filters.pop("page_size", None)
        filters.pop("sort_by", None)
        filters.pop("sort_order", None)

        self._set_status("Exporting...")
        self.btn_export.configure(state="disabled")

        def worker():
            try:
                n = self.ch.export_filtered_csv(path, **filters)
                self.root.after(0, lambda: self._set_status(f"Exported {n:,} rows to {path}"))
            except FileNotFoundError:
                self.root.after(0, lambda: self._set_status("No database found. Click Sync first."))
            except Exception as exc:
                logger.exception("Export error")
                err_msg = f"Export error: {exc}"
                self.root.after(0, lambda msg=err_msg: self._set_status(msg, error=True))
            finally:
                self.root.after(0, lambda: self.btn_export.configure(state="normal"))

        threading.Thread(target=worker, daemon=True).start()

    # ── Background tasks ─────────────────────────────────────────────

    def _set_status(self, msg: str, *, error: bool = False) -> None:
        self.task_label.configure(text=msg, foreground="red" if error else "gray")

    def _set_buttons_state(self, state: str) -> None:
        for btn in (self.btn_sync, self.btn_download, self.btn_process,
                    self.btn_search, self.btn_export):
            btn.configure(state=state)

    def _run_task(self, target, label: str) -> None:
        if self._task_running:
            self._set_status("A task is already running.", error=True)
            return
        with self._lock:
            self._task_running = True
            self._task_error = None
            self._task_message = label
        self._set_status(label)
        self._set_buttons_state("disabled")
        self.progress.pack(fill="x", padx=14, pady=2, before=self.task_label)
        self.progress.start(15)
        threading.Thread(target=target, daemon=True).start()
        self.root.after(500, self._poll_task)

    def _poll_task(self) -> None:
        with self._lock:
            msg = self._task_message
            err = self._task_error
            running = self._task_running
        self._set_status(msg, error=bool(err))
        if running:
            self.root.after(500, self._poll_task)
        else:
            self.progress.stop()
            self.progress.pack_forget()
            self._set_buttons_state("normal")
            self._refresh_db_info()

    def _progress_cb(self, msg: str) -> None:
        """Callback for download/process progress — routes to UI."""
        with self._lock:
            self._task_message = msg

    def _on_sync(self) -> None:
        def worker() -> None:
            try:
                csv_files = self.ch.download(progress_callback=self._progress_cb)
                self._progress_cb(f"Processing {len(csv_files)} files...")
                n = self.ch.process(csv_files=csv_files, progress_callback=self._progress_cb)
                self._progress_cb(f"Sync complete! {n:,} companies loaded.")
            except Exception as exc:
                logger.exception("Sync failed")
                with self._lock:
                    self._task_error = str(exc)
                    self._task_message = f"Sync failed: {exc}"
            finally:
                with self._lock:
                    self._task_running = False
        self._run_task(worker, "Starting sync...")

    def _on_download(self) -> None:
        def worker() -> None:
            try:
                csv_files = self.ch.download(progress_callback=self._progress_cb)
                self._progress_cb(
                    f"Download complete! {len(csv_files)} files ready. "
                    "Click Process to build the database."
                )
            except Exception as exc:
                logger.exception("Download failed")
                with self._lock:
                    self._task_error = str(exc)
                    self._task_message = f"Download failed: {exc}"
            finally:
                with self._lock:
                    self._task_running = False
        self._run_task(worker, "Downloading...")

    def _on_process(self) -> None:
        def worker() -> None:
            try:
                n = self.ch.process(progress_callback=self._progress_cb)
                self._progress_cb(f"Processing complete! {n:,} companies loaded.")
            except Exception as exc:
                logger.exception("Processing failed")
                with self._lock:
                    self._task_error = str(exc)
                    self._task_message = f"Processing failed: {exc}"
            finally:
                with self._lock:
                    self._task_running = False
        self._run_task(worker, "Processing...")

    # ── Window close ─────────────────────────────────────────────────

    def _on_close(self) -> None:
        """Handle window close — warn if a task is running."""
        if self._task_running:
            # Let the daemon thread finish on its own
            self._set_status("Closing... waiting for background task to finish safely.")
        self.root.destroy()

    # ── Run ──────────────────────────────────────────────────────────

    def run(self) -> None:
        """Start the Tkinter main loop."""
        self.root.mainloop()


def main(db_path: str = "ch_bulk.duckdb", data_dir: str = "./data") -> None:
    """Launch the Tkinter GUI."""
    # Route all logging to a file when running as GUI — keep terminal clean
    log_path = Path(data_dir) / "ch_bulk.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    root_logger = logging.getLogger()
    # Remove any existing handlers (e.g., from CLI's basicConfig)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    root_logger.addHandler(file_handler)
    root_logger.setLevel(logging.INFO)
    # Silence httpx request logging to file too (noisy)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    app = ChBulkApp(db_path=db_path, data_dir=data_dir)
    app.run()


if __name__ == "__main__":
    main()
