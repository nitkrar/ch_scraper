"""Tkinter GUI for Companies House + CQC bulk data explorer.

Layout (left-rail navigation):
    ┌──────────────┬────────────────────────────────────────┐
    │ CH Companies │  Tab title                              │
    │ CQC          │  ─────────────────────────────────────  │
    │              │  DB status + per-tab action buttons     │
    │              │  ─────────────────────────────────────  │
    │              │  (CQC only) sub-tabs: Locations | Provs │
    │              │  ─────────────────────────────────────  │
    │              │  Filter inputs                          │
    │              │  Search  Clear  Export                  │
    │              │  ─────────────────────────────────────  │
    │              │  Results table + Prev / Page X / Next   │
    └──────────────┴────────────────────────────────────────┘
                    Bottom: progress bar + task status

Each outer pane is self-contained. CQC sub-tabs share the filter frame
and DB status row but render different result tables and call different
queries.
"""

from __future__ import annotations

import logging
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any

from ch_bulk.api import ChBulk
from ch_bulk.core.paths import DEFAULT_DATA_DIR, default_db_path
from ch_bulk.companies_house.processor import SanityCheckError

logger = logging.getLogger(__name__)

PAGE_SIZE = 50

CH_COLUMNS = [
    ("company_number", "Company No", 90),
    ("company_name", "Company Name", 240),
    ("company_status", "Status", 80),
    ("company_type", "Type", 60),
    ("sic_code_1", "SIC 1", 60),
    ("postcode", "Postcode", 80),
    ("incorporation_date", "Inc. Date", 90),
    ("is_active", "In Scrape", 70),
]

CQC_LOCATION_COLUMNS = [
    ("location_id", "Location ID", 100),
    ("name", "Location Name", 220),
    ("service_types", "Service Types", 200),
    ("provider_name", "Provider", 200),
    ("postcode", "Postcode", 80),
    ("region", "Region", 110),
    ("is_active", "In Scrape", 70),
]

CQC_PROVIDER_COLUMNS = [
    ("provider_id", "Provider ID", 110),
    ("provider_name", "Provider Name", 240),
    ("active_location_count", "Active Locs", 80),
    ("service_types_list", "Service Types", 220),
    ("regions_list", "Regions", 160),
    ("local_authorities_list", "Local Authorities", 200),
    ("is_active", "In Scrape", 70),
]


# ─────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────

def _format_sanity_failure(exc: SanityCheckError) -> str:
    r = exc.result
    lines = ["Sanity check failed:\n"]
    if r.dup_distinct_numbers > 0:
        sample = ", ".join(str(s) for s in r.dup_sample[:5])
        lines.append(
            f"  • Duplicate keys in source: {r.dup_distinct_numbers} distinct "
            f"({r.dup_excess_rows} excess rows). Sample: [{sample}]"
        )
        lines.append("    THIS CANNOT BE OVERRIDDEN — fix the source CSV first.")
    if r.companies_exists and r.row_pct is not None and r.row_pct > 5.0:
        lines.append(
            f"  • Row count delta: {r.row_pct:.2f}% "
            f"({r.old_total:,} → {r.new_total:,}, {r.row_delta:+,})"
        )
    if r.companies_exists and r.inactive_pct is not None and r.inactive_pct > 5.0:
        lines.append(
            f"  • Inactive churn: {r.inactive_pct:.2f}% — would mark "
            f"{r.would_be_inactivated:,} of {r.currently_active:,} active rows inactive"
        )
    return "\n".join(lines)


def _format_cell(v: Any) -> str:
    """Stringify a cell value; lists shown inline."""
    if v is None:
        return ""
    if isinstance(v, list):
        return ", ".join(str(x) for x in v)
    return str(v)


def _make_filter_grid_responsive(filter_frame: ttk.LabelFrame) -> None:
    """Configure column weights + stretch every direct child widget so
    a filter grid grows/shrinks with the window. Value-columns (odd-
    numbered) get weight 1; label-columns (even-numbered) stay sized to
    their content. Children that were grid()'d with sticky="w" get
    upgraded to sticky="ew"."""
    # 6 logical columns (0..5): labels in 0,2,4 ; values in 1,3,5
    for col in (0, 2, 4):
        filter_frame.grid_columnconfigure(col, weight=0)
    for col in (1, 3, 5):
        filter_frame.grid_columnconfigure(col, weight=1)
    for child in filter_frame.winfo_children():
        info = child.grid_info()
        if not info:
            continue  # not in this frame's grid
        col = int(info.get("column", 0))
        if col in (1, 3, 5) and info.get("sticky") in ("w", ""):
            child.grid_configure(sticky="ew")


# ─────────────────────────────────────────────────────────────────────
# Pane base
# ─────────────────────────────────────────────────────────────────────

class _PaneBase:
    """One left-rail pane. Subclasses build their own content into self.frame."""

    title: str = ""

    def __init__(self, parent: ttk.Frame, app: "ChBulkApp"):
        self.app = app
        self.ch = app.ch
        self.frame = ttk.Frame(parent)

    def show(self) -> None:
        self.frame.pack(fill="both", expand=True)
        self.refresh()

    def hide(self) -> None:
        self.frame.pack_forget()

    def refresh(self) -> None:
        pass

    # Each pane exposes its action buttons so the app can disable them
    # while a task runs.
    def action_buttons(self) -> list[ttk.Widget]:
        return []


# ─────────────────────────────────────────────────────────────────────
# CH pane
# ─────────────────────────────────────────────────────────────────────

class CHPane(_PaneBase):
    title = "CH Companies"

    def __init__(self, parent: ttk.Frame, app: "ChBulkApp"):
        super().__init__(parent, app)
        self.page = 1
        self.total_pages = 1
        self.sort_by = "company_name"
        self.sort_order = "ASC"
        self._build()
        self.refresh()

    def _build(self) -> None:
        ttk.Label(
            self.frame, text=self.title,
            font=("TkDefaultFont", 14, "bold"),
        ).pack(pady=(8, 4), anchor="w", padx=10)

        # DB status + buttons
        self.status_frame = ttk.LabelFrame(self.frame, text="Database")
        self.status_frame.pack(fill="x", padx=10, pady=4)
        row = ttk.Frame(self.status_frame)
        row.pack(fill="x", padx=8, pady=6)
        self.status_indicator = ttk.Label(
            row, text="● Not loaded", foreground="red",
            font=("TkDefaultFont", 11, "bold"),
        )
        self.status_indicator.pack(side="left")
        self.status_detail = ttk.Label(row, text="", foreground="gray")
        self.status_detail.pack(side="left", padx=(8, 0))
        self.btn_process = ttk.Button(row, text="Process", command=self._on_process)
        self.btn_process.pack(side="right", padx=2)
        self.btn_download = ttk.Button(row, text="Download", command=self._on_download)
        self.btn_download.pack(side="right", padx=2)
        self.btn_sync = ttk.Button(row, text="Sync", command=self._on_sync)
        self.btn_sync.pack(side="right", padx=2)

        # Filters
        self.filter_frame = ttk.LabelFrame(self.frame, text="Filters")
        self.filter_frame.pack(fill="x", padx=10, pady=4)
        f = self.filter_frame

        ttk.Label(f, text="SIC code(s):").grid(row=0, column=0, sticky="e", padx=4, pady=2)
        self.sic_var = tk.StringVar()
        ttk.Entry(f, textvariable=self.sic_var, width=20).grid(row=0, column=1, sticky="w", padx=4)

        ttk.Label(f, text="Status:").grid(row=0, column=2, sticky="e", padx=4)
        self.status_var = tk.StringVar(value="Active")
        self.status_combo = ttk.Combobox(f, textvariable=self.status_var, state="readonly", width=18)
        self.status_combo.grid(row=0, column=3, sticky="w", padx=4)
        self.status_combo.bind("<<ComboboxSelected>>", lambda _e: self._on_search())

        ttk.Label(f, text="Postcode prefix:").grid(row=0, column=4, sticky="e", padx=4)
        self.postcode_var = tk.StringVar()
        ttk.Entry(f, textvariable=self.postcode_var, width=10).grid(row=0, column=5, sticky="w", padx=4)

        ttk.Label(f, text="Year from:").grid(row=1, column=0, sticky="e", padx=4, pady=2)
        self.year_from_var = tk.StringVar()
        ttk.Entry(f, textvariable=self.year_from_var, width=8).grid(row=1, column=1, sticky="w", padx=4)
        ttk.Label(f, text="Year to:").grid(row=1, column=2, sticky="e", padx=4)
        self.year_to_var = tk.StringVar()
        ttk.Entry(f, textvariable=self.year_to_var, width=8).grid(row=1, column=3, sticky="w", padx=4)

        ttk.Label(f, text="Country:").grid(row=1, column=4, sticky="e", padx=4)
        self.country_var = tk.StringVar()
        self.country_combo = ttk.Combobox(f, textvariable=self.country_var, state="readonly", width=24)
        self.country_combo.grid(row=1, column=5, sticky="w", padx=4)
        self.country_combo.bind("<<ComboboxSelected>>", lambda _e: self._on_search())

        ttk.Label(f, text="In latest scrape:").grid(row=2, column=0, sticky="e", padx=4, pady=2)
        self.active_var = tk.StringVar(value="Active only")
        self.active_combo = ttk.Combobox(
            f, textvariable=self.active_var, state="readonly", width=18,
            values=["Active only", "Inactive only", "All"],
        )
        self.active_combo.grid(row=2, column=1, sticky="w", padx=4)
        self.active_combo.bind("<<ComboboxSelected>>", lambda _e: self._on_search())

        action_row = ttk.Frame(f)
        action_row.grid(row=3, column=0, columnspan=6, sticky="e", padx=4, pady=(6, 4))
        self.btn_search = ttk.Button(action_row, text="Search", command=self._on_search)
        self.btn_search.pack(side="left", padx=2)
        ttk.Button(action_row, text="Clear", command=self._on_clear).pack(side="left", padx=2)
        self.btn_export = ttk.Button(action_row, text="Export CSV", command=self._on_export)
        self.btn_export.pack(side="left", padx=2)

        _make_filter_grid_responsive(f)

        # Results
        rf = ttk.Frame(self.frame)
        rf.pack(fill="both", expand=True, padx=10, pady=4)
        self.count_label = ttk.Label(rf, text="")
        self.count_label.pack(anchor="w")
        tree_frame = ttk.Frame(rf)
        tree_frame.pack(fill="both", expand=True)
        # Use grid inside tree_frame so we can place both scrollbars cleanly
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)
        cols = [c[0] for c in CH_COLUMNS]
        self.tree = ttk.Treeview(tree_frame, columns=cols, show="headings", selectmode="browse")
        for key, heading, width in CH_COLUMNS:
            self.tree.heading(key, text=heading, command=lambda k=key: self._on_sort(k))
            # stretch=False keeps the column at its declared width when
            # the window narrows — overflow is reached via the hbar
            # instead of squishing the column.
            self.tree.column(key, width=width, minwidth=width, stretch=False)
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        pag = ttk.Frame(rf)
        pag.pack(fill="x", pady=4)
        self.btn_prev = ttk.Button(pag, text="<< Prev", command=self._on_prev)
        self.btn_prev.pack(side="left")
        self.page_label = ttk.Label(pag, text="Page 1 of 1")
        self.page_label.pack(side="left", padx=10)
        self.btn_next = ttk.Button(pag, text="Next >>", command=self._on_next)
        self.btn_next.pack(side="left")

    def action_buttons(self) -> list[ttk.Widget]:
        return [self.btn_sync, self.btn_download, self.btn_process,
                self.btn_search, self.btn_export]

    def _get_filters(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        sic = self.sic_var.get().strip()
        if sic:
            out["sic_codes"] = sic
        if self.status_var.get():
            out["status"] = self.status_var.get()
        if self.postcode_var.get().strip():
            out["postcode_prefix"] = self.postcode_var.get().strip()
        try:    out["year_from"] = int(self.year_from_var.get())
        except (TypeError, ValueError): pass
        try:    out["year_to"]   = int(self.year_to_var.get())
        except (TypeError, ValueError): pass
        if self.country_var.get():
            out["country"] = self.country_var.get()
        choice = self.active_var.get()
        if choice == "Active only":
            out["is_active"] = True
        elif choice == "Inactive only":
            out["is_active"] = False
        out["sort_by"] = self.sort_by
        out["sort_order"] = self.sort_order
        out["page"] = self.page
        out["page_size"] = PAGE_SIZE
        return out

    def _on_clear(self) -> None:
        for v in (self.sic_var, self.postcode_var, self.year_from_var,
                  self.year_to_var, self.country_var):
            v.set("")
        self.status_var.set("Active")
        self.active_var.set("Active only")
        self.sort_by = "company_name"
        self.sort_order = "ASC"
        self.page = 1
        self.tree.delete(*self.tree.get_children())
        self.count_label.configure(text="")
        self.page_label.configure(text="Page 1 of 1")

    def _on_search(self) -> None:
        self.page = 1
        self._run_query()

    def _on_sort(self, col: str) -> None:
        if self.sort_by == col and self.sort_order == "ASC":
            self.sort_order = "DESC"
        else:
            self.sort_by, self.sort_order = col, "ASC"
        self.page = 1
        self._run_query()

    def _on_prev(self) -> None:
        if self.page > 1:
            self.page -= 1
            self._run_query()

    def _on_next(self) -> None:
        if self.page < self.total_pages:
            self.page += 1
            self._run_query()

    def _run_query(self) -> None:
        if self.app._task_running:
            self.app._set_status("A task is already running.", error=True); return
        self.app._set_status("Searching...")
        self.btn_search.configure(state="disabled"); self.btn_export.configure(state="disabled")
        def worker():
            try:
                rows, total = self.ch.query_advanced(**self._get_filters())
                self.app.root.after(0, lambda: self._display(rows, total))
            except FileNotFoundError:
                self.app.root.after(0, lambda: self.app._set_status("No DB. Click Sync first."))
            except Exception as exc:
                logger.exception("CH query error")
                err = f"Query error: {exc}"
                self.app.root.after(0, lambda m=err: self.app._set_status(m, error=True))
            finally:
                self.app.root.after(0, lambda: self.btn_search.configure(state="normal"))
                self.app.root.after(0, lambda: self.btn_export.configure(state="normal"))
        threading.Thread(target=worker, daemon=True).start()

    def _display(self, rows, total):
        self.total_pages = max(1, -(-total // PAGE_SIZE))
        self.tree.delete(*self.tree.get_children())
        for row in rows:
            self.tree.insert("", "end", values=[_format_cell(row.get(c[0])) for c in CH_COLUMNS])
        self.count_label.configure(text=f"{total:,} rows")
        self.page_label.configure(text=f"Page {self.page} of {self.total_pages}")
        self.btn_prev.configure(state="normal" if self.page > 1 else "disabled")
        self.btn_next.configure(state="normal" if self.page < self.total_pages else "disabled")

    def _on_export(self) -> None:
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile="ch_export.csv",
        )
        if not path: return
        filters = self._get_filters()
        for k in ("page", "page_size", "sort_by", "sort_order"):
            filters.pop(k, None)
        self.app._set_status("Exporting...")
        def worker():
            try:
                n = self.ch.export_filtered_csv(path, **filters)
                self.app.root.after(0, lambda: self.app._set_status(f"Exported {n:,} rows to {path}"))
            except Exception as exc:
                logger.exception("CH export error")
                err = f"Export error: {exc}"
                self.app.root.after(0, lambda m=err: self.app._set_status(m, error=True))
        threading.Thread(target=worker, daemon=True).start()

    # ── DB tasks ──────────────────────────────────────────────────────

    def _on_sync(self) -> None:
        self.app._dispatch_with_sanity(
            lambda force: self.ch.sync(force=force),
            success_msg=lambda n: f"CH sync complete: {n:,} companies",
            label="CH syncing...",
        )

    def _on_download(self) -> None:
        def worker() -> None:
            try:
                csv_files = self.ch.download(progress_callback=self.app._progress_cb)
                self.app._progress_cb(f"CH download complete: {len(csv_files)} files")
            except Exception as exc:
                logger.exception("CH download failed")
                self.app._set_task_error(f"CH download failed: {exc}")
            finally:
                self.app._task_done()
        self.app._run_task(worker, "CH downloading...")

    def _on_process(self) -> None:
        self.app._dispatch_with_sanity(
            lambda force: self.ch.process(progress_callback=self.app._progress_cb, force=force),
            success_msg=lambda n: f"CH process complete: {n:,} companies",
            label="CH processing...",
        )

    def refresh(self) -> None:
        try:
            stats = self.ch.info()
            opts = self.ch.get_filter_options()
        except FileNotFoundError:
            stats = None
            opts = {"statuses": [], "company_types": [], "countries": []}
        if stats:
            total = stats.get("total_companies", 0)
            self.status_indicator.configure(
                text=f"● Ready ({total:,} companies)", foreground="green",
            )
            modified = stats.get("db_file_modified", "")
            self.status_detail.configure(text=f"Last updated: {modified}" if modified else "")
            self.status_combo["values"] = [""] + opts.get("statuses", [])
            self.country_combo["values"] = [""] + opts.get("countries", [])
        else:
            self.status_indicator.configure(text="● Not Setup", foreground="red")
            self.status_detail.configure(text="Click Sync to download and build")


# ─────────────────────────────────────────────────────────────────────
# CQC pane (with sub-tabs)
# ─────────────────────────────────────────────────────────────────────

class CQCPane(_PaneBase):
    title = "CQC"

    def __init__(self, parent: ttk.Frame, app: "ChBulkApp"):
        super().__init__(parent, app)
        self.sub_view = "Locations"  # or "Providers"
        self.page = 1
        self.total_pages = 1
        self.sort_by_loc = "name"
        self.sort_by_prov = "provider_name"
        self.sort_order = "ASC"
        self._build()
        self.refresh()

    def _build(self) -> None:
        ttk.Label(
            self.frame, text=self.title,
            font=("TkDefaultFont", 14, "bold"),
        ).pack(pady=(8, 4), anchor="w", padx=10)

        # DB status (shared across sub-views)
        self.status_frame = ttk.LabelFrame(self.frame, text="Database")
        self.status_frame.pack(fill="x", padx=10, pady=4)
        row = ttk.Frame(self.status_frame)
        row.pack(fill="x", padx=8, pady=6)
        self.status_indicator = ttk.Label(
            row, text="● Not loaded", foreground="red",
            font=("TkDefaultFont", 11, "bold"),
        )
        self.status_indicator.pack(side="left")
        self.status_detail = ttk.Label(row, text="", foreground="gray")
        self.status_detail.pack(side="left", padx=(8, 0))
        self.btn_process = ttk.Button(row, text="Process", command=self._on_process)
        self.btn_process.pack(side="right", padx=2)
        self.btn_download = ttk.Button(row, text="Download", command=self._on_download)
        self.btn_download.pack(side="right", padx=2)
        self.btn_sync = ttk.Button(row, text="Sync", command=self._on_sync)
        self.btn_sync.pack(side="right", padx=2)

        # Sub-tabs (Locations / Providers)
        subtab_row = ttk.Frame(self.frame)
        subtab_row.pack(fill="x", padx=10, pady=(8, 0))
        self.subtab_var = tk.StringVar(value="Locations")
        for name in ("Locations", "Providers"):
            ttk.Radiobutton(
                subtab_row, text=name, variable=self.subtab_var, value=name,
                command=self._on_subtab_change,
            ).pack(side="left", padx=4)

        # Filters (shared between sub-views, with two location-only entries
        # that get disabled when Providers is selected)
        self.filter_frame = ttk.LabelFrame(self.frame, text="Filters")
        self.filter_frame.pack(fill="x", padx=10, pady=4)
        f = self.filter_frame

        # Row 0: text inputs
        ttk.Label(f, text="Provider name:").grid(row=0, column=0, sticky="e", padx=4, pady=2)
        self.provider_var = tk.StringVar()
        ttk.Entry(f, textvariable=self.provider_var, width=24).grid(row=0, column=1, sticky="w", padx=4)

        self.loc_name_label = ttk.Label(f, text="Location name:")
        self.loc_name_label.grid(row=0, column=2, sticky="e", padx=4)
        self.loc_name_var = tk.StringVar()
        self.loc_name_entry = ttk.Entry(f, textvariable=self.loc_name_var, width=24)
        self.loc_name_entry.grid(row=0, column=3, sticky="w", padx=4)

        self.postcode_label = ttk.Label(f, text="Postcode prefix:")
        self.postcode_label.grid(row=0, column=4, sticky="e", padx=4)
        self.postcode_var = tk.StringVar()
        self.postcode_entry = ttk.Entry(f, textvariable=self.postcode_var, width=10)
        self.postcode_entry.grid(row=0, column=5, sticky="w", padx=4)

        # Row 1: single-select dropdowns (one value each — empty = no filter)
        ttk.Label(f, text="Service type:").grid(row=1, column=0, sticky="e", padx=4, pady=2)
        self.service_var = tk.StringVar()
        self.service_combo = ttk.Combobox(
            f, textvariable=self.service_var, state="readonly", width=28,
        )
        self.service_combo.grid(row=1, column=1, sticky="w", padx=4)
        self.service_combo.bind("<<ComboboxSelected>>", lambda _e: self._on_search())

        ttk.Label(f, text="Region:").grid(row=1, column=2, sticky="e", padx=4)
        self.region_var = tk.StringVar()
        self.region_combo = ttk.Combobox(
            f, textvariable=self.region_var, state="readonly", width=22,
        )
        self.region_combo.grid(row=1, column=3, sticky="w", padx=4)
        self.region_combo.bind("<<ComboboxSelected>>", lambda _e: self._on_search())

        ttk.Label(f, text="Local authority:").grid(row=1, column=4, sticky="e", padx=4)
        self.la_var = tk.StringVar()
        self.la_combo = ttk.Combobox(
            f, textvariable=self.la_var, state="readonly", width=22,
        )
        self.la_combo.grid(row=1, column=5, sticky="w", padx=4)
        self.la_combo.bind("<<ComboboxSelected>>", lambda _e: self._on_search())

        # Row 2: active filter + provider-only location-count filter + action buttons
        ttk.Label(f, text="In latest scrape:").grid(row=2, column=0, sticky="e", padx=4, pady=4)
        self.active_var = tk.StringVar(value="Active only")
        self.active_combo = ttk.Combobox(
            f, textvariable=self.active_var, state="readonly", width=18,
            values=["Active only", "Inactive only", "All"],
        )
        self.active_combo.grid(row=2, column=1, sticky="w", padx=4)
        self.active_combo.bind("<<ComboboxSelected>>", lambda _e: self._on_search())

        self.min_locs_label = ttk.Label(f, text="Min active locations:")
        self.min_locs_label.grid(row=2, column=2, sticky="e", padx=4, pady=4)
        self.min_locs_var = tk.StringVar()
        self.min_locs_entry = ttk.Entry(f, textvariable=self.min_locs_var, width=8)
        self.min_locs_entry.grid(row=2, column=3, sticky="w", padx=4)

        action_row = ttk.Frame(f)
        action_row.grid(row=3, column=0, columnspan=6, sticky="e", padx=4, pady=(6, 4))
        self.btn_search = ttk.Button(action_row, text="Search", command=self._on_search)
        self.btn_search.pack(side="left", padx=2)
        ttk.Button(action_row, text="Clear", command=self._on_clear).pack(side="left", padx=2)
        self.btn_export = ttk.Button(action_row, text="Export CSV", command=self._on_export)
        self.btn_export.pack(side="left", padx=2)

        _make_filter_grid_responsive(f)

        # Results
        self.results_frame = ttk.Frame(self.frame)
        self.results_frame.pack(fill="both", expand=True, padx=10, pady=4)
        self.count_label = ttk.Label(self.results_frame, text="")
        self.count_label.pack(anchor="w")
        self._build_results_tree()  # creates self.tree using current sub_view columns

        pag = ttk.Frame(self.results_frame)
        pag.pack(fill="x", pady=4)
        self.btn_prev = ttk.Button(pag, text="<< Prev", command=self._on_prev)
        self.btn_prev.pack(side="left")
        self.page_label = ttk.Label(pag, text="Page 1 of 1")
        self.page_label.pack(side="left", padx=10)
        self.btn_next = ttk.Button(pag, text="Next >>", command=self._on_next)
        self.btn_next.pack(side="left")

    def _build_results_tree(self) -> None:
        """Rebuild the treeview with the active sub-view's columns."""
        for widget in self.results_frame.winfo_children():
            if isinstance(widget, ttk.Treeview) or (
                isinstance(widget, ttk.Frame)
                and any(isinstance(c, ttk.Treeview) for c in widget.winfo_children())
            ):
                widget.destroy()
        cols_spec = CQC_LOCATION_COLUMNS if self.sub_view == "Locations" else CQC_PROVIDER_COLUMNS
        tree_frame = ttk.Frame(self.results_frame)
        tree_frame.pack(fill="both", expand=True)
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)
        cols = [c[0] for c in cols_spec]
        self.tree = ttk.Treeview(tree_frame, columns=cols, show="headings", selectmode="browse")
        for key, heading, width in cols_spec:
            self.tree.heading(key, text=heading, command=lambda k=key: self._on_sort(k))
            # See CHPane: stretch=False makes the table scroll horizontally
            # instead of compressing columns when the window narrows.
            self.tree.column(key, width=width, minwidth=width, stretch=False)
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        self._cols_spec = cols_spec

    def action_buttons(self) -> list[ttk.Widget]:
        return [self.btn_sync, self.btn_download, self.btn_process,
                self.btn_search, self.btn_export]

    def _on_subtab_change(self) -> None:
        self.sub_view = self.subtab_var.get()
        # Show/hide location-only widgets (label + entry, both columns)
        if self.sub_view == "Locations":
            self.loc_name_label.grid();   self.loc_name_entry.grid()
            self.postcode_label.grid();   self.postcode_entry.grid()
            # Hide provider-only
            self.min_locs_label.grid_remove()
            self.min_locs_entry.grid_remove()
            self.min_locs_var.set("")
        else:  # Providers
            self.loc_name_label.grid_remove()
            self.loc_name_entry.grid_remove()
            self.postcode_label.grid_remove()
            self.postcode_entry.grid_remove()
            self.loc_name_var.set(""); self.postcode_var.set("")
            # Show provider-only
            self.min_locs_label.grid()
            self.min_locs_entry.grid()
        # Rebuild the result tree with new columns, replace pagination row
        for widget in self.results_frame.winfo_children():
            widget.destroy()
        self.count_label = ttk.Label(self.results_frame, text="")
        self.count_label.pack(anchor="w")
        self._build_results_tree()
        pag = ttk.Frame(self.results_frame)
        pag.pack(fill="x", pady=4)
        self.btn_prev = ttk.Button(pag, text="<< Prev", command=self._on_prev)
        self.btn_prev.pack(side="left")
        self.page_label = ttk.Label(pag, text="Page 1 of 1")
        self.page_label.pack(side="left", padx=10)
        self.btn_next = ttk.Button(pag, text="Next >>", command=self._on_next)
        self.btn_next.pack(side="left")
        self.page = 1
        # Refresh results for the new sub-view (skip during initial build
        # — the app's task_label doesn't exist yet)
        if hasattr(self.app, "task_label"):
            self._run_query()

    def _get_filters(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.provider_var.get().strip():
            out["provider_name_contains"] = self.provider_var.get().strip()
        if self.service_var.get():
            out["service_types"] = self.service_var.get()  # backend accepts str OR list
        if self.region_var.get():
            out["regions"] = self.region_var.get()
        if self.la_var.get():
            out["local_authorities"] = self.la_var.get()
        choice = self.active_var.get()
        if choice == "Active only":   out["is_active"] = True
        elif choice == "Inactive only": out["is_active"] = False

        if self.sub_view == "Locations":
            if self.loc_name_var.get().strip():
                out["name_contains"] = self.loc_name_var.get().strip()
            if self.postcode_var.get().strip():
                out["postcode_prefix"] = self.postcode_var.get().strip()
            out["sort_by"] = self.sort_by_loc
        else:
            # Providers-only: min active location count
            try:
                min_v = int(self.min_locs_var.get())
                out["min_active_location_count"] = min_v
            except (TypeError, ValueError):
                pass
            out["sort_by"] = self.sort_by_prov
        out["sort_order"] = self.sort_order
        out["page"] = self.page
        out["page_size"] = PAGE_SIZE
        return out

    def _on_clear(self) -> None:
        self.provider_var.set("")
        self.loc_name_var.set("")
        self.postcode_var.set("")
        self.service_var.set("")
        self.region_var.set("")
        self.la_var.set("")
        self.min_locs_var.set("")
        self.active_var.set("Active only")
        self.page = 1
        self.tree.delete(*self.tree.get_children())
        self.count_label.configure(text="")
        self.page_label.configure(text="Page 1 of 1")

    def _on_search(self) -> None:
        self.page = 1
        self._run_query()

    def _on_sort(self, col: str) -> None:
        attr = "sort_by_loc" if self.sub_view == "Locations" else "sort_by_prov"
        if getattr(self, attr) == col and self.sort_order == "ASC":
            self.sort_order = "DESC"
        else:
            setattr(self, attr, col)
            self.sort_order = "ASC"
        self.page = 1
        self._run_query()

    def _on_prev(self) -> None:
        if self.page > 1:
            self.page -= 1; self._run_query()

    def _on_next(self) -> None:
        if self.page < self.total_pages:
            self.page += 1; self._run_query()

    def _run_query(self) -> None:
        if self.app._task_running:
            self.app._set_status("A task is already running.", error=True); return
        self.app._set_status("Searching...")
        self.btn_search.configure(state="disabled"); self.btn_export.configure(state="disabled")
        def worker():
            try:
                filters = self._get_filters()
                if self.sub_view == "Locations":
                    rows, total = self.ch.query_cqc_locations_advanced(**filters)
                else:
                    rows, total = self.ch.query_cqc_providers_advanced(**filters)
                self.app.root.after(0, lambda: self._display(rows, total))
            except FileNotFoundError:
                self.app.root.after(0, lambda: self.app._set_status("No DB. Click Sync first."))
            except Exception as exc:
                logger.exception("CQC query error")
                err = f"Query error: {exc}"
                self.app.root.after(0, lambda m=err: self.app._set_status(m, error=True))
            finally:
                self.app.root.after(0, lambda: self.btn_search.configure(state="normal"))
                self.app.root.after(0, lambda: self.btn_export.configure(state="normal"))
        threading.Thread(target=worker, daemon=True).start()

    def _display(self, rows, total):
        self.total_pages = max(1, -(-total // PAGE_SIZE))
        self.tree.delete(*self.tree.get_children())
        for row in rows:
            self.tree.insert(
                "", "end",
                values=[_format_cell(row.get(c[0])) for c in self._cols_spec],
            )
        self.count_label.configure(text=f"{total:,} rows")
        self.page_label.configure(text=f"Page {self.page} of {self.total_pages}")
        self.btn_prev.configure(state="normal" if self.page > 1 else "disabled")
        self.btn_next.configure(state="normal" if self.page < self.total_pages else "disabled")

    def _on_export(self) -> None:
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile=f"cqc_{self.sub_view.lower()}_export.csv",
        )
        if not path: return
        filters = self._get_filters()
        for k in ("page", "page_size", "sort_by", "sort_order"):
            filters.pop(k, None)
        self.app._set_status("Exporting...")
        def worker():
            try:
                if self.sub_view == "Locations":
                    n = self.ch.export_cqc_locations_csv(path, **filters)
                else:
                    n = self.ch.export_cqc_providers_csv(path, **filters)
                self.app.root.after(0, lambda: self.app._set_status(f"Exported {n:,} rows to {path}"))
            except Exception as exc:
                logger.exception("CQC export error")
                err = f"Export error: {exc}"
                self.app.root.after(0, lambda m=err: self.app._set_status(m, error=True))
        threading.Thread(target=worker, daemon=True).start()

    # ── DB tasks ──────────────────────────────────────────────────────

    def _on_sync(self) -> None:
        self.app._dispatch_with_sanity(
            lambda force: self.ch.sync_cqc(progress_callback=self.app._progress_cb, force=force),
            success_msg=lambda n: f"CQC sync complete: {n:,} locations",
            label="CQC syncing...",
        )

    def _on_download(self) -> None:
        def worker() -> None:
            try:
                p = self.ch.download_cqc(progress_callback=self.app._progress_cb)
                self.app._progress_cb(f"CQC download complete: {p.name}")
            except Exception as exc:
                logger.exception("CQC download failed")
                self.app._set_task_error(f"CQC download failed: {exc}")
            finally:
                self.app._task_done()
        self.app._run_task(worker, "CQC downloading...")

    def _on_process(self) -> None:
        self.app._dispatch_with_sanity(
            lambda force: self.ch.process_cqc(progress_callback=self.app._progress_cb, force=force),
            success_msg=lambda n: f"CQC process complete: {n:,} locations",
            label="CQC processing...",
        )

    def refresh(self) -> None:
        import duckdb
        try:
            con = duckdb.connect(str(self.ch.db_path), read_only=True)
            loc = con.execute("SELECT COUNT(*) FROM cqc_locations").fetchone()[0]
            active = con.execute("SELECT COUNT(*) FROM cqc_locations WHERE is_active = TRUE").fetchone()[0]
            prov = con.execute("SELECT COUNT(*) FROM cqc_providers").fetchone()[0]
            con.close()
            self.status_indicator.configure(
                text=f"● Ready ({loc:,} locations / {prov:,} providers)",
                foreground="green",
            )
            self.status_detail.configure(text=f"{active:,} active")
        except Exception:
            self.status_indicator.configure(text="● Not Setup", foreground="red")
            self.status_detail.configure(text="Click Sync")

        opts = self.ch.get_cqc_filter_options()
        self.service_combo["values"] = [""] + opts.get("service_types", [])
        self.region_combo["values"]  = [""] + opts.get("regions", [])
        self.la_combo["values"]      = [""] + opts.get("local_authorities", [])

        # Ensure the per-subview disabled state is correct after first build
        self._on_subtab_change()


# ─────────────────────────────────────────────────────────────────────
# Settings pane
# ─────────────────────────────────────────────────────────────────────

class SettingsPane(_PaneBase):
    """Persistent app settings backed by ``<data_dir>/settings.json``."""

    title = "Settings"

    def __init__(self, parent: ttk.Frame, app: "ChBulkApp"):
        super().__init__(parent, app)
        self._settings = None  # loaded lazily in refresh()
        self._build()
        self.refresh()

    def _build(self) -> None:
        ttk.Label(
            self.frame, text=self.title,
            font=("TkDefaultFont", 14, "bold"),
        ).pack(pady=(8, 4), anchor="w", padx=10)

        info = ttk.Label(
            self.frame,
            text=(
                "Configure API keys and other persistent settings. "
                "Stored unencrypted at <data_dir>/settings.json."
            ),
            foreground="gray", wraplength=720, justify="left",
        )
        info.pack(anchor="w", padx=10, pady=(0, 8))

        # ── API keys section ─────────────────────────────────────────
        keys_frame = ttk.LabelFrame(self.frame, text="API keys")
        keys_frame.pack(fill="x", padx=10, pady=4)

        ttk.Label(keys_frame, text="Companies House:").grid(
            row=0, column=0, sticky="e", padx=4, pady=4,
        )
        self.ch_key_var = tk.StringVar()
        self.ch_key_entry = ttk.Entry(keys_frame, textvariable=self.ch_key_var, width=60)
        self.ch_key_entry.grid(row=0, column=1, sticky="ew", padx=4, pady=4)

        ttk.Label(keys_frame, text="CQC:").grid(
            row=1, column=0, sticky="e", padx=4, pady=4,
        )
        self.cqc_key_var = tk.StringVar()
        self.cqc_key_entry = ttk.Entry(keys_frame, textvariable=self.cqc_key_var, width=60)
        self.cqc_key_entry.grid(row=1, column=1, sticky="ew", padx=4, pady=4)

        keys_frame.grid_columnconfigure(1, weight=1)

        # ── Buttons + status ────────────────────────────────────────
        action_row = ttk.Frame(self.frame)
        action_row.pack(fill="x", padx=10, pady=8)
        self.btn_save = ttk.Button(action_row, text="Save", command=self._on_save)
        self.btn_save.pack(side="left", padx=2)
        self.btn_reload = ttk.Button(action_row, text="Reload from disk", command=self.refresh)
        self.btn_reload.pack(side="left", padx=2)
        self.save_status = ttk.Label(action_row, text="", foreground="gray")
        self.save_status.pack(side="left", padx=(12, 0))

        # Footer: file path
        from ch_bulk.core.settings import settings_path
        self.path_label = ttk.Label(
            self.frame,
            text=f"Settings file: {settings_path(self.ch.data_dir)}",
            foreground="gray", font=("TkDefaultFont", 9),
        )
        self.path_label.pack(anchor="w", padx=10, pady=(20, 4))

    def refresh(self) -> None:
        from ch_bulk.core.settings import load_settings
        self._settings = load_settings(self.ch.data_dir)
        self.ch_key_var.set(self._settings.get("api_keys", {}).get("companies_house", ""))
        self.cqc_key_var.set(self._settings.get("api_keys", {}).get("cqc", ""))
        self.save_status.configure(text="Loaded from disk", foreground="gray")

    def _on_save(self) -> None:
        from ch_bulk.core.settings import save_settings
        from datetime import datetime
        if self._settings is None:
            self._settings = {}
        self._settings.setdefault("api_keys", {})
        self._settings["api_keys"]["companies_house"] = self.ch_key_var.get().strip()
        self._settings["api_keys"]["cqc"] = self.cqc_key_var.get().strip()
        try:
            save_settings(self.ch.data_dir, self._settings)
            self.save_status.configure(
                text=f"Saved at {datetime.now().strftime('%H:%M:%S')}",
                foreground="green",
            )
        except Exception as exc:
            logger.exception("Failed to save settings")
            self.save_status.configure(text=f"Save failed: {exc}", foreground="red")


# ─────────────────────────────────────────────────────────────────────
# Main app
# ─────────────────────────────────────────────────────────────────────

class ChBulkApp:
    def __init__(
        self,
        db_path: str | Path | None = None,
        data_dir: str | Path | None = None,
    ) -> None:
        resolved_data_dir = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
        resolved_db_path = Path(db_path) if db_path is not None else default_db_path(resolved_data_dir)
        self.ch = ChBulk(data_dir=resolved_data_dir, db_path=resolved_db_path)
        self._task_running = False
        self._task_message = ""
        self._task_error: str | None = None
        self._lock = threading.Lock()
        self._build_ui()

    def _build_ui(self) -> None:
        self.root = tk.Tk()
        self.root.title("UK Bulk Data Explorer (CH + CQC)")
        self.root.geometry("1200x800")
        self.root.minsize(950, 600)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        # Outer layout: left nav | content | (bottom strip pinned below)
        outer = ttk.Frame(self.root)
        outer.pack(fill="both", expand=True)

        self.nav = ttk.Frame(outer, padding=4)
        self.nav.pack(side="left", fill="y")

        # Collapse toggle at the very top of the rail
        self._nav_collapsed = False
        self.nav_toggle = ttk.Button(
            self.nav, text="◀", width=3, command=self._toggle_nav,
        )
        self.nav_toggle.pack(anchor="w", pady=(2, 8))

        self.nav_header = ttk.Label(
            self.nav, text="Data sources",
            font=("TkDefaultFont", 9, "bold"), foreground="gray",
        )
        self.nav_header.pack(anchor="w", pady=(0, 6))

        self.content = ttk.Frame(outer)
        self.content.pack(side="left", fill="both", expand=True)

        # Build panes
        self.panes: dict[str, _PaneBase] = {
            "CH Companies": CHPane(self.content, self),
            "CQC": CQCPane(self.content, self),
            "Settings": SettingsPane(self.content, self),
        }
        # Abbreviations used when the rail is collapsed
        self._nav_abbrev = {
            "CH Companies": "CH",
            "CQC": "CQ",
            "Settings": "⚙",
        }
        self.current_pane_name: str | None = None

        # Left nav buttons
        self.nav_buttons: dict[str, ttk.Button] = {}
        for name in self.panes:
            btn = ttk.Button(self.nav, text=name, width=14,
                             command=lambda n=name: self._show_pane(n))
            btn.pack(fill="x", pady=2)
            self.nav_buttons[name] = btn

        # Bottom strip: progress + status
        self.progress = ttk.Progressbar(self.root, mode="indeterminate")
        self.task_label = ttk.Label(self.root, text="", foreground="gray")
        self.task_label.pack(fill="x", padx=14, pady=2, side="bottom")

        self._show_pane("CH Companies")

        # Start with the nav collapsed; user can expand via the ◀ button
        self._toggle_nav()

    def _toggle_nav(self) -> None:
        self._nav_collapsed = not self._nav_collapsed
        if self._nav_collapsed:
            self.nav_toggle.configure(text="▶")
            self.nav_header.pack_forget()
            for name, btn in self.nav_buttons.items():
                btn.configure(text=self._nav_abbrev.get(name, name[:2]), width=4)
            # Re-apply active marker (compact form)
            self._mark_active_button()
        else:
            self.nav_toggle.configure(text="◀")
            self.nav_header.pack(anchor="w", pady=(0, 6),
                                  before=list(self.nav_buttons.values())[0])
            for name, btn in self.nav_buttons.items():
                btn.configure(text=name, width=14)
            self._mark_active_button()

    def _mark_active_button(self) -> None:
        for n, btn in self.nav_buttons.items():
            if n == self.current_pane_name:
                label = self._nav_abbrev[n] if self._nav_collapsed else n
                btn.configure(text=("▶ " + label))
            else:
                btn.configure(
                    text=self._nav_abbrev[n] if self._nav_collapsed else n
                )

    def _show_pane(self, name: str) -> None:
        if self.current_pane_name == name:
            return
        if self.current_pane_name:
            self.panes[self.current_pane_name].hide()
        self.panes[name].show()
        self.current_pane_name = name
        self._mark_active_button()

    # ── Shared status / task plumbing ────────────────────────────────

    def _set_status(self, msg: str, *, error: bool = False) -> None:
        self.task_label.configure(text=msg, foreground="red" if error else "gray")

    def _set_task_msg(self, msg: str) -> None:
        with self._lock:
            self._task_message = msg

    def _set_task_error(self, msg: str) -> None:
        with self._lock:
            self._task_error = msg
            self._task_message = msg

    def _task_done(self) -> None:
        with self._lock:
            self._task_running = False

    def _disable_action_buttons(self, state: str) -> None:
        for pane in self.panes.values():
            for btn in pane.action_buttons():
                btn.configure(state=state)

    def _run_task(self, target, label: str) -> None:
        if self._task_running:
            self._set_status("A task is already running.", error=True); return
        with self._lock:
            self._task_running = True
            self._task_error = None
            self._task_message = label
        self._set_status(label)
        self._disable_action_buttons("disabled")
        self.progress.pack(fill="x", padx=14, pady=2, side="bottom", before=self.task_label)
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
            self._disable_action_buttons("normal")
            for pane in self.panes.values():
                pane.refresh()

    def _progress_cb(self, msg: str) -> None:
        self._set_task_msg(msg)

    def _dispatch_with_sanity(self, work_fn, *, success_msg, label: str) -> None:
        """Run `work_fn(force=False)` in a worker; on SanityCheckError, ask
        the user to confirm force and re-dispatch."""
        def worker(force: bool = False):
            try:
                n = work_fn(force)
                self._progress_cb(success_msg(n))
            except SanityCheckError as exc:
                logger.warning("Sanity check failed: %s", exc)
                def on_main():
                    body = _format_sanity_failure(exc)
                    if exc.result.dup_distinct_numbers > 0:
                        messagebox.showerror("Sanity check failed", body + "\n\nAborted.")
                    elif messagebox.askyesno(
                        "Sanity check failed — force?",
                        body + "\n\nForce-process this scrape anyway?",
                    ):
                        with self._lock:
                            self._task_running = True
                            self._task_error = None
                            self._task_message = f"{label} (force)"
                        self._set_status(self._task_message)
                        threading.Thread(
                            target=lambda: worker(force=True), daemon=True
                        ).start()
                        self.root.after(500, self._poll_task)
                self.root.after(0, on_main)
                self._set_task_msg("Cancelled (sanity check failed)")
            except Exception as exc:
                logger.exception("Task failed")
                self._set_task_error(f"Task failed: {exc}")
            finally:
                self._task_done()
        self._run_task(worker, label)

    def _on_close(self) -> None:
        if self._task_running:
            self._set_status("Closing... waiting for background task to finish safely.")
        self.root.destroy()

    def run(self) -> None:
        self.root.mainloop()


def main(
    db_path: str | Path | None = None,
    data_dir: str | Path | None = None,
) -> None:
    """Launch the Tkinter GUI."""
    resolved_data_dir = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
    resolved_db_path = Path(db_path) if db_path is not None else default_db_path(resolved_data_dir)
    app = ChBulkApp(db_path=resolved_db_path, data_dir=resolved_data_dir)
    app.run()


if __name__ == "__main__":
    main()
