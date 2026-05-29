# HSCA Browse UI — Implementation Plan

**Goal**: Add an interactive HSCA browse/search/export sub-tab to the GUI's CQC
pane, backed by a joined query (HSCA × CQC locations × companies), and convert
the existing Locations/Providers radiobuttons to a `ttk.Notebook` tab selector.

**Design**: `docs/plans/2026-05-29-hsca-browse-design.md` (read first).

**Architecture**: Mirror the existing CQC query → API facade → GUI layering.
Reuse `_build_cqc_locations_where` (extended with an `alias` param) and
`get_cqc_filter_options`. INNER join HSCA↔CQC on `location_id`, LEFT join
companies on `h.provider_companies_house_number = c.company_number`.

**Tech stack**: Python 3, DuckDB, Tkinter/ttk. Tests = stdlib `unittest`.

## Hard Rules

1. **Code is source of truth, not this doc.** If a signature/column diverges
   from live code during execution, trust the code and fold the discrepancy in.
2. **Tests use stdlib `unittest`, NOT pytest.** Run via
   `.venv/bin/python -m unittest discover -s tests`. Run a single test with
   `.venv/bin/python -m unittest tests.<module>.<Class>.<test>`.
3. **`core/paths.py` is the single source of truth for paths.** Do not add
   path literals elsewhere.
4. **Backward compatibility:** the `alias` param defaults to `""` so existing
   CQC callers are byte-identical. No migration; no schema change.
5. **One phase = one commit.** Stage subcommits during execution if helpful;
   main session squashes before any push. Do NOT push.

## Test command reference

```bash
# Full suite (baseline 99/0/0):
.venv/bin/python -m unittest discover -s tests
# Single new test module:
.venv/bin/python -m unittest tests.test_hsca_query -v
```

## Task dependency table

| Group | Steps | Can parallelize | Depends on |
|-------|-------|-----------------|------------|
| 1 | Step 1 (alias param + regression guard) | No | — |
| 2 | Steps 2–4 (query_hsca_locations, has_ch_number, export) | No (same file) | Group 1 |
| 3 | Step 5 (API facade) | No | Group 2 |
| 4 | Step 6 (GUI tabs conversion), Step 7 (HSCA sub-view) | No (same file) | Group 3 |
| 5 | Step 8 (full suite + GUI smoke) | No | All |

---

## Step 1: Add `alias` param to `_build_cqc_locations_where` (regression-safe)

**File**: `ch_bulk/cqc/query.py`, `tests/test_hsca_query.py`.

### 1a. Write failing test
Add to a test module (new `tests/test_hsca_query.py`):
```python
import unittest
from ch_bulk.cqc.query import _build_cqc_locations_where

class TestWhereAlias(unittest.TestCase):
    def test_default_alias_unqualified(self):
        where, params = _build_cqc_locations_where(name_contains="acme")
        self.assertIn("LOWER(name) LIKE", where)
        self.assertNotIn("LOWER(l.name)", where)

    def test_alias_qualifies_columns(self):
        where, params = _build_cqc_locations_where(
            name_contains="acme", regions=["London"],
            postcode_prefix="SW1", alias="l",
        )
        self.assertIn("LOWER(l.name) LIKE", where)
        self.assertIn("l.region IN", where)
        self.assertIn("STARTS_WITH(l.postcode", where)
```

### 1b. Run to verify it fails
```bash
.venv/bin/python -m unittest tests.test_hsca_query.TestWhereAlias -v
```

### 1c. Implement
In `_build_cqc_locations_where`, add `alias: str = ""` to the signature. Define
a local helper `q = (lambda c: f"{alias}.{c}" if alias else c)` and wrap every
column reference (`name`, `service_types`, `region`, `local_authority`,
`provider_name`, `postcode`, `is_active`) with `q(...)`. Leave the
`_string_any_like(...)` helper callable with the qualified column string (pass
`q("service_types")`). `_list_any(...)` is used in the providers WHERE builder,
not in `_build_cqc_locations_where`. Bind params unchanged.

### 1d. Run to verify it passes
Same command as 1b.

### 1e. Subcommit
```bash
git add -A && git commit -m "hsca: add alias param to cqc-locations where-builder"
```

---

## Step 2: `HSCA_LOCATION_SORT_COLUMNS` + `query_hsca_locations` skeleton

**File**: `ch_bulk/cqc/query.py`, `tests/test_hsca_query.py`.

### 2a. Write failing test
Build a tiny in-test DuckDB directly in `tests/test_hsca_query.py`; there is no
existing dedicated `tests/test_cqc_query.py` fixture/module to mirror. Create
three tables: `cqc_locations` (location_id, name,
provider_name, postcode, region, local_authority, service_types, is_active),
`cqc_hsca_locations` (location_id, provider_id, provider_companies_house_number,
number_of_beds, care_home, dormant, provider_ownership_type,
provider_brand_name, st_* booleans, bulk_imported_at, bulk_file_date, raw_row),
and `companies` (company_number, company_name, company_status). Insert:
- L1: in both HSCA and CQC, CH number C1 present in companies.
- L2: in both HSCA and CQC, CH number NULL.
- L3: HSCA only (no CQC row) — must be EXCLUDED by INNER join.
```python
def test_inner_join_excludes_unmatched(self):
    rows, total = query_hsca_locations(self.db)
    ids = {r["location_id"] for r in rows}
    self.assertEqual(ids, {"L1", "L2"})
    self.assertEqual(total, 2)

def test_company_fields_via_left_join(self):
    rows, _ = query_hsca_locations(self.db)
    by_id = {r["location_id"]: r for r in rows}
    self.assertEqual(by_id["L1"]["company_name"], "ACME CARE LTD")
    self.assertIsNone(by_id["L2"]["company_name"])
```

### 2b. Run to verify it fails
```bash
.venv/bin/python -m unittest tests.test_hsca_query.TestQueryHsca -v
```

### 2c. Implement
Add `HSCA_LOCATION_SORT_COLUMNS` (whitelist of qualified names, e.g.
`{"name": "l.name", "provider_name": "l.provider_name", "number_of_beds":
"h.number_of_beds", "company_name": "c.company_name", ...}`; default key
`"name"`). Implement `query_hsca_locations(db_path, *, name_contains=None,
service_types=None, regions=None, local_authorities=None,
provider_name_contains=None, postcode_prefix=None, is_active=None,
has_ch_number=None, sort_by="name", sort_order="ASC", page=1, page_size=50)`:
- `where_sql, params = _build_cqc_locations_where(<filters>, alias="l")`
- FROM/JOIN per design (INNER cqc_locations, LEFT companies).
- SELECT explicit projection (design §Query layer).
- `recover_interrupted_compaction`, connect, COUNT + paged SELECT, return
  `(rows, total)`. (`has_ch_number` handled in Step 3.)

### 2d. Run to verify it passes
Same as 2b.

### 2e. Subcommit
```bash
git add -A && git commit -m "hsca: query_hsca_locations with inner/left joins"
```

---

## Step 3: `has_ch_number` filter

**File**: `ch_bulk/cqc/query.py`, `tests/test_hsca_query.py`.

### 3a. Write failing test
```python
def test_has_ch_number_true(self):
    rows, total = query_hsca_locations(self.db, has_ch_number=True)
    self.assertEqual({r["location_id"] for r in rows}, {"L1"})
def test_has_ch_number_false(self):
    rows, total = query_hsca_locations(self.db, has_ch_number=False)
    self.assertEqual({r["location_id"] for r in rows}, {"L2"})
def test_has_ch_number_none_returns_all(self):
    rows, _ = query_hsca_locations(self.db, has_ch_number=None)
    self.assertEqual(len(rows), 2)
```

### 3b. Run to verify it fails
```bash
.venv/bin/python -m unittest tests.test_hsca_query.TestQueryHsca.test_has_ch_number_true -v
```

### 3c. Implement
After building the shared WHERE, append the HSCA-only condition: if
`has_ch_number is True` → `h.provider_companies_house_number IS NOT NULL`;
if `False` → `... IS NULL`; if `None` → skip. Compose into the final
WHERE clause (handle the "no other conditions" case so the SQL is valid with
or without a leading `WHERE`).

### 3d. Run to verify it passes
Run the full `TestQueryHsca` class.

### 3e. Subcommit
```bash
git add -A && git commit -m "hsca: has_ch_number filter"
```

---

## Step 4: `export_hsca_locations_csv`

**File**: `ch_bulk/cqc/query.py`, `tests/test_hsca_query.py`.

### 4a. Write failing test
```python
def test_export_writes_rows(self):
    out = self.tmp / "hsca.csv"
    n = export_hsca_locations_csv(self.db, out, has_ch_number=True)
    self.assertEqual(n, 1)
    text = out.read_text()
    self.assertIn("company_name", text.splitlines()[0])  # header present
```

### 4b. Run to verify it fails
```bash
.venv/bin/python -m unittest tests.test_hsca_query.TestQueryHsca.test_export_writes_rows -v
```

### 4c. Implement
`export_hsca_locations_csv(db_path, output_path, **filters) -> int`: mkdir
parent, build joined SELECT + WHERE (reuse the same join/where assembly as
`query_hsca_locations` — extract a private `_hsca_from_where(**filters)` helper
returning `(from_join_sql, where_sql, params)` so query + export share it),
`COPY (<select>) TO '<escaped path>' (HEADER, DELIMITER ',')`, return COUNT.

### 4d. Run to verify it passes
Run full `TestQueryHsca` + the alias regression test.

### 4e. Subcommit
```bash
git add -A && git commit -m "hsca: export_hsca_locations_csv"
```

---

## Step 5: API facade methods

**File**: `ch_bulk/api.py`.

### 5a. Write failing test
In `tests/test_hsca_query.py` (create a dedicated API test class there; there
is no existing dedicated CQC query/API module to mirror exactly):
```python
def test_api_query_hsca(self):
    from ch_bulk import ChBulk
    temp_data_dir = self.tmp / "data"
    temp_data_dir.mkdir()
    ch = ChBulk(data_dir=temp_data_dir, db_path=self.db)
    rows, total = ch.query_hsca_locations_advanced(has_ch_number=True)
    self.assertEqual(total, 1)
```
Passing a temp `data_dir` matters because `ChBulk.__init__` always calls
`setup_logging(self.data_dir)`.

### 5b. Run to verify it fails
```bash
.venv/bin/python -m unittest tests.test_hsca_query.TestApiHsca -v
```

### 5c. Implement
Match the existing `ch_bulk/api.py` pattern exactly: extend the top-level CQC
query import block, then add thin pass-through methods next to
`query_cqc_locations_advanced`:
```python
from ch_bulk.cqc.query import (
    export_hsca_locations_csv as _export_hsca_locations_csv,
    query_hsca_locations,
)

...

def query_hsca_locations_advanced(self, **filters) -> tuple[list[dict], int]:
    return query_hsca_locations(self.db_path, **filters)

def export_hsca_locations_csv(self, output_path, **filters) -> int:
    return _export_hsca_locations_csv(self.db_path, output_path, **filters)
```
Do not switch this file to method-local imports; the existing CQC facade does
not do that.

### 5d. Run to verify it passes
Same as 5b, then full suite.

### 5e. Subcommit
```bash
git add -A && git commit -m "hsca: api facade query+export"
```

---

## Step 6: Convert CQCPane sub-tabs Radiobuttons → ttk.Notebook

**File**: `ch_bulk/gui.py` (`CQCPane`).

> GUI has no unit tests (Tkinter). Verify by import + manual smoke. Keep this
> step mechanical and small.

### 6a. Implement
In `CQCPane._build`, replace the `subtab_row` Radiobutton block (lines ~599–607)
with a `ttk.Notebook`:
```python
self.subtabs = ttk.Notebook(self.frame)
self.subtabs.pack(fill="x", padx=10, pady=(8, 0))
for name in ("Locations", "Providers", "HSCA"):
    self.subtabs.add(ttk.Frame(self.subtabs), text=name)
self.subtabs.bind("<<NotebookTabChanged>>", self._on_subtab_change)
```
Update `_on_subtab_change` to accept `event=None` and read the selected tab text
via `self.subtabs.tab(self.subtabs.select(), "text")` instead of
`self.subtab_var`. `refresh()` currently calls `_on_subtab_change()` directly,
so the handler must work both with and without an event object. Remove
`self.subtab_var` references. Keep the shared filter frame + results tree below
the notebook (unchanged parenting to `self.frame`). Bind after the dependent
widgets exist, or guard the handler so an early notebook event cannot access
missing widgets/results state.

### 6b. Verify import + no crash
```bash
.venv/bin/python -c "import ch_bulk.gui"
.venv/bin/python -m unittest discover -s tests   # ensure nothing regressed
```

### 6c. Subcommit
```bash
git add -A && git commit -m "hsca: convert CQC sub-tabs to ttk.Notebook"
```

---

## Step 7: Wire HSCA sub-view (columns, filters, search/export routing)

**File**: `ch_bulk/gui.py` (`CQCPane`).

### 7a. Implement
- `_build_results_tree`: add an `"HSCA"` branch with columns: location name,
  provider name, company name, company status, CH number, beds, care_home,
  dormant, ownership type, brand, region, postcode, local authority, service
  flags. Define column ids + headings mirroring the Locations branch. The
  current implementation is a two-way `Locations` vs `Providers` branch, so
  convert it to an explicit three-way branch.
- Filter frame: when `sub_view == "HSCA"`, show the same fields as Locations
  plus a "Has CH number" combobox (`Yes` / `No` / `All`, default `All`). Hide
  the Providers-only "Min active locations" field (reuse existing show/hide).
  Add `self.has_ch_var` StringVar + combobox in `_build`, shown/hidden in the
  sub-view switch.
- `_get_filters`: when `sub_view == "HSCA"`, collect the shared Locations-style
  filters plus `has_ch_number`, and route sorting via a new `self.sort_by_hsca`
  default (for example `"name"`).
- `_run_query`: add the actual HSCA dispatch branch. `_on_search` itself stays a
  thin page-reset wrapper that delegates to `_run_query`; it does not contain
  the query call today.
- `_on_export`: when `sub_view == "HSCA"`, call
  `self.ch.export_hsca_locations_csv(path, **same filters)`.
- Sort: add `self.sort_by_hsca` default `"name"` and extend `_on_sort` beyond
  its current two-way `Locations`/`Providers` attribute selection.

### 7b. Verify import + suite
```bash
.venv/bin/python -c "import ch_bulk.gui"
.venv/bin/python -m unittest discover -s tests
```

### 7c. Subcommit
```bash
git add -A && git commit -m "hsca: wire HSCA browse sub-view in CQC pane"
```

---

## Step 8: End-to-end verification + GUI smoke

### 8a. Full suite (expect baseline + new HSCA tests, all green)
```bash
.venv/bin/python -m unittest discover -s tests
```

### 8b. GUI smoke (user-run)
```bash
.venv/bin/ch-bulk ui
```
Checklist:
- CQC pane shows three real tabs (Locations / Providers / HSCA).
- HSCA tab lists rows; company name/status columns populate where CH matches.
- "Has CH number" Yes/No/All filters the list.
- Service type / region / LA / postcode / name filters work.
- Export CSV writes a file with the joined columns.
- Switching tabs doesn't crash; pagination + sort work on HSCA.

### 8c. Squash to single phase commit (main session, after user OK; do NOT push)
```bash
git reset --soft <pre-phase bookmark or first subcommit^> && \
  git commit -m "Phase 7: HSCA browse sub-tab in CQC pane (joined HSCA×CQC×companies)"
```

## Files touched

- `ch_bulk/cqc/query.py` — `alias` param, `HSCA_LOCATION_SORT_COLUMNS`,
  `_hsca_from_where`, `query_hsca_locations`, `export_hsca_locations_csv`.
- `ch_bulk/api.py` — `query_hsca_locations_advanced`,
  `export_hsca_locations_csv`.
- `ch_bulk/gui.py` — `CQCPane`: Notebook selector, HSCA sub-view.
- `tests/test_hsca_query.py` — new.
