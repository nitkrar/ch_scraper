# Plan: Replace Flask/HTMX with Tkinter GUI

**Goal**: Replace browser-based Flask UI with a native Tkinter desktop GUI. Zero additional dependencies.

**Reference**: Feature list at docs/plans/flask-feature-list.md (47 features)

## Changes

### Step 1: Create ch_bulk/gui.py (~300 lines)
Single file, native Tkinter GUI implementing all 47 features from the Flask UI.

### Step 2: Update cli.py 
Replace Flask import with Tkinter gui import in the `ui` command.

### Step 3: Update pyproject.toml
Remove Flask optional dep, remove ch-bulk-ui entry point, remove package-data for web templates.

### Step 4: Update PyInstaller spec
Remove template/static datas. Simpler bundle.

### Step 5: Delete ch_bulk/web/ directory
Remove all Flask files.

### Step 6: Verify
