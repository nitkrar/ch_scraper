#!/usr/bin/env bash
#
# One-command setup for ch-bulk.
# Creates a Python virtual environment, installs dependencies,
# and verifies the installation.
#
# Usage:
#   ./setup.sh
#
# After setup, activate the environment with:
#   source .venv/bin/activate
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON=""
for candidate in python3.12 python3 python; do
    if command -v "$candidate" &>/dev/null; then
        version=$("$candidate" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || echo "0.0")
        major=$(echo "$version" | cut -d. -f1)
        minor=$(echo "$version" | cut -d. -f2)
        if [ "$major" -ge 3 ] && [ "$minor" -ge 12 ]; then
            PYTHON="$candidate"
            break
        fi
    fi
done

if [ -z "$PYTHON" ]; then
    echo "ERROR: Python 3.12+ is required but not found."
    echo "Install it from https://www.python.org/downloads/"
    exit 1
fi

echo "Using $PYTHON ($($PYTHON --version))"

# Major.minor of the interpreter we picked, used for version-specific hints.
PYVER=$("$PYTHON" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")

# Check for Tkinter (required for the GUI)
TK_OK=true
if ! "$PYTHON" -c "import tkinter" 2>/dev/null; then
    TK_OK=false
    echo ""
    echo "WARNING: Tkinter is not available for this Python ($PYVER)."
    echo "The CLI will work fine, but 'ch-bulk ui' (the GUI) will not."
    echo ""
    echo "To install Tkinter:"
    echo "  macOS (Homebrew):  brew install python-tk@$PYVER"
    echo "  Ubuntu/Debian:     sudo apt install python3-tk"
    echo "  Fedora/RHEL:       sudo dnf install python3-tkinter"
    echo "  Windows:           Reinstall Python with 'tcl/tk' checked"
    echo ""
    echo "You can install it later and re-run this script."
    echo ""
fi

# Create virtual environment.
#
# An existing .venv is not enough: a venv is a set of symlinks into a specific
# interpreter, so upgrading or uninstalling that interpreter (e.g. Homebrew
# replacing python@3.12 with python@3.14) leaves the directory in place with a
# dangling .venv/bin/python. Every entrypoint then fails with a confusing
# "no such file or directory". Probe the interpreter and rebuild if it is
# broken or no longer matches the Python we selected above.
RECREATE_REASON=""
if [ ! -d ".venv" ]; then
    RECREATE_REASON="not present"
elif ! .venv/bin/python -c "pass" 2>/dev/null; then
    RECREATE_REASON="its interpreter no longer runs (was the system Python upgraded or removed?)"
else
    VENV_PYVER=$(.venv/bin/python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    if [ "$VENV_PYVER" != "$PYVER" ]; then
        RECREATE_REASON="it is on Python $VENV_PYVER but this setup targets $PYVER"
    fi
fi

if [ -n "$RECREATE_REASON" ]; then
    if [ -d ".venv" ]; then
        echo "Rebuilding virtual environment: $RECREATE_REASON."
        rm -rf .venv
    else
        echo "Creating virtual environment..."
    fi
    "$PYTHON" -m venv .venv
else
    echo "Virtual environment already exists and is healthy."
fi

echo "Installing dependencies..."
.venv/bin/pip install --upgrade pip --quiet
# [dev] adds pytest so the test suite runs on a fresh clone.
.venv/bin/pip install -e '.[dev]' --quiet

# Verify
echo ""
echo "Verifying installation..."
.venv/bin/python -c "from ch_bulk import ChBulk; print('  Python API: OK')"
.venv/bin/ch-bulk --help > /dev/null 2>&1 && echo "  CLI:        OK" || echo "  CLI:        FAILED"
if .venv/bin/python -c "import tkinter" 2>/dev/null; then
    # Tkinter importing is necessary but not sufficient — check the GUI module
    # itself loads, so a broken import surfaces here rather than on first launch.
    if .venv/bin/python -c "import ch_bulk.gui" 2>/dev/null; then
        echo "  GUI (Tk):   OK"
    else
        echo "  GUI (Tk):   Tk present but 'import ch_bulk.gui' FAILED:"
        .venv/bin/python -c "import ch_bulk.gui" 2>&1 | tail -3 | sed 's/^/              /'
    fi
else
    TK_OK=false
    echo "  GUI (Tk):   MISSING — run: brew install python-tk@$PYVER  (macOS/Homebrew)"
fi

# Create Desktop shortcut (macOS only)
if [ "$TK_OK" = true ] && [ "$(uname)" = "Darwin" ]; then
    DESKTOP="$HOME/Desktop"
    APP_PATH="$DESKTOP/CH Bulk.app"

    if [ ! -d "$APP_PATH" ]; then
        echo ""
        echo "Creating Desktop shortcut..."
        osacompile -o "$APP_PATH" -e "
            do shell script \"cd '$SCRIPT_DIR' && .venv/bin/python -m ch_bulk.gui &> /dev/null &\"
        " 2>/dev/null

        if [ -d "$APP_PATH" ]; then
            echo "  Desktop shortcut: OK (CH Bulk.app)"
        else
            echo "  Desktop shortcut: SKIPPED (osacompile not available)"
        fi
    else
        echo "  Desktop shortcut: Already exists"
    fi
fi

echo ""
echo "========================================="
echo "  Setup complete!"
echo "========================================="
echo ""
echo "To get started:"
echo ""
echo "  Double-click 'CH Bulk' on your Desktop"
echo "  (or run: source .venv/bin/activate && ch-bulk ui)"
echo ""
echo "CLI commands (after activating venv):"
echo ""
echo "  ch-bulk sync              # Download and build database"
echo "  ch-bulk query 62012       # Search by SIC code"
echo "  ch-bulk info              # Show database stats"
echo ""
