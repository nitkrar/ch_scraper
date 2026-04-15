#!/usr/bin/env bash
#
# Build CH Bulk as a Mac .app and package as a .dmg
#
# Usage:
#   ./build_app.sh
#
# Prerequisites:
#   - Python 3.12+
#   - Optional: brew install create-dmg (for prettier DMGs)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

VERSION="0.1.0"
APP_NAME="CH Bulk"
DMG_NAME="CH-Bulk-${VERSION}-mac"

echo "=== Building ${APP_NAME} v${VERSION} ==="

# Step 1: Clean previous builds
rm -rf build/ dist/

# Step 2: Create isolated build environment
echo "Creating build environment..."
python3 -m venv .build-venv
.build-venv/bin/pip install --quiet --upgrade pip
.build-venv/bin/pip install --quiet -e .
.build-venv/bin/pip install --quiet pyinstaller

# Step 3: Build .app bundle
echo "Building .app bundle..."
.build-venv/bin/pyinstaller ch_bulk.spec --noconfirm

# Step 4: Ad-hoc code signing
echo "Signing .app (ad-hoc)..."
codesign --force --deep --sign - "dist/${APP_NAME}.app"

# Step 5: Verify .app exists
if [ ! -d "dist/${APP_NAME}.app" ]; then
    echo "ERROR: .app not found in dist/"
    rm -rf .build-venv
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
    echo "  (create-dmg not found, using hdiutil)"
    hdiutil create -volname "${APP_NAME}" \
        -srcfolder "dist/${APP_NAME}.app" \
        -ov -format UDZO \
        "dist/${DMG_NAME}.dmg"
fi

# Cleanup build venv
rm -rf .build-venv

echo ""
echo "========================================="
echo "  Build complete!"
echo "========================================="
echo ""
echo "  .app:  dist/${APP_NAME}.app"
echo "  .dmg:  dist/${DMG_NAME}.dmg"
echo "  Size:  $(du -sh "dist/${DMG_NAME}.dmg" | cut -f1)"
echo ""
echo "To distribute:"
echo "  1. Create a GitHub Release at your repo"
echo "  2. Upload dist/${DMG_NAME}.dmg as a release asset"
echo ""
echo "For prettier DMGs: brew install create-dmg"
echo ""
