# Test fixtures

Small representative data files used by the test suite. Tracked in git.

Tests source these via `tests.support.paths.fixture_path(...)`, NEVER from `data/staging/` or `data/old_staging/`.

## Adding a fixture

1. Copy the file under the appropriate subfolder (for example `financials/ixbrl/<company_number>/`).
2. Keep it small. The soft cap is under 200 KB per file.
3. Add a one-line entry here explaining what the fixture represents.

## Current fixtures

- `financials/ixbrl/07545840/sample.ixbrl` — exact historical iXBRL for company `07545840`, used by parser and financials pipeline tests.
