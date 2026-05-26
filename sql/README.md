# SQL library

Versioned SQL files used by `ch_bulk/processor.py`. Each file is
standalone-runnable in the DuckDB CLI for debugging and ad-hoc work.

```
sql/
├── ch/                              ← Companies House bulk pipeline
│   ├── sanity_row_count.sql         ← compare staging vs companies totals
│   ├── sanity_inactive_churn.sql    ← count would-be-inactivated rows
│   ├── bootstrap_companies.sql      ← first-time: CREATE TABLE companies
│   ├── upsert_companies.sql         ← merge staging into companies
│   ├── indexes.sql                  ← idempotent CREATE INDEX block
│   └── _examples/
│       └── staging_ingest.sql.example   ← reference copy of the Python ingest
└── cqc/                             ← (placeholder — populated when we add CQC)
```

## When to use these directly

The Python orchestrator (`ch_bulk/processor.py`) runs the right files in the
right order for normal use. You'd reach into this directory when:

- **Debugging a failed upsert.** Open the DB in `duckdb` and re-run a single
  step to see what it produces.
- **Inspecting the sanity checks** before a Process — run the sanity SQLs
  against a snapshot to see what the orchestrator would decide.
- **Forensic on the existing DB.** All files except `staging_ingest` only
  read or modify `companies` and `companies_staging` — no destructive
  filesystem operations.

## Standalone usage

```bash
$ duckdb ch_bulk.duckdb
D .read sql/ch/sanity_row_count.sql       -- just look
D .read sql/ch/sanity_inactive_churn.sql  -- just look
D BEGIN TRANSACTION;
D .read sql/ch/upsert_companies.sql       -- actually merge
D COMMIT;                                  -- or ROLLBACK
```

The `staging_ingest.sql.example` file needs you to manually substitute the
`{FILE_LIST}` placeholder before running. The Python orchestrator builds
that string at runtime, which is why the live version isn't in this dir.

## Conventions

- One operation per file. If you find yourself wanting `if-then-else`,
  that branching belongs in the Python orchestrator, not SQL.
- Idempotent where possible (`IF NOT EXISTS`, `IF EXISTS`).
- Comment at the top of each file: what it does, when it runs, how to
  invoke standalone.
- No side effects outside the DB (no file I/O, no shell-outs).

## When to add files vs. extend existing

- **New sanity check?** New file: `sanity_<name>.sql`. Keep each check
  in its own file so the orchestrator can choose which to run.
- **New index?** Add to `indexes.sql`. The file is one logical concept.
- **New data source (e.g. CQC, PSC)?** New subdirectory: `sql/<source>/`.
  Don't cross sources in a single file.
