"""Ingest Companies House CSV files into a DuckDB database.

Upsert semantics: the database is treated as an ongoing record across
many bulk pulls, not a snapshot. Existing rows are updated in place,
new rows are inserted, and rows that drop out of a pull are marked
inactive (with the scrape date they first stopped appearing) rather
than deleted.

The orchestration logic lives in this module. The actual SQL — the
sanity checks, the bootstrap, the upsert merge — lives in
`../sql/ch/` so each step is runnable standalone in the duckdb CLI.

See sql/README.md for the layout and how to run those files by hand.
"""

from __future__ import annotations

import functools
import logging
import re
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable, TypeVar

import duckdb
from rich.console import Console

from ch_bulk.core.paths import SQL_DIR as _ROOT_SQL_DIR

logger = logging.getLogger(__name__)
console = Console()

_F = TypeVar("_F", bound=Callable)


def timed_phase(label: str) -> Callable[[_F], _F]:
    """Decorator that logs how long the wrapped function took.

    Writes ``Phase '<label>' took Xs`` to the module logger as soon as
    the function returns (or raises). Goes to ``data/ch_bulk.log`` via
    the file handler installed by the GUI's main(), and to stderr
    otherwise — either way it survives a kill mid-pipeline because
    each phase logs as it completes, not at the end.

    Usage::

        @timed_phase("ingest staging")
        def _do_ingest(con, files):
            ...
    """
    def decorator(fn: _F) -> _F:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            t0 = time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - t0
                logger.info("Phase %r took %.2fs", label, elapsed)
        return wrapper
    return decorator

# Thresholds for the upsert sanity checks. Mirror the comment block in
# sql/ch/sanity_checks.sql. The duplicate check is strict zero and
# cannot be overridden by force=True.
ROW_COUNT_PCT_THRESHOLD = 5.0
INACTIVE_CHURN_PCT_THRESHOLD = 5.0

SQL_DIR = _ROOT_SQL_DIR / "ch"

# Match the CH bulk filename pattern, e.g. BasicCompanyData-2026-05-01-part1_7.csv
_CH_DATE_RE = re.compile(r"BasicCompanyData-(\d{4}-\d{2}-\d{2})-part\d+_\d+\.csv$")


@dataclass(frozen=True)
class SanityCheckResult:
    """One row's worth of metrics from sql/ch/sanity_checks.sql.

    Exposed on :class:`SanityCheckError` so callers (the GUI popup,
    the CLI) can render a useful message without re-running the query.
    """

    companies_exists: bool

    # Staging duplicates (strict zero, no override)
    dup_distinct_numbers: int
    dup_excess_rows: int
    dup_sample: list[str]

    # Row-count delta (5% threshold, force overrides)
    old_total: int
    new_total: int
    row_delta: int
    row_pct: float | None

    # Inactive-churn (5% threshold, force overrides)
    currently_active: int
    would_be_inactivated: int
    inactive_pct: float | None


class SanityCheckError(RuntimeError):
    """Raised when a sanity check fails.

    Inspect ``.result`` for the raw numbers.

    Duplicate violations are NEVER overridable (the source CSV has a
    real data problem). Row-count and inactive-churn violations can be
    overridden by re-running with ``force=True`` (CLI) or the GUI's
    "Force process" popup button.
    """

    def __init__(self, result: SanityCheckResult, message: str) -> None:
        super().__init__(message)
        self.result = result


def _escape_path(p: Path) -> str:
    """Escape a file path for safe embedding in DuckDB SQL strings."""
    s = str(p)
    if "\x00" in s:
        raise ValueError(f"Path contains null bytes: {p}")
    return s.replace("\\", "\\\\").replace("'", "''")


def _run_sql_file(con: duckdb.DuckDBPyConnection, name: str, **subs) -> None:
    """Execute one SQL file from ``sql/ch/``.

    Optional keyword arguments substitute ``{{name}}`` placeholders in
    the SQL text before execution. Use for values DuckDB's parameter
    binding can't handle (e.g., DATE literals inside MERGE INTO).
    Substitution is plain ``str.replace`` — only safe for trusted values
    (filename-derived dates, internal constants), never user input.
    """
    sql = (SQL_DIR / name).read_text()
    for key, value in subs.items():
        sql = sql.replace("{{" + key + "}}", str(value))
    con.execute(sql)


def _fetchone_sql_file(
    con: duckdb.DuckDBPyConnection, name: str, **subs
) -> tuple:
    """Execute a SQL file expected to return one row, return the row."""
    sql = (SQL_DIR / name).read_text()
    for key, value in subs.items():
        sql = sql.replace("{{" + key + "}}", str(value))
    return con.execute(sql).fetchone()


def scrape_date_from_files(csv_files: list[Path]) -> date:
    """Extract the YYYY-MM-DD scrape date from CH bulk filenames.

    All input files must share the same date prefix. Raises ValueError
    otherwise (the orchestrator should split by month before calling this).
    """
    dates: set[str] = set()
    for f in csv_files:
        m = _CH_DATE_RE.match(f.name)
        if not m:
            raise ValueError(
                f"Filename does not match expected BasicCompanyData "
                f"pattern: {f.name}"
            )
        dates.add(m.group(1))
    if len(dates) > 1:
        raise ValueError(
            f"Files span multiple scrape dates: {sorted(dates)}. Group "
            f"by month before calling process_csvs()."
        )
    d_str = dates.pop()
    return date.fromisoformat(d_str)


def _ingest_to_staging(
    con: duckdb.DuckDBPyConnection,
    csv_files: list[Path],
) -> None:
    """Load CSV files into ``companies_staging`` (creating the table).

    The big SELECT lives here in Python (not in a SQL file) because the
    file list is dynamic. A standalone reference copy of this query
    lives at ``sql/ch/_examples/staging_ingest.sql.example`` for ad-hoc
    debugging — any change here must be mirrored there.
    """
    con.execute("DROP TABLE IF EXISTS companies_staging")

    file_list = ", ".join(f"'{_escape_path(f)}'" for f in csv_files)

    con.execute(rf"""
        CREATE TABLE companies_staging AS
        SELECT
            "CompanyNumber"                         AS company_number,
            "CompanyName"                           AS company_name,
            "CompanyStatus"                         AS company_status,
            "CompanyCategory"                       AS company_type,

            -- SIC codes: extract numeric prefix + keep full text
            CASE
                WHEN "SICCode.SicText_1" IS NOT NULL
                     AND regexp_extract("SICCode.SicText_1", '^\s*(\d+)', 1) != ''
                THEN regexp_extract("SICCode.SicText_1", '^\s*(\d+)', 1)
                ELSE NULL
            END                                     AS sic_code_1,
            "SICCode.SicText_1"                     AS sic_text_1,

            CASE
                WHEN "SICCode.SicText_2" IS NOT NULL
                     AND regexp_extract("SICCode.SicText_2", '^\s*(\d+)', 1) != ''
                THEN regexp_extract("SICCode.SicText_2", '^\s*(\d+)', 1)
                ELSE NULL
            END                                     AS sic_code_2,
            "SICCode.SicText_2"                     AS sic_text_2,

            CASE
                WHEN "SICCode.SicText_3" IS NOT NULL
                     AND regexp_extract("SICCode.SicText_3", '^\s*(\d+)', 1) != ''
                THEN regexp_extract("SICCode.SicText_3", '^\s*(\d+)', 1)
                ELSE NULL
            END                                     AS sic_code_3,
            "SICCode.SicText_3"                     AS sic_text_3,

            CASE
                WHEN "SICCode.SicText_4" IS NOT NULL
                     AND regexp_extract("SICCode.SicText_4", '^\s*(\d+)', 1) != ''
                THEN regexp_extract("SICCode.SicText_4", '^\s*(\d+)', 1)
                ELSE NULL
            END                                     AS sic_code_4,
            "SICCode.SicText_4"                     AS sic_text_4,

            -- Address fields (individual + combined)
            TRIM("RegAddress.CareOf")               AS address_care_of,
            TRIM("RegAddress.POBox")                AS address_po_box,
            TRIM("RegAddress.AddressLine1")         AS address_line_1,
            TRIM("RegAddress.AddressLine2")         AS address_line_2,
            TRIM("RegAddress.PostTown")             AS address_post_town,
            TRIM("RegAddress.County")               AS address_county,
            TRIM("RegAddress.Country")              AS address_country,
            TRIM("RegAddress.PostCode")             AS postcode,

            CONCAT_WS(', ',
                NULLIF(TRIM("RegAddress.CareOf"), ''),
                NULLIF(TRIM("RegAddress.POBox"), ''),
                NULLIF(TRIM("RegAddress.AddressLine1"), ''),
                NULLIF(TRIM("RegAddress.AddressLine2"), ''),
                NULLIF(TRIM("RegAddress.PostTown"), ''),
                NULLIF(TRIM("RegAddress.County"), ''),
                NULLIF(TRIM("RegAddress.Country"), '')
            )                                       AS registered_address,

            -- Dates
            COALESCE(
                TRY_CAST(strptime("IncorporationDate", '%d/%m/%Y') AS DATE),
                TRY_CAST("IncorporationDate" AS DATE)
            )                                       AS incorporation_date,
            COALESCE(
                TRY_CAST(strptime("DissolutionDate", '%d/%m/%Y') AS DATE),
                TRY_CAST("DissolutionDate" AS DATE)
            )                                       AS dissolution_date,

            "CountryOfOrigin"                       AS country_of_origin,

            -- Accounts
            TRY_CAST("Accounts.AccountRefDay" AS INTEGER)   AS accounts_ref_day,
            TRY_CAST("Accounts.AccountRefMonth" AS INTEGER) AS accounts_ref_month,
            COALESCE(
                TRY_CAST(strptime("Accounts.NextDueDate", '%d/%m/%Y') AS DATE),
                TRY_CAST("Accounts.NextDueDate" AS DATE)
            )                                       AS accounts_next_due,
            COALESCE(
                TRY_CAST(strptime("Accounts.LastMadeUpDate", '%d/%m/%Y') AS DATE),
                TRY_CAST("Accounts.LastMadeUpDate" AS DATE)
            )                                       AS accounts_last_made_up,
            "Accounts.AccountCategory"              AS accounts_category,

            -- Returns / Confirmation Statement
            COALESCE(
                TRY_CAST(strptime("Returns.NextDueDate", '%d/%m/%Y') AS DATE),
                TRY_CAST("Returns.NextDueDate" AS DATE)
            )                                       AS returns_next_due,
            COALESCE(
                TRY_CAST(strptime("Returns.LastMadeUpDate", '%d/%m/%Y') AS DATE),
                TRY_CAST("Returns.LastMadeUpDate" AS DATE)
            )                                       AS returns_last_made_up,
            COALESCE(
                TRY_CAST(strptime("ConfStmtNextDueDate", '%d/%m/%Y') AS DATE),
                TRY_CAST("ConfStmtNextDueDate" AS DATE)
            )                                       AS conf_stmt_next_due,
            COALESCE(
                TRY_CAST(strptime("ConfStmtLastMadeUpDate", '%d/%m/%Y') AS DATE),
                TRY_CAST("ConfStmtLastMadeUpDate" AS DATE)
            )                                       AS conf_stmt_last_made_up,

            -- Mortgages / Charges
            TRY_CAST("Mortgages.NumMortCharges" AS INTEGER)        AS num_mort_charges,
            TRY_CAST("Mortgages.NumMortOutstanding" AS INTEGER)    AS num_mort_outstanding,
            TRY_CAST("Mortgages.NumMortPartSatisfied" AS INTEGER)  AS num_mort_part_satisfied,
            TRY_CAST("Mortgages.NumMortSatisfied" AS INTEGER)      AS num_mort_satisfied,

            -- Limited Partnerships
            TRY_CAST("LimitedPartnerships.NumGenPartners" AS INTEGER)  AS num_gen_partners,
            TRY_CAST("LimitedPartnerships.NumLimPartners" AS INTEGER)  AS num_lim_partners,

            -- URI
            "URI"                                   AS uri,

            -- Previous names (up to 10)
            "PreviousName_1.CONDATE"                AS prev_name_1_date,
            "PreviousName_1.CompanyName"            AS prev_name_1,
            "PreviousName_2.CONDATE"                AS prev_name_2_date,
            "PreviousName_2.CompanyName"            AS prev_name_2,
            "PreviousName_3.CONDATE"                AS prev_name_3_date,
            "PreviousName_3.CompanyName"            AS prev_name_3,
            "PreviousName_4.CONDATE"                AS prev_name_4_date,
            "PreviousName_4.CompanyName"            AS prev_name_4,
            "PreviousName_5.CONDATE"                AS prev_name_5_date,
            "PreviousName_5.CompanyName"            AS prev_name_5,
            "PreviousName_6.CONDATE"                AS prev_name_6_date,
            "PreviousName_6.CompanyName"            AS prev_name_6,
            "PreviousName_7.CONDATE"                AS prev_name_7_date,
            "PreviousName_7.CompanyName"            AS prev_name_7,
            "PreviousName_8.CONDATE"                AS prev_name_8_date,
            "PreviousName_8.CompanyName"            AS prev_name_8,
            "PreviousName_9.CONDATE"                AS prev_name_9_date,
            "PreviousName_9.CompanyName"            AS prev_name_9,
            "PreviousName_10.CONDATE"               AS prev_name_10_date,
            "PreviousName_10.CompanyName"            AS prev_name_10

        FROM read_csv_auto(
            [{file_list}],
            header = true,
            ignore_errors = true,
            all_varchar = true
        )
    """)


def _companies_table_exists(con: duckdb.DuckDBPyConnection) -> bool:
    return con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'companies'"
    ).fetchone()[0] > 0


def _check_sanity(con: duckdb.DuckDBPyConnection) -> SanityCheckResult:
    """Run the sanity SQL files.

    Always runs ``sanity_staging.sql`` (duplicate check).
    Additionally runs ``sanity_upsert.sql`` if ``companies`` exists
    (row-count and inactive-churn checks).
    """
    companies_exists = _companies_table_exists(con)

    (
        dup_distinct_numbers,
        dup_excess_rows,
        dup_sample,
    ) = _fetchone_sql_file(con, "sanity_staging.sql")

    if companies_exists:
        (
            old_total,
            new_total,
            row_delta,
            row_pct,
            currently_active,
            would_be_inactivated,
            inactive_pct,
        ) = _fetchone_sql_file(con, "sanity_upsert.sql")
    else:
        old_total = new_total = row_delta = 0
        row_pct = None
        currently_active = would_be_inactivated = 0
        inactive_pct = None

    return SanityCheckResult(
        companies_exists=companies_exists,
        dup_distinct_numbers=int(dup_distinct_numbers or 0),
        dup_excess_rows=int(dup_excess_rows or 0),
        dup_sample=list(dup_sample or []),
        old_total=int(old_total or 0),
        new_total=int(new_total or 0),
        row_delta=int(row_delta or 0),
        row_pct=None if row_pct is None else float(row_pct),
        currently_active=int(currently_active or 0),
        would_be_inactivated=int(would_be_inactivated or 0),
        inactive_pct=None if inactive_pct is None else float(inactive_pct),
    )


def _enforce_sanity(result: SanityCheckResult, force: bool) -> None:
    """Raise SanityCheckError if any check fails.

    Duplicate violations always raise (force does not override).
    Row-count and inactive-churn violations raise unless force=True.
    """
    failures: list[str] = []

    if result.dup_distinct_numbers > 0:
        sample = ", ".join(str(s) for s in result.dup_sample[:5])
        failures.append(
            f"DUPLICATE company_numbers in staging "
            f"({result.dup_distinct_numbers} distinct numbers duplicated; "
            f"{result.dup_excess_rows} excess rows). Sample: [{sample}]. "
            f"This is a real data problem in the source CSV — "
            f"investigate before re-running. force=True does NOT override."
        )

    if result.companies_exists:
        if (
            result.row_pct is not None
            and result.row_pct > ROW_COUNT_PCT_THRESHOLD
        ):
            failures.append(
                f"ROW COUNT delta {result.row_pct:.2f}% exceeds "
                f"{ROW_COUNT_PCT_THRESHOLD:.2f}% threshold "
                f"({result.old_total:,} → {result.new_total:,}, "
                f"{result.row_delta:+,})."
            )
        if (
            result.inactive_pct is not None
            and result.inactive_pct > INACTIVE_CHURN_PCT_THRESHOLD
        ):
            failures.append(
                f"INACTIVE CHURN {result.inactive_pct:.2f}% exceeds "
                f"{INACTIVE_CHURN_PCT_THRESHOLD:.2f}% threshold "
                f"(would mark {result.would_be_inactivated:,} of "
                f"{result.currently_active:,} currently-active rows inactive)."
            )

    if not failures:
        return

    # Duplicate check always blocks; others can be force-overridden
    if result.dup_distinct_numbers > 0:
        raise SanityCheckError(
            result,
            "Sanity check FAILED:\n  - " + "\n  - ".join(failures),
        )
    if not force:
        msg = (
            "Sanity check FAILED:\n  - "
            + "\n  - ".join(failures)
            + "\n\nIf the new CSV is correct, re-run with force=True "
            "(CLI) or click 'Force process' in the GUI."
        )
        raise SanityCheckError(result, msg)


def process_csvs(
    csv_files: list[str | Path],
    db_path: str | Path,
    progress_callback: callable | None = None,
    force: bool = False,
    scrape_date_override: date | None = None,
    compact: bool = True,
) -> int:
    """Ingest CSV files into the DuckDB database via the upsert pipeline.

    All input files must share the same scrape date (the YYYY-MM-DD
    prefix in the BasicCompanyData filename). Group by month upstream
    if you have files from multiple months.

    First call (no ``companies`` table): bootstraps from staging.
    Subsequent calls: merges via ``sql/ch/upsert_companies.sql``, with
    sanity checks gating the merge.

    Args:
        csv_files: List of paths to CSV files to ingest. Must all be
            from the same scrape date.
        db_path: Path to the DuckDB database file (created if missing).
        progress_callback: Optional callable for progress messages.
        force: If True, skip the row-count and inactive-churn sanity
            guards. Does NOT override the duplicate check.
        scrape_date_override: Override the auto-detected scrape date.
            Only useful for testing.
        compact: If True (default), reclaim disk space after the merge
            by rebuilding the database file. Compaction is part of the
            normal pipeline — disable only when you'll compact later
            yourself (e.g., between back-to-back month upserts).

    Returns:
        Total number of rows in ``companies`` after the operation.

    Raises:
        FileNotFoundError: If any CSV file does not exist.
        ValueError: If filenames don't all share one scrape date.
        SanityCheckError: If a sanity check fails (force=False or
            duplicate violation).
        duckdb.Error: On database errors.
    """
    csv_files = [Path(f) for f in csv_files]
    db_path = Path(db_path)

    for f in csv_files:
        if not f.exists():
            raise FileNotFoundError(f"CSV file not found: {f}")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    scrape_date = scrape_date_override or scrape_date_from_files(csv_files)

    def _notify(msg: str) -> None:
        if progress_callback:
            progress_callback(msg)
        else:
            console.print(f"[bold blue]{msg}[/]")

    # Wrap each phase in a tiny closure decorated with @timed_phase so
    # the per-phase elapsed time hits the logger as soon as the phase
    # ends — even if we get killed in a later phase.

    @timed_phase("ingest_to_staging")
    def _do_ingest(con):
        _ingest_to_staging(con, csv_files)

    @timed_phase("sanity_checks")
    def _do_sanity(con):
        return _check_sanity(con)

    @timed_phase("bootstrap_companies")
    def _do_bootstrap(con):
        _run_sql_file(
            con, "bootstrap_companies.sql",
            scrape_date=scrape_date.isoformat(),
        )

    @timed_phase("upsert_companies")
    def _do_upsert(con):
        con.execute("BEGIN TRANSACTION")
        try:
            _run_sql_file(
                con, "upsert_companies.sql",
                scrape_date=scrape_date.isoformat(),
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    @timed_phase("create_indexes")
    def _do_indexes(con):
        _run_sql_file(con, "indexes.sql")

    _notify(f"Processing scrape_date {scrape_date.isoformat()}")
    t_total = time.perf_counter()
    con = duckdb.connect(str(db_path))
    try:
        _do_ingest(con)

        result = _do_sanity(con)
        _enforce_sanity(result, force=force)

        if not result.companies_exists:
            _do_bootstrap(con)
        else:
            _notify(
                f"  Sanity OK: row delta {result.row_pct:.2f}%, "
                f"inactive churn {result.inactive_pct:.2f}%"
            )
            _do_upsert(con)

        _do_indexes(con)

        con.execute("DROP TABLE IF EXISTS companies_staging")

        row_count: int = con.execute(
            "SELECT COUNT(*) FROM companies"
        ).fetchone()[0]
        active_count: int = con.execute(
            "SELECT COUNT(*) FROM companies WHERE is_active = TRUE"
        ).fetchone()[0]

        total = time.perf_counter() - t_total
        msg = (
            f"Done! {row_count:,} companies ({active_count:,} active) "
            f"in {total:.1f}s (see ch_bulk.log for per-phase timings)"
        )
        _notify(msg)
        logger.info(
            "Processed %d rows (%d active) for scrape_date=%s into %s in %.1fs",
            row_count, active_count, scrape_date.isoformat(), db_path, total,
        )
    finally:
        con.close()

    if compact:
        compact_database(db_path, progress_callback=progress_callback)

    return row_count


@timed_phase("compact_database")
def compact_database(
    db_path: str | Path,
    progress_callback: callable | None = None,
) -> None:
    """Reclaim disk space by rebuilding the database file.

    DuckDB does not reclaim pages after ``DROP TABLE`` — ``CHECKPOINT``
    only releases space from row-level ``DELETE``\\s, and ``VACUUM`` is
    a no-op for disk footprint. The single-file format requires copying
    the data into a fresh file to actually shrink.

    Reference:
        https://duckdb.org/docs/current/operations_manual/footprint_of_duckdb/reclaiming_space

    Args:
        db_path: Path to the DuckDB database file.
        progress_callback: If provided, called with status strings.

    Raises:
        FileNotFoundError: If the database does not exist.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB database not found: {db_path}")

    # Heal the known pre-WP3 HSCA FK shape before COPY FROM DATABASE.
    # This migration is intentionally narrow and no-ops on normal CH/CQC
    # databases that never created the HSCA snapshot tables.
    from ch_bulk.db.bootstrap import repair_pipeline_schema

    repair_con = duckdb.connect(str(db_path))
    try:
        repair_pipeline_schema(repair_con)
    finally:
        repair_con.close()

    if progress_callback:
        progress_callback("Compacting database (one-time copy to reclaim space)...")
    else:
        console.print("[bold blue]Compacting database (one-time copy to reclaim space)...[/]")

    tmp_db = db_path.with_suffix(db_path.suffix + ".compact.tmp")
    if tmp_db.exists():
        tmp_db.unlink()

    src = _escape_path(db_path)
    dst = _escape_path(tmp_db)

    con = duckdb.connect()
    try:
        con.execute(f"ATTACH '{src}' AS src (READ_ONLY)")
        con.execute(f"ATTACH '{dst}' AS dst")
        con.execute("COPY FROM DATABASE src TO dst")
        con.execute("DETACH src")
        con.execute("DETACH dst")
    finally:
        con.close()

    db_path.unlink()
    tmp_db.rename(db_path)
