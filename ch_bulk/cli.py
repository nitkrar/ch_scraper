"""Typer CLI for ch-bulk — thin wrapper around the ChBulk Python API."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from ch_bulk.api import ChBulk
from ch_bulk.core.paths import DEFAULT_DATA_DIR, DEFAULT_DB_PATH

app = typer.Typer(
    name="ch-bulk",
    help="Download and query UK Companies House bulk data by SIC code.",
    no_args_is_help=True,
)
console = Console()
cqc_enrich_app = typer.Typer(help="CQC API enrichment commands.")
ch_enrich_app = typer.Typer(help="Companies House enrichment commands.")
migration_app = typer.Typer(help="Migration bundle commands.")
app.add_typer(cqc_enrich_app, name="cqc-enrich")
app.add_typer(ch_enrich_app, name="ch-enrich")
app.add_typer(migration_app, name="migration")

# Set up basic logging so library messages are visible
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)


def _parse_ids(ids: Optional[str]) -> list[str] | None:
    if ids is None:
        return None
    cleaned = [value.strip() for value in ids.split(",") if value.strip()]
    return cleaned or None


@app.command()
def download(
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", "-d", help="Directory for downloads."
    ),
    month: Optional[str] = typer.Option(
        None, "--month", "-m", help="Month as YYYY-MM (auto-detected if omitted)."
    ),
    keep_zips: bool = typer.Option(
        False, "--keep-zips", help="Keep ZIP files after extraction."
    ),
) -> None:
    """Download Companies House bulk CSV data (7 parts)."""
    ch = ChBulk(data_dir=data_dir)
    csv_files = ch.download(month=month, keep_zips=keep_zips)
    console.print(f"[bold green]Downloaded {len(csv_files)} CSV files.[/]")


@app.command()
def process(
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", "-d", help="Directory with CSV files."
    ),
    db_path: Path = typer.Option(
        DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
    ),
) -> None:
    """Ingest downloaded CSVs into a DuckDB database."""
    ch = ChBulk(data_dir=data_dir, db_path=db_path)
    row_count = ch.process()
    console.print(f"[bold green]Processed {row_count:,} companies.[/]")


@app.command()
def query(
    sic_codes: str = typer.Argument(
        ..., help="Comma-separated SIC codes to search for."
    ),
    status: Optional[str] = typer.Option(
        "Active", "--status", "-s", help="Filter by company status."
    ),
    output_csv: Optional[Path] = typer.Option(
        None, "--output-csv", "-o", help="Export results to CSV file."
    ),
    db_path: Path = typer.Option(
        DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", "-n", help="Max results to return."
    ),
) -> None:
    """Query companies by SIC code."""
    ch = ChBulk(db_path=db_path)

    if output_csv:
        # Export directly via DuckDB COPY — no memory load
        from ch_bulk.companies_house.query import export_query_csv
        try:
            n = export_query_csv(
                db_path, sic_codes, output_csv,
                status=status, limit=limit,
            )
            console.print(f"[green]Exported {n:,} companies to {output_csv}[/]")
        except FileNotFoundError:
            console.print("[red]Database not found. Run 'ch-bulk sync' first.[/]")
        return

    try:
        results = ch.query(
            sic_codes=sic_codes,
            status=status,
            limit=limit,
        )
    except FileNotFoundError:
        console.print("[red]Database not found. Run 'ch-bulk sync' first.[/]")
        return

    if not results:
        console.print("[yellow]No companies found.[/]")
        return

    table = Table(title=f"Companies with SIC code(s): {sic_codes}")
    table.add_column("Company Number", style="cyan")
    table.add_column("Company Name")
    table.add_column("Status")
    table.add_column("Postcode")
    table.add_column("SIC 1")

    display_limit = min(len(results), 50)
    for r in results[:display_limit]:
        table.add_row(
            str(r.get("company_number", "")),
            str(r.get("company_name", "")),
            str(r.get("company_status", "")),
            str(r.get("postcode", "")),
            str(r.get("sic_code_1", "")),
        )

    console.print(table)
    console.print(f"\n[bold]{len(results):,} total results[/]")


@app.command()
def sync(
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", "-d", help="Directory for downloads."
    ),
    db_path: Path = typer.Option(
        DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
    ),
    month: Optional[str] = typer.Option(
        None, "--month", "-m", help="Month as YYYY-MM (auto-detected if omitted)."
    ),
    keep_zips: bool = typer.Option(
        False, "--keep-zips", help="Keep ZIP files after extraction."
    ),
) -> None:
    """Download and process in one step."""
    ch = ChBulk(data_dir=data_dir, db_path=db_path)
    row_count = ch.sync(month=month, keep_zips=keep_zips)
    console.print(f"[bold green]Synced {row_count:,} companies.[/]")


@app.command("match")
def match_command(
    mode: str = typer.Option(
        "incremental",
        "--mode",
        help="One of: incremental, all.",
    ),
    db_path: Path = typer.Option(
        DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
    ),
) -> None:
    """Match active SIC-88100 companies to relevant CQC providers."""
    ch = ChBulk(db_path=db_path)
    summary = ch.match(mode=mode)
    console.print(
        "[bold green]CH↔CQC match complete:[/] "
        f"mode={summary['mode']} "
        f"companies={summary['companies_considered']} "
        f"providers={summary['providers_considered']} "
        f"matches={summary['matches_found']} "
        f"written={summary['written']} "
        f"auto={summary['auto_confirmed']} "
        f"review={summary['needs_review']}"
    )


@app.command("export-sqlite")
def export_sqlite(
    db_path: Path = typer.Option(
        DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
    ),
    output: Path = typer.Option(
        Path("ch_bulk.sqlite"), "--output", "-o", help="Output SQLite file path."
    ),
) -> None:
    """Export DuckDB database to SQLite."""
    ch = ChBulk(db_path=db_path)
    result_path = ch.export_sqlite(output)
    console.print(f"[bold green]Exported to SQLite:[/] {result_path}")


@app.command()
def info(
    db_path: Path = typer.Option(
        DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
    ),
) -> None:
    """Show database summary statistics."""
    ch = ChBulk(db_path=db_path)
    stats = ch.info()

    console.print()
    console.print(f"[bold]Companies House Database:[/] {db_path}")
    console.print(f"[bold]Total companies:[/] {stats['total_companies']:,}")

    console.print()
    console.print("[bold]Status breakdown:[/]")
    status_table = Table(show_header=True)
    status_table.add_column("Status")
    status_table.add_column("Count", justify="right")
    for status, count in stats["status_breakdown"].items():
        status_table.add_row(str(status), f"{count:,}")
    console.print(status_table)

    console.print()
    console.print("[bold]Top 20 SIC codes:[/]")
    sic_table = Table(show_header=True)
    sic_table.add_column("SIC Code")
    sic_table.add_column("Count", justify="right")
    for entry in stats["top_sic_codes"]:
        sic_table.add_row(entry["sic_code"], f"{entry['count']:,}")
    console.print(sic_table)


@app.command()
def ui(
    db_path: Path = typer.Option(
        DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
    ),
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", "-d", help="Directory for downloads."
    ),
) -> None:
    """Launch the desktop GUI."""
    try:
        from ch_bulk.gui import main as gui_main
    except ImportError:
        console.print("[red]Error:[/] Tkinter is not installed.")
        console.print()
        console.print("To fix:")
        console.print("  macOS (Homebrew):  [bold]brew install python-tk@3.12[/]")
        console.print("  Ubuntu/Debian:     [bold]sudo apt install python3-tk[/]")
        console.print("  Fedora/RHEL:       [bold]sudo dnf install python3-tkinter[/]")
        console.print("  Windows:           Reinstall Python with 'tcl/tk' option checked")
        raise typer.Exit(1)

    gui_main(db_path=str(db_path), data_dir=str(data_dir))


@app.command("load-staging")
def load_staging(
    sync_type: str = typer.Option(
        ...,
        "--sync-type",
        help="One of: api_providers, api_locations, ch_directors, classifications, website_finder, financials.",
    ),
    batch_id: Optional[str] = typer.Option(
        None,
        "--batch-id",
        help="Specific batch_id to replay; loads all pending files when omitted.",
    ),
    db_path: Path = typer.Option(
        DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
    ),
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", "-d", help="Directory containing staging files."
    ),
) -> None:
    ch = ChBulk(data_dir=data_dir, db_path=db_path)
    summary = ch.load_staging(sync_type=sync_type, batch_id=batch_id)
    console.print(
        "[bold green]Staging load complete:[/] "
        f"sync_type={summary['sync_type']} "
        f"batches={summary['loaded_batches']} "
        f"fetched={summary['records_fetched']} "
        f"updated={summary['records_updated']} "
        f"errors={summary['error_count']}"
    )


@app.command("classify")
def classify_command(
    mode: str = typer.Option(
        "incremental",
        "--mode",
        help="One of: incremental, all, list.",
    ),
    ids: Optional[str] = typer.Option(
        None,
        "--ids",
        help="Comma-separated company numbers when --mode=list.",
    ),
    db_path: Path = typer.Option(
        DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
    ),
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", "-d", help="Directory with settings.json."
    ),
    batch_size: int = typer.Option(
        100,
        "--batch-size",
        help="Buffered staging size before each flush+fsync checkpoint.",
    ),
) -> None:
    ch = ChBulk(data_dir=data_dir, db_path=db_path)
    summary = ch.classify(
        mode=mode,
        ids=_parse_ids(ids),
        batch_size=batch_size,
    )
    console.print(
        "[bold green]Website classification complete:[/] "
        f"batch={summary['batch_id']} "
        f"requested={summary['requested']} "
        f"classified={summary['records_updated']} "
        f"unable={summary['unable_count']} "
        f"errors={summary['error_count']}"
    )


@app.command("find-websites")
def find_websites_command(
    mode: str = typer.Option(
        "incremental",
        "--mode",
        help="One of: incremental, all, list.",
    ),
    ids: Optional[str] = typer.Option(
        None,
        "--ids",
        help="Comma-separated company numbers when --mode=list.",
    ),
    pause_seconds: float = typer.Option(
        0.7,
        "--pause-seconds",
        help="Delay between DDG searches to avoid rate limits.",
    ),
    db_path: Path = typer.Option(
        DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
    ),
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", "-d", help="Directory with staging/log files."
    ),
) -> None:
    ch = ChBulk(data_dir=data_dir, db_path=db_path)
    summary = ch.find_websites(
        mode=mode,
        ids=_parse_ids(ids),
        pause_seconds=pause_seconds,
    )
    console.print(
        "[bold green]Website discovery complete:[/] "
        f"batch={summary['batch_id']} "
        f"requested={summary['requested']} "
        f"fetched={summary['records_fetched']} "
        f"inserted={summary['records_updated']} "
        f"no_match={summary['no_match_count']} "
        f"errors={summary['error_count']} "
        f"pause={summary['pause_seconds']:.1f}s "
        f"min_score={summary['min_score']} "
        f"resumed={'yes' if summary['resumed'] else 'no'}"
    )


@cqc_enrich_app.command("providers")
def cqc_enrich_providers(
    mode: str = typer.Option(
        "incremental",
        "--mode",
        help="One of: incremental, all, list.",
    ),
    ids: Optional[str] = typer.Option(
        None,
        "--ids",
        help="Comma-separated provider IDs when --mode=list.",
    ),
    db_path: Path = typer.Option(
        DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
    ),
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", "-d", help="Directory with settings.json."
    ),
    batch_size: int = typer.Option(
        1000,
        "--batch-size",
        help="Buffered staging size before each flush+fsync checkpoint.",
    ),
) -> None:
    ch = ChBulk(data_dir=data_dir, db_path=db_path)
    summary = ch.cqc_enrich_providers(
        mode=mode,
        ids=_parse_ids(ids),
        batch_size=batch_size,
    )
    console.print(
        "[bold green]CQC provider enrich complete:[/] "
        f"batch={summary['batch_id']} "
        f"requested={summary['requested']} "
        f"updated={summary['records_updated']} "
        f"errors={summary['error_count']}"
    )


@cqc_enrich_app.command("locations")
def cqc_enrich_locations(
    mode: str = typer.Option(
        "incremental",
        "--mode",
        help="One of: incremental, all, list.",
    ),
    ids: Optional[str] = typer.Option(
        None,
        "--ids",
        help="Comma-separated location IDs when --mode=list.",
    ),
    db_path: Path = typer.Option(
        DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
    ),
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", "-d", help="Directory with settings.json."
    ),
    batch_size: int = typer.Option(
        1000,
        "--batch-size",
        help="Buffered staging size before each flush+fsync checkpoint.",
    ),
) -> None:
    ch = ChBulk(data_dir=data_dir, db_path=db_path)
    summary = ch.cqc_enrich_locations(
        mode=mode,
        ids=_parse_ids(ids),
        batch_size=batch_size,
    )
    console.print(
        "[bold green]CQC location enrich complete:[/] "
        f"batch={summary['batch_id']} "
        f"requested={summary['requested']} "
        f"updated={summary['records_updated']} "
        f"errors={summary['error_count']}"
    )


@ch_enrich_app.command("directors")
def ch_enrich_directors(
    sic: str = typer.Option("88100", "--sic", help="SIC code filter."),
    company_numbers: Optional[str] = typer.Option(
        None,
        "--company-numbers",
        help="Comma-separated company numbers to enrich explicitly.",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Re-enrich rows even if director DOB years already exist.",
    ),
    db_path: Path = typer.Option(
        DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
    ),
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", "-d", help="Directory with settings.json."
    ),
    batch_size: int = typer.Option(
        1000,
        "--batch-size",
        help="Buffered staging size before each flush+fsync checkpoint.",
    ),
) -> None:
    ch = ChBulk(data_dir=data_dir, db_path=db_path)
    summary = ch.ch_enrich_directors(
        sic=sic,
        company_numbers=_parse_ids(company_numbers),
        force=force,
        batch_size=batch_size,
    )
    console.print(
        "[bold green]CH director enrich complete:[/] "
        f"requested={summary['requested']} "
        f"enriched={summary['enriched']} "
        f"no_active_directors={summary['no_active_directors']} "
        f"errors={summary['error_count']}"
    )


@ch_enrich_app.command("revenue")
def ch_enrich_revenue(
    sic: str = typer.Option("88100", "--sic", help="SIC code filter."),
    company_numbers: Optional[str] = typer.Option(
        None,
        "--company-numbers",
        help="Comma-separated company numbers to enrich explicitly.",
    ),
    db_path: Path = typer.Option(
        DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
    ),
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", "-d", help="Directory with settings.json."
    ),
) -> None:
    ch = ChBulk(data_dir=data_dir, db_path=db_path)
    summary = ch.ch_enrich_revenue(
        sic=sic,
        company_numbers=_parse_ids(company_numbers),
    )
    console.print(
        "[bold green]CH revenue enrich complete:[/] "
        f"requested={summary['requested']} "
        f"estimated={summary['estimated']} "
        f"skipped={summary['skipped']}"
    )


@app.command("enrich-financials")
def enrich_financials_command(
    mode: str = typer.Option(
        "incremental",
        "--mode",
        help="incremental, all, or list.",
    ),
    ids: Optional[str] = typer.Option(
        None,
        "--ids",
        help="Comma-separated company numbers for mode=list.",
    ),
    workers: int = typer.Option(
        3,
        "--workers",
        min=1,
        help="Worker threads. All workers share one global CH throttle.",
    ),
    parser_workers: int = typer.Option(
        4,
        "--parser-workers",
        min=1,
        help="Parser worker processes. Independent from fetch worker threads.",
    ),
    batch_size: int = typer.Option(
        100,
        "--batch-size",
        help="Buffered staging size before each flush+fsync checkpoint.",
    ),
    db_path: Path = typer.Option(
        DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
    ),
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", "-d", help="Directory with settings.json."
    ),
) -> None:
    ch = ChBulk(data_dir=data_dir, db_path=db_path)
    summary = ch.enrich_financials(
        mode=mode,
        ids=_parse_ids(ids),
        workers=workers,
        parser_workers=parser_workers,
        batch_size=batch_size,
    )
    console.print(
        "[bold green]Financials enrich complete:[/] "
        f"batch={summary['batch_id']} "
        f"requested={summary['requested']} "
        f"updated={summary['records_updated']} "
        f"ok={summary['ok_count']} "
        f"partial={summary['partial_count']} "
        f"pdf_no_text_layer={summary['pdf_no_text_layer_count']} "
        f"errors={summary['error_count']}"
    )


@migration_app.command("export")
def migration_export(
    bundle: Path = typer.Option(
        ...,
        "--bundle",
        help="Output Parquet bundle directory.",
    ),
    db_path: Path = typer.Option(
        DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
    ),
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", "-d", help="Directory containing logs/settings."
    ),
) -> None:
    ch = ChBulk(data_dir=data_dir, db_path=db_path)
    summary = ch.migration_export(bundle_dir=bundle)
    console.print(
        "[bold green]Migration export complete:[/] "
        f"bundle={summary['bundle_dir']} "
        f"tables={len(summary['table_files'])} "
        f"size={summary['bundle_size']}"
    )


@migration_app.command("import")
def migration_import(
    bundle: Path = typer.Option(
        ...,
        "--bundle",
        help="Parquet bundle directory to import from.",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Clear the bundle's portable tables before importing.",
    ),
    db_path: Path = typer.Option(
        DEFAULT_DB_PATH, "--db-path", help="DuckDB database path."
    ),
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", "-d", help="Directory containing logs/settings."
    ),
) -> None:
    ch = ChBulk(data_dir=data_dir, db_path=db_path)
    summary = ch.migration_import(bundle_dir=bundle, force=force)
    console.print(
        "[bold green]Migration import complete:[/] "
        f"bundle={summary['bundle_dir']} "
        f"tables={len(summary['row_counts'])} "
        f"force={summary['force']}"
    )
