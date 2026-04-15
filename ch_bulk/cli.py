"""Typer CLI for ch-bulk — thin wrapper around the ChBulk Python API."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from ch_bulk.api import ChBulk

app = typer.Typer(
    name="ch-bulk",
    help="Download and query UK Companies House bulk data by SIC code.",
    no_args_is_help=True,
)
console = Console()

# Set up basic logging so library messages are visible
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)


@app.command()
def download(
    data_dir: Path = typer.Option(
        Path("./data"), "--data-dir", "-d", help="Directory for downloads."
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
        Path("./data"), "--data-dir", "-d", help="Directory with CSV files."
    ),
    db_path: Path = typer.Option(
        Path("ch_bulk.duckdb"), "--db-path", help="DuckDB database path."
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
        Path("ch_bulk.duckdb"), "--db-path", help="DuckDB database path."
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", "-n", help="Max results to return."
    ),
) -> None:
    """Query companies by SIC code."""
    ch = ChBulk(db_path=db_path)

    if output_csv:
        # Export directly via DuckDB COPY — no memory load
        from ch_bulk.query import export_query_csv
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
        Path("./data"), "--data-dir", "-d", help="Directory for downloads."
    ),
    db_path: Path = typer.Option(
        Path("ch_bulk.duckdb"), "--db-path", help="DuckDB database path."
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


@app.command("export-sqlite")
def export_sqlite(
    db_path: Path = typer.Option(
        Path("ch_bulk.duckdb"), "--db-path", help="DuckDB database path."
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
        Path("ch_bulk.duckdb"), "--db-path", help="DuckDB database path."
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
        Path("ch_bulk.duckdb"), "--db-path", help="DuckDB database path."
    ),
    data_dir: Path = typer.Option(
        Path("./data"), "--data-dir", "-d", help="Directory for downloads."
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
