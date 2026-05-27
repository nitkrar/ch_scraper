"""Parquet bundle export/import helpers for portable pipeline tables."""

from __future__ import annotations

import json
import logging
import subprocess
import warnings
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from ch_bulk.core.paths import REPO_ROOT
from ch_bulk.db.bootstrap import ensure_pipeline_schema

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "1"
MANIFEST_FILENAME = "manifest.json"
PORTABLE_TABLES = [
    "exclusion_lists",
    "excluded_companies",
    "excluded_staging",
    "cqc_sync_batches",
    "cqc_api_responses",
    "cqc_providers_enriched",
    "cqc_locations_enriched",
    "company_enrichment",
    "classification_batches",
    "classifications",
    "ch_cqc_matches",
    "company_websites",
]

DELETE_ORDER = list(reversed(PORTABLE_TABLES))

PRIMARY_KEYS = {
    "exclusion_lists": ["list_name"],
    "excluded_companies": ["company_number", "list_name"],
    "excluded_staging": ["row_id"],
    "cqc_sync_batches": ["batch_id"],
    "cqc_api_responses": ["response_id"],
    "cqc_providers_enriched": ["provider_id"],
    "cqc_locations_enriched": ["location_id"],
    "company_enrichment": ["company_number"],
    "classification_batches": ["batch_id"],
    "classifications": ["company_number", "source_type"],
    "ch_cqc_matches": ["company_number", "cqc_provider_id"],
    "company_websites": ["website_id"],
}

SEQUENCE_ADVANCE = {
    "excluded_staging": ("excluded_staging_row_id_seq", "row_id"),
    "cqc_api_responses": ("cqc_api_response_id_seq", "response_id"),
    "company_websites": ("company_website_id_seq", "website_id"),
}


def _quote_path(path: Path) -> str:
    return str(path).replace("\\", "\\\\").replace("'", "''")


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _repo_root() -> Path:
    return REPO_ROOT


def _source_git_sha() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=_repo_root(),
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    if result.returncode != 0:
        return None
    sha = result.stdout.strip()
    return sha or None


def _table_exists(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {table_name} LIMIT 0")
        return True
    except duckdb.CatalogException:
        return False


def _row_count(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def _table_columns(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> list[str]:
    rows = con.execute(
        f"PRAGMA table_info({_quote_ident(table_name)})"
    ).fetchall()
    return [str(row[1]) for row in rows]


def _warn(message: str) -> None:
    logger.warning(message)
    warnings.warn(message, RuntimeWarning, stacklevel=2)


def _bundle_manifest_path(bundle_dir: Path) -> Path:
    return bundle_dir / MANIFEST_FILENAME


def _write_manifest(
    bundle_dir: Path,
    *,
    row_counts: dict[str, int],
    table_files: list[str],
) -> dict[str, object]:
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "source_git_sha": _source_git_sha(),
        "row_counts": row_counts,
        "table_files": table_files,
    }
    _bundle_manifest_path(bundle_dir).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def _read_manifest(bundle_dir: Path) -> dict[str, object]:
    manifest_path = _bundle_manifest_path(bundle_dir)
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Migration bundle manifest not found: {manifest_path}"
        )
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _manifest_table_files(manifest: dict[str, object]) -> list[str]:
    raw_files = manifest.get("table_files")
    if isinstance(raw_files, list):
        files = [str(item) for item in raw_files]
    else:
        row_counts = manifest.get("row_counts", {})
        if not isinstance(row_counts, dict):
            raise ValueError(
                "Invalid migration manifest: row_counts must be an object"
            )
        files = [f"{table_name}.parquet" for table_name in row_counts]

    deduped: list[str] = []
    seen: set[str] = set()
    for file_name in files:
        if file_name in seen:
            continue
        seen.add(file_name)
        deduped.append(file_name)
    return deduped


def _reset_sequence(
    con: duckdb.DuckDBPyConnection,
    *,
    sequence_name: str,
    next_value: int,
) -> None:
    con.execute(
        f"CREATE OR REPLACE SEQUENCE {sequence_name} START {max(next_value, 1)}"
    )


def _ensure_companies_table_populated(
    con: duckdb.DuckDBPyConnection,
) -> None:
    if not _table_exists(con, "companies") or _row_count(con, "companies") <= 0:
        raise RuntimeError(
            "Parquet import requires Companies House bulk data to be loaded first. "
            "Run `ch-bulk sync` before importing the bundle."
        )


def _upsert_from_parquet(
    con: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    parquet_path: Path,
    replace_existing: bool,
) -> None:
    quoted_table = _quote_ident(table_name)
    parquet_sql_path = _quote_path(parquet_path)
    if replace_existing:
        con.execute(
            f"""
            INSERT INTO {quoted_table} BY NAME
            SELECT *
            FROM read_parquet('{parquet_sql_path}')
            """
        )
        return

    key_columns = PRIMARY_KEYS[table_name]
    update_columns = [
        column
        for column in _table_columns(con, table_name)
        if column not in key_columns
    ]
    conflict_target = ", ".join(_quote_ident(column) for column in key_columns)
    if update_columns:
        assignments = ", ".join(
            f"{_quote_ident(column)} = EXCLUDED.{_quote_ident(column)}"
            for column in update_columns
        )
        action_sql = f"DO UPDATE SET {assignments}"
    else:
        action_sql = "DO NOTHING"

    con.execute(
        f"""
        INSERT INTO {quoted_table} BY NAME
        SELECT *
        FROM read_parquet('{parquet_sql_path}')
        ON CONFLICT ({conflict_target}) {action_sql}
        """
    )


def export_to_parquet(
    db_path: str | Path,
    bundle_dir: str | Path,
) -> dict[str, object]:
    source_path = Path(db_path)
    if not source_path.exists():
        raise FileNotFoundError(f"Source database not found: {source_path}")

    bundle_dir = Path(bundle_dir)
    bundle_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = _bundle_manifest_path(bundle_dir)
    if manifest_path.exists():
        manifest_path.unlink()

    for table_name in PORTABLE_TABLES:
        table_path = bundle_dir / f"{table_name}.parquet"
        if table_path.exists():
            table_path.unlink()

    row_counts: dict[str, int] = {}
    table_files: list[str] = []
    file_sizes: dict[str, int] = {}

    con = duckdb.connect(str(source_path), read_only=True)
    try:
        for table_name in PORTABLE_TABLES:
            if not _table_exists(con, table_name):
                _warn(f"Skipping missing source table during export: {table_name}")
                continue
            output_path = bundle_dir / f"{table_name}.parquet"
            row_counts[table_name] = _row_count(con, table_name)
            con.execute(
                f"""
                COPY (
                    SELECT *
                    FROM {table_name}
                )
                TO '{_quote_path(output_path)}'
                (
                    FORMAT PARQUET,
                    COMPRESSION SNAPPY
                )
                """
            )
            table_files.append(output_path.name)
            file_sizes[output_path.name] = output_path.stat().st_size

        manifest = _write_manifest(
            bundle_dir,
            row_counts=row_counts,
            table_files=table_files,
        )
        file_sizes[MANIFEST_FILENAME] = manifest_path.stat().st_size
    finally:
        con.close()

    return {
        "bundle_dir": str(bundle_dir),
        "manifest_path": str(manifest_path),
        "schema_version": SCHEMA_VERSION,
        "exported_at": manifest["exported_at"],
        "source_git_sha": manifest["source_git_sha"],
        "table_files": table_files,
        "row_counts": row_counts,
        "file_sizes": file_sizes,
        "bundle_size": sum(file_sizes.values()),
    }


def import_from_parquet(
    bundle_dir: str | Path,
    db_path: str | Path,
    *,
    force: bool = False,
) -> dict[str, object]:
    bundle_dir = Path(bundle_dir)
    if not bundle_dir.exists():
        raise FileNotFoundError(f"Migration bundle not found: {bundle_dir}")

    manifest = _read_manifest(bundle_dir)
    table_files = _manifest_table_files(manifest)
    requested_tables = [Path(file_name).stem for file_name in table_files]

    target_path = Path(db_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(target_path))
    try:
        ensure_pipeline_schema(con)
        _ensure_companies_table_populated(con)

        manifest_version = manifest.get("schema_version")
        if manifest_version != SCHEMA_VERSION:
            _warn(
                "Importing bundle with unexpected schema_version "
                f"{manifest_version!r}; expected {SCHEMA_VERSION!r}"
            )

        imported_tables: list[str] = []
        skipped_tables: list[str] = []
        row_counts: dict[str, int] = {}

        con.execute("BEGIN TRANSACTION")
        try:
            if force:
                for table_name in DELETE_ORDER:
                    if table_name not in requested_tables:
                        continue
                    if not _table_exists(con, table_name):
                        continue
                    con.execute(f"DELETE FROM {table_name}")

            for file_name in table_files:
                table_name = Path(file_name).stem
                if table_name not in PORTABLE_TABLES:
                    skipped_tables.append(table_name)
                    _warn(
                        f"Skipping non-portable bundle table during import: {table_name}"
                    )
                    continue
                if not _table_exists(con, table_name):
                    skipped_tables.append(table_name)
                    _warn(
                        "Skipping bundle table missing from the current schema: "
                        f"{table_name}"
                    )
                    continue

                parquet_path = bundle_dir / file_name
                if not parquet_path.exists():
                    raise FileNotFoundError(
                        f"Bundle file listed in manifest is missing: {parquet_path}"
                    )

                _upsert_from_parquet(
                    con,
                    table_name=table_name,
                    parquet_path=parquet_path,
                    replace_existing=force,
                )
                imported_tables.append(table_name)
                row_counts[table_name] = _row_count(con, table_name)

            for table_name, (sequence_name, id_column) in SEQUENCE_ADVANCE.items():
                if not _table_exists(con, table_name):
                    continue
                next_value = int(
                    con.execute(
                        f"SELECT COALESCE(MAX({id_column}), 0) + 1 FROM {table_name}"
                    ).fetchone()[0]
                )
                _reset_sequence(
                    con,
                    sequence_name=sequence_name,
                    next_value=next_value,
                )

            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        con.execute("CHECKPOINT")
        return {
            "bundle_dir": str(bundle_dir),
            "manifest_path": str(_bundle_manifest_path(bundle_dir)),
            "target_path": str(target_path),
            "force": force,
            "imported_tables": imported_tables,
            "skipped_tables": skipped_tables,
            "row_counts": row_counts,
        }
    finally:
        con.close()
