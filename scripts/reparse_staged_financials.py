#!/usr/bin/env python3
"""Rebuild a financials staging JSONL from saved raw filings on disk."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import date
from pathlib import Path

import duckdb

from ch_bulk.financials_enricher import (
    FINANCIALS_SYNC_TYPE,
    IXBRL_EXTENSION,
    PDF_EXTENSION,
    FilingCandidate,
    FinancialTarget,
    _build_row,
    _parse_ixbrl_bytes,
    _parse_pdf_bytes,
)
from ch_bulk.staging import staging_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Re-parse saved raw filing artifacts for a financials staging batch "
            "and emit a fresh pending JSONL in the current schema."
        )
    )
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--data-dir", required=True)
    return parser.parse_args()


def _source_path(data_dir: Path, batch_id: str) -> Path:
    pending_path = staging_path(
        data_dir,
        sync_type=FINANCIALS_SYNC_TYPE,
        batch_id=batch_id,
    )
    loaded_path = pending_path.with_name(f"{pending_path.name}.loaded")
    if loaded_path.exists():
        return loaded_path
    if pending_path.exists():
        return pending_path
    raise FileNotFoundError(
        f"No staging source found for batch {batch_id}: "
        f"checked {loaded_path} and {pending_path}"
    )


def _accounts_dates(
    db_path: Path,
    company_numbers: list[str],
) -> dict[str, date | None]:
    if not company_numbers:
        return {}
    placeholders = ", ".join(["?"] * len(company_numbers))
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute(
            f"""
            SELECT company_number, accounts_last_made_up
            FROM companies
            WHERE company_number IN ({placeholders})
            """,
            company_numbers,
        ).fetchall()
    finally:
        con.close()
    return {str(company_number): accounts_last_made_up for company_number, accounts_last_made_up in rows}


def _reparse_row(
    *,
    payload: dict[str, object],
    data_dir: Path,
    accounts_last_made_up: date | None,
) -> tuple[str, str]:
    company_number = str(payload["company_number"])
    filing_id = str(payload["filing_id"])
    filing_format = str(payload["filing_format"])
    if filing_format == "ixbrl":
        extension = IXBRL_EXTENSION
        facts = _parse_ixbrl_bytes(
            (data_dir / "staging" / "filings" / company_number / f"{filing_id}.{extension}").read_bytes()
        )
    elif filing_format == "pdf":
        extension = PDF_EXTENSION
        facts = _parse_pdf_bytes(
            (data_dir / "staging" / "filings" / company_number / f"{filing_id}.{extension}").read_bytes()
        )
    else:
        raise ValueError(
            f"Unsupported filing_format={filing_format!r} for company {company_number}"
        )

    filing_date_raw = payload.get("filing_date")
    filing_date = (
        date.fromisoformat(str(filing_date_raw))
        if filing_date_raw
        else None
    )
    row = _build_row(
        target=FinancialTarget(
            company_number=company_number,
            accounts_last_made_up=accounts_last_made_up,
        ),
        filing=FilingCandidate(
            filing_id=filing_id,
            filing_date=filing_date,
            made_up_date=accounts_last_made_up,
            document_metadata_url="",
        ),
        filing_format=filing_format,
        facts=facts,
    )
    return row.to_json_line(), facts.parse_status


def main() -> int:
    args = _parse_args()
    data_dir = Path(args.data_dir).resolve()
    db_path = Path(args.db_path).resolve()
    batch_id = args.batch_id

    source_path = _source_path(data_dir, batch_id)
    pending_path = staging_path(
        data_dir,
        sync_type=FINANCIALS_SYNC_TYPE,
        batch_id=batch_id,
    )
    rebuild_path = pending_path.with_name(f"{pending_path.name}.rebuild")

    rows: list[dict[str, object]] = []
    with source_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))

    company_numbers = sorted(
        {str(row["company_number"]) for row in rows}
    )
    accounts_dates = _accounts_dates(db_path, company_numbers)

    if rebuild_path.exists():
        rebuild_path.unlink()
    status_counts: Counter[str] = Counter()
    format_counts: Counter[str] = Counter()
    with rebuild_path.open("w", encoding="utf-8") as handle:
        for payload in rows:
            company_number = str(payload["company_number"])
            line, parse_status = _reparse_row(
                payload=payload,
                data_dir=data_dir,
                accounts_last_made_up=accounts_dates.get(company_number),
            )
            handle.write(f"{line}\n")
            status_counts[parse_status] += 1
            format_counts[str(payload["filing_format"])] += 1
        handle.flush()

    if pending_path.exists():
        pending_path.unlink()
    rebuild_path.rename(pending_path)

    print(
        json.dumps(
            {
                "batch_id": batch_id,
                "source_path": str(source_path),
                "pending_path": str(pending_path),
                "row_count": len(rows),
                "format_counts": dict(sorted(format_counts.items())),
                "status_counts": dict(sorted(status_counts.items())),
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
