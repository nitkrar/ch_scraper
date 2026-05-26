#!/usr/bin/env python3
"""Re-parse saved financial filing artifacts from disk into a fresh staging batch."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb

from ch_bulk.bootstrap import ensure_pipeline_schema
from ch_bulk.financials_enricher import (
    FINANCIALS_SYNC_TYPE,
    FilingCandidate,
    FinancialTarget,
    IXBRL_EXTENSION,
    PDF_EXTENSION,
    _build_row,
    _parse_date,
    _parse_ixbrl_bytes,
    _parse_pdf_bytes,
    _raw_filing_path,
    load_financials_staging,
)
from ch_bulk.staging import StagingWriter, truncate_incomplete_jsonl_tail
from ch_bulk.sync_batches import insert_sync_batch


@dataclass(frozen=True)
class SavedRawCandidate:
    company_number: str
    filing_id: str
    filing_format: str
    raw_path: Path
    filing_date: date | None
    fetched_at: str | None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Re-parse saved .ixbrl/.pdf artifacts under data/staging/filings and "
            "load the latest saved filing per company into company_enrichment."
        )
    )
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--data-dir", required=True)
    return parser.parse_args()


def _candidate_sort_key(candidate: SavedRawCandidate) -> tuple[int, str, int, float, str]:
    filing_date = candidate.filing_date.toordinal() if candidate.filing_date is not None else -1
    fetched_at = candidate.fetched_at or ""
    format_rank = 1 if candidate.filing_format == "ixbrl" else 0
    return (
        filing_date,
        fetched_at,
        format_rank,
        candidate.raw_path.stat().st_mtime,
        candidate.filing_id,
    )


def _iter_metadata_paths(data_dir: Path) -> list[Path]:
    staging_dir = data_dir / "staging"
    paths: list[Path] = []
    for pattern in (
        "financials_*.jsonl",
        "financials_*.jsonl.loaded",
        "financials_fetch_*.jsonl",
        "financials_fetch_*.jsonl.loaded",
    ):
        for path in staging_dir.glob(pattern):
            if path.name.endswith(".rebuild"):
                continue
            paths.append(path)
    return sorted(paths)


def _load_saved_candidates(data_dir: Path) -> tuple[list[SavedRawCandidate], int]:
    by_key: dict[tuple[str, str, str], SavedRawCandidate] = {}
    metadata_rows = 0
    for path in _iter_metadata_paths(data_dir):
        truncate_incomplete_jsonl_tail(path)
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                payload = json.loads(line)
                company_number = str(payload.get("company_number") or "").strip()
                filing_id = str(payload.get("filing_id") or "").strip()
                filing_format = str(payload.get("filing_format") or "").strip().lower()
                if not company_number or not filing_id or filing_format not in {"ixbrl", "pdf"}:
                    continue
                raw_path = _raw_filing_path(
                    data_dir=data_dir,
                    company_number=company_number,
                    filing_id=filing_id,
                    filing_format=filing_format,
                )
                if raw_path is None:
                    raw_path_value = str(payload.get("raw_path") or "").strip()
                    if raw_path_value:
                        candidate_path = Path(raw_path_value)
                        if candidate_path.exists():
                            raw_path = candidate_path
                if raw_path is None:
                    continue
                metadata_rows += 1
                candidate = SavedRawCandidate(
                    company_number=company_number,
                    filing_id=filing_id,
                    filing_format=filing_format,
                    raw_path=raw_path,
                    filing_date=_parse_date(payload.get("filing_date")),
                    fetched_at=str(payload.get("fetched_at") or "").strip() or None,
                )
                key = (company_number, filing_id, filing_format)
                existing = by_key.get(key)
                if existing is None or _candidate_sort_key(candidate) > _candidate_sort_key(existing):
                    by_key[key] = candidate

    for raw_path in data_dir.glob("staging/filings/*/*"):
        if raw_path.suffix not in {f".{IXBRL_EXTENSION}", f".{PDF_EXTENSION}"}:
            continue
        company_number = raw_path.parent.name
        filing_id = raw_path.stem
        filing_format = raw_path.suffix.lstrip(".").lower()
        key = (company_number, filing_id, filing_format)
        if key not in by_key:
            by_key[key] = SavedRawCandidate(
                company_number=company_number,
                filing_id=filing_id,
                filing_format=filing_format,
                raw_path=raw_path,
                filing_date=None,
                fetched_at=None,
            )

    latest_by_company: dict[str, SavedRawCandidate] = {}
    for candidate in by_key.values():
        existing = latest_by_company.get(candidate.company_number)
        if existing is None or _candidate_sort_key(candidate) > _candidate_sort_key(existing):
            latest_by_company[candidate.company_number] = candidate
    return sorted(latest_by_company.values(), key=lambda candidate: candidate.company_number), metadata_rows


def _accounts_dates(db_path: Path, company_numbers: list[str]) -> dict[str, date | None]:
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


def _parse_candidate(
    *,
    candidate: SavedRawCandidate,
    accounts_last_made_up: date | None,
) -> object:
    content = candidate.raw_path.read_bytes()
    if candidate.filing_format == "ixbrl":
        facts = _parse_ixbrl_bytes(content)
    elif candidate.filing_format == "pdf":
        facts = _parse_pdf_bytes(content)
    else:
        raise ValueError(f"Unsupported filing_format={candidate.filing_format!r}")
    row = _build_row(
        target=FinancialTarget(
            company_number=candidate.company_number,
            accounts_last_made_up=accounts_last_made_up,
        ),
        filing=FilingCandidate(
            filing_id=candidate.filing_id,
            filing_date=candidate.filing_date,
            made_up_date=accounts_last_made_up,
            paper_filed=candidate.filing_format == "pdf",
            document_metadata_url="",
        ),
        filing_format=candidate.filing_format,
        facts=facts,
        fetched_at=candidate.fetched_at,
    )
    return row


def main() -> int:
    args = _parse_args()
    db_path = Path(args.db_path).resolve()
    data_dir = Path(args.data_dir).resolve()

    candidates, metadata_rows = _load_saved_candidates(data_dir)
    company_numbers = [candidate.company_number for candidate in candidates]
    accounts_dates = _accounts_dates(db_path, company_numbers)

    con = duckdb.connect(str(db_path))
    try:
        ensure_pipeline_schema(con)
        batch_id = insert_sync_batch(
            con,
            sync_type=FINANCIALS_SYNC_TYPE,
            mode="incremental",
        )
    finally:
        con.close()

    status_counts: Counter[str] = Counter()
    format_counts: Counter[str] = Counter()
    writer = StagingWriter(
        data_dir,
        sync_type=FINANCIALS_SYNC_TYPE,
        batch_id=batch_id,
    )
    try:
        for candidate in candidates:
            row = _parse_candidate(
                candidate=candidate,
                accounts_last_made_up=accounts_dates.get(candidate.company_number),
            )
            payload = json.loads(row.to_json_line())
            writer.append(row)
            status_counts[str(payload["parse_status"])] += 1
            format_counts[str(payload["filing_format"])] += 1
        writer.flush_and_fsync()
    finally:
        writer.close()

    load_summary = load_financials_staging(
        data_dir,
        db_path,
        batch_id=batch_id,
    )
    print(
        json.dumps(
            {
                "batch_id": batch_id,
                "discovered_raw_files": len(
                    [
                        path
                        for path in data_dir.glob("staging/filings/*/*")
                        if path.suffix in {f".{IXBRL_EXTENSION}", f".{PDF_EXTENSION}"}
                    ]
                ),
                "metadata_rows": metadata_rows,
                "selected_companies": len(candidates),
                "format_counts": dict(sorted(format_counts.items())),
                "status_counts": dict(sorted(status_counts.items())),
                "load_summary": load_summary,
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
