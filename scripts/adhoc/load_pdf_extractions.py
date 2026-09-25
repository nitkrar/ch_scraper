"""Load hand-read PDF financials (JSON list) into company_enrichment.

Converts each extraction into the same staging row shape qwen_ocr_extract.py
emits (filing_format='ocr_pdf'), writes it as a pending financials staging
batch, and loads it through the normal load-staging merge.

Run: python scripts/adhoc/load_pdf_extractions.py /tmp/ciw_pdf_extract_*.json
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from ch_bulk.api import ChBulk
from ch_bulk.companies_house.financials_contracts import FINANCIALS_SYNC_TYPE
from ch_bulk.companies_house.financials_parsers import coerce_employee_count
from ch_bulk.core.paths import DEFAULT_DATA_DIR, DEFAULT_DB_PATH
from ch_bulk.db.bootstrap import ensure_pipeline_schema
from ch_bulk.db.staging import staging_path
from ch_bulk.db.sync_batches import insert_sync_batch

PL_FIELDS = ("revenue", "gross_profit", "profit_before_tax", "profit_after_tax")


def to_row(rec: dict, fetched_at: str, classifier: str) -> dict:
    def num(field):
        v = rec.get(field)
        return float(v) if v not in (None, "") else None

    employees = coerce_employee_count(rec.get("employee_count"))
    has_pl = any(num(f) is not None for f in PL_FIELDS)
    has_any = has_pl or employees is not None or num("net_assets") is not None
    return {
        "company_number": rec["cn"],
        "filing_id": rec["filing_id"],
        "filing_date": None,
        "filing_format": "ocr_pdf",
        "filing_period_start": rec.get("filing_period_start"),
        "filing_period_end": rec.get("filing_period_end"),
        "revenue": num("revenue"),
        "turnover": num("revenue"),
        "employee_count": employees,
        "gross_profit": num("gross_profit"),
        "profit_before_tax": num("profit_before_tax"),
        "profit_after_tax": num("profit_after_tax"),
        "fixed_assets": None,
        "current_assets": None,
        "total_assets": num("total_assets"),
        "net_assets": num("net_assets"),
        "net_current_assets": num("net_current_assets"),
        "filing_age_months": None,
        "parse_status": (
            "ok" if num("revenue") is not None and employees is not None
            else "partial" if has_any else "ocr_extract_error"
        ),
        "parse_failure_reason": None if has_any else "manual_read_no_figures",
        "profit_loss_exempt": bool(rec.get("profit_loss_exempt")) and not has_pl,
        "fetched_at": fetched_at,
        "classifier": classifier,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--classifier", default="claude_pdf_read")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    args = parser.parse_args()

    records = [rec for path in args.inputs for rec in json.loads(path.read_text())]
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = [to_row(rec, fetched_at, args.classifier) for rec in records]

    con = duckdb.connect(str(args.db_path))
    ensure_pipeline_schema(con)
    batch_id = insert_sync_batch(con, sync_type=FINANCIALS_SYNC_TYPE, mode="list")
    con.close()

    out = staging_path(args.data_dir, sync_type=FINANCIALS_SYNC_TYPE, batch_id=batch_id)
    out.write_text("".join(json.dumps(r) + "\n" for r in rows))
    print(f"Staged {len(rows)} rows -> {out}")

    summary = ChBulk(data_dir=args.data_dir, db_path=args.db_path).load_staging(
        sync_type=FINANCIALS_SYNC_TYPE, batch_id=batch_id
    )
    print(summary)


if __name__ == "__main__":
    main()
