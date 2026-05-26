"""One-shot recovery tool for classifier parse_error rows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from ch_bulk.bootstrap import ensure_pipeline_schema
from ch_bulk.classifier import (
    CLASSIFICATION_SYNC_TYPE,
    StagedClassification,
    VALID_VERDICTS,
    _canonical_verdict,
    _extract_json_object,
    insert_classification_batch,
    load_classification_staging,
)
from ch_bulk.staging import with_duckdb_connection

RECOVERY_CLASSIFIER = "repair:parse_error_salvage"
RECOVERY_MODEL_VERSION = "salvage:reparse"


def _batch_stage_paths(data_dir: str | Path, batch_id: str) -> list[Path]:
    stage_dir = Path(data_dir) / "staging"
    patterns = (
        f"{CLASSIFICATION_SYNC_TYPE}_{batch_id}*.jsonl",
        f"{CLASSIFICATION_SYNC_TYPE}_{batch_id}*.jsonl.loaded",
        f"{CLASSIFICATION_SYNC_TYPE}_{batch_id}*.jsonl.open",
    )
    paths: list[Path] = []
    for pattern in patterns:
        paths.extend(stage_dir.glob(pattern))
    return sorted({path.resolve(): path for path in paths}.values())


def _latest_rows_by_company(paths: list[Path]) -> dict[str, StagedClassification]:
    latest: dict[str, tuple[object, StagedClassification]] = {}
    for path in paths:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line:
                    continue
                row = StagedClassification.from_json_line(line)
                fetched_at = row.fetched_at_value()
                previous = latest.get(row.entity_id)
                if previous is None or fetched_at >= previous[0]:
                    latest[row.entity_id] = (fetched_at, row)
    return {entity_id: row for entity_id, (_, row) in latest.items()}


def _recover_row(
    row: StagedClassification,
) -> tuple[StagedClassification | None, dict[str, Any] | None]:
    raw_json = dict(row.raw_json)
    if str(raw_json.get("failure_reason") or "").strip().lower() != "parse_error":
        return None, None
    raw_response = raw_json.get("raw_response")
    if not isinstance(raw_response, str) or not raw_response.strip():
        return None, None
    parsed = _extract_json_object(raw_response)
    verdict = _canonical_verdict((parsed or {}).get("verdict"))
    if verdict not in VALID_VERDICTS:
        return None, None
    evidence = str((parsed or {}).get("evidence") or "").strip()
    raw_json["verdict"] = verdict
    raw_json["evidence"] = evidence
    raw_json["failure_reason"] = None
    raw_json["error"] = None
    return (
        StagedClassification(
            entity_id=row.entity_id,
            entity_type=row.entity_type,
            fetched_at=row.fetched_at,
            http_status=row.http_status,
            raw_json=raw_json,
        ),
        {
            "entity_id": row.entity_id,
            "raw_response": raw_response,
            "parsed_verdict": verdict,
            "parsed_evidence": evidence,
        },
    )


def build_recovery_report(
    *,
    data_dir: str | Path,
    batch_id: str,
    sample_limit: int = 5,
) -> tuple[dict[str, Any], list[StagedClassification]]:
    paths = _batch_stage_paths(data_dir, batch_id)
    latest_rows = _latest_rows_by_company(paths)
    parse_error_count = 0
    recovered_rows: list[StagedClassification] = []
    recovered_samples: list[dict[str, Any]] = []
    unrecovered_examples: list[dict[str, Any]] = []

    for entity_id in sorted(latest_rows):
        row = latest_rows[entity_id]
        raw_json = row.raw_json
        if str(raw_json.get("failure_reason") or "").strip().lower() != "parse_error":
            continue
        parse_error_count += 1
        recovered, sample = _recover_row(row)
        if recovered is None:
            if len(unrecovered_examples) < sample_limit:
                unrecovered_examples.append(
                    {
                        "entity_id": row.entity_id,
                        "raw_response": raw_json.get("raw_response"),
                        "error": raw_json.get("error"),
                    }
                )
            continue
        recovered_rows.append(recovered)
        if len(recovered_samples) < sample_limit and sample is not None:
            recovered_samples.append(sample)

    report = {
        "batch_id": batch_id,
        "stage_file_count": len(paths),
        "latest_row_count": len(latest_rows),
        "parse_error_count": parse_error_count,
        "recoverable_count": len(recovered_rows),
        "remaining_parse_error_count": parse_error_count - len(recovered_rows),
        "sample_recovered": recovered_samples,
        "sample_unrecovered": unrecovered_examples,
        "stage_files": [str(path) for path in paths],
    }
    return report, recovered_rows


def _create_recovery_batch(db_path: str | Path, input_count: int) -> str:
    def create(con):
        ensure_pipeline_schema(con)
        return insert_classification_batch(
            con,
            classifier=RECOVERY_CLASSIFIER,
            source_type="website",
            input_count=input_count,
            model_version=RECOVERY_MODEL_VERSION,
        )

    return with_duckdb_connection(db_path, create)


def apply_recovery(
    *,
    data_dir: str | Path,
    db_path: str | Path,
    recovered_rows: list[StagedClassification],
) -> dict[str, Any]:
    if not recovered_rows:
        return {
            "repair_batch_id": None,
            "repair_staging_path": None,
            "load_summary": {
                "records_fetched": 0,
                "records_updated": 0,
                "unable_count": 0,
                "error_count": 0,
                "loaded_paths": [],
                "batch_totals": [],
            },
        }
    repair_batch_id = _create_recovery_batch(db_path, len(recovered_rows))
    stage_dir = Path(data_dir) / "staging"
    stage_dir.mkdir(parents=True, exist_ok=True)
    repair_path = stage_dir / f"{CLASSIFICATION_SYNC_TYPE}_{repair_batch_id}.jsonl"
    with repair_path.open("w", encoding="utf-8") as handle:
        for row in recovered_rows:
            handle.write(f"{row.to_json_line()}\n")
    load_summary = load_classification_staging(
        data_dir,
        db_path,
        batch_id=repair_batch_id,
    )
    return {
        "repair_batch_id": repair_batch_id,
        "repair_staging_path": str(repair_path),
        "load_summary": load_summary,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Recover classifier parse_error rows from staged raw responses."
    )
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--db-path", default="data/db/ch_bulk.duckdb")
    parser.add_argument("--sample-limit", type=int, default=5)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write recovered rows to a repair staging file and bulk-load them.",
    )
    args = parser.parse_args()

    report, recovered_rows = build_recovery_report(
        data_dir=args.data_dir,
        batch_id=args.batch_id,
        sample_limit=max(1, args.sample_limit),
    )
    if args.apply:
        report["apply"] = apply_recovery(
            data_dir=args.data_dir,
            db_path=args.db_path,
            recovered_rows=recovered_rows,
        )
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
