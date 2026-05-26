#!/usr/bin/env python3
"""Helper for the inline classifier session.

Subcommands:
  auto-fail   Write Unable-to-classify rows for all failure_reason entries
              not yet in the output JSONL.

  next-batch  Print the next N classifiable (non-failure) rows not yet in
              the output JSONL, as a JSON array. Classifier reads these and
              emits verdicts.

  write-batch Read a JSON array of verdict dicts from stdin and append to
              the output JSONL.

  stats       Print counts of prefetch vs output progress.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

CLASSIFIER = "llm:adhoc:claude-sonnet-4-6-classifier-2"


def isoformat_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_prefetch(path: Path) -> list[dict]:
    rows = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def load_done_ids(path: Path) -> set[str]:
    done: set[str] = set()
    if not path.exists():
        return done
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    done.add(str(json.loads(line)["entity_id"]))
                except Exception:
                    pass
    return done


def make_unable_row(prefetch_row: dict, failure_reason: str | None) -> dict:
    return {
        "entity_id": prefetch_row["company_number"],
        "entity_type": "classification",
        "fetched_at": prefetch_row["fetched_at"],
        "http_status": prefetch_row.get("http_status"),
        "raw_json": {
            "verdict": "Unable to classify",
            "evidence": "",
            "classifier": CLASSIFIER,
            "source_url": prefetch_row.get("source_url"),
            "pages_used": prefetch_row.get("pages_used", []),
            "truncated": prefetch_row.get("truncated", False),
            "used_playwright": False,
            "failure_reason": failure_reason,
        },
    }


def cmd_auto_fail(prefetch: Path, output: Path) -> None:
    rows = load_prefetch(prefetch)
    done = load_done_ids(output)
    written = 0
    with output.open("a", encoding="utf-8") as f:
        for row in rows:
            cn = row["company_number"]
            if cn in done:
                continue
            fr = row.get("failure_reason")
            if not fr:
                continue
            result = make_unable_row(row, fr)
            f.write(json.dumps(result, ensure_ascii=True, sort_keys=True) + "\n")
            f.flush()
            done.add(cn)
            written += 1
    print(f"auto-fail: wrote {written} rows", file=sys.stderr)


def cmd_next_batch(prefetch: Path, output: Path, n: int) -> None:
    rows = load_prefetch(prefetch)
    done = load_done_ids(output)
    batch = []
    for row in rows:
        if row["company_number"] in done:
            continue
        if row.get("failure_reason"):
            continue  # handled by auto-fail
        if len(row.get("content") or "") < 200:
            continue
        batch.append({
            "company_number": row["company_number"],
            "company_name": row.get("company_name", ""),
            "source_url": row.get("source_url", ""),
            "fetched_at": row["fetched_at"],
            "http_status": row.get("http_status"),
            "pages_used": row.get("pages_used", []),
            "truncated": row.get("truncated", False),
            "content": row.get("content", ""),
        })
        if len(batch) >= n:
            break
    print(json.dumps(batch, ensure_ascii=False, indent=2))


def cmd_write_batch(output: Path) -> None:
    verdicts = json.load(sys.stdin)
    written = 0
    with output.open("a", encoding="utf-8") as f:
        for v in verdicts:
            row = {
                "entity_id": v["entity_id"],
                "entity_type": "classification",
                "fetched_at": v["fetched_at"],
                "http_status": v.get("http_status"),
                "raw_json": {
                    "verdict": v["verdict"],
                    "evidence": v.get("evidence", ""),
                    "classifier": CLASSIFIER,
                    "source_url": v.get("source_url"),
                    "pages_used": v.get("pages_used", []),
                    "truncated": v.get("truncated", False),
                    "used_playwright": False,
                    "failure_reason": v.get("failure_reason"),
                },
            }
            f.write(json.dumps(row, ensure_ascii=True, sort_keys=True) + "\n")
            f.flush()
            written += 1
    print(f"write-batch: wrote {written} rows", file=sys.stderr)


def cmd_stats(prefetch: Path, output: Path) -> None:
    rows = load_prefetch(prefetch)
    done = load_done_ids(output)
    failures = sum(1 for r in rows if r.get("failure_reason"))
    classifiable = sum(1 for r in rows if not r.get("failure_reason") and len(r.get("content") or "") >= 200)
    classified = len(done)
    print(f"prefetch_rows={len(rows)} failures={failures} classifiable={classifiable} classified={classified} remaining={len(rows)-classified}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prefetch", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("auto-fail")
    nb = sub.add_parser("next-batch")
    nb.add_argument("--n", type=int, default=30)
    sub.add_parser("write-batch")
    sub.add_parser("stats")

    args = parser.parse_args()
    if args.cmd == "auto-fail":
        cmd_auto_fail(args.prefetch, args.output)
    elif args.cmd == "next-batch":
        cmd_next_batch(args.prefetch, args.output, args.n)
    elif args.cmd == "write-batch":
        cmd_write_batch(args.output)
    elif args.cmd == "stats":
        cmd_stats(args.prefetch, args.output)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
