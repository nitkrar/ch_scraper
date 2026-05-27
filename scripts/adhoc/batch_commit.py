#!/usr/bin/env python3
"""Commit a verdicts JSON file to the classifications JSONL.

Usage:
    python adhoc_batch_commit.py /tmp/verdicts.json

Verdicts file: array of {company_number, verdict, evidence, copy_failure_reason}
  copy_failure_reason: bool — if true, copies failure_reason from prefetch row.

Looks up prefetch data, builds JSONL rows, appends to output file.
"""
import json
import sys
from pathlib import Path

CLASSIFIER = "llm:adhoc:claude-sonnet-4-6-classifier-3"
PREFETCH = Path("/Users/nitinkum/Projects/nitkrar/ch_scraper/data/staging/prefetch_a4d4bcab-c2a6-45c7-b5a7-43c14136700b.jsonl")
OUT = Path("/Users/nitinkum/Projects/nitkrar/ch_scraper/data/staging/classifications_a4d4bcab-c2a6-45c7-b5a7-43c14136700b.jsonl")

verdicts_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/tmp/verdicts.json")
verdicts = json.loads(verdicts_path.read_text())

pf = {}
for line in PREFETCH.read_text().splitlines():
    if not line.strip():
        continue
    r = json.loads(line)
    pf[r["company_number"]] = r

rows = []
for v in verdicts:
    cn = v["company_number"]
    p = pf.get(cn)
    if p is None:
        print(f"WARN: {cn} not in prefetch, skipping")
        continue
    failure_reason = p.get("failure_reason") if v.get("copy_failure_reason") else None
    rows.append({
        "entity_id": cn,
        "entity_type": "classification",
        "fetched_at": p["fetched_at"],
        "http_status": p.get("http_status"),
        "raw_json": {
            "classifier": CLASSIFIER,
            "evidence": v.get("evidence", ""),
            "failure_reason": failure_reason,
            "pages_used": p.get("pages_used", []),
            "source_url": p.get("source_url"),
            "truncated": p.get("truncated", False),
            "used_playwright": False,
            "verdict": v["verdict"],
        },
    })

with OUT.open("a", encoding="utf-8") as f:
    for row in rows:
        f.write(json.dumps(row, ensure_ascii=True, sort_keys=True) + "\n")

total = sum(1 for l in OUT.read_text().splitlines() if l.strip())
print(f"committed={len(rows)} total_classified={total}/2753")
