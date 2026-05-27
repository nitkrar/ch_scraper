#!/usr/bin/env python3
"""Dump next N unclassified prefetch rows to a temp file for inline classification.

Usage:
    python adhoc_batch_prep.py <n>

Writes /tmp/classify_batch.json — array of {company_number, company_name,
source_url, http_status, failure_reason, content, pages_used, truncated}.
Prints count of rows written and total prefetched/classified.
"""
import json
import sys
from pathlib import Path

PREFETCH = Path("/Users/nitinkum/Projects/nitkrar/ch_scraper/data/staging/prefetch_a4d4bcab-c2a6-45c7-b5a7-43c14136700b.jsonl")
OUT = Path("/Users/nitinkum/Projects/nitkrar/ch_scraper/data/staging/classifications_a4d4bcab-c2a6-45c7-b5a7-43c14136700b.jsonl")
BATCH_FILE = Path("/tmp/classify_batch.json")

n = int(sys.argv[1]) if len(sys.argv) > 1 else 20

done = set()
if OUT.exists():
    for line in OUT.read_text().splitlines():
        if line.strip():
            try:
                done.add(json.loads(line)["entity_id"])
            except Exception:
                pass

batch = []
pf_total = 0
for line in PREFETCH.read_text().splitlines():
    if not line.strip():
        continue
    pf_total += 1
    r = json.loads(line)
    if r["company_number"] not in done and len(batch) < n:
        batch.append({
            "company_number": r["company_number"],
            "company_name": r.get("company_name", ""),
            "source_url": r.get("source_url", ""),
            "http_status": r.get("http_status"),
            "failure_reason": r.get("failure_reason"),
            "truncated": r.get("truncated", False),
            "pages_used": r.get("pages_used", []),
            "content": r.get("content", "")[:800],
        })

BATCH_FILE.write_text(json.dumps(batch, ensure_ascii=False, indent=2))
print(f"batch={len(batch)} prefetched={pf_total}/2753 classified={len(done)}/2753 written_to={BATCH_FILE}")
