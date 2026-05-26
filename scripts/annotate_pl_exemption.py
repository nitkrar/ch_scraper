#!/usr/bin/env python3
"""Annotate an extraction JSONL with profit_loss_exempt boolean.

For each row, runs tight s444/445A / "directors have elected not to deliver"
regex patterns against the full <filing_id>.ocr.txt file (NOT the filtered
version — page filter sometimes drops the cover/notes page where the marker
appears). Adds `profit_loss_exempt: true|false` field, writes new JSONL.

The boolean follows the iXBRL pipeline's intent — it marks rows where the
filing explicitly invokes the small-co P&L exemption, so the loader can
distinguish "legitimately missing revenue" from "extraction miss".

Defaults to in-place edit. Pass --out PATH for a new file.

Usage:
  python scripts/annotate_pl_exemption.py \\
      --input data/staging/extraction_merged.jsonl \\
      --staging-dir data/staging/filings
"""
from __future__ import annotations
import argparse
import json
import re
import sys
from pathlib import Path

# Tight patterns derived from ch_bulk/financials_enricher.py
# IXBRL_PROFIT_LOSS_EXEMPTION_PATTERNS, MINUS the loose "small companies regime"
# match (which fires on boilerplate FRS 102 1A compliance language).
EXEMPTION_PATTERNS = (
    re.compile(r"statementthatdirectorshaveelectednottodeliverprofitlossaccount", re.IGNORECASE),
    re.compile(r"profit\s*(?:and|&)?\s*loss\s+account\s+has\s+not\s+been\s+delivered", re.IGNORECASE),
    re.compile(r"statement\s+of\s+income(?:\s+and\s+retained\s+earnings)?\s+has\s+not\s+been\s+delivered", re.IGNORECASE),
    re.compile(r"directors\s+have\s+elected\s+not\s+to\s+deliver", re.IGNORECASE),
    re.compile(r"section\s*444\s*(?:\(a\)|a)?\s+of\s+the\s+companies\s+act", re.IGNORECASE),
    re.compile(r"section\s*444\s*5a", re.IGNORECASE),
)


def is_exempt(text: str) -> bool:
    return any(p.search(text) for p in EXEMPTION_PATTERNS)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="JSONL to annotate")
    p.add_argument("--staging-dir", default="data/staging/filings",
                   help="Where to look for {cn}/{filing_id}.ocr.txt")
    p.add_argument("--out", help="Output path (default: in-place)")
    args = p.parse_args()

    inp = Path(args.input)
    staging = Path(args.staging_dir)
    if not inp.exists():
        print(f"ERROR: input not found: {inp}", file=sys.stderr)
        return 2
    if not staging.exists():
        print(f"ERROR: staging dir not found: {staging}", file=sys.stderr)
        return 2

    out = Path(args.out) if args.out else inp
    tmp = out.with_suffix(out.suffix + ".tmp")

    in_count = exempt_count = ocr_missing = 0
    overrides_to_false = 0
    with inp.open() as fh_in, tmp.open("w") as fh_out:
        for ln in fh_in:
            ln = ln.rstrip("\n")
            if not ln:
                continue
            d = json.loads(ln)
            in_count += 1
            cn = d.get("company_number")
            fid = d.get("filing_id")
            ocr = staging / cn / f"{fid}.ocr.txt"
            if not ocr.exists():
                d["profit_loss_exempt"] = False
                ocr_missing += 1
            else:
                text = ocr.read_text(errors="ignore")
                regex_hit = is_exempt(text)
                # Defensive: if regex matched but row HAS revenue, the marker
                # is boilerplate not actual exemption — override to False.
                has_pl_data = any(
                    d.get(f) is not None
                    for f in ("revenue", "gross_profit", "profit_before_tax", "profit_after_tax")
                )
                if regex_hit and has_pl_data:
                    d["profit_loss_exempt"] = False
                    overrides_to_false += 1
                else:
                    d["profit_loss_exempt"] = bool(regex_hit)
            if d["profit_loss_exempt"]:
                exempt_count += 1
            fh_out.write(json.dumps(d) + "\n")

    tmp.replace(out)
    print(f"rows processed:                {in_count}")
    print(f"  profit_loss_exempt=True:    {exempt_count}")
    print(f"  ocr.txt missing (set False): {ocr_missing}")
    print(f"  regex-hit-but-has-revenue overridden to False: {overrides_to_false}")
    print(f"wrote: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
