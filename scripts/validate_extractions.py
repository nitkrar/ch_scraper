#!/usr/bin/env python3
"""Sense-check extraction JSONLs (Qwen + Opus) using statistical outlier detection.

Produces a CSV report ranked by suspicion score. Does NOT modify any data.

Signals collected per row:
  - Hard structural impossibilities (impossible regardless of distribution):
      * total_assets < net_assets   (accounting identity violation)
      * period_start >= period_end  (date ordering)
      * employee_count < 0          (impossible)
      * gross_profit > revenue      (impossible)
      * profit_before_tax > revenue (very rare; flag)
      * period length not in [180, 545] days  (way off 12-month standard)

  - Distributional outliers (log-MAD robust z-score, threshold 3.5):
      For each of: revenue, employee_count, total_assets, net_assets,
                   gross_profit, profit_after_tax
      Flag rows where log-transformed value is >3.5 MAD-units from median.

  - Ratio outliers:
      revenue / employee_count   (UK homecare typical band)
      revenue / total_assets     (turnover-to-assets ratio)
      profit_after_tax / revenue (margin)
      Each flagged via log-MAD on the ratio.

Output: data/staging/extraction_validation_report.csv
        Per-row signals + cumulative suspicion score, sorted desc.
Also prints distribution stats so user can sanity-check what "normal" looks like.

Usage:
  python scripts/validate_extractions.py \\
      --inputs data/staging/extraction_merged_annotated.jsonl \\
               data/staging/extraction_qwen_round2_annotated.jsonl \\
               data/staging/extraction_qwen_round3.jsonl
"""
from __future__ import annotations
import argparse
import csv
import json
import math
import statistics
import sys
from datetime import date
from pathlib import Path

# Severity weights for cumulative suspicion score
W_STRUCTURAL = 10  # impossible accounting / dates
W_FIELD_OUTLIER = 2   # single-field log-MAD outlier
W_RATIO_OUTLIER = 4   # cross-field ratio outlier (stronger signal)

MAD_THRESHOLD = 3.5  # robust z-score; ~equivalent to 4σ in normal world


def safe_log(v):
    if v is None or v <= 0:
        return None
    try:
        return math.log(v)
    except (TypeError, ValueError):
        return None


def log_mad_stats(values: list[float]) -> tuple[float, float, int]:
    """Returns (median_log, mad_log, n) from positive numeric values."""
    logs = [math.log(v) for v in values if v is not None and v > 0]
    if len(logs) < 8:
        return (float("nan"), float("nan"), len(logs))
    med = statistics.median(logs)
    abs_dev = [abs(x - med) for x in logs]
    mad = statistics.median(abs_dev) or 1e-9
    # 1.4826 makes MAD comparable to stddev under normality
    return (med, mad * 1.4826, len(logs))


def parse_date(s):
    if not s or not isinstance(s, str):
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def percentile(sorted_vals, p):
    if not sorted_vals:
        return None
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)


def describe(name, vals, currency=False):
    pos = sorted(v for v in vals if v is not None and not (isinstance(v, float) and math.isnan(v)))
    if not pos:
        return f"  {name:<24} (no data)"
    fmt = lambda v: (f"£{v:,.0f}" if currency else f"{v:,.1f}")
    return (f"  {name:<24} n={len(pos):>5,}  "
            f"min={fmt(pos[0])}  p5={fmt(percentile(pos,5))}  "
            f"median={fmt(percentile(pos,50))}  p95={fmt(percentile(pos,95))}  "
            f"p99={fmt(percentile(pos,99))}  max={fmt(pos[-1])}")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--inputs", nargs="+", required=True)
    p.add_argument("--out", default="data/staging/extraction_validation_report.csv")
    args = p.parse_args()

    rows = []
    for fp in args.inputs:
        f = Path(fp)
        if not f.exists():
            print(f"WARN: missing {f}", file=sys.stderr)
            continue
        src = f.name
        for ln in f.open():
            ln = ln.rstrip("\n")
            if not ln:
                continue
            try:
                d = json.loads(ln)
            except json.JSONDecodeError:
                continue
            d["__source"] = src
            rows.append(d)
    print(f"Loaded {len(rows):,} rows from {len(args.inputs)} files")

    NUMERIC_FIELDS = (
        "revenue", "employee_count", "total_assets", "net_assets",
        "gross_profit", "profit_before_tax", "profit_after_tax",
        "net_current_assets",
    )

    # Compute log-MAD stats per field
    print("\n" + "=" * 70)
    print("DISTRIBUTION STATISTICS (positive values only)")
    print("=" * 70)
    field_stats = {}
    for f in NUMERIC_FIELDS:
        vals = [r.get(f) for r in rows]
        med, mad, n = log_mad_stats([v for v in vals if v is not None and v > 0])
        field_stats[f] = (med, mad, n)
        print(describe(f, vals, currency=(f != "employee_count")))

    # Compute ratios
    rev_per_emp = []
    rev_per_assets = []
    margin = []
    for r in rows:
        rev, emp = r.get("revenue"), r.get("employee_count")
        ta = r.get("total_assets")
        pat = r.get("profit_after_tax")
        if rev and emp and emp > 0 and rev > 0:
            rev_per_emp.append(rev / emp)
        if rev and ta and ta > 0 and rev > 0:
            rev_per_assets.append(rev / ta)
        if rev and pat is not None and rev > 0:
            # Margin can be negative, store separately
            margin.append(pat / rev)

    print("\nDERIVED RATIOS:")
    print(describe("revenue / employee", rev_per_emp, currency=True))
    print(describe("revenue / total_assets", rev_per_assets))
    if margin:
        margin_sorted = sorted(margin)
        print(f"  {'pat / revenue (margin)':<24} n={len(margin):>5,}  "
              f"min={margin_sorted[0]:>6.1%}  p5={percentile(margin_sorted,5):>6.1%}  "
              f"median={percentile(margin_sorted,50):>6.1%}  p95={percentile(margin_sorted,95):>6.1%}  "
              f"p99={percentile(margin_sorted,99):>6.1%}  max={margin_sorted[-1]:>6.1%}")

    # Ratio MAD stats
    rev_emp_med, rev_emp_mad, _ = log_mad_stats(rev_per_emp)
    rev_ta_med, rev_ta_mad, _ = log_mad_stats(rev_per_assets)

    # Now classify each row
    print("\n" + "=" * 70)
    print(f"SCORING ROWS (W_struct={W_STRUCTURAL}, W_field={W_FIELD_OUTLIER}, W_ratio={W_RATIO_OUTLIER}; MAD threshold={MAD_THRESHOLD})")
    print("=" * 70)

    out_rows = []
    cnt_struct = 0
    cnt_field = 0
    cnt_ratio = 0
    for r in rows:
        sigs = []
        score = 0
        rev = r.get("revenue")
        emp = r.get("employee_count")
        ta = r.get("total_assets")
        na = r.get("net_assets")
        gp = r.get("gross_profit")
        pbt = r.get("profit_before_tax")
        pat = r.get("profit_after_tax")
        ps = parse_date(r.get("filing_period_start"))
        pe = parse_date(r.get("filing_period_end"))

        # Structural hard checks
        if ta is not None and na is not None and ta < na:
            sigs.append("ta<na"); score += W_STRUCTURAL
        if ps and pe and ps >= pe:
            sigs.append("ps>=pe"); score += W_STRUCTURAL
        if emp is not None and emp < 0:
            sigs.append("emp<0"); score += W_STRUCTURAL
        if rev is not None and gp is not None and gp > rev:
            sigs.append("gp>rev"); score += W_STRUCTURAL
        if rev is not None and pbt is not None and pbt > rev:
            sigs.append("pbt>rev"); score += W_STRUCTURAL
        if ps and pe and ps < pe:
            days = (pe - ps).days
            if days < 180 or days > 545:
                sigs.append(f"period={days}d"); score += W_STRUCTURAL

        # Absolute reasonability checks (distribution-independent — catches systematic ×1000 errors
        # where median/MAD get inflated by the bad data itself).
        # Largest UK private homecare operator (HC-One) ~£430M turnover; £1B is exceptional.
        if rev is not None and rev > 1_000_000_000:
            sigs.append(f"rev>£1B={rev:,.0f}"); score += W_STRUCTURAL
        # UK homecare revenue/employee almost always £20K-£200K. Anything ≥£1M/employee with >5 staff
        # is structurally impossible — caught Care UK NORTH LONDON £14B/333emp = £43M/emp.
        if rev is not None and emp is not None and emp > 5 and rev > 0:
            rpe_abs = rev / emp
            if rpe_abs > 1_000_000:
                sigs.append(f"rpe_abs>£1M={rpe_abs:,.0f}"); score += W_STRUCTURAL
        # Major UK operators with >£100M revenue have >500 employees. Mismatch is a red flag.
        if rev is not None and rev > 100_000_000 and emp is not None and 0 < emp < 200:
            sigs.append(f"big_rev_few_emp={rev:,.0f}/{emp}"); score += W_STRUCTURAL
        # Service business revenue rarely exceeds 5× total_assets. >10× is almost always a unit error
        # (rev inflated ×1000 while total_assets wasn't, or vice versa).
        if rev is not None and ta is not None and ta > 0 and rev > 10 * ta:
            sigs.append(f"rev>10×ta={rev/ta:.1f}x"); score += W_STRUCTURAL

        if sigs:
            cnt_struct += 1

        # Field-level log-MAD outliers
        for f in NUMERIC_FIELDS:
            v = r.get(f)
            med, mad, _ = field_stats[f]
            if v is None or v <= 0 or math.isnan(med):
                continue
            z = abs(math.log(v) - med) / mad
            if z > MAD_THRESHOLD:
                sigs.append(f"{f}_z={z:.1f}"); score += W_FIELD_OUTLIER
                cnt_field += 1

        # Ratio outliers
        if rev and emp and emp > 0 and rev > 0:
            r_re = rev / emp
            if not math.isnan(rev_emp_med):
                z = abs(math.log(r_re) - rev_emp_med) / rev_emp_mad
                if z > MAD_THRESHOLD:
                    sigs.append(f"rev_per_emp_z={z:.1f}_val={r_re:,.0f}"); score += W_RATIO_OUTLIER
                    cnt_ratio += 1
        if rev and ta and ta > 0 and rev > 0:
            r_ra = rev / ta
            if not math.isnan(rev_ta_med):
                z = abs(math.log(r_ra) - rev_ta_med) / rev_ta_mad
                if z > MAD_THRESHOLD:
                    sigs.append(f"rev_per_ta_z={z:.1f}_val={r_ra:.1f}"); score += W_RATIO_OUTLIER
                    cnt_ratio += 1

        out_rows.append({
            "company_number": r.get("company_number"),
            "filing_id": r.get("filing_id"),
            "source_file": r["__source"],
            "classifier": r.get("classifier"),
            "suspicion_score": score,
            "signals": ";".join(sigs),
            "n_signals": len(sigs),
            "revenue": rev,
            "employee_count": emp,
            "total_assets": ta,
            "net_assets": na,
            "gross_profit": gp,
            "profit_before_tax": pbt,
            "profit_after_tax": pat,
            "rev_per_emp": (rev/emp if rev and emp and emp > 0 else None),
            "period_start": r.get("filing_period_start"),
            "period_end": r.get("filing_period_end"),
            "parse_status": r.get("parse_status"),
            "profit_loss_exempt": r.get("profit_loss_exempt"),
        })

    out_rows.sort(key=lambda x: x["suspicion_score"], reverse=True)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)

    # Summary
    print()
    print(f"Total rows scored:                {len(rows):,}")
    print(f"  with structural impossibility:  {cnt_struct:,}")
    print(f"  with at least one field outlier: {cnt_field:,}  (note: each row can have multiple)")
    print(f"  with ratio outlier:              {cnt_ratio:,}")
    flagged = sum(1 for r in out_rows if r["n_signals"] > 0)
    print(f"  total with ANY signal:           {flagged:,}  ({100*flagged/len(rows):.1f}%)")
    print()
    print(f"Suspicion-score distribution:")
    scores = sorted([r["suspicion_score"] for r in out_rows], reverse=True)
    score_buckets = {0: 0, 1: 0, 5: 0, 10: 0, 15: 0, 20: 0}
    for s in scores:
        if s == 0: score_buckets[0] += 1
        elif s < 5: score_buckets[1] += 1
        elif s < 10: score_buckets[5] += 1
        elif s < 15: score_buckets[10] += 1
        elif s < 20: score_buckets[15] += 1
        else: score_buckets[20] += 1
    for k, v in score_buckets.items():
        bucket = {0: "0 (clean)", 1: "1-4 (1 field outlier)", 5: "5-9", 10: "10-14 (1 struct)",
                  15: "15-19", 20: ">=20 (multi-struct or many outliers)"}[k]
        print(f"  score {bucket:<40} {v:>6,}")
    print()
    print(f"TOP 15 most suspicious rows:")
    for r in out_rows[:15]:
        print(f"  cn={r['company_number']:<10} score={r['suspicion_score']:>3} "
              f"rev={r['revenue']} emp={r['employee_count']} ta={r['total_assets']} na={r['net_assets']} "
              f"sigs=[{r['signals']}]")
    print(f"\nReport written: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
