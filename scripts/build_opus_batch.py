#!/usr/bin/env python3
"""Build an opus extraction batch JSON file.

Two modes:
  next   — pick the next N undone PDFs from the snapshot (skips .opus.done + .qwen.done)
  cns    — build a batch from an explicit cn list (skips conflict check)

Examples:
  python scripts/build_opus_batch.py --mode next --label L --size 10 \
      --snapshot /tmp/extraction_cn_list.json \
      --out /tmp/opus_batches/batch_L.json \
      --exclude /tmp/opus_batches/batch_K.json

  python scripts/build_opus_batch.py --mode cns --label X \
      --snapshot /tmp/extraction_cn_list.json \
      --out /tmp/opus_batches/batch_X.json \
      --cns 02994396,02992723,02969357
"""
from __future__ import annotations
import argparse, json, os, sys

def load_snapshot(path: str) -> list[dict]:
    with open(path) as f:
        return json.load(f)

def is_done(rec: dict) -> bool:
    dirp = os.path.dirname(rec['filtered_path'])
    fid = rec['filing_id']
    return os.path.exists(f"{dirp}/{fid}.opus.done") or os.path.exists(f"{dirp}/{fid}.qwen.done")

def mode_next(snap: list[dict], size: int, excludes: list[str]) -> list[dict]:
    exclude_cns: set[str] = set()
    for ex in excludes:
        for r in json.load(open(ex)):
            exclude_cns.add(r['cn'])
    todo = [r for r in snap if not is_done(r) and r['cn'] not in exclude_cns]
    todo.sort(key=lambda r: r['cn'])
    return todo[:size]

def mode_cns(snap: list[dict], cns: list[str]) -> list[dict]:
    by_cn: dict[str, dict] = {}
    for r in snap:
        # If a cn appears twice in snapshot (multiple filings), prefer first
        by_cn.setdefault(r['cn'], r)
    out = []
    missing = []
    for cn in cns:
        cn = cn.strip()
        if cn in by_cn:
            out.append(by_cn[cn])
        else:
            missing.append(cn)
    if missing:
        print(f"WARN: cns not in snapshot: {missing}", file=sys.stderr)
    return out

def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument('--mode', choices=['next', 'cns'], required=True)
    p.add_argument('--label', required=True, help='batch label (A, B, X, etc.)')
    p.add_argument('--snapshot', default='/tmp/extraction_cn_list.json')
    p.add_argument('--out', required=True)
    p.add_argument('--size', type=int, default=10, help='for --mode next')
    p.add_argument('--exclude', action='append', default=[],
                   help='batch JSON to exclude cns from (repeatable, for --mode next)')
    p.add_argument('--cns', help='comma-separated cns for --mode cns')
    args = p.parse_args()

    snap = load_snapshot(args.snapshot)
    if args.mode == 'next':
        batch = mode_next(snap, args.size, args.exclude)
    else:
        if not args.cns:
            p.error('--mode cns requires --cns')
        batch = mode_cns(snap, args.cns.split(','))

    if not batch:
        print(f"ERROR: empty batch for label {args.label}", file=sys.stderr)
        return 1

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, 'w') as f:
        json.dump(batch, f, indent=2)
    print(f"batch_{args.label}: {len(batch)} records -> {args.out}")
    print(f"cns: {[r['cn'] for r in batch]}")
    total_bytes = sum(r.get('filtered_size', 0) for r in batch)
    print(f"filtered_size total: {total_bytes:,} bytes")
    return 0

if __name__ == '__main__':
    sys.exit(main())
