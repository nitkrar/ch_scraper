"""Qwen-based OCR extraction worker.

Reads /tmp/extraction_cn_list.json (snapshot list of PDFs with sidecars).
Iterates in DESCENDING company_number order.
For each PDF:
  - skip if {filing_id}.qwen.done OR {filing_id}.opus.done exists
  - read {filing_id}.filtered.txt
  - POST to localhost:9741 (Qwen v2 prompt)
  - parse JSON response, normalize
  - append to data/staging/extraction_qwen.jsonl with iXBRL-style schema
  - touch {filing_id}.qwen.done
  - signal claude-nitin every 100 PDFs (broker chat to req-0076)

Uses --concurrency to fire multiple llama-server calls in parallel
(server has 2 slots configured; default --concurrency 2).
Idempotent + resumable.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests

LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("qwen_extract")

QWEN_URL = "http://localhost:9741/v1/chat/completions"
QWEN_MODEL_NAME = "qwen2.5-14b-instruct"
QWEN_MAX_TOKENS = 1200
QWEN_TIMEOUT_SECS = 300

PROMPT_PATH = Path("/tmp/extraction_prompt_v2.txt")
SNAPSHOT_PATH = Path("/tmp/extraction_cn_list.json")
OUTPUT_JSONL = Path("/Users/nitinkum/Projects/nitkrar/ch_scraper/data/staging/extraction_qwen.jsonl")
STAGING_DIR_DEFAULT = Path("/Users/nitinkum/Projects/nitkrar/ch_scraper/data/staging/filings")
SIGNAL_EVERY = 100


def load_prompt() -> str:
    return PROMPT_PATH.read_text(encoding="utf-8")


def scan_staging(staging_dir: Path) -> list[dict]:
    """Walk staging filings dir, return all extractable PDFs not yet processed.

    Skips PDFs where .opus.done or .qwen.done exists, or where .filtered.txt
    is missing or empty. Returns records matching the snapshot schema so the
    downstream worker code is unchanged.
    """
    out: list[dict] = []
    for cn_dir in staging_dir.iterdir():
        if not cn_dir.is_dir():
            continue
        cn = cn_dir.name
        for pdf in cn_dir.glob("*.pdf"):
            filing_id = pdf.stem
            filtered = cn_dir / f"{filing_id}.filtered.txt"
            if not filtered.exists():
                continue
            if (cn_dir / f"{filing_id}.opus.done").exists():
                continue
            if (cn_dir / f"{filing_id}.qwen.done").exists():
                continue
            try:
                sz = filtered.stat().st_size
            except OSError:
                continue
            if sz == 0:
                continue
            out.append({
                "cn": cn,
                "filing_id": filing_id,
                "pdf_path": str(pdf),
                "filtered_path": str(filtered),
                "filtered_size": sz,
            })
    return out


def signal_broker(text: str) -> None:
    try:
        subprocess.run(
            ["agent-broker", "rooms", "send", "--room", "side:req-0076",
             "--from", "claude-nitin", "--text", text],
            capture_output=True, timeout=15, check=False,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("broker signal failed: %s", exc)


def call_qwen(prompt: str, filtered_text: str) -> tuple[dict | None, str, float]:
    """Return (parsed_dict, raw_response, elapsed_seconds)."""
    payload = {
        "model": "qwen",
        "messages": [{"role": "user", "content": prompt + filtered_text + "\n---"}],
        "max_tokens": QWEN_MAX_TOKENS,
        "temperature": 0,
    }
    t0 = time.time()
    resp = requests.post(QWEN_URL, json=payload, timeout=QWEN_TIMEOUT_SECS)
    elapsed = time.time() - t0
    resp.raise_for_status()
    raw = resp.json()["choices"][0]["message"]["content"]
    parsed: dict | None = None
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if m:
        try:
            parsed = json.loads(m.group(0))
        except json.JSONDecodeError:
            parsed = None
    return parsed, raw, elapsed


def build_jsonl_row(rec: dict, parsed: dict | None, raw: str, source: str, period_iso: str | None) -> dict:
    """Match the iXBRL-style staging row schema used by load-staging financials.

    Each row goes into a financials staging JSONL. The loader pulls the per-field
    values from raw_json. Source attribution: classifier-name field tags this
    extraction so we can distinguish in DB later.
    """
    cn = rec["cn"]
    filing_id = rec["filing_id"]
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    # Extract fields with defensive defaults
    def num(field):
        if not parsed:
            return None
        v = parsed.get(field)
        if v is None or v == "" or (isinstance(v, str) and not v.strip()):
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    def int_field(field):
        v = num(field)
        return int(v) if v is not None else None

    def date_field(field):
        if not parsed:
            return None
        v = parsed.get(field)
        if not v or not isinstance(v, str):
            return None
        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            return None

    def bool_field(field):
        if not parsed:
            return False
        v = parsed.get(field)
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.strip().lower() in ("true", "yes", "1")
        return False

    # profit_loss_exempt: trust Qwen's boolean, but sanity-override to False
    # if any P&L field is actually present (Qwen sometimes flags exempt on
    # boilerplate "small companies regime" language even when revenue is filed).
    qwen_exempt = bool_field("profit_loss_exempt")
    has_pl_data = any(
        num(f) is not None
        for f in ("revenue", "gross_profit", "profit_before_tax", "profit_after_tax")
    )
    profit_loss_exempt = qwen_exempt and not has_pl_data

    row = {
        "company_number": cn,
        "filing_id": filing_id,
        "filing_date": None,
        "filing_format": "ocr_pdf",
        "filing_period_start": date_field("filing_period_start"),
        "filing_period_end": date_field("filing_period_end"),
        "revenue": num("revenue"),
        "turnover": num("revenue"),
        "employee_count": int_field("employee_count"),
        "gross_profit": num("gross_profit"),
        "profit_before_tax": num("profit_before_tax"),
        "profit_after_tax": num("profit_after_tax"),
        "fixed_assets": None,
        "current_assets": None,
        "total_assets": num("total_assets"),
        "net_assets": num("net_assets"),
        "net_current_assets": num("net_current_assets"),
        "filing_age_months": None,
        "parse_status": "ok" if (parsed and num("revenue") is not None and int_field("employee_count") is not None) else (
            "partial" if parsed else "ocr_extract_error"
        ),
        "parse_failure_reason": None if parsed else "ocr_extract_parse_failed",
        "profit_loss_exempt": profit_loss_exempt,
        "fetched_at": now_iso,
        "classifier": source,
    }
    return row


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=0, help="0 = unlimited")
    p.add_argument("--source", default="llm:qwen-ocr-extract")
    p.add_argument("--concurrency", type=int, default=2,
                   help="Parallel llama-server calls; matches --parallel slots (default 2)")
    p.add_argument("--snapshot", default=str(SNAPSHOT_PATH),
                   help=f"Snapshot JSON path (default {SNAPSHOT_PATH}); ignored if --scan-staging is set")
    p.add_argument("--scan-staging", action="store_true",
                   help="Auto-discover pending PDFs by walking --staging-dir; bypasses --snapshot")
    p.add_argument("--staging-dir", default=str(STAGING_DIR_DEFAULT),
                   help=f"Staging filings dir for --scan-staging (default {STAGING_DIR_DEFAULT})")
    p.add_argument("--output", default=str(OUTPUT_JSONL),
                   help=f"Output JSONL path (default {OUTPUT_JSONL})")
    args = p.parse_args()

    output_path = Path(args.output)

    if not PROMPT_PATH.exists():
        logger.error("prompt not found: %s", PROMPT_PATH)
        return 2

    prompt = load_prompt()

    if args.scan_staging:
        staging_dir = Path(args.staging_dir)
        if not staging_dir.exists():
            logger.error("staging dir not found: %s", staging_dir)
            return 2
        logger.info("Scanning staging dir for pending PDFs: %s", staging_dir)
        records = scan_staging(staging_dir)
        logger.info("Scan complete: %d pending records found", len(records))
    else:
        snapshot_path = Path(args.snapshot)
        if not snapshot_path.exists():
            logger.error("snapshot list not found: %s — run the snapshot builder first or use --scan-staging", snapshot_path)
            return 2
        records = json.loads(snapshot_path.read_text())
    # DESCENDING by cn — Qwen direction
    records.sort(key=lambda r: r["cn"], reverse=True)

    if args.limit > 0:
        records = records[: args.limit]
    logger.info("Qwen extraction starting: %d candidates (descending order) concurrency=%d",
                len(records), args.concurrency)
    logger.info("Output JSONL: %s", output_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    state_lock = threading.Lock()
    write_lock = threading.Lock()
    state = {"processed": 0, "skipped": 0, "errors": 0, "last_signal_at": 0}
    started_at = time.time()

    out_fh = output_path.open("a", encoding="utf-8")

    def process_one(rec: dict) -> str:
        cn = rec["cn"]
        filing_id = rec["filing_id"]
        filing_dir = Path(rec["filtered_path"]).parent
        qwen_done = filing_dir / f"{filing_id}.qwen.done"
        opus_done = filing_dir / f"{filing_id}.opus.done"
        if qwen_done.exists() or opus_done.exists():
            with state_lock:
                state["skipped"] += 1
            return "skipped"

        filt_path = Path(rec["filtered_path"])
        try:
            text = filt_path.read_text(encoding="utf-8")
        except Exception as exc:  # noqa: BLE001
            logger.warning("cn=%s could not read sidecar: %s", cn, exc)
            with state_lock:
                state["errors"] += 1
            return "read_error"

        if len(text) > 13000:
            text = text[:13000]

        try:
            parsed, raw, elapsed = call_qwen(prompt, text)
        except Exception as exc:  # noqa: BLE001
            logger.warning("cn=%s qwen call failed: %s", cn, exc)
            with state_lock:
                state["errors"] += 1
            return "call_error"

        row = build_jsonl_row(rec, parsed, raw, args.source, None)
        line = json.dumps(row, ensure_ascii=False) + "\n"
        with write_lock:
            out_fh.write(line)
            out_fh.flush()
        qwen_done.touch()

        with state_lock:
            state["processed"] += 1
            processed_now = state["processed"]
            if processed_now % 10 == 0:
                rate = processed_now / max(time.time() - started_at, 1)
                logger.info(
                    "qwen processed=%d skipped=%d errors=%d rate=%.2f/sec (last cn=%s elapsed=%.1fs)",
                    processed_now, state["skipped"], state["errors"], rate, cn, elapsed,
                )
            if processed_now - state["last_signal_at"] >= SIGNAL_EVERY:
                state["last_signal_at"] = processed_now
                signal_now = True
            else:
                signal_now = False
        if signal_now:
            signal_broker(
                f"[claude-nitin] qwen-ocr-extract: processed={state['processed']} skipped={state['skipped']} "
                f"errors={state['errors']} output={output_path} — ready for load-staging"
            )
        return "processed"

    try:
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            futures = [pool.submit(process_one, rec) for rec in records]
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as exc:  # noqa: BLE001
                    logger.exception("worker raised: %s", exc)
    finally:
        out_fh.close()

    signal_broker(
        f"[claude-nitin] qwen-ocr-extract COMPLETE: processed={state['processed']} "
        f"skipped={state['skipped']} errors={state['errors']} output={output_path}"
    )
    logger.info("DONE: processed=%d skipped=%d errors=%d",
                state["processed"], state["skipped"], state["errors"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
