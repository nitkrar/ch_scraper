"""OCR + filter sidecar producer for staged Companies House PDFs.

Scans data/staging/filings/<cn>/<filing_id>.pdf, and for each PDF:

  1. If <filing_id>.ocr.txt does not exist, run OCR (pdf2image + tesseract)
     and write all-pages text to <filing_id>.ocr.txt
  2. If <filing_id>.filtered.txt does not exist, apply the tuned page filter
     (scripts/financial_page_filter.py) and write the trimmed text to
     <filing_id>.filtered.txt, plus a per-page label sidecar at
     <filing_id>.pages.json

Each step is idempotent and resumable: existing sidecar files are detected
and skipped. Run repeatedly during/after the iXBRL enrichment to ingest new
staged PDFs.

Usage:
    python scripts/ocr_staged_pdfs.py \\
        --staging-dir data/staging/filings \\
        --limit 0        # 0 = unlimited
        --workers 1      # parallel PDFs (tesseract is CPU-bound; >1 if cores available)
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    from pdf2image import convert_from_path
    import pytesseract
except ImportError as exc:
    print(f"ERROR: missing OCR deps ({exc}). Install with: .venv/bin/pip install pytesseract pdf2image",
          file=sys.stderr)
    sys.exit(1)

# Import the tuned filter — it lives in scripts/financial_page_filter.py
sys.path.insert(0, str(Path(__file__).parent))
try:
    from financial_page_filter import keep_page
except ImportError:
    print("ERROR: scripts/financial_page_filter.py not found. "
          "Promote /tmp/filter_tuning_function.py there first.", file=sys.stderr)
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ocr_staged_pdfs")

PAGE_BREAK = "\n--- PAGE BREAK ---\n"
# 200 dpi renders tables with the numeric columns dropped by tesseract on
# real CH filings; 400 recovers them at ~4x the render cost. See --dpi.
OCR_DPI = 400


def ocr_pdf(pdf_path: Path, dpi: int = OCR_DPI) -> tuple[int, str]:
    """Return (page_count, joined_text)."""
    images = convert_from_path(str(pdf_path), dpi=dpi)
    parts = [pytesseract.image_to_string(img, lang="eng") for img in images]
    return len(images), PAGE_BREAK.join(parts)


def filter_text(full_text: str) -> tuple[str, list[dict]]:
    """Run tuned keep_page() per page. Return (joined_kept_text, per_page_labels)."""
    pages = full_text.split(PAGE_BREAK)
    labels = []
    kept_pages = []
    for i, page in enumerate(pages):
        keep, score, sig = keep_page(page)
        labels.append({"page": i, "keep": keep, "score": score, "reason": sig.get("reason"), "len": len(page)})
        if keep:
            kept_pages.append(page)
    return PAGE_BREAK.join(kept_pages), labels


def process_pdf(pdf_path: Path, dpi: int = OCR_DPI) -> dict:
    """Idempotent: produce .ocr.txt and .filtered.txt + .pages.json sidecars next to PDF."""
    cn = pdf_path.parent.name
    stem = pdf_path.stem
    ocr_path = pdf_path.with_name(f"{stem}.ocr.txt")
    filtered_path = pdf_path.with_name(f"{stem}.filtered.txt")
    pages_path = pdf_path.with_name(f"{stem}.pages.json")

    result = {
        "cn": cn,
        "pdf": str(pdf_path),
        "ocr_skipped": False,
        "filter_skipped": False,
        "ocr_time_sec": None,
        "ocr_pages": None,
        "ocr_chars": None,
        "filtered_chars": None,
        "filtered_pages": None,
    }

    # ---- OCR step (cache-aware) ----
    if ocr_path.exists():
        result["ocr_skipped"] = True
        full_text = ocr_path.read_text(encoding="utf-8")
        result["ocr_chars"] = len(full_text)
        result["ocr_pages"] = full_text.count(PAGE_BREAK) + 1 if full_text else 0
    else:
        try:
            t0 = time.time()
            n_pages, full_text = ocr_pdf(pdf_path, dpi=dpi)
            elapsed = time.time() - t0
        except Exception as exc:
            logger.exception("OCR failed for %s", pdf_path)
            result["error"] = f"ocr_failed:{type(exc).__name__}:{exc}"
            return result
        ocr_path.write_text(full_text, encoding="utf-8")
        result["ocr_time_sec"] = round(elapsed, 1)
        result["ocr_pages"] = n_pages
        result["ocr_chars"] = len(full_text)

    # ---- Filter step (cache-aware) ----
    if filtered_path.exists() and pages_path.exists():
        result["filter_skipped"] = True
        result["filtered_chars"] = filtered_path.stat().st_size
        labels = json.loads(pages_path.read_text(encoding="utf-8"))
        result["filtered_pages"] = sum(1 for x in labels if x.get("keep"))
        return result

    try:
        filtered_text, labels = filter_text(full_text)
    except Exception as exc:
        logger.exception("Filter failed for %s", pdf_path)
        result["error"] = f"filter_failed:{type(exc).__name__}:{exc}"
        return result

    filtered_path.write_text(filtered_text, encoding="utf-8")
    pages_path.write_text(json.dumps(labels), encoding="utf-8")
    result["filtered_chars"] = len(filtered_text)
    result["filtered_pages"] = sum(1 for x in labels if x.get("keep"))
    return result


def _default_staging_dir() -> Path:
    from ch_bulk.core.paths import raw_dir

    return raw_dir("data", "companies_house") / "filings"


def discover_pdfs(staging_dir: Path) -> list[Path]:
    """Return all .pdf files under staging_dir/<cn>/."""
    pdfs = []
    for sub in sorted(staging_dir.iterdir()):
        if not sub.is_dir():
            continue
        pdfs.extend(sub.glob("*.pdf"))
    return pdfs


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    # Derive from the same helper the enrichment pipeline writes with, rather
    # than hardcoding: filings land in
    # data/staging/raw/companies_house/filings/<cn>/<filing_id>.pdf
    p.add_argument("--staging-dir", type=Path, default=_default_staging_dir())
    p.add_argument("--dpi", type=int, default=OCR_DPI,
                   help="Render DPI. 200 loses numeric table columns; 400 recovers them.")
    p.add_argument("--limit", type=int, default=0, help="0 = unlimited; process at most N PDFs")
    p.add_argument("--workers", type=int, default=1,
                   help="Concurrent PDFs. Tesseract is CPU-bound; >1 only if cores available.")
    args = p.parse_args()

    if not args.staging_dir.exists():
        logger.error("staging dir not found: %s", args.staging_dir)
        return 2

    pdfs = discover_pdfs(args.staging_dir)
    logger.info("Discovered %d staged PDFs under %s", len(pdfs), args.staging_dir)

    # Identify pending = needs OCR or needs filter
    pending = []
    for pdf in pdfs:
        stem = pdf.stem
        ocr_path = pdf.with_name(f"{stem}.ocr.txt")
        filtered_path = pdf.with_name(f"{stem}.filtered.txt")
        if not ocr_path.exists() or not filtered_path.exists():
            pending.append(pdf)
    logger.info("Pending (need OCR or filter): %d", len(pending))

    if args.limit > 0:
        pending = pending[: args.limit]
        logger.info("Limit applied: processing first %d pending", len(pending))

    if not pending:
        logger.info("Nothing to do.")
        return 0

    done = 0
    ocr_ran = 0
    filter_ran = 0
    errs = 0
    bytes_total = 0
    bytes_filtered = 0

    if args.workers == 1:
        for pdf in pending:
            r = process_pdf(pdf, dpi=args.dpi)
            done += 1
            if "error" in r:
                errs += 1
                logger.warning("error cn=%s: %s", r["cn"], r["error"])
                continue
            if not r["ocr_skipped"]:
                ocr_ran += 1
            if not r["filter_skipped"]:
                filter_ran += 1
            bytes_total += r["ocr_chars"] or 0
            bytes_filtered += r["filtered_chars"] or 0
            if done % 10 == 0:
                logger.info("progress=%d/%d ocr_new=%d filter_new=%d errs=%d",
                            done, len(pending), ocr_ran, filter_ran, errs)
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(process_pdf, pdf, args.dpi): pdf for pdf in pending}
            for fut in as_completed(futures):
                r = fut.result()
                done += 1
                if "error" in r:
                    errs += 1
                    logger.warning("error cn=%s: %s", r["cn"], r["error"])
                    continue
                if not r["ocr_skipped"]:
                    ocr_ran += 1
                if not r["filter_skipped"]:
                    filter_ran += 1
                bytes_total += r["ocr_chars"] or 0
                bytes_filtered += r["filtered_chars"] or 0
                if done % 10 == 0:
                    logger.info("progress=%d/%d ocr_new=%d filter_new=%d errs=%d",
                                done, len(pending), ocr_ran, filter_ran, errs)

    pct = (100.0 * bytes_filtered / bytes_total) if bytes_total else 0.0
    logger.info(
        "DONE: pdfs=%d ocr_new=%d filter_new=%d errors=%d ocr_bytes=%d filter_bytes=%d (%.1f%%)",
        done, ocr_ran, filter_ran, errs, bytes_total, bytes_filtered, pct,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
