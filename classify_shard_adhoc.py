#!/usr/bin/env python3
"""Fetch-only helper for adhoc shard classification.

Reads the shard CSV, fetches all pages per company in parallel
(homepage + fallback paths, 12 000-char cap, trafilatura markdown),
and writes a staging JSONL for the classifier session to read and classify.

Usage:
    python classify_shard_adhoc.py \
        --shard   classify_shard_1.csv \
        --staging classify_shard_1_fetched.jsonl \
        [--workers 8] [--resume]

Output JSONL schema per line:
    {
        "company_number": "...",
        "website":        "...",
        "fetched_at":     "2026-...",
        "http_status":    200 | null,
        "source_url":     "...",
        "content":        "...",          # empty string when fetch failed
        "pages_used":     [{"path": "", "len": N, "status": 200}, ...],
        "truncated":      false,
        "failure_reason": null | "all_pages_unreachable" | "no_content" | "<error>"
    }
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
import trafilatura

MAX_CONTENT_LEN = 12_000
MIN_TEXT_LEN = 200
USER_AGENT = "Mozilla/5.0 UK-Homecare-Toolkit"

PAGE_PATHS = ("", "services", "about", "what-we-do", "care-services", "our-services", "our-care")


def isoformat_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_url(url: str) -> str:
    cleaned = (url or "").strip()
    if not cleaned:
        return ""
    if "://" not in cleaned:
        cleaned = f"https://{cleaned}"
    parts = urlsplit(cleaned)
    if not parts.netloc:
        return cleaned
    return urlunsplit((parts.scheme or "https", parts.netloc, parts.path or "", parts.query, parts.fragment))


def page_url(base: str, path: str) -> str:
    norm = normalize_url(base)
    if not path:
        return norm
    return urljoin(norm.rstrip("/") + "/", path)


def extract_markdown(html: str | None) -> str | None:
    if not html:
        return None
    try:
        return trafilatura.extract(html, output_format="markdown", include_tables=True, deduplicate=True)
    except Exception:
        return None


def fetch_one_page(session: requests.Session, url: str) -> dict:
    path = urlsplit(url).path.strip("/")
    try:
        resp = session.get(url, timeout=15, allow_redirects=True)
        html = resp.text if resp.ok else None
        md = extract_markdown(html)
        return {"path": path, "url": url, "status": resp.status_code, "markdown": md, "error": None}
    except requests.exceptions.Timeout:
        return {"path": path, "url": url, "status": None, "markdown": None, "error": "timeout"}
    except requests.exceptions.SSLError:
        return {"path": path, "url": url, "status": None, "markdown": None, "error": "ssl_error"}
    except requests.exceptions.ConnectionError:
        return {"path": path, "url": url, "status": None, "markdown": None, "error": "connection_error"}
    except Exception as exc:
        return {"path": path, "url": url, "status": None, "markdown": None, "error": str(exc)[:80]}


def collect_content(session: requests.Session, base_url: str) -> dict:
    urls = [page_url(base_url, p) for p in PAGE_PATHS]
    raw_pages: list[dict] = []
    with ThreadPoolExecutor(max_workers=len(urls)) as ex:
        futures = {ex.submit(fetch_one_page, session, u): u for u in urls}
        for f in as_completed(futures):
            raw_pages.append(f.result())

    seen_hashes: set[str] = set()
    parts: list[str] = []
    pages_used: list[dict] = []
    http_status: int | None = None

    for page in raw_pages:
        if page["status"] and 200 <= page["status"] < 400:
            if http_status is None:
                http_status = page["status"]
            md = page["markdown"]
            if md and len(md) >= 80:
                digest = hashlib.sha1(md[:100].encode()).hexdigest()
                if digest not in seen_hashes:
                    seen_hashes.add(digest)
                    label = f"/{page['path']}" if page["path"] else "/"
                    parts.append(f"## Page: {label}\n\n{md}\n\n---\n\n")
                    pages_used.append({"path": page["path"], "len": len(md), "status": page["status"]})

    full = "".join(parts)
    truncated = len(full) > MAX_CONTENT_LEN
    content = full[:MAX_CONTENT_LEN]
    failure_reason: str | None = None
    if len(content) < MIN_TEXT_LEN:
        failure_reason = "all_pages_unreachable" if http_status is None else "no_content"

    return {
        "source_url": base_url,
        "content": content,
        "truncated": truncated,
        "pages_used": pages_used,
        "http_status": http_status,
        "failure_reason": failure_reason,
    }


def load_done_company_numbers(staging_path: Path) -> set[str]:
    done: set[str] = set()
    if not staging_path.exists():
        return done
    with open(staging_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                done.add(str(json.loads(line)["company_number"]))
            except Exception:
                pass
    return done


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard", required=True)
    parser.add_argument("--staging", required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    shard_path = Path(args.shard)
    staging_path = Path(args.staging)
    workers: int = args.workers

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    adapter = requests.adapters.HTTPAdapter(pool_connections=30, pool_maxsize=30)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    rows: list[tuple[str, str]] = []
    with open(shard_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append((row["company_number"], row["website"]))

    total = len(rows)
    done_ids: set[str] = set()
    if args.resume:
        done_ids = load_done_company_numbers(staging_path)

    pending = [(cn, url) for cn, url in rows if cn not in done_ids]
    print(f"shard={shard_path.name} total={total} already_done={len(done_ids)} pending={len(pending)}", flush=True)

    outfile = open(staging_path, "a", encoding="utf-8", buffering=1)
    completed = len(done_ids)
    start = time.monotonic()

    def process(item: tuple[str, str]) -> tuple[str, str, dict]:
        cn, url = item
        fetched_at = isoformat_utc()
        site = collect_content(session, url)
        return cn, fetched_at, site

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(process, item): item for item in pending}
        for future in as_completed(futures):
            cn, url = futures[future]
            try:
                cn, fetched_at, site = future.result()
            except Exception as exc:
                fetched_at = isoformat_utc()
                site = {
                    "source_url": url,
                    "content": "",
                    "truncated": False,
                    "pages_used": [],
                    "http_status": None,
                    "failure_reason": f"fetch_exception: {type(exc).__name__}",
                }

            record = {
                "company_number": cn,
                "website": url,
                "fetched_at": fetched_at,
                "http_status": site["http_status"],
                "source_url": site["source_url"],
                "content": site["content"],
                "pages_used": site["pages_used"],
                "truncated": site["truncated"],
                "failure_reason": site["failure_reason"],
            }
            outfile.write(json.dumps(record, ensure_ascii=True, sort_keys=True) + "\n")
            outfile.flush()

            completed += 1
            elapsed = time.monotonic() - start
            processed_this_run = completed - len(done_ids)
            rate = processed_this_run / elapsed if elapsed > 0 else 0
            remaining = total - completed
            eta_min = (remaining / rate / 60) if rate > 0 else 0
            pct = (completed / total) * 100
            fr = site.get("failure_reason")
            content_len = len(site.get("content") or "")
            print(
                f"[{completed}/{total} {pct:.1f}%] {cn} content_len={content_len}"
                + (f" FAIL={fr}" if fr else "")
                + f" | rate={rate:.1f}/s eta={eta_min:.0f}m",
                flush=True,
            )

    outfile.close()
    session.close()

    elapsed = time.monotonic() - start
    print(f"\nFetch complete: total={total} done={completed} elapsed={elapsed:.0f}s", flush=True)
    print(f"Staging file: {staging_path}", flush=True)


if __name__ == "__main__":
    main()
