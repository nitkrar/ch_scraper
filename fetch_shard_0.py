#!/usr/bin/env python3
"""Fetch-only script for shard 0. No LLM calls.

For each row in classify_shard_0.csv, fetches homepage + fallback pages
(services, about, care-services) in parallel, assembles content up to
12000 chars, writes one JSON line to prefetch_shard_0.jsonl.

Schema per line:
  {company_number, company_name, website, source_url, http_status,
   pages_used, content, truncated, failure_reason}

Supports resume: skips rows whose company_number is already in output.
"""
from __future__ import annotations

import csv
import hashlib
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
import trafilatura
from requests.adapters import HTTPAdapter

SHARD_CSV = Path("/Users/nitinkum/Projects/nitkrar/ch_scraper/data/staging/classify_shard_0.csv")
PREFETCH_OUT = Path("/Users/nitinkum/Projects/nitkrar/ch_scraper/data/staging/prefetch_shard_0.jsonl")

PAGE_PATHS = ("", "services", "about", "care-services")
MAX_CONTENT = 12_000
MIN_CONTENT = 200
TIMEOUT = 12.0
OUTER_WORKERS = 20
INNER_WORKERS = len(PAGE_PATHS)
USER_AGENT = "Mozilla/5.0 UK-Homecare-Toolkit"


def ts() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if "://" not in url:
        url = f"https://{url}"
    return url


def page_url(base: str, path: str) -> str:
    if not path:
        return base
    return urljoin(base.rstrip("/") + "/", path)


def extract_markdown(html: str | None) -> str | None:
    if not html:
        return None
    try:
        return trafilatura.extract(
            html,
            output_format="markdown",
            include_tables=True,
            deduplicate=True,
        )
    except Exception:
        return None


def fetch_one(session: requests.Session, url: str, path: str) -> dict:
    try:
        resp = session.get(url, timeout=TIMEOUT, allow_redirects=True)
        html = resp.text if resp.ok else None
        md = extract_markdown(html)
        return {"path": path, "url": url, "status": resp.status_code, "markdown": md, "error": None}
    except requests.exceptions.Timeout:
        return {"path": path, "url": url, "status": None, "markdown": None, "error": "timeout"}
    except requests.exceptions.SSLError:
        return {"path": path, "url": url, "status": None, "markdown": None, "error": "ssl_error"}
    except requests.exceptions.ConnectionError:
        return {"path": path, "url": url, "status": None, "markdown": None, "error": "connection_error"}
    except Exception as e:
        return {"path": path, "url": url, "status": None, "markdown": None, "error": str(e)[:80]}


def collect_site(session: requests.Session, base_url: str) -> dict:
    urls = [(path, page_url(base_url, path)) for path in PAGE_PATHS]
    pages_raw: list[dict] = []
    with ThreadPoolExecutor(max_workers=INNER_WORKERS) as ex:
        futs = {ex.submit(fetch_one, session, u, p): p for p, u in urls}
        for fut in as_completed(futs):
            try:
                pages_raw.append(fut.result())
            except Exception as e:
                pages_raw.append({"path": futs[fut], "url": "", "status": None, "markdown": None, "error": str(e)})

    path_order = {p: i for i, p in enumerate(PAGE_PATHS)}
    pages_raw.sort(key=lambda p: path_order.get(p["path"], 99))

    seen: set[str] = set()
    bundle_parts: list[str] = []
    pages_used: list[dict] = []
    any_success = False
    first_status: int | None = None

    for page in pages_raw:
        if page["status"] is not None and 200 <= page["status"] < 400:
            any_success = True
            if first_status is None:
                first_status = page["status"]
        md = page.get("markdown")
        if md and len(md) >= 80:
            dig = hashlib.sha1(md[:100].encode()).hexdigest()
            if dig in seen:
                continue
            seen.add(dig)
            header = "/" if not page["path"] else f"/{page['path']}"
            bundle_parts.append(f"## Page: {header}\n\n{md}\n\n---\n\n")
            pages_used.append({"path": page["path"], "len": len(md), "status": page["status"]})

    full = "".join(bundle_parts)
    truncated = len(full) > MAX_CONTENT
    content = full[:MAX_CONTENT]

    failure_reason: str | None = None
    if len(content) < MIN_CONTENT:
        failure_reason = "no_content" if any_success else "all_pages_unreachable"

    return {
        "source_url": base_url,
        "http_status": first_status,
        "pages_used": pages_used,
        "content": content,
        "truncated": truncated,
        "failure_reason": failure_reason,
    }


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    adapter = HTTPAdapter(max_retries=0, pool_connections=2, pool_maxsize=2)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


_local = threading.local()


def get_session() -> requests.Session:
    if not hasattr(_local, "session"):
        _local.session = make_session()
    return _local.session


def load_done(path: Path) -> set[str]:
    done: set[str] = set()
    if not path.exists():
        return done
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                done.add(str(json.loads(line)["company_number"]))
            except Exception:
                pass
    return done


def process_row(row: dict) -> dict:
    session = get_session()
    cn = row["company_number"]
    url = normalize_url(row.get("website") or "")
    if not url:
        return {
            "company_number": cn,
            "company_name": row.get("company_name", ""),
            "website": row.get("website", ""),
            "source_url": "",
            "http_status": None,
            "pages_used": [],
            "content": "",
            "truncated": False,
            "failure_reason": "all_pages_unreachable",
            "fetched_at": ts(),
        }
    site = collect_site(session, url)
    return {
        "company_number": cn,
        "company_name": row.get("company_name", ""),
        "website": row.get("website", ""),
        **site,
        "fetched_at": ts(),
    }


def main() -> None:
    with SHARD_CSV.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    total = len(rows)
    print(f"[fetch-shard-0] {total} rows in CSV", flush=True)

    done = load_done(PREFETCH_OUT)
    todo = [r for r in rows if r["company_number"] not in done]
    print(f"[fetch-shard-0] skip={total - len(todo)} todo={len(todo)}", flush=True)

    write_lock = threading.Lock()
    counter = {"n": total - len(todo)}
    t0 = time.monotonic()

    def worker(row: dict) -> None:
        result = process_row(row)
        line = json.dumps(result, ensure_ascii=True, sort_keys=True)
        with write_lock:
            with PREFETCH_OUT.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
                fh.flush()
            counter["n"] += 1
            n = counter["n"]
        if n % 100 == 0 or n == total:
            elapsed = time.monotonic() - t0
            rate = (n - (total - len(todo))) / max(elapsed, 0.001)
            eta = (total - n) / max(rate, 0.001)
            print(f"[fetch-shard-0] {n}/{total} ({100*n/total:.1f}%) rate={rate:.1f}/s eta={eta:.0f}s", flush=True)

    with ThreadPoolExecutor(max_workers=OUTER_WORKERS) as ex:
        futs = [ex.submit(worker, row) for row in todo]
        for fut in as_completed(futs):
            try:
                fut.result()
            except Exception as e:
                print(f"[fetch-shard-0] worker error: {e}", file=sys.stderr, flush=True)

    elapsed = time.monotonic() - t0
    print(f"[fetch-shard-0] done in {elapsed:.0f}s", flush=True)


if __name__ == "__main__":
    main()
