#!/usr/bin/env python3
"""Batch page fetcher for adhoc classification.

Usage:
    python adhoc_fetch_batch.py <csv_path> <start_row> <end_row>

Reads rows [start_row, end_row) from csv (0-indexed, after header).
For each row fetches homepage + /services + /about + /care-services.
Prints a JSON array to stdout — one object per row.
Classification reasoning happens outside this script.
"""
from __future__ import annotations

import csv
import hashlib
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
import trafilatura
from requests.adapters import HTTPAdapter

USER_AGENT = "Mozilla/5.0 UK-Homecare-Toolkit"
PAGE_PATHS = ("", "services", "about", "care-services")
MAX_CONTENT = 12_000
MIN_TEXT = 200
TIMEOUT = 12.0


def _isoformat_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_url(url: str) -> str:
    cleaned = (url or "").strip()
    if not cleaned:
        return ""
    if "://" not in cleaned:
        cleaned = f"https://{cleaned}"
    parts = urlsplit(cleaned)
    if not parts.netloc:
        return cleaned
    return urlunsplit((parts.scheme or "https", parts.netloc, parts.path or "", parts.query, parts.fragment))


def _page_url(base: str, path: str) -> str:
    norm = _normalize_url(base)
    if not norm or not path:
        return norm
    return urljoin(norm.rstrip("/") + "/", path)


def _strip_www(url: str) -> str | None:
    if url.startswith("https://www."):
        return "https://" + url[len("https://www."):]
    return None


def _extract_markdown(html: str | None) -> str | None:
    if not html:
        return None
    try:
        return trafilatura.extract(html, output_format="markdown", include_tables=True, deduplicate=True)
    except Exception:
        return None


def _fetch_one(session: requests.Session, url: str, path: str) -> dict[str, Any]:
    for candidate in [url, _strip_www(url)]:
        if candidate is None:
            continue
        try:
            resp = session.get(candidate, timeout=TIMEOUT, allow_redirects=True)
            html = resp.text if resp.ok else None
            md = _extract_markdown(html)
            return {
                "path": path,
                "url": candidate,
                "status": resp.status_code,
                "len": len(md) if md else 0,
                "markdown": md,
                "error": None,
            }
        except requests.exceptions.Timeout:
            err = "timeout"
        except requests.exceptions.SSLError:
            err = "ssl_error"
        except requests.exceptions.ConnectionError:
            err = "connection_error"
        except Exception as exc:
            err = type(exc).__name__
        if candidate == url and _strip_www(url) is not None:
            continue
        break
    return {"path": path, "url": url, "status": None, "len": 0, "markdown": None, "error": err}


def _fetch_site(session: requests.Session, base_url: str) -> dict[str, Any]:
    page_urls = [(_page_url(base_url, p), p) for p in PAGE_PATHS]
    results: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=len(PAGE_PATHS)) as ex:
        futures = {ex.submit(_fetch_one, session, pu, p): (pu, p) for pu, p in page_urls}
        for f in as_completed(futures):
            try:
                results.append(f.result())
            except Exception as exc:
                pu, p = futures[f]
                results.append({"path": p, "url": pu, "status": None, "len": 0, "markdown": None, "error": str(exc)})

    # Deduplicate by first 100-char SHA1 of markdown, assemble bundle
    seen: set[str] = set()
    bundle_parts: list[str] = []
    pages_used: list[dict[str, Any]] = []
    first_ok_status: int | None = None

    for r in sorted(results, key=lambda x: PAGE_PATHS.index(x["path"]) if x["path"] in PAGE_PATHS else 99):
        if r["status"] and 200 <= r["status"] < 400 and first_ok_status is None:
            first_ok_status = r["status"]
        md = r.get("markdown") or ""
        if not md or len(md) < 80:
            continue
        if r["status"] is None or not (200 <= r["status"] < 400):
            continue
        digest = hashlib.sha1(md[:100].encode()).hexdigest()
        if digest in seen:
            continue
        seen.add(digest)
        header = "/" if not r["path"] else f"/{r['path']}"
        bundle_parts.append(f"## Page: {header}\n\n{md}\n\n---\n\n")
        pages_used.append({"path": r["path"], "len": len(md), "status": r["status"]})

    full = "".join(bundle_parts)
    truncated = len(full) > MAX_CONTENT
    content = full[:MAX_CONTENT]
    any_ok = first_ok_status is not None
    failure_reason: str | None = None
    if len(content) < MIN_TEXT:
        failure_reason = "no_content" if any_ok else "all_pages_unreachable"

    return {
        "source_url": _normalize_url(base_url),
        "http_status": first_ok_status,
        "content": content,
        "truncated": truncated,
        "pages_used": pages_used,
        "failure_reason": failure_reason,
        "fetched_at": _isoformat_utc(),
    }


def main() -> None:
    if len(sys.argv) < 4:
        print("Usage: adhoc_fetch_batch.py <csv_path> <start_row> <end_row>", file=sys.stderr)
        sys.exit(1)

    csv_path, start_row, end_row = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    batch = rows[start_row:end_row]

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    adapter = HTTPAdapter(max_retries=0, pool_connections=20, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    output: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {
            ex.submit(_fetch_site, session, row["website"]): row
            for row in batch
        }
        for f in as_completed(futures):
            row = futures[f]
            try:
                site = f.result()
            except Exception as exc:
                site = {
                    "source_url": row.get("website", ""),
                    "http_status": None,
                    "content": "",
                    "truncated": False,
                    "pages_used": [],
                    "failure_reason": str(exc),
                    "fetched_at": _isoformat_utc(),
                }
            output.append({
                "company_number": row["company_number"],
                "company_name": row.get("company_name", ""),
                "website": row.get("website", ""),
                **site,
            })

    session.close()
    # Sort by original order
    order = {row["company_number"]: i for i, row in enumerate(batch)}
    output.sort(key=lambda x: order.get(x["company_number"], 9999))
    print(json.dumps(output, ensure_ascii=True))


if __name__ == "__main__":
    main()
