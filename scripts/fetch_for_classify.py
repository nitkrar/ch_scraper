"""Fetch-only helper for adhoc classifier sessions.

Reads a shard CSV of (company_number, company_name, website), fetches each
site's homepage + a small set of canonical paths in parallel, normalizes to
markdown, and writes a prefetch JSONL with the assembled text.

The classifier session reads this JSONL and emits verdicts inline using its
own reasoning — this script does NOT call any LLM.

Resumable: skips company_numbers already present in the output JSONL.

Usage:
    python scripts/fetch_for_classify.py \\
        --shard-csv data/staging/classify_shard_0.csv \\
        --out data/staging/prefetch_<batch_id>.jsonl \\
        --workers 8

Output JSONL schema (one row per company):
    {
        "company_number": "12345678",
        "company_name": "...",
        "source_url": "https://example.org",
        "fetched_at": "2026-05-25T11:30:00Z",
        "http_status": 200,
        "content": "## Page: /\\n\\n...",
        "pages_used": [{"path": "", "len": 812, "status": 200}],
        "truncated": false,
        "failure_reason": null
    }
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import requests

try:
    import trafilatura
except ImportError:
    print("ERROR: trafilatura not installed. Run: .venv/bin/pip install trafilatura", file=sys.stderr)
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("fetch_for_classify")

PAGE_PATHS = (
    "",
    "services",
    "about",
    "what-we-do",
    "care-services",
    "our-services",
    "our-care",
)
USER_AGENT = "Mozilla/5.0 UK-Homecare-Toolkit"
TIMEOUT_SECONDS = 20
MIN_PAGE_MARKDOWN_LEN = 80
MIN_CLASSIFIABLE_TEXT_LEN = 200
CONTENT_CAP_CHARS = 12_000


@dataclass
class PageFetch:
    path: str
    url: str
    status: int | None
    markdown: str | None = None
    fetch_error: str | None = None


@dataclass
class SiteResult:
    company_number: str
    company_name: str
    source_url: str
    fetched_at: str
    http_status: int | None
    content: str
    pages_used: list[dict] = field(default_factory=list)
    truncated: bool = False
    failure_reason: str | None = None


def _strip_https_www(url: str) -> str | None:
    parts = urlsplit(url)
    if parts.netloc.startswith("www."):
        return parts._replace(netloc=parts.netloc[4:]).geturl()
    return None


def _page_url(base_url: str, path: str) -> str:
    base = base_url.rstrip("/")
    if not path:
        return base
    return f"{base}/{path.lstrip('/')}"


def _extract_markdown(html: str | None) -> str | None:
    if not html:
        return None
    try:
        md = trafilatura.extract(
            html,
            include_links=False,
            include_images=False,
            include_tables=False,
            favor_recall=True,
        )
        return md
    except Exception:
        return None


def _fetch_page(session: requests.Session, url: str) -> PageFetch:
    path = urlsplit(url).path.strip("/")
    last_err: str | None = None
    for candidate in [url, _strip_https_www(url)]:
        if candidate is None:
            continue
        try:
            resp = session.get(candidate, timeout=TIMEOUT_SECONDS)
            html = resp.text if resp.ok else None
            md = _extract_markdown(html)
            return PageFetch(path=path, url=candidate, status=int(resp.status_code), markdown=md)
        except requests.exceptions.Timeout:
            last_err = "timeout"
        except requests.exceptions.SSLError:
            last_err = "ssl_error"
        except requests.exceptions.ConnectionError:
            last_err = "connection_error"
        except requests.exceptions.RequestException as exc:
            last_err = type(exc).__name__
    return PageFetch(path=path, url=url, status=None, fetch_error=last_err)


def _fetch_site(session: requests.Session, company_number: str, company_name: str, base_url: str) -> SiteResult:
    page_urls = [_page_url(base_url, p) for p in PAGE_PATHS]
    fetched: list[PageFetch] = []
    with ThreadPoolExecutor(max_workers=len(PAGE_PATHS)) as ex:
        futures = {ex.submit(_fetch_page, session, u): u for u in page_urls}
        for fut in as_completed(futures):
            try:
                fetched.append(fut.result())
            except Exception as exc:
                u = futures[fut]
                fetched.append(PageFetch(path=urlsplit(u).path.strip("/"), url=u, status=None, fetch_error=type(exc).__name__))

    fetched.sort(key=lambda p: PAGE_PATHS.index(p.path) if p.path in PAGE_PATHS else len(PAGE_PATHS))

    seen: set[str] = set()
    bundle: list[str] = []
    used: list[dict] = []
    for page in fetched:
        if page.status is None or not (200 <= page.status < 400):
            continue
        if not page.markdown or len(page.markdown) < MIN_PAGE_MARKDOWN_LEN:
            continue
        digest = hashlib.sha1(page.markdown[:100].encode("utf-8")).hexdigest()
        if digest in seen:
            continue
        seen.add(digest)
        header = "/" if not page.path else f"/{page.path}"
        bundle.append(f"## Page: {header}\n\n{page.markdown}\n\n---\n\n")
        used.append({"path": page.path, "len": len(page.markdown), "status": page.status})

    full = "".join(bundle)
    truncated = len(full) > CONTENT_CAP_CHARS
    content = full[:CONTENT_CAP_CHARS]
    any_success = any(p.status is not None and 200 <= p.status < 400 for p in fetched)

    failure_reason: str | None = None
    if len(content) < MIN_CLASSIFIABLE_TEXT_LEN:
        failure_reason = "no_content" if any_success else "all_pages_unreachable"

    return SiteResult(
        company_number=company_number,
        company_name=company_name,
        source_url=base_url,
        fetched_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        http_status=200 if any_success else None,
        content=content,
        pages_used=used,
        truncated=truncated,
        failure_reason=failure_reason,
    )


def _load_done_ids(out_path: Path) -> set[str]:
    done: set[str] = set()
    if not out_path.exists():
        return done
    with out_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            cn = row.get("company_number")
            if cn:
                done.add(str(cn))
    return done


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard-csv", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Concurrent sites (default 1 = sequential). Each site still fetches its 7 paths in parallel internally. Max recommended: 2.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Stop after N new rows (0 = no limit)")
    args = parser.parse_args()

    if not args.shard_csv.exists():
        logger.error("Shard CSV not found: %s", args.shard_csv)
        return 2

    args.out.parent.mkdir(parents=True, exist_ok=True)
    done = _load_done_ids(args.out)
    if done:
        logger.info("Resuming: %d company_numbers already in %s", len(done), args.out)

    rows: list[tuple[str, str, str]] = []
    with args.shard_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            cn = (r.get("company_number") or "").strip()
            if not cn or cn in done:
                continue
            name = (r.get("company_name") or "").strip()
            url = (r.get("website") or "").strip()
            if not url:
                continue
            if not url.lower().startswith(("http://", "https://")):
                url = "https://" + url
            rows.append((cn, name, url))

    if args.limit > 0:
        rows = rows[: args.limit]
    total = len(rows)
    if total == 0:
        logger.info("Nothing to fetch (all done or shard empty).")
        return 0

    logger.info("Fetching %d sites with %d workers -> %s", total, args.workers, args.out)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html,*/*"})

    done_count = 0
    err_count = 0
    no_content_count = 0
    with args.out.open("a", encoding="utf-8") as out_f, ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_fetch_site, session, cn, name, url): (cn, url) for (cn, name, url) in rows}
        for fut in as_completed(futures):
            cn, url = futures[fut]
            try:
                result = fut.result()
            except Exception as exc:
                err_count += 1
                logger.warning("Fetch failed cn=%s url=%s: %s", cn, url, exc)
                result = SiteResult(
                    company_number=cn,
                    company_name="",
                    source_url=url,
                    fetched_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    http_status=None,
                    content="",
                    failure_reason=f"fetcher_exception:{type(exc).__name__}",
                )
            row = {
                "company_number": result.company_number,
                "company_name": result.company_name,
                "source_url": result.source_url,
                "fetched_at": result.fetched_at,
                "http_status": result.http_status,
                "content": result.content,
                "pages_used": result.pages_used,
                "truncated": result.truncated,
                "failure_reason": result.failure_reason,
            }
            out_f.write(json.dumps(row, ensure_ascii=False) + "\n")
            out_f.flush()
            done_count += 1
            if result.failure_reason:
                no_content_count += 1
            if done_count % 50 == 0:
                logger.info(
                    "progress=%d/%d (%.1f%%) errors=%d no_content=%d",
                    done_count, total, 100.0 * done_count / total, err_count, no_content_count,
                )

    logger.info(
        "DONE: fetched=%d errors=%d no_content=%d out=%s",
        done_count, err_count, no_content_count, args.out,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
