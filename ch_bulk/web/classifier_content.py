"""Page fetch + Playwright fallback prep for website classification."""

from __future__ import annotations

import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
import trafilatura

from ch_bulk.web import browser

logger = logging.getLogger(__name__)

MIN_CLASSIFIABLE_TEXT_LEN = 200
PAGE_PATHS = (
    "",
    "services",
    "about",
    "what-we-do",
    "care-services",
    "our-services",
    "our-care",
)


@dataclass(frozen=True)
class PageFetch:
    path: str
    url: str
    status: int | None
    html: str | None
    markdown: str | None
    used_playwright: bool = False
    fetch_error: str | None = None

    @property
    def text_len(self) -> int:
        return len(self.markdown or "")


@dataclass(frozen=True)
class SiteContent:
    source_url: str | None
    pages: list[PageFetch]
    content: str
    truncated: bool
    used_playwright: bool
    failure_reason: str | None
    http_status: int | None
    retry_pages: tuple[tuple[str, str], ...] = ()

    @property
    def text_len(self) -> int:
        return len(self.content)


@dataclass(frozen=True)
class FallbackTask:
    company_number: str
    site: SiteContent
    fetch_latency: float


def _normalize_url(url: str) -> str:
    cleaned = (url or "").strip()
    if not cleaned:
        return ""
    if "://" not in cleaned:
        cleaned = f"https://{cleaned}"
    parts = urlsplit(cleaned)
    if not parts.netloc:
        return cleaned
    path = parts.path or ""
    return urlunsplit(
        (parts.scheme or "https", parts.netloc, path, parts.query, parts.fragment)
    )


def _page_url(base_url: str, path: str) -> str:
    normalized = _normalize_url(base_url)
    if not normalized:
        return normalized
    if not path:
        return normalized
    return urljoin(normalized.rstrip("/") + "/", path)


def _strip_https_www(url: str) -> str | None:
    if not url.startswith("https://www."):
        return None
    return "https://" + url[len("https://www.") :]


def _extract_markdown(html: str | None) -> str | None:
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
        logger.exception("Failed to extract markdown")
        return None


def _fetch_page(
    http_session: requests.Session,
    url: str,
    *,
    timeout_seconds: float = 15.0,
) -> PageFetch:
    last_error: Exception | None = None
    last_error_kind: str | None = None
    for candidate_url in [url, _strip_https_www(url)]:
        if candidate_url is None:
            continue
        try:
            response = http_session.get(
                candidate_url,
                timeout=timeout_seconds,
            )
            html = response.text if response.ok else None
            markdown = _extract_markdown(html)
            return PageFetch(
                path=urlsplit(url).path.strip("/"),
                url=candidate_url,
                status=int(response.status_code),
                html=html,
                markdown=markdown,
            )
        except requests.exceptions.Timeout as exc:
            last_error = exc
            last_error_kind = "timeout"
            if candidate_url == url and _strip_https_www(url) is not None:
                continue
            break
        except requests.exceptions.SSLError as exc:
            last_error = exc
            last_error_kind = "ssl_error"
            if candidate_url == url and _strip_https_www(url) is not None:
                continue
            break
        except requests.exceptions.ConnectionError as exc:
            last_error = exc
            last_error_kind = "connection_error"
            if candidate_url == url and _strip_https_www(url) is not None:
                continue
            break
    if last_error is not None:
        logger.debug("Page fetch failed for %s: %s", url, last_error)
    return PageFetch(
        path=urlsplit(url).path.strip("/"),
        url=url,
        status=None,
        html=None,
        markdown=None,
        fetch_error=last_error_kind,
    )


def _page_needs_playwright(page: PageFetch) -> bool:
    if page.status == 403:
        return True
    if page.status == 200 and page.text_len < MIN_CLASSIFIABLE_TEXT_LEN:
        return True
    return False


def _assemble_site_content(
    base_url: str | None,
    fetched_pages: list[PageFetch],
) -> SiteContent:
    retry_pages = tuple(
        (page.path, page.url) for page in fetched_pages if _page_needs_playwright(page)
    )
    seen_hashes: set[str] = set()
    bundle_parts: list[str] = []
    pages_used: list[PageFetch] = []
    for page in fetched_pages:
        if page.status is None or not (200 <= page.status < 400):
            continue
        if page.markdown is None or len(page.markdown) < 80:
            continue
        digest = hashlib.sha1(page.markdown[:100].encode("utf-8")).hexdigest()
        if digest in seen_hashes:
            continue
        seen_hashes.add(digest)
        page_header = "/" if not page.path else f"/{page.path}"
        bundle_parts.append(f"## Page: {page_header}\n\n{page.markdown}\n\n---\n\n")
        pages_used.append(page)

    full_content = "".join(bundle_parts)
    truncated = len(full_content) > 12_000
    content = full_content[:12_000]
    any_success = any(
        page.status is not None and 200 <= page.status < 400 for page in fetched_pages
    )
    failure_reason: str | None = None
    if len(content) < MIN_CLASSIFIABLE_TEXT_LEN:
        failure_reason = "no_content" if any_success else "all_pages_unreachable"

    return SiteContent(
        source_url=base_url,
        pages=pages_used,
        content=content,
        truncated=truncated,
        used_playwright=any(page.used_playwright for page in fetched_pages),
        failure_reason=failure_reason,
        http_status=200 if any_success else None,
        retry_pages=retry_pages,
    )


def _fetch_site_pages_parallel(
    http_session: requests.Session,
    base_url: str,
) -> list[PageFetch]:
    page_urls = [_page_url(base_url, path) for path in PAGE_PATHS]
    fetched_pages: list[PageFetch | None] = [None] * len(page_urls)
    with ThreadPoolExecutor(max_workers=len(PAGE_PATHS)) as executor:
        futures = {
            executor.submit(_fetch_page, http_session, page_url): index
            for index, page_url in enumerate(page_urls)
        }
        for future in as_completed(futures):
            index = futures[future]
            page_url = page_urls[index]
            try:
                fetched_pages[index] = future.result()
            except Exception as exc:
                logger.exception("Parallel page fetch failed for %s", page_url)
                fetched_pages[index] = PageFetch(
                    path=urlsplit(page_url).path.strip("/"),
                    url=page_url,
                    status=None,
                    html=None,
                    markdown=None,
                    fetch_error=type(exc).__name__,
                )
    return [page for page in fetched_pages if page is not None]


def _collect_site_content(
    http_session: requests.Session,
    base_url: str,
) -> SiteContent:
    fetched_pages = _fetch_site_pages_parallel(http_session, base_url)
    return _assemble_site_content(base_url, fetched_pages)


def _collect_site_content_with_playwright(
    site: SiteContent,
    session: browser.PlaywrightSession,
) -> SiteContent:
    fetched_pages: list[PageFetch] = []
    for path, page_url in site.retry_pages:
        rendered_html = browser.fetch_rendered(
            page_url,
            session=session,
        )
        markdown = _extract_markdown(rendered_html)
        fetched_pages.append(
            PageFetch(
                path=path,
                url=page_url,
                status=200 if rendered_html else None,
                html=rendered_html,
                markdown=markdown,
                used_playwright=rendered_html is not None,
                fetch_error=None if rendered_html else "playwright_error",
            )
        )
    return _assemble_site_content(site.source_url, fetched_pages)
