"""DuckDuckGo HTML search helpers for discovering company homepages."""

from __future__ import annotations

import re
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

DDG_HTML_URL = "https://html.duckduckgo.com/html/"
USER_AGENT = "UK-Homecare-Toolkit/1.0 (+research)"
DEFAULT_MAX_RESULTS = 10
DEFAULT_MIN_SCORE = 4
DEFAULT_DDG_TIMEOUT_SECONDS = 15
DEFAULT_URL_CHECK_TIMEOUT_SECONDS = 10.0
DEFAULT_URL_CHECK_WORKERS = 10
MISSING_STATUS_CODES = {404, 410}
REACHABLE_NON_2XX_STATUS_CODES = {401, 403, 405}

# Domains we don't want to surface as a company's website.
DENYLIST = {
    "facebook.com",
    "twitter.com",
    "linkedin.com",
    "instagram.com",
    "tiktok.com",
    "youtube.com",
    "youtu.be",
    "pinterest.com",
    "indeed.com",
    "indeed.co.uk",
    "glassdoor.com",
    "glassdoor.co.uk",
    "trustpilot.com",
    "trustpilot.co.uk",
    "find-and-update.company-information.service.gov.uk",
    "companieshouse.gov.uk",
    "endole.co.uk",
    "opencorporates.com",
    "google.com",
    "bing.com",
    "duckduckgo.com",
    "yell.com",
    "yelp.com",
    "yelp.co.uk",
    "homecare.co.uk",
    "cqc.org.uk",
}


@dataclass(frozen=True)
class SearchResult:
    rank: int
    title: str
    url: str
    snippet: str


@dataclass(frozen=True)
class URLCheckResult:
    reachable: bool
    status_code: int | None
    final_url: str | None
    error: str | None
    checked_via: str | None


@dataclass(frozen=True)
class ScoredSearchResult:
    rank: int
    title: str
    url: str
    snippet: str
    domain: str
    score: int
    score_signals: tuple[str, ...]
    denylisted: bool
    reachable: bool
    reachability_status: int | None
    final_url: str | None
    reachability_error: str | None
    checked_via: str | None

    @property
    def preferred_url(self) -> str:
        return _root_url(self.final_url or self.url)

    def to_payload(self) -> dict[str, object]:
        return {
            "rank": self.rank,
            "title": self.title,
            "url": self.url,
            "snippet": self.snippet,
            "domain": self.domain,
            "score": self.score,
            "score_signals": list(self.score_signals),
            "denylisted": self.denylisted,
            "reachable": self.reachable,
            "reachability_status": self.reachability_status,
            "final_url": self.final_url,
            "reachability_error": self.reachability_error,
            "checked_via": self.checked_via,
        }


@dataclass(frozen=True)
class SearchOutcome:
    query: str
    search_results: list[ScoredSearchResult]
    picked_result: ScoredSearchResult | None
    picked_reason: str
    min_score: int

    @property
    def picked_url(self) -> str | None:
        if self.picked_result is None:
            return None
        return self.picked_result.preferred_url

    @property
    def picked_score(self) -> int | None:
        if self.picked_result is None:
            return None
        return self.picked_result.score

    @property
    def picked_rank(self) -> int | None:
        if self.picked_result is None:
            return None
        return self.picked_result.rank

    @property
    def picked_reachable(self) -> bool | None:
        if self.picked_result is None:
            return None
        return self.picked_result.reachable


def build_company_query(company_name: str, postcode: str | None = None) -> str:
    query_parts = [company_name.strip(), "homecare"]
    if postcode and postcode.strip():
        query_parts.append(postcode.strip())
    return " ".join(query_parts)


def _resolve_ddg_redirect(href: str) -> str | None:
    if not href:
        return None
    if href.startswith("/l/?"):
        qs = urllib.parse.urlparse(href).query
        params = urllib.parse.parse_qs(qs)
        target = params.get("uddg", [None])[0]
        if target:
            return urllib.parse.unquote(target)
        return None
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return None


def _normalize_url(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    normalized_path = parts.path or "/"
    return urllib.parse.urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            normalized_path,
            parts.query,
            "",
        )
    )


def _root_url(url: str) -> str:
    parts = urllib.parse.urlsplit(_normalize_url(url))
    return urllib.parse.urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            "/",
            "",
            "",
        )
    )


def _domain_of(url: str) -> str:
    return urllib.parse.urlparse(url).netloc.lower().lstrip("www.")


def _is_root_url(url: str) -> bool:
    parts = urllib.parse.urlsplit(url)
    return (parts.path or "/") == "/" and not parts.query


def _is_denylisted(domain: str) -> bool:
    if not domain:
        return False
    return any(domain == denied or domain.endswith("." + denied) for denied in DENYLIST)


def _name_tokens(company_name: str) -> set[str]:
    cleaned_name = re.sub(r"[^a-z0-9 ]", "", company_name.lower())
    stop_words = {"ltd", "limited", "the", "uk", "care", "services"}
    return {
        token
        for token in cleaned_name.split()
        if token and token not in stop_words
    }


def _describe_exception(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


def _is_reachable_status(status_code: int) -> bool:
    if 200 <= status_code < 400:
        return True
    return status_code in REACHABLE_NON_2XX_STATUS_CODES


def _check_url_reachability(
    url: str,
    *,
    timeout_seconds: float = DEFAULT_URL_CHECK_TIMEOUT_SECONDS,
) -> URLCheckResult:
    headers = {"User-Agent": USER_AGENT}
    head_error: str | None = None

    try:
        response = requests.head(
            url,
            headers=headers,
            timeout=timeout_seconds,
            allow_redirects=True,
        )
        status_code = int(response.status_code)
        final_url = response.url or url
        response.close()
        if _is_reachable_status(status_code):
            return URLCheckResult(
                reachable=True,
                status_code=status_code,
                final_url=final_url,
                error=None,
                checked_via="head",
            )
        if status_code in MISSING_STATUS_CODES or status_code >= 500:
            return URLCheckResult(
                reachable=False,
                status_code=status_code,
                final_url=final_url,
                error=None,
                checked_via="head",
            )
    except requests.RequestException as exc:
        head_error = _describe_exception(exc)

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=timeout_seconds,
            allow_redirects=True,
            stream=True,
        )
        status_code = int(response.status_code)
        final_url = response.url or url
        response.close()
        reachable = status_code not in MISSING_STATUS_CODES and status_code < 500
        return URLCheckResult(
            reachable=reachable,
            status_code=status_code,
            final_url=final_url,
            error=head_error,
            checked_via="get",
        )
    except requests.RequestException as exc:
        error = _describe_exception(exc)
        if head_error:
            error = f"{head_error}; {error}"
        return URLCheckResult(
            reachable=False,
            status_code=None,
            final_url=None,
            error=error,
            checked_via="get",
        )


def _check_results_in_parallel(
    results: list[SearchResult],
    *,
    timeout_seconds: float = DEFAULT_URL_CHECK_TIMEOUT_SECONDS,
    max_workers: int = DEFAULT_URL_CHECK_WORKERS,
) -> dict[int, URLCheckResult]:
    if not results:
        return {}

    checks: dict[int, URLCheckResult] = {}
    worker_count = max(1, min(max_workers, len(results)))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_result = {
            executor.submit(
                _check_url_reachability,
                result.url,
                timeout_seconds=timeout_seconds,
            ): result.rank
            for result in results
        }
        for future in as_completed(future_to_result):
            rank = future_to_result[future]
            checks[rank] = future.result()
    return checks


def score_search_results(
    results: list[SearchResult],
    company_name: str,
    *,
    url_check_timeout_seconds: float = DEFAULT_URL_CHECK_TIMEOUT_SECONDS,
    url_check_workers: int = DEFAULT_URL_CHECK_WORKERS,
) -> list[ScoredSearchResult]:
    name_tokens = _name_tokens(company_name)
    reachability = _check_results_in_parallel(
        results,
        timeout_seconds=url_check_timeout_seconds,
        max_workers=url_check_workers,
    )

    scored_results: list[ScoredSearchResult] = []
    for result in results:
        domain = _domain_of(result.url)
        score = 0
        score_signals: list[str] = []

        if domain.endswith(".co.uk") or domain.endswith(".org.uk") or domain.endswith(".uk"):
            score += 3
            score_signals.append("uk_tld")
        elif domain.endswith(".com"):
            score += 1
            score_signals.append("com_tld")

        if _is_root_url(result.url):
            score += 2
            score_signals.append("root_url")

        normalized_domain = domain.replace("-", "")
        for token in sorted(name_tokens):
            if len(token) >= 3 and token in normalized_domain:
                score += 4
                score_signals.append(f"name_token:{token}")

        checked = reachability.get(
            result.rank,
            URLCheckResult(
                reachable=False,
                status_code=None,
                final_url=None,
                error=None,
                checked_via=None,
            ),
        )
        scored_results.append(
            ScoredSearchResult(
                rank=result.rank,
                title=result.title,
                url=result.url,
                snippet=result.snippet,
                domain=domain,
                score=score,
                score_signals=tuple(score_signals),
                denylisted=_is_denylisted(domain),
                reachable=checked.reachable,
                reachability_status=checked.status_code,
                final_url=checked.final_url,
                reachability_error=checked.error,
                checked_via=checked.checked_via,
            )
        )

    return scored_results


def pick_search_result(
    results: list[ScoredSearchResult],
    *,
    min_score: int = DEFAULT_MIN_SCORE,
) -> ScoredSearchResult | None:
    candidates = [result for result in results if not result.denylisted]
    if not candidates:
        return None
    candidates.sort(
        key=lambda result: (-int(result.reachable), -result.score, result.rank)
    )
    picked = candidates[0]
    if picked.score < min_score:
        return None
    return picked


def pick_reason_for(result: ScoredSearchResult | None) -> str:
    if result is None:
        return "no_match_above_threshold"
    if "uk_tld" in result.score_signals:
        return "top_scored_with_uk_tld"
    if any(signal.startswith("name_token:") for signal in result.score_signals):
        return "top_scored_name_overlap"
    if "root_url" in result.score_signals:
        return "top_scored_root_url"
    if "com_tld" in result.score_signals:
        return "top_scored_with_com_tld"
    return "top_scored_above_threshold"


class DuckDuckGoSearcher:
    """Stateful DDG HTML search client with a reused requests.Session."""

    def __init__(
        self,
        *,
        ddg_timeout_seconds: int = DEFAULT_DDG_TIMEOUT_SECONDS,
        url_check_timeout_seconds: float = DEFAULT_URL_CHECK_TIMEOUT_SECONDS,
        url_check_workers: int = DEFAULT_URL_CHECK_WORKERS,
        session: requests.Session | None = None,
    ) -> None:
        self.ddg_timeout_seconds = ddg_timeout_seconds
        self.url_check_timeout_seconds = url_check_timeout_seconds
        self.url_check_workers = max(1, url_check_workers)
        self._session = session or requests.Session()
        self._owns_session = session is None
        adapter = HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=0)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def __enter__(self) -> "DuckDuckGoSearcher":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def search(
        self,
        query: str,
        *,
        max_results: int = DEFAULT_MAX_RESULTS,
    ) -> list[SearchResult]:
        params = {"q": query, "kl": "uk-en"}
        headers = {"User-Agent": USER_AGENT}
        response = self._session.post(
            DDG_HTML_URL,
            data=params,
            headers=headers,
            timeout=self.ddg_timeout_seconds,
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        results: list[SearchResult] = []
        for block in soup.select("div.result"):
            title_el = block.select_one("a.result__a")
            snippet_el = block.select_one("a.result__snippet, .result__snippet")
            if not title_el:
                continue
            href = title_el.get("href", "")
            url = _resolve_ddg_redirect(href)
            if not url:
                continue
            results.append(
                SearchResult(
                    rank=len(results) + 1,
                    title=title_el.get_text(strip=True),
                    url=_normalize_url(url),
                    snippet=snippet_el.get_text(strip=True) if snippet_el else "",
                )
            )
            if len(results) >= max_results:
                break
        return results

    def search_company(
        self,
        company_name: str,
        postcode: str | None = None,
        *,
        max_results: int = DEFAULT_MAX_RESULTS,
        min_score: int = DEFAULT_MIN_SCORE,
    ) -> SearchOutcome:
        query = build_company_query(company_name, postcode)
        raw_results = self.search(query, max_results=max_results)
        scored_results = score_search_results(
            raw_results,
            company_name,
            url_check_timeout_seconds=self.url_check_timeout_seconds,
            url_check_workers=self.url_check_workers,
        )
        picked = pick_search_result(scored_results, min_score=min_score)
        return SearchOutcome(
            query=query,
            search_results=scored_results,
            picked_result=picked,
            picked_reason=pick_reason_for(picked),
            min_score=min_score,
        )
