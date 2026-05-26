"""Optional Playwright helpers for JS-rendered website fetches."""

from __future__ import annotations

import logging
import re
from typing import Any

USER_AGENT = "Mozilla/5.0 UK-Homecare-Toolkit"
COOKIE_BUTTON_LABELS = (
    "Accept all",
    "Accept",
    "OK",
    "Got it",
    "Agree",
)

logger = logging.getLogger(__name__)


def _playwright_sync_api() -> Any | None:
    try:
        from playwright import sync_api
    except ImportError:
        return None
    return sync_api


def is_playwright_available() -> bool:
    return _playwright_sync_api() is not None


class PlaywrightSession:
    """Reusable Chromium session for the classifier slow path."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self._sync_api: Any | None = None
        self._playwright_cm: Any | None = None
        self._playwright: Any | None = None
        self._browser: Any | None = None

    def __enter__(self) -> "PlaywrightSession":
        self._sync_api = _playwright_sync_api()
        if self._sync_api is None:
            raise RuntimeError("Playwright is not available")
        self._playwright_cm = self._sync_api.sync_playwright()
        self._playwright = self._playwright_cm.__enter__()
        self._browser = self._playwright.chromium.launch(headless=True)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._browser is not None:
            self._browser.close()
            self._browser = None
        if self._playwright_cm is not None:
            self._playwright_cm.__exit__(exc_type, exc, tb)
            self._playwright_cm = None
            self._playwright = None
        self._sync_api = None

    def fetch(
        self,
        url: str,
        *,
        timeout_seconds: float | None = None,
    ) -> str | None:
        if self._browser is None or self._sync_api is None:
            raise RuntimeError("PlaywrightSession is not open")

        timeout_ms = max(
            1,
            int((timeout_seconds or self.timeout_seconds) * 1000),
        )
        context = self._browser.new_context(
            user_agent=USER_AGENT,
            ignore_https_errors=True,
        )
        try:
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            _dismiss_cookie_banners(page, self._sync_api)
            try:
                page.wait_for_load_state("networkidle", timeout=5_000)
            except Exception:
                pass
            return page.content()
        except Exception as exc:
            logger.debug("Playwright fetch failed for %s: %s", url, exc)
            return None
        finally:
            context.close()


def _dismiss_cookie_banners(page: Any, sync_api: Any) -> None:
    for label in COOKIE_BUTTON_LABELS:
        patterns = (
            page.get_by_role("button", name=re.compile(f"^{re.escape(label)}$", re.I)),
            page.get_by_text(re.compile(f"^{re.escape(label)}$", re.I)),
        )
        for locator in patterns:
            try:
                if locator.count() <= 0:
                    continue
                locator.first.click(timeout=1_000)
                return
            except Exception:
                continue


def fetch_rendered(
    url: str,
    *,
    timeout_seconds: float = 30.0,
    session: PlaywrightSession | None = None,
) -> str | None:
    """Return rendered page HTML when Playwright is available."""
    if session is not None:
        return session.fetch(url, timeout_seconds=timeout_seconds)

    if _playwright_sync_api() is None:
        return None

    with PlaywrightSession(timeout_seconds=timeout_seconds) as playwright_session:
        return playwright_session.fetch(url, timeout_seconds=timeout_seconds)
