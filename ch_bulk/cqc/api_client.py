"""Thin wrapper over the public CQC syndication API."""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import httpx

from ch_bulk.core.cancellation import cancellable_sleep
from ch_bulk.core.paths import DEFAULT_DATA_DIR
from ch_bulk.core.rate_limit import SlidingWindowThrottle
from ch_bulk.core.settings import load_settings

logger = logging.getLogger(__name__)

API_BASE = "https://api.service.cqc.org.uk/public/v1"
RETRYABLE_STATUS_CODES = {429, 502, 503, 504}
DEFAULT_RETRY_AFTER_SECONDS = 60


@dataclass(frozen=True)
class APIResult:
    status_code: int
    payload: dict | list | str


class CQCAPIClient:
    """HTTP client for provider/location reads from the public CQC API."""

    def __init__(
        self,
        data_dir: str | Path | None = None,
        *,
        api_key: str | None = None,
    ) -> None:
        data_dir_path = Path(data_dir) if data_dir is not None else DEFAULT_DATA_DIR
        if api_key is None:
            api_key = load_settings(data_dir_path)["api_keys"]["cqc"]
        if not api_key:
            raise RuntimeError("CQC API key is not configured in settings.json")

        self._client = httpx.Client(
            base_url=API_BASE,
            headers={"Ocp-Apim-Subscription-Key": api_key},
            timeout=60,
            follow_redirects=True,
        )
        self._throttle = SlidingWindowThrottle(
            2000,
            60,
            label="cqc_api",
            logger=logger,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "CQCAPIClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _decode_payload(self, response: httpx.Response) -> dict | list | str:
        try:
            return response.json()
        except json.JSONDecodeError:
            return response.text

    def _get(
        self,
        path: str,
        *,
        cancel_event: threading.Event | None = None,
    ) -> APIResult:
        for attempt in range(5):
            if cancel_event is None:
                self._throttle.wait()
            else:
                self._throttle.wait(cancel_event=cancel_event)
            response = self._client.get(path)
            payload = self._decode_payload(response)

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                try:
                    pause = int(retry_after) if retry_after else DEFAULT_RETRY_AFTER_SECONDS
                except ValueError:
                    pause = DEFAULT_RETRY_AFTER_SECONDS
                logger.warning("CQC API 429 for %s, sleeping %ss", path, pause)
                cancellable_sleep(
                    cancel_event,
                    pause,
                    reason=f"CQC API retry cancelled: {path}",
                    sleep_fn=time.sleep,
                )
                continue

            if response.status_code in {502, 503, 504}:
                pause = 2 ** attempt
                logger.warning(
                    "CQC API %s for %s, retrying in %ss",
                    response.status_code,
                    path,
                    pause,
                )
                cancellable_sleep(
                    cancel_event,
                    pause,
                    reason=f"CQC API retry cancelled: {path}",
                    sleep_fn=time.sleep,
                )
                continue

            if response.status_code in {200, 404}:
                return APIResult(status_code=response.status_code, payload=payload)

            response.raise_for_status()

        raise RuntimeError(f"CQC API failed after retries: {path}")

    def get_provider(
        self,
        provider_id: str,
        *,
        cancel_event: threading.Event | None = None,
    ) -> APIResult:
        if cancel_event is None:
            return self._get(f"/providers/{provider_id}")
        return self._get(f"/providers/{provider_id}", cancel_event=cancel_event)

    def get_location(
        self,
        location_id: str,
        *,
        cancel_event: threading.Event | None = None,
    ) -> APIResult:
        if cancel_event is None:
            return self._get(f"/locations/{location_id}")
        return self._get(f"/locations/{location_id}", cancel_event=cancel_event)
