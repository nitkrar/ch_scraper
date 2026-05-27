"""LLM prompt + call + JSON/verdict parsing for website classification."""

from __future__ import annotations

import ast
import json
import logging
import re
import time
from typing import Any

import httpx

from ch_bulk.db.staging import isoformat_utc
from ch_bulk.web.classifier_content import MIN_CLASSIFIABLE_TEXT_LEN, SiteContent
from ch_bulk.web.classifier_staging import StagedClassification

logger = logging.getLogger(__name__)

VALID_VERDICTS = {
    "Majority domiciliary",
    "Majority Supported living",
    "Majority residential",
    "Mixed_domiciliary_supported",
    "Mixed_residential_domiciliary",
    "Unable to classify",
}
LLM_REQUEST_FAILURE_REASON = "llm_request_error"
CLASSIFICATION_ERROR_REASONS = {
    "all_pages_unreachable",
    LLM_REQUEST_FAILURE_REASON,
    "parse_error",
}
VERDICT_NORMALIZATION = {
    "majority domiciliary care": "Majority domiciliary",
    "majority domiciliary": "Majority domiciliary",
    "majority supported living": "Majority Supported living",
    "majority residential": "Majority residential",
    "mixed_domiciliary_supported": "Mixed_domiciliary_supported",
    "mixed residential domiciliary": "Mixed_residential_domiciliary",
    "mixed_residential_domiciliary": "Mixed_residential_domiciliary",
    "unable to classify": "Unable to classify",
}
PROMPT_TEMPLATE = """You are classifying UK homecare company websites for an M&A targeting exercise.

Choose ONE verdict that best reflects the PRIMARY business:
- 'Majority domiciliary' — care delivered in the client's own home (visiting care, live-in)
- 'Majority Supported living' — clients live in their own tenancy with on-site/visiting support, typically learning disabilities
- 'Majority residential' — care home where clients live full-time
- 'Mixed_domiciliary_supported' — roughly equal mix of domiciliary + supported living
- 'Mixed_residential_domiciliary' — roughly equal mix of residential + domiciliary
- 'Unable to classify' — insufficient information

Site content (markdown):
<<<
{content}
>>>

Return JSON only: {{"verdict": "<one of above>", "evidence": "<one short quote from the content>"}}"""


def _canonical_verdict(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    return VERDICT_NORMALIZATION.get(normalized.lower(), normalized)


def _extract_text_content(message: dict[str, Any]) -> str:
    content = message.get("content")
    if isinstance(content, str) and content.strip():
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text = item.get("text")
                if text:
                    parts.append(str(text))
        joined = "\n".join(parts).strip()
        if joined:
            return joined
    reasoning = message.get("reasoning_content")
    if isinstance(reasoning, str):
        return reasoning
    return ""


def _parse_json_object_candidate(raw: str) -> dict[str, Any] | None:
    candidate = raw.strip()
    if not candidate:
        return None
    try:
        payload = json.loads(candidate)
    except json.JSONDecodeError:
        try:
            payload = ast.literal_eval(candidate)
        except (SyntaxError, ValueError):
            return None
    if isinstance(payload, dict) and "verdict" in payload:
        return payload
    return None


def _extract_json_object(text: str) -> dict[str, Any] | None:
    for match in re.finditer(r"\{", text):
        payload = _parse_json_object_candidate(text[match.start() :])
        if payload is not None:
            return payload
    return None


def _call_llm(
    llm_client: httpx.Client,
    *,
    content: str,
    model: str,
    llm_config: dict[str, Any],
) -> tuple[str | None, dict[str, Any] | None]:
    payload = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": PROMPT_TEMPLATE.format(content=content),
            }
        ],
        "max_tokens": int(llm_config.get("max_tokens") or 400),
        "temperature": float(llm_config.get("temperature") or 0.1),
        "response_format": {"type": "json_object"},
    }
    response = llm_client.post(
        "chat/completions",
        json=payload,
    )
    response.raise_for_status()
    payload_json = response.json()
    choices = payload_json.get("choices") or []
    if not choices:
        return None, None
    message = (choices[0] or {}).get("message") or {}
    raw_content = _extract_text_content(message)
    parsed = _extract_json_object(raw_content) if raw_content else None
    return raw_content or None, parsed


def _unable_row(
    *,
    company_number: str,
    site: SiteContent,
    classifier_name: str,
    failure_reason: str,
    raw_response: str | None = None,
    error: str | None = None,
) -> StagedClassification:
    return StagedClassification(
        entity_id=company_number,
        entity_type="classification",
        fetched_at=isoformat_utc(),
        http_status=site.http_status,
        raw_json={
            "verdict": "Unable to classify",
            "evidence": "",
            "classifier": classifier_name,
            "source_url": site.source_url,
            "pages_used": [
                {
                    "path": page.path,
                    "len": page.text_len,
                    "status": page.status,
                }
                for page in site.pages
            ],
            "truncated": site.truncated,
            "used_playwright": site.used_playwright,
            "failure_reason": failure_reason,
            "raw_response": raw_response,
            "error": error,
        },
    )


def _classify_site_content(
    *,
    company_number: str,
    site: SiteContent,
    llm_client: httpx.Client,
    model: str,
    llm_config: dict[str, Any],
    classifier_name: str,
) -> tuple[StagedClassification, dict[str, object]]:
    if site.text_len < MIN_CLASSIFIABLE_TEXT_LEN:
        staged = _unable_row(
            company_number=company_number,
            site=site,
            classifier_name=classifier_name,
            failure_reason=site.failure_reason or "no_content",
        )
        return staged, {
            "n_pages": len(site.pages),
            "text_len": site.text_len,
            "verdict": "Unable to classify",
            "used_playwright": site.used_playwright,
            "llm_latency": 0.0,
        }

    llm_started = time.monotonic()
    try:
        raw_response, parsed = _call_llm(
            llm_client,
            content=site.content,
            model=model,
            llm_config=llm_config,
        )
    except Exception as exc:
        logger.exception("LLM classification failed for %s", company_number)
        staged = _unable_row(
            company_number=company_number,
            site=site,
            classifier_name=classifier_name,
            failure_reason=LLM_REQUEST_FAILURE_REASON,
            error=str(exc),
        )
        return staged, {
            "n_pages": len(site.pages),
            "text_len": site.text_len,
            "verdict": "Unable to classify",
            "used_playwright": site.used_playwright,
            "llm_latency": time.monotonic() - llm_started,
        }

    llm_latency = time.monotonic() - llm_started
    verdict = _canonical_verdict((parsed or {}).get("verdict"))
    if verdict not in VALID_VERDICTS:
        staged = _unable_row(
            company_number=company_number,
            site=site,
            classifier_name=classifier_name,
            failure_reason="parse_error",
            raw_response=raw_response,
        )
        return staged, {
            "n_pages": len(site.pages),
            "text_len": site.text_len,
            "verdict": "Unable to classify",
            "used_playwright": site.used_playwright,
            "llm_latency": llm_latency,
        }

    staged = StagedClassification(
        entity_id=company_number,
        entity_type="classification",
        fetched_at=isoformat_utc(),
        http_status=site.http_status,
        raw_json={
            "verdict": verdict,
            "evidence": str((parsed or {}).get("evidence") or "").strip(),
            "classifier": classifier_name,
            "source_url": site.source_url,
            "pages_used": [
                {
                    "path": page.path,
                    "len": page.text_len,
                    "status": page.status,
                }
                for page in site.pages
            ],
            "truncated": site.truncated,
            "used_playwright": site.used_playwright,
            "failure_reason": None,
        },
    )
    return staged, {
        "n_pages": len(site.pages),
        "text_len": site.text_len,
        "verdict": verdict,
        "used_playwright": site.used_playwright,
        "llm_latency": llm_latency,
    }
