"""Website classification facade preserving imports and patch targets."""

from __future__ import annotations

import logging

import httpx
import requests

from ch_bulk.web import browser
from ch_bulk.web.classifier_llm import _extract_json_object
from ch_bulk.web.classifier_pipeline import WebsiteClassifier
from ch_bulk.web.classifier_staging import (
    StagedClassification,
    insert_classification_batch,
    load_classification_staging,
)

logger = logging.getLogger(__name__)

__all__ = [
    "WebsiteClassifier",
    "StagedClassification",
    "load_classification_staging",
    "insert_classification_batch",
    "_extract_json_object",
]
