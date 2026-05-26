"""Persistent app settings stored as JSON next to the data directory.

Single source of truth for things the user can configure (API keys,
later: default filter values, theme preferences, etc.). Lives at
``<data_dir>/settings.json``.

Schema is intentionally nested by topic so new sections can be added
without breaking existing readers:

    {
      "api_keys": {
        "companies_house": "...",
        "cqc": "..."
      },
      "llm": {
        "provider": "llamacpp",
        "endpoint": "http://localhost:9741/v1",
        "model": "Qwen2.5-14B-Instruct-Q4_K_M.gguf",
        "context": 16384,
        "max_tokens": 400,
        "temperature": 0.1,
        "classifier_workers": 3,
        "playwright_fallback": null
      }
    }
"""

from __future__ import annotations

import copy
import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SETTINGS_FILENAME = "settings.json"

DEFAULT_SETTINGS: dict[str, Any] = {
    "api_keys": {
        "companies_house": "",
        "cqc": "",
    },
    "llm": {
        "provider": "llamacpp",
        "endpoint": "http://localhost:9741/v1",
        "model": "Qwen2.5-14B-Instruct-Q4_K_M.gguf",
        "context": 16384,
        "max_tokens": 400,
        "temperature": 0.1,
        "classifier_workers": 3,
        "playwright_fallback": None,
    },
}


def settings_path(data_dir: str | Path) -> Path:
    return Path(data_dir) / SETTINGS_FILENAME


def load_settings(data_dir: str | Path) -> dict[str, Any]:
    """Read settings.json. Returns DEFAULT_SETTINGS (deep-merged) if the
    file is missing, empty, or unparseable. Never raises — this is a
    config file, not a data integrity check."""
    path = settings_path(data_dir)
    if not path.exists():
        return _deep_merge(DEFAULT_SETTINGS, {})
    try:
        with path.open("r", encoding="utf-8") as f:
            stored = json.load(f) or {}
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read %s: %s — using defaults", path, exc)
        return _deep_merge(DEFAULT_SETTINGS, {})
    return _deep_merge(DEFAULT_SETTINGS, stored)


def save_settings(data_dir: str | Path, settings: dict[str, Any]) -> Path:
    """Write settings.json atomically. Returns the file path."""
    path = settings_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(settings, f, indent=2, sort_keys=True)
    tmp.replace(path)
    logger.info("Settings saved to %s", path)
    return path


def _deep_merge(base: dict, override: dict) -> dict:
    """Return base with override values layered on top, recursing into
    nested dicts. Defaults from DEFAULT_SETTINGS survive when the user's
    file is missing a key.

    Deep-copies `base` so the caller can mutate the result without
    polluting DEFAULT_SETTINGS for subsequent loads."""
    out = copy.deepcopy(base)
    for k, v in override.items():
        if (
            k in out
            and isinstance(out[k], dict)
            and isinstance(v, dict)
        ):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out
