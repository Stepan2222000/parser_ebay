"""helpers for skipping "Results matching fewer words" blocks in catalog pages."""

from __future__ import annotations

from typing import Any
import os


def _as_bool(value: str | None) -> bool:
    if not value:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


_FILTER_ENABLED = _as_bool(os.getenv("CATALOG_FILTER_FEWER_WORDS"))


def should_stop(li: Any) -> bool:
    if not _FILTER_ENABLED:
        return False

    classes = li.get("class") or []
    if "srp-river-answer--REWRITE_START" in classes:
        return True

    text_node = li.find(string=lambda text: text and "Results matching fewer words" in text)
    if text_node is not None:
        return True

    marker = li.find_previous("li", class_="srp-river-answer--REWRITE_START")
    if marker is not None:
        return True

    marker_text = li.find_previous(lambda tag: tag.name == "li" and tag.find(
        string=lambda text: text and "Results matching fewer words" in text
    ))
    return marker_text is not None
