from __future__ import annotations

"""Проверка названий на стоп-слова из окружения."""

import logging
import os
from pathlib import Path
from typing import Optional, Tuple

import re

_WORDS_ENV = "BLOCKED_TITLE_WORDS"
_ENABLED_ENV = "BLOCKED_TITLE_FILTER"
_LOG_ENABLED_ENV = "BLOCKED_TITLE_LOG"
_LOG_FILE_ENV = "BLOCKED_TITLE_LOG_FILE"
_WHITELIST_ENABLED_ENV = "BLOCKED_TITLE_WHITELIST_FILTER"
_WHITELIST_WORDS_ENV = "BLOCKED_TITLE_WHITELIST_WORDS"
_DEFAULT_LOG_FILE = Path(__file__).resolve().parent / "blocked_title.log"


def _as_bool(value: Optional[str]) -> bool:
    if not value:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _prepare_words(raw: Optional[str]) -> tuple[str, ...]:
    if not raw:
        return tuple()
    words = [chunk.strip().lower() for chunk in raw.split(",") if chunk.strip()]
    return tuple(words)


def _normalize(value: str) -> str:
    """Приводит строку к нижнему регистру и убирает всё, кроме [a-z0-9]."""

    lowered = value.lower()
    return re.sub(r"[^a-z0-9]", "", lowered)


_FILTER_ENABLED: bool = _as_bool(os.getenv(_ENABLED_ENV))
_BLOCK_WORDS: tuple[str, ...] = _prepare_words(os.getenv(_WORDS_ENV))
_NORMALIZED_BLOCK_WORDS: tuple[Tuple[str, str], ...] = tuple(
    (word, normalized)
    for word in _BLOCK_WORDS
    if (normalized := _normalize(word))
)

_WHITELIST_ENABLED: bool = _as_bool(os.getenv(_WHITELIST_ENABLED_ENV))
_WHITELIST_WORDS: tuple[str, ...] = _prepare_words(os.getenv(_WHITELIST_WORDS_ENV))
_NORMALIZED_WHITELIST_WORDS: tuple[str, ...] = tuple(
    normalized
    for word in _WHITELIST_WORDS
    if (normalized := _normalize(word))
)


def blocked_words() -> tuple[str, ...]:
    """Возвращает текущий набор стоп-слов."""

    if not _FILTER_ENABLED:
        return tuple()
    return _BLOCK_WORDS


def passes_whitelist(title: Optional[str]) -> bool:
    """Проверяет, проходит ли заголовок валидацию белым списком."""

    if not _WHITELIST_ENABLED:
        return True
    if not _NORMALIZED_WHITELIST_WORDS:
        return False
    if not title:
        return False
    normalized_title = _normalize(title)
    if not normalized_title:
        return False
    return any(word in normalized_title for word in _NORMALIZED_WHITELIST_WORDS)


def _init_logger() -> Optional[logging.Logger]:
    if not _FILTER_ENABLED:
        return None
    if not _as_bool(os.getenv(_LOG_ENABLED_ENV)):
        return None

    log_file = os.getenv(_LOG_FILE_ENV)
    path = Path(log_file) if log_file else _DEFAULT_LOG_FILE

    logger = logging.getLogger("blocked_titles")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


_LOGGER = _init_logger()


def check_title(title: Optional[str]) -> Tuple[bool, Optional[str]]:
    """Проверяет, содержит ли title одно из стоп-слов."""

    if not _FILTER_ENABLED:
        return False, None
    if not title:
        return False, None
    normalized_title = _normalize(title)
    for original, normalized in _NORMALIZED_BLOCK_WORDS:
        if normalized and normalized in normalized_title:
            return True, original
    return False, None


def log_block(item_id: str, word: str, title: str) -> None:
    """Пишет событие блокировки в лог при включённом флаге."""

    if _LOGGER is None:
        return
    _LOGGER.info("item_id=%s reason=%r title=%r", item_id, word, title)
