from __future__ import annotations

"""Проверка описаний на стоп-слова из окружения."""

import logging
import os
from pathlib import Path
from typing import Optional, Tuple

import re

_WORDS_ENV = "BLOCKED_DESC_WORDS"
_COMBO_WORDS_ENV = "BLOCKED_DESC_COMBO_WORDS"
_ENABLED_ENV = "BLOCKED_DESC_FILTER"
_LOG_ENABLED_ENV = "BLOCKED_DESC_LOG"
_LOG_FILE_ENV = "BLOCKED_DESC_LOG_FILE"
_DEFAULT_LOG_FILE = Path(__file__).resolve().parent / "blocked_description.log"


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


def _prepare_combo_words(raw: Optional[str]) -> tuple[tuple[tuple[str, str], ...], ...]:
    """Парсит комбо-правила вида 'word1+word2,word3+word4+word5'.

    Возвращает кортеж групп, где каждая группа — кортеж пар (original, normalized).
    Блокировка срабатывает если ВСЕ слова из группы присутствуют.
    """
    if not raw:
        return tuple()

    result = []
    for group in raw.split(","):
        group = group.strip()
        if not group:
            continue
        words_in_group = []
        for word in group.split("+"):
            word = word.strip().lower()
            if word:
                normalized = _normalize(word)
                if normalized:
                    words_in_group.append((word, normalized))
        if len(words_in_group) >= 2:  # Комбо только если 2+ слова
            result.append(tuple(words_in_group))
    return tuple(result)


_FILTER_ENABLED: bool = _as_bool(os.getenv(_ENABLED_ENV))
_BLOCK_WORDS: tuple[str, ...] = _prepare_words(os.getenv(_WORDS_ENV))
_NORMALIZED_BLOCK_WORDS: tuple[Tuple[str, str], ...] = tuple(
    (word, normalized)
    for word in _BLOCK_WORDS
    if (normalized := _normalize(word))
)
_COMBO_BLOCK_WORDS: tuple[tuple[tuple[str, str], ...], ...] = _prepare_combo_words(
    os.getenv(_COMBO_WORDS_ENV)
)


def blocked_words() -> tuple[str, ...]:
    """Возвращает текущий набор стоп-слов."""
    if not _FILTER_ENABLED:
        return tuple()
    return _BLOCK_WORDS


def combo_words() -> tuple[str, ...]:
    """Возвращает текущие комбо-правила в читаемом виде."""
    if not _FILTER_ENABLED:
        return tuple()
    result = []
    for group in _COMBO_BLOCK_WORDS:
        words = [orig for orig, _ in group]
        result.append(" + ".join(words))
    return tuple(result)


def _init_logger() -> Optional[logging.Logger]:
    if not _FILTER_ENABLED:
        return None
    if not _as_bool(os.getenv(_LOG_ENABLED_ENV)):
        return None

    log_file = os.getenv(_LOG_FILE_ENV)
    path = Path(log_file) if log_file else _DEFAULT_LOG_FILE

    logger = logging.getLogger("blocked_descriptions")
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


def check_description(description: Optional[str]) -> Tuple[bool, Optional[str]]:
    """Проверяет, содержит ли description одно из стоп-слов или комбо-правил.

    Возвращает (blocked, reason):
    - Сначала проверяет одиночные стоп-слова
    - Затем проверяет комбо-правила (блокировка если ВСЕ слова группы присутствуют)
    """
    if not _FILTER_ENABLED:
        return False, None
    if not description:
        return False, None

    normalized_description = _normalize(description)

    # Проверка одиночных стоп-слов
    for original, normalized in _NORMALIZED_BLOCK_WORDS:
        if normalized and normalized in normalized_description:
            return True, original

    # Проверка комбо-правил (все слова группы должны присутствовать)
    for group in _COMBO_BLOCK_WORDS:
        all_present = all(
            normalized in normalized_description
            for _, normalized in group
        )
        if all_present:
            # Формируем причину как "word1 + word2"
            reason = " + ".join(orig for orig, _ in group)
            return True, f"COMBO({reason})"

    return False, None


def log_block(item_id: str, word: str, description: str) -> None:
    """Пишет событие блокировки в лог при включённом флаге."""

    if _LOGGER is None:
        return
    # Ограничиваем превью описания до 200 символов
    preview = description[:200] if description else ""
    _LOGGER.info("item_id=%s reason=%r description_preview=%r", item_id, word, preview)
