"""Управляет блокировками item_id в Redis, чтобы не дублировать запросы."""

from __future__ import annotations

import logging
import os
from typing import Iterable, List

from redis.asyncio import Redis

_LOCK_PREFIX = "dup_guard:item:"
_LOG_FILE = os.getenv("DUPLICATE_GUARD_LOG_FILE", "duplicate_guard.log")
_DEFAULT_TTL = int(os.getenv("DUPLICATE_GUARD_TTL", "600"))

_logger = logging.getLogger("duplicate_guard")
if not _logger.handlers:
    _handler = logging.FileHandler(_LOG_FILE, encoding="utf-8")
    _handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    _logger.addHandler(_handler)
_logger.setLevel(logging.INFO)
_logger.propagate = False


class DuplicateGuard:
    """Оборачивает операции блокировки item_id в Redis и ведёт лог."""

    def __init__(self, redis_client: Redis, ttl_seconds: int | None = None) -> None:
        self._redis = redis_client
        ttl = ttl_seconds if ttl_seconds is not None else _DEFAULT_TTL
        self._ttl = max(ttl, 1)

    async def acquire(self, item_id: str, *, query: str | None = None) -> bool:
        """Пробует установить блокировку для item_id; True, если успешна."""

        key = _LOCK_PREFIX + str(item_id)
        try:
            acquired = await self._redis.set(key, query or "", nx=True, ex=self._ttl)
        except Exception as exc:  # pragma: no cover - лог, чтобы не потерять ошибку
            _logger.warning("lock_error item=%s query=%s err=%s", item_id, query, exc)
            return False
        if acquired:
            _logger.info("lock_acquired item=%s query=%s", item_id, query)
            return True
        _logger.info("lock_skip item=%s query=%s", item_id, query)
        return False

    async def release(self, item_id: str) -> None:
        """Снимает блокировку; ошибки только логируем (KISS)."""

        key = _LOCK_PREFIX + str(item_id)
        try:
            await self._redis.delete(key)
        except Exception as exc:  # pragma: no cover - важнее сохранить лог
            _logger.warning("lock_release_error item=%s err=%s", item_id, exc)
        else:
            _logger.info("lock_released item=%s", item_id)

    async def filter_unlocked(
        self,
        item_ids: Iterable[str],
        *,
        query: str | None = None,
    ) -> List[str]:
        """Возвращает только те item_id, для которых нет активного замка."""

        free: list[str] = []
        for raw_id in item_ids:
            item_id = str(raw_id)
            key = _LOCK_PREFIX + item_id
            try:
                locked = await self._redis.exists(key)
            except Exception as exc:  # pragma: no cover
                _logger.warning("lock_exists_error item=%s err=%s", item_id, exc)
                locked = False
            if locked:
                _logger.info("filter_skip item=%s query=%s", item_id, query)
                continue
            free.append(item_id)
        return free
