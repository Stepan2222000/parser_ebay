"""Логирует и считает отправки сообщений в Redis Stream."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

from redis.asyncio import Redis

_COUNTER_KEY = os.getenv("REDIS_STREAM_COUNTER_KEY", "redis_stream:sent_total")
_LOG_FILE = os.getenv("REDIS_STREAM_LOG_FILE", "redis_stream_payloads.log")

_logger = logging.getLogger("redis_stream_recorder")
if not _logger.handlers:
    handler = logging.FileHandler(_LOG_FILE, encoding="utf-8")
    handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    _logger.addHandler(handler)
_logger.setLevel(logging.INFO)
_logger.propagate = False


class RedisStreamRecorder:
    """Ведёт счётчик отправок и пишет payload в лог-файл."""

    def __init__(self, redis_client: Redis, counter_key: str = _COUNTER_KEY) -> None:
        self._redis = redis_client
        self._counter_key = counter_key

    async def record(self, item_id: str, payload: dict[str, Any], entry_id: str) -> None:
        """Увеличивает счётчик и логирует содержимое сообщения."""

        try:
            await self._redis.incr(self._counter_key)
        except Exception as exc:  # pragma: no cover
            _logger.warning(
                "counter_increment_failed item=%s entry=%s err=%s", item_id, entry_id, exc
            )
        try:
            dump = json.dumps({item_id: payload}, ensure_ascii=False)
        except Exception as exc:  # pragma: no cover
            _logger.warning("payload_dump_failed item=%s entry=%s err=%s", item_id, entry_id, exc)
            dump = str(payload)
        _logger.info("entry=%s item=%s payload=%s", entry_id, item_id, dump)
