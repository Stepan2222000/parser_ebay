"""Простейший heartbeat: записывает отметку в Redis с заданным интервалом."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

from redis.asyncio import Redis


_DEFAULT_INTERVAL = 30  # секунды
_DEFAULT_TTL = 90       # секунды


def _init_logger() -> logging.Logger:
    log_file = os.getenv("HEARTBEAT_LOG_FILE", "heartbeat.log")
    logger = logging.getLogger("heartbeat")
    if not logger.handlers:
        handler = logging.FileHandler(log_file, encoding="utf-8")
        handler.setFormatter(logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


_LOGGER = _init_logger()


async def _beat(redis: Redis, key: str, interval: int, ttl: int) -> None:
    _LOGGER.info("heartbeat started key=%s interval=%s ttl=%s", key, interval, ttl)
    try:
        while True:
            now = int(time.time())
            try:
                await redis.set(key, now, ex=ttl)
            except Exception as ex:  # не даём heartbeat упасть из-за Redis
                _LOGGER.warning("heartbeat redis error key=%s err=%s", key, ex)
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        _LOGGER.info("heartbeat cancelled key=%s", key)
        raise


def start(redis: Redis, *, worker_id: Optional[str] = None) -> Optional[asyncio.Task[None]]:
    """Создаёт фоновую задачу heartbeat. Возвращает task или None, если отключено."""

    if os.getenv("HEARTBEAT_ENABLED", "1").strip().lower() not in {"1", "true", "yes", "on"}:
        return None

    interval = int(os.getenv("HEARTBEAT_INTERVAL", str(_DEFAULT_INTERVAL)))
    ttl = int(os.getenv("HEARTBEAT_TTL", str(_DEFAULT_TTL)))
    if interval <= 0:
        return None
    ttl = max(ttl, interval * 2)

    key = f"hb:{worker_id or os.getenv('HEARTBEAT_WORKER_ID', 'worker')}"
    return asyncio.create_task(_beat(redis, key, interval, ttl))
