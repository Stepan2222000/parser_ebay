from __future__ import annotations

import os
from typing import Any, Optional

from json import dumps

import redis.asyncio as redis

from redis_stream_logger import RedisStreamRecorder

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


_client: Optional[redis.Redis] = None
_stream_key: Optional[str] = None
_recorder: Optional[RedisStreamRecorder] = None


def _init_from_env() -> None:
    """Поднимает клиента и рекордер один раз (KISS)."""

    global _client, _stream_key, _recorder
    if _client is not None and _recorder is not None:
        return

    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    password = os.getenv("REDIS_PASSWORD")
    _stream_key = os.getenv("REDIS_STREAM_KEY", "ebay_validation_stream")

    _client = redis.Redis(host=host, port=port, password=password)
    _recorder = RedisStreamRecorder(_client)


async def push_product(item_id: str, inner_payload: dict[str, Any]) -> str:
    """Публикует товар в Redis Stream и пишет логи/счётчик."""

    global _client, _stream_key, _recorder
    if _client is None or _stream_key is None or _recorder is None:
        _init_from_env()

    assert _client is not None
    assert _stream_key is not None
    assert _recorder is not None

    outer = {item_id: inner_payload}
    data = dumps(outer, ensure_ascii=False, separators=(",", ":"))
    entry_id = await _client.xadd(_stream_key, {"data": data})
    if isinstance(entry_id, bytes):
        entry_id = entry_id.decode()
    await _recorder.record(item_id, inner_payload, entry_id)
    return entry_id


async def aclose() -> None:
    """Закрывает Redis клиент (если инициализировался)."""

    global _client
    if _client is not None:
        try:
            await _client.aclose()
        finally:
            _client = None
