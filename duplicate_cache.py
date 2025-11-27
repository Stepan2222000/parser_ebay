from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
import socket
from typing import Iterable, Optional, Sequence, Set

from redis.asyncio import Redis
from sqlalchemy import text

try:
    # package mode
    from .package_commit import DataBase  # type: ignore
except Exception:  # pragma: no cover - top-level execution
    from package_commit import DataBase  # type: ignore


def _as_bool(value: Optional[str]) -> bool:
    if not value:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


_ENABLED: bool = _as_bool(os.getenv("DUPLICATE_CACHE_ENABLED"))
_CACHE: Set[str] = set()
_LOADED: bool = False
_FILE_PATH: Optional[Path] = None
_LOGGER: logging.Logger = logging.getLogger("duplicate_cache")
_LOCK: asyncio.Lock = asyncio.Lock()

_REDIS_KEY: str = os.getenv("DUPLICATE_CACHE_REDIS_KEY", "duplicate_cache:ids")
_REDIS_HOST: str = os.getenv("DUPLICATE_CACHE_REDIS_HOST", os.getenv("REDIS_HOST", "redis"))
_REDIS_PORT: int = int(os.getenv("DUPLICATE_CACHE_REDIS_PORT", os.getenv("REDIS_PORT", "6379")))
_REDIS_PASSWORD: Optional[str] = os.getenv(
    "DUPLICATE_CACHE_REDIS_PASSWORD", os.getenv("REDIS_PASSWORD")
)
_REDIS_DB: int = int(os.getenv("DUPLICATE_CACHE_REDIS_DB", "0"))
_redis_client: Optional[Redis] = None
_redis_lock: asyncio.Lock = asyncio.Lock()

_BOOTSTRAP_LOGGER: logging.Logger = logging.getLogger("duplicate_cache.bootstrap")
_BOOTSTRAP_LOG_FILE: str = os.getenv(
    "DUPLICATE_CACHE_BOOTSTRAP_LOG_FILE", "duplicate_cache_bootstrap.log"
)
_BOOTSTRAP_LOCK_KEY: str = os.getenv(
    "DUPLICATE_CACHE_BOOTSTRAP_LOCK_KEY", "duplicate_cache:bootstrap_lock"
)
_BOOTSTRAP_DONE_KEY: str = os.getenv(
    "DUPLICATE_CACHE_BOOTSTRAP_DONE_KEY", "duplicate_cache:bootstrap_done"
)
_BOOTSTRAP_LOCK_TTL: int = int(os.getenv("DUPLICATE_CACHE_BOOTSTRAP_LOCK_TTL", "300"))
_BOOTSTRAP_DONE_TTL: int = int(os.getenv("DUPLICATE_CACHE_BOOTSTRAP_DONE_TTL", "900"))
_BOOTSTRAP_WAIT_TIMEOUT: float = float(
    os.getenv("DUPLICATE_CACHE_BOOTSTRAP_WAIT_TIMEOUT", "300")
)
_BOOTSTRAP_WAIT_INTERVAL: float = float(
    os.getenv("DUPLICATE_CACHE_BOOTSTRAP_WAIT_INTERVAL", "0.5")
)


def enabled() -> bool:
    return _ENABLED


def _configure_logger(project_root: Path) -> None:
    if _LOGGER.handlers:
        return
    log_file = os.getenv("DUPLICATE_CACHE_LOG_FILE") or "duplicate_cache.log"
    path = Path(log_file)
    if not path.is_absolute():
        path = project_root / log_file
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    _LOGGER.setLevel(logging.INFO)
    _LOGGER.addHandler(handler)
    _LOGGER.propagate = False


def _configure_bootstrap_logger(project_root: Path) -> None:
    if _BOOTSTRAP_LOGGER.handlers:
        return
    path = Path(_BOOTSTRAP_LOG_FILE)
    if not path.is_absolute():
        path = project_root / path
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    _BOOTSTRAP_LOGGER.setLevel(logging.INFO)
    _BOOTSTRAP_LOGGER.addHandler(handler)
    _BOOTSTRAP_LOGGER.propagate = False


async def bootstrap(project_root: Path) -> None:
    """Загружает кеш из файла и БД, переносит данные в Redis и локальный set."""

    global _LOADED, _FILE_PATH

    if not enabled() or _LOADED:
        return

    _configure_logger(project_root)
    _configure_bootstrap_logger(project_root)

    file_name = os.getenv("DUPLICATE_CACHE_FILE") or "duplicate_cache.txt"
    file_path = Path(file_name)
    if not file_path.is_absolute():
        file_path = project_root / file_name
    file_path.parent.mkdir(parents=True, exist_ok=True)
    _FILE_PATH = file_path

    redis = await _get_redis()
    lock_acquired = False
    lock_owner = f"{socket.gethostname()}:{os.getpid()}"
    waited_for_peer = False

    if redis is not None:
        try:
            lock_acquired = bool(
                await redis.set(_BOOTSTRAP_LOCK_KEY, lock_owner, ex=_BOOTSTRAP_LOCK_TTL, nx=True)
            )
            if lock_acquired:
                _BOOTSTRAP_LOGGER.info(
                    "bootstrap_lock_acquired owner=%s lock_key=%s",
                    lock_owner,
                    _BOOTSTRAP_LOCK_KEY,
                )
        except Exception as exc:  # pragma: no cover - фиксируем, но не блокируем запуск
            _BOOTSTRAP_LOGGER.warning("bootstrap_lock_error action=acquire err=%s", exc)

        if not lock_acquired:
            waited_for_peer = True
            _BOOTSTRAP_LOGGER.info(
                "bootstrap_wait_for_peer owner=%s lock_key=%s", lock_owner, _BOOTSTRAP_LOCK_KEY
            )
            lock_acquired = await _wait_for_peer_or_lock(redis, lock_owner)

        if not lock_acquired:
            done_by = await _read_bootstrap_done(redis)
            if done_by:
                _BOOTSTRAP_LOGGER.info(
                    "bootstrap_skip owner=%s done_by=%s", lock_owner, done_by
                )
                file_ids = _read_cache_file(file_path)
                await _populate_redis(sorted(file_ids))
                await _hydrate_local_cache()
                _LOADED = True
                return
            _BOOTSTRAP_LOGGER.warning(
                "bootstrap_fallback_without_lock owner=%s lock_key=%s",
                lock_owner,
                _BOOTSTRAP_LOCK_KEY,
            )
    else:
        _BOOTSTRAP_LOGGER.warning(
            "bootstrap_no_redis owner=%s action=fallback_without_lock", lock_owner
        )

    try:
        db_ids: Set[str] = set()
        try:
            async with DataBase.session_marker() as session:
                result = await session.execute(text("select number from ebay"))
                for row in result:
                    number = row[0]
                    if number is not None:
                        db_ids.add(str(number))
        except Exception as exc:
            _LOGGER.warning("cache_db_load_error err=%s", exc)

        missing_count = 0
        async with _LOCK:
            file_ids = _read_cache_file(file_path)
            missing = sorted(db_ids - file_ids)
            missing_count = len(missing)
            if missing:
                await _append_lines(missing)
                file_ids.update(missing)

            combined = sorted(file_ids | db_ids)
            await _populate_redis(combined)
            await _hydrate_local_cache()

        if redis is not None and lock_acquired:
            try:
                await redis.set(_BOOTSTRAP_DONE_KEY, lock_owner, ex=_BOOTSTRAP_DONE_TTL)
            except Exception as exc:  # pragma: no cover - важен лог, не падение
                _BOOTSTRAP_LOGGER.warning(
                    "bootstrap_done_mark_failed owner=%s err=%s", lock_owner, exc
                )

        _LOADED = True
        _LOGGER.info(
            "cache_initialized size=%s file=%s added_from_db=%s redis_key=%s",
            len(_CACHE),
            file_path,
            missing_count,
            _REDIS_KEY,
        )
    finally:
        if redis is not None and lock_acquired:
            try:
                await redis.delete(_BOOTSTRAP_LOCK_KEY)
                _BOOTSTRAP_LOGGER.info(
                    "bootstrap_lock_released owner=%s waited_for_peer=%s",
                    lock_owner,
                    waited_for_peer,
                )
            except Exception as exc:  # pragma: no cover
                _BOOTSTRAP_LOGGER.warning(
                    "bootstrap_lock_release_failed owner=%s err=%s", lock_owner, exc
                )


async def contains(item_id: str) -> bool:
    """Проверяет наличие item_id в кеше (локальном или Redis)."""
    if not enabled():
        return False

    normalized = _normalize_item_id(item_id)
    if not normalized:
        return False

    if normalized in _CACHE:
        return True

    redis = await _get_redis()
    if redis is None:
        return False

    try:
        exists = await redis.sismember(_REDIS_KEY, normalized)
    except Exception as exc:  # pragma: no cover - фиксируем, но не останавливаем процесс
        _LOGGER.warning("cache_redis_check_error item=%s err=%s", normalized, exc)
        return False

    if exists:
        _CACHE.add(normalized)
    return bool(exists)


async def record_seen(item_id: str, *, query: Optional[str] = None) -> None:
    """Добавляет новый item_id в кеш (Redis + файл)."""

    if not enabled():
        return

    normalized = _normalize_item_id(item_id)
    if not normalized:
        return

    async with _LOCK:
        redis = await _get_redis()
        added_to_redis = False
        redis_available = redis is not None
        if redis is not None:
            try:
                added_to_redis = bool(await redis.sadd(_REDIS_KEY, normalized))
            except Exception as exc:  # pragma: no cover
                redis_available = False
                _LOGGER.warning("cache_redis_add_error item_id=%s err=%s", normalized, exc)

        if normalized not in _CACHE:
            _CACHE.add(normalized)

        if added_to_redis or not redis_available:
            await _append_lines([normalized])
            _LOGGER.info("cache_append item_id=%s query=%s", normalized, query)


def log_skip(item_id: str, query: Optional[str] = None) -> None:
    if not enabled():
        return
    _LOGGER.info("cache_skip item_id=%s query=%s", item_id, query)


async def _append_lines(lines: Iterable[str]) -> None:
    if not lines or _FILE_PATH is None:
        return
    data = "".join(f"{line}\n" for line in lines)
    await asyncio.to_thread(_sync_append, data)


def _sync_append(data: str) -> None:
    if _FILE_PATH is None:
        return
    with _FILE_PATH.open("a", encoding="utf-8") as fh:
        fh.write(data)


def _normalize_item_id(value: str | int | None) -> str:
    if value is None:
        return ""
    return str(value).strip()


async def _get_redis() -> Optional[Redis]:
    global _redis_client
    if _redis_client is not None:
        return _redis_client

    async with _redis_lock:
        if _redis_client is None:
            try:
                _redis_client = Redis(
                    host=_REDIS_HOST,
                    port=_REDIS_PORT,
                    password=_REDIS_PASSWORD,
                    db=_REDIS_DB,
                )
            except Exception as exc:  # pragma: no cover
                _LOGGER.warning("cache_redis_init_error err=%s", exc)
                return None
    return _redis_client


async def _populate_redis(ids: Sequence[str]) -> None:
    if not ids:
        return
    redis = await _get_redis()
    if redis is None:
        _CACHE.update(ids)
        return

    try:
        for chunk in _chunked(ids, 1000):
            await redis.sadd(_REDIS_KEY, *chunk)
    except Exception as exc:  # pragma: no cover
        _LOGGER.warning("cache_redis_populate_error err=%s", exc)
    else:
        _CACHE.update(ids)


async def _hydrate_local_cache() -> None:
    redis = await _get_redis()
    if redis is None:
        return
    try:
        members = await redis.smembers(_REDIS_KEY)
    except Exception as exc:  # pragma: no cover
        _LOGGER.warning("cache_redis_load_error err=%s", exc)
        return

    decoded = {
        member.decode("utf-8") if isinstance(member, bytes) else str(member)
        for member in members
        if member
    }
    _CACHE.update(decoded)


def _chunked(seq: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for idx in range(0, len(seq), size):
        yield seq[idx : idx + size]


async def _wait_for_peer_or_lock(redis: Redis, lock_owner: str) -> bool:
    if _BOOTSTRAP_WAIT_TIMEOUT <= 0:
        return False
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # pragma: no cover - на случай вызова вне цикла
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    deadline = loop.time() + _BOOTSTRAP_WAIT_TIMEOUT
    while loop.time() < deadline:
        done_by = await _read_bootstrap_done(redis)
        if done_by:
            return False
        try:
            lock_acquired = bool(
                await redis.set(
                    _BOOTSTRAP_LOCK_KEY, lock_owner, ex=_BOOTSTRAP_LOCK_TTL, nx=True
                )
            )
        except Exception as exc:  # pragma: no cover
            _BOOTSTRAP_LOGGER.warning("bootstrap_lock_error action=reacquire err=%s", exc)
            lock_acquired = False
        if lock_acquired:
            _BOOTSTRAP_LOGGER.info(
                "bootstrap_lock_acquired_after_wait owner=%s lock_key=%s",
                lock_owner,
                _BOOTSTRAP_LOCK_KEY,
            )
            return True
        try:
            exists = await redis.exists(_BOOTSTRAP_LOCK_KEY)
        except Exception as exc:  # pragma: no cover
            _BOOTSTRAP_LOGGER.warning("bootstrap_lock_exists_error err=%s", exc)
            exists = True
        if not exists:
            continue
        await asyncio.sleep(_BOOTSTRAP_WAIT_INTERVAL)
    return False


async def _read_bootstrap_done(redis: Redis) -> str:
    try:
        value = await redis.get(_BOOTSTRAP_DONE_KEY)
    except Exception as exc:  # pragma: no cover
        _BOOTSTRAP_LOGGER.warning("bootstrap_done_read_error err=%s", exc)
        return ""
    if not value:
        return ""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:  # pragma: no cover
            return repr(value)
    return str(value)


def _read_cache_file(file_path: Path) -> Set[str]:
    cache_ids: Set[str] = set()
    if not file_path.exists():
        return cache_ids
    try:
        raw = file_path.read_text(encoding="utf-8")
    except Exception as exc:  # pragma: no cover
        _LOGGER.warning("cache_file_read_error path=%s err=%s", file_path, exc)
        return cache_ids
    for line in raw.splitlines():
        line = line.strip()
        if line:
            cache_ids.add(line)
    return cache_ids
