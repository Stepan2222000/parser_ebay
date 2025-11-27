from __future__ import annotations

"""Контроль актуальности объявлений по изменению title."""

import logging
import os
import asyncio
from typing import Dict, List, Optional
from pathlib import Path
from json import dumps

import asyncpg

try:
    # Что: подробное логирование через excel_verbose.log при доступности
    # Зачем: фиксировать случаи переиндексации title
    from . import excel_log  # package режим
except Exception:  # pragma: no cover - режим исполнения как скрипт
    try:
        import excel_log  # type: ignore
    except Exception:  # noqa: F401
        excel_log = None  # type: ignore


_POOL: Optional[asyncpg.Pool] = None


def _as_bool(value: Optional[str]) -> bool:
    """Возвращает True, если значение окружения трактуется как включённое."""
    if not value:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def enabled() -> bool:
    """Проверяет флаг RECHECK_TITLES в окружении."""
    return _as_bool(os.getenv("RECHECK_TITLES"))


def log_enabled() -> bool:
    """Проверяет флаг расширенного логирования RECHECK_TITLES_LOG."""
    return _as_bool(os.getenv("RECHECK_TITLES_LOG"))


async def _get_pool() -> asyncpg.Pool:
    """Создаёт (лениво) пул asyncpg для прямого подключения к Postgres."""
    global _POOL
    if _POOL is not None:
        return _POOL

    host = os.getenv("RECHECK_DB_HOST", "81.30.105.134")
    port = int(os.getenv("RECHECK_DB_PORT", "5421"))
    user = os.getenv("RECHECK_DB_USER", "admin")
    password = os.getenv("RECHECK_DB_PASSWORD", "Password123")
    database = os.getenv("RECHECK_DB_NAME", "eb")

    _POOL = await asyncpg.create_pool(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        min_size=1,
        max_size=4,
    )
    return _POOL


async def recheck_and_delete_if_changed(
    titles_by_id: Dict[int, Optional[str]],
    query: str,
) -> List[int]:
    """Удаляет устаревшие записи, если title отличается от каталога."""
    if not titles_by_id:
        return []

    ids: List[int] = list(titles_by_id.keys())

    for attempt in range(3):
        try:
            pool = await _get_pool()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    rows = await conn.fetch(
                        "SELECT number, title FROM ebay WHERE number = ANY($1::bigint[])",
                        ids,
                    )
                    return await _process_rows(conn, rows, titles_by_id, query)
        except Exception as exc:
            if attempt == 2:
                raise exc
            await asyncio.sleep(0.1 * (attempt + 1))

    return []


async def _process_rows(
    conn: asyncpg.Connection,
    rows: List[asyncpg.Record],
    titles_by_id: Dict[int, Optional[str]],
    query: str,
) -> List[int]:
    """Сравнивает заголовки и удаляет устаревшие записи."""
    db_titles = {int(row["number"]): row["title"] for row in rows}

    changed: List[int] = []
    for num, new_title in titles_by_id.items():
        old_title = db_titles.get(num)
        if old_title is not None and old_title != new_title:
            changed.append(num)
            if log_enabled():
                logging.info(
                    "title_changed number=%s old=%r new=%r query=%r",
                    num,
                    old_title,
                    new_title,
                    query,
                )
            try:
                _append_mismatch(num, old_title, new_title, query)
            except Exception:
                pass
            try:
                if excel_log and excel_log.enabled():  # type: ignore[attr-defined]
                    excel_log.log_event(  # type: ignore[attr-defined]
                        "title_changed",
                        number=num,
                        old=old_title,
                        new=new_title,
                        query=query,
                    )
            except Exception:
                pass

    if not changed:
        return []

    await conn.execute(
        """
        DELETE FROM item_specifics
        WHERE ebay_id IN (
            SELECT id FROM ebay WHERE number = ANY($1::bigint[])
        )
        """,
        changed,
    )
    await conn.execute(
        "DELETE FROM ebay WHERE number = ANY($1::bigint[])",
        changed,
    )
    return changed


async def aclose() -> None:
    """Закрывает пул asyncpg (используется в тестах/при завершении)."""
    global _POOL
    if _POOL is not None:
        await _POOL.close()
        _POOL = None


def _log_file_path() -> Path:
    """Возвращает путь к файлу-реестру несовпадений title (JSONL)."""
    name = os.getenv("RECHECK_TITLES_FILE") or "title_mismatch.log"
    return Path(__file__).resolve().parent / name


def _append_mismatch(number: int, old: Optional[str], new: Optional[str], query: str) -> None:
    """Добавляет одну запись о несовпадении в JSONL-файл.

    Что: одна строка = один JSON с полями number, old, new, query.
    Зачем: последующий анализ примеров вне кода (KISS).
    """
    payload = {
        "number": number,
        "old": old,
        "new": new,
        "query": query,
    }
    path = _log_file_path()
    # Что: дозапись в конец; Зачем: лог растёт по мере работы
    path.write_text("", encoding="utf-8") if not path.exists() else None
    with path.open("a", encoding="utf-8") as f:
        f.write(dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
