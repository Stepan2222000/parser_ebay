from __future__ import annotations

"""Провайдер лимита цены по SMART.

Назначение: опционально получать лимит цены по артикулу через 2 БД
(parts_admin → smart, ebay_admin → view parts_prices). Используется в Excel‑режиме.
"""

import os
import re
from pathlib import Path
from typing import Optional

import asyncpg

try:
    # Что: пробуем подгрузить переменные из .env
    # Зачем: конфиг хранится в ebay_my_news_done_without_del/.env
    from dotenv import load_dotenv  # type: ignore

    _here = Path(__file__).resolve().parent
    env_path = Path("/root/new_service/ebay_my_news_done_without_del/.env")
    if not env_path.exists():
        env_path = _here / ".env"
    load_dotenv(env_path, override=True)
except Exception:
    # KISS: игнорируем отсутствие python-dotenv/файлов
    pass


_PARTS_POOL: Optional[asyncpg.Pool] = None
_EBAY_POOL: Optional[asyncpg.Pool] = None
_ARTICLE2SMART: dict[str, str] = {}
_MAPPING_LOADED = False


def enabled() -> bool:
    """Возвращает True, если модуль включён (USE_SMART_PRICE=1)."""
    v = os.getenv("USE_SMART_PRICE", "0").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _norm(article: str) -> str:
    """Нормализует артикул: upper + удалить все не [A-Z0-9]."""
    s = article.upper()
    return re.sub(r"[^A-Z0-9]", "", s)


async def _get_parts_pool() -> asyncpg.Pool:
    """Ленивое создание пула к parts_admin."""
    global _PARTS_POOL
    if _PARTS_POOL:
        return _PARTS_POOL
    host = os.getenv("PARTS_ADMIN_HOST", "localhost")
    port = int(os.getenv("PARTS_ADMIN_PORT", "5432"))
    user = os.getenv("PARTS_ADMIN_USER", "admin")
    password = os.getenv("PARTS_ADMIN_PASSWORD", "")
    database = os.getenv("PARTS_ADMIN_DB", "parts_admin")
    _PARTS_POOL = await asyncpg.create_pool(
        host=host, port=port, user=user, password=password, database=database,
        min_size=1, max_size=2,
    )
    return _PARTS_POOL


async def _get_ebay_pool() -> asyncpg.Pool:
    """Ленивое создание пула к ebay_admin."""
    global _EBAY_POOL
    if _EBAY_POOL:
        return _EBAY_POOL
    host = os.getenv("EBAY_ADMIN_HOST", "localhost")
    port = int(os.getenv("EBAY_ADMIN_PORT", "5432"))
    user = os.getenv("EBAY_ADMIN_USER", "admin")
    password = os.getenv("EBAY_ADMIN_PASSWORD", "")
    database = os.getenv("EBAY_ADMIN_DB", "ebay_admin")
    _EBAY_POOL = await asyncpg.create_pool(
        host=host, port=port, user=user, password=password, database=database,
        min_size=1, max_size=2,
    )
    return _EBAY_POOL


async def _load_mapping_once() -> None:
    """Единожды загружает карту: нормализованный_артикул → smart."""
    global _MAPPING_LOADED
    if _MAPPING_LOADED:
        return
    pool = await _get_parts_pool()
    # Что: читаем smart и массив "артикул"
    # Зачем: сформировать быстрый поиск по нормализованному артикулу
    async with pool.acquire() as conn:
        rows = await conn.fetch('SELECT smart, "артикул" FROM smart')
    for r in rows:
        smart: str = r["smart"]
        articles = r["артикул"] or []
        for a in articles:
            na = _norm(str(a))
            if na and na not in _ARTICLE2SMART:
                _ARTICLE2SMART[na] = smart
    _MAPPING_LOADED = True


async def get_smart(article: str) -> Optional[str]:
    """Возвращает smart по артикулу (или None, если не найден)."""
    await _load_mapping_once()
    return _ARTICLE2SMART.get(_norm(article))


async def get_price_by_smart(smart: str) -> Optional[float]:
    """Возвращает минимальную из цен (market, user) по smart из view parts_prices."""
    pool = await _get_ebay_pool()
    sql = (
        "SELECT min_price_by_market, min_price_by_user "
        "FROM parts_prices WHERE smart = $1"
    )
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, smart)
    if not row:
        return None
    market = row["min_price_by_market"]
    user = row["min_price_by_user"]
    vals = [float(v) for v in (market, user) if v is not None]
    return min(vals) if vals else None


async def get_limit_for_article(article: str, excel_limit: Optional[float]) -> Optional[float]:
    """Возвращает лимит для артикула: сначала из SMART, иначе из Excel.

    Правило: берём минимальную доступную цену из (market, user). Если цены
    в SMART нет — возвращаем excel_limit (может быть None).
    """
    smart = await get_smart(article)
    if not smart:
        return excel_limit
    price = await get_price_by_smart(smart)
    return price if price is not None else excel_limit


async def aclose() -> None:
    """Закрывает пулы соединений (если создавались)."""
    global _PARTS_POOL, _EBAY_POOL
    if _PARTS_POOL is not None:
        await _PARTS_POOL.close()
        _PARTS_POOL = None
    if _EBAY_POOL is not None:
        await _EBAY_POOL.close()
        _EBAY_POOL = None
