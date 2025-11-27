#!/usr/bin/env python3
"""Проверка результатов парсинга в БД"""

import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text


async def check_results():
    """Проверяет результаты парсинга"""
    engine = create_async_engine(
        'postgresql+asyncpg://admin:Password123@81.30.105.134:5421/eb'
    )

    async with engine.begin() as conn:
        # Проверяем таблицу count
        result = await conn.execute(text("SELECT COUNT(*) as cnt FROM count"))
        count_total = result.scalar()
        print(f"📊 Таблица count: {count_total} записей")

        if count_total > 0:
            result = await conn.execute(text("SELECT query, value FROM count ORDER BY value DESC LIMIT 10"))
            print("\n🔝 Топ-10 запросов по количеству запусков:")
            for row in result:
                print(f"  • {row.query}: {row.value} раз")

        # Проверяем таблицу ebay
        result = await conn.execute(text("SELECT COUNT(*) as cnt FROM ebay"))
        ebay_total = result.scalar()
        print(f"\n📦 Таблица ebay: {ebay_total} товаров")

        if ebay_total > 0:
            result = await conn.execute(text("""
                SELECT number, title, price, seller, query
                FROM ebay
                ORDER BY id DESC
                LIMIT 5
            """))
            print("\n🆕 Последние 5 товаров:")
            for row in result:
                print(f"  • [{row.query}] {row.title[:50] if row.title else 'N/A'}")
                print(f"    Цена: ${row.price}, Продавец: {row.seller}, ID: {row.number}")

        # Проверяем таблицу item_specifics
        result = await conn.execute(text("SELECT COUNT(*) as cnt FROM item_specifics"))
        specs_total = result.scalar()
        print(f"\n🔍 Таблица item_specifics: {specs_total} характеристик")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(check_results())
