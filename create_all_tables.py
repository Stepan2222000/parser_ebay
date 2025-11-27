#!/usr/bin/env python3
"""Скрипт для создания всех необходимых таблиц в БД"""

import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text


async def create_all_tables():
    """Создает все таблицы если они не существуют"""
    engine = create_async_engine(
        'postgresql+asyncpg://admin:Password123@81.30.105.134:5421/eb'
    )

    async with engine.begin() as conn:
        print("Создание таблиц...")

        # 1. Таблица count
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS count (
                id BIGSERIAL PRIMARY KEY,
                query VARCHAR(255) NOT NULL UNIQUE,
                value INTEGER NOT NULL DEFAULT 0
            );
        """))
        print("✓ Таблица count")

        # 2. Таблица ebay (основная таблица товаров)
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ebay (
                id BIGSERIAL PRIMARY KEY,
                query VARCHAR(255),
                number BIGINT UNIQUE NOT NULL,
                price NUMERIC(10,2),
                price_without_delivery NUMERIC(10,2),
                location VARCHAR(510),
                condition VARCHAR(255),
                title VARCHAR(255),
                delivery_price NUMERIC(10,2),
                seller VARCHAR(255),
                datetime_iso TIMESTAMP WITH TIME ZONE,
                cycle INTEGER DEFAULT 0,
                archive BOOLEAN DEFAULT FALSE,
                not_actual BOOLEAN DEFAULT FALSE
            );
        """))
        print("✓ Таблица ebay")

        # 3. Таблица ebay_input
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ebay_input (
                c0 TEXT
            );
        """))
        print("✓ Таблица ebay_input")

        # 4. Таблица input_articles
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS input_articles (
                id BIGSERIAL PRIMARY KEY,
                all_articles TEXT[],
                max_price NUMERIC(12,2),
                sent_at TIMESTAMP WITH TIME ZONE
            );
        """))
        print("✓ Таблица input_articles")

        # 5. Таблица item_specifics (характеристики товаров)
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS item_specifics (
                id BIGSERIAL PRIMARY KEY,
                key VARCHAR(120),
                value VARCHAR(1280),
                ebay_id BIGINT REFERENCES ebay(id) ON DELETE CASCADE
            );
        """))
        print("✓ Таблица item_specifics")

        # Создаем индекс для item_specifics если его нет
        await conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_item_specifics_ebay_id
            ON item_specifics(ebay_id);
        """))
        print("  ✓ Индекс idx_item_specifics_ebay_id")

        # 6. Таблица matched_articles
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS matched_articles (
                articles TEXT,
                brand TEXT,
                price DOUBLE PRECISION,
                title TEXT,
                database TEXT,
                source_article TEXT
            );
        """))
        print("✓ Таблица matched_articles")

        # 7. Таблица proxies
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS proxies (
                proxy VARCHAR(64) PRIMARY KEY,
                wait TIMESTAMP WITH TIME ZONE
            );
        """))
        print("✓ Таблица proxies")

        # 8. Таблица task
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS task (
                value VARCHAR(255) PRIMARY KEY,
                priority INTEGER DEFAULT 0
            );
        """))
        print("✓ Таблица task")

        # Проверяем созданные таблицы
        result = await conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """))

        print("\n✓ Все таблицы успешно созданы!\n")
        print("Список таблиц в БД:")
        for row in result:
            print(f"  • {row.table_name}")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(create_all_tables())
