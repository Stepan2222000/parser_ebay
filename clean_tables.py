#!/usr/bin/env python3
"""Скрипт для очистки всех таблиц в БД"""

import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text


async def clean_all_tables():
    """Очищает все таблицы в БД"""
    engine = create_async_engine(
        'postgresql+asyncpg://admin:Password123@81.30.105.134:5421/eb'
    )

    async with engine.begin() as conn:
        print("Очистка таблиц...")

        # Отключаем проверку внешних ключей
        await conn.execute(text("SET session_replication_role = 'replica';"))

        # Очищаем все таблицы
        tables = [
            'item_specifics',
            'ebay',
            'count',
            'ebay_input',
            'input_articles',
            'matched_articles',
            'proxies',
            'task'
        ]

        for table in tables:
            try:
                await conn.execute(text(f"TRUNCATE TABLE {table} CASCADE;"))
                print(f"✓ Очищена таблица {table}")
            except Exception as e:
                print(f"✗ Ошибка при очистке {table}: {e}")

        # Включаем обратно проверку внешних ключей
        await conn.execute(text("SET session_replication_role = 'origin';"))

        # Проверяем результат
        print("\n📊 Результаты после очистки:")
        for table in tables:
            try:
                result = await conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                print(f"  • {table}: {count} записей")
            except Exception as e:
                print(f"  • {table}: ошибка - {e}")

    await engine.dispose()
    print("\n✅ Очистка завершена!")


if __name__ == "__main__":
    asyncio.run(clean_all_tables())
