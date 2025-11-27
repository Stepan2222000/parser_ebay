#!/usr/bin/env python3
"""Полная очистка: БД, Redis, логи."""

import asyncio
import os
from pathlib import Path

from redis.asyncio import Redis
from sqlalchemy import text
from dotenv import load_dotenv

# Загрузка .env
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(env_path, override=True)

# Импорт DataBase из package_commit
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
from package_commit import DataBase


async def clean_redis():
    """Очистка внешнего Redis."""
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    password = os.getenv("REDIS_PASSWORD")

    print(f"\n🧹 Очистка Redis {host}:{port}...")
    r = Redis(host=host, port=port, password=password, decode_responses=True)

    try:
        all_keys = await r.keys("*")
        if all_keys:
            print(f"   Найдено ключей: {len(all_keys)}")
            deleted = await r.delete(*all_keys)
            print(f"   ✓ Удалено ключей: {deleted}")
        else:
            print("   ✓ Redis уже пуст")

        remaining = await r.dbsize()
        print(f"   ✓ Осталось: {remaining}")
    finally:
        await r.aclose()


async def clean_database():
    """Очистка таблиц PostgreSQL."""
    print("\n🗄️  Очистка PostgreSQL...")

    # Инициализация подключения
    DataBase.init()

    async with DataBase.session_marker() as session:
        # Удаляем все из item_specifics (зависимая таблица)
        result = await session.execute(text("DELETE FROM item_specifics"))
        print(f"   ✓ Удалено из item_specifics: {result.rowcount} записей")

        # Удаляем все из ebay
        result = await session.execute(text("DELETE FROM ebay"))
        print(f"   ✓ Удалено из ebay: {result.rowcount} записей")

        # Коммит
        await session.commit()

        # Проверка
        result = await session.execute(text("SELECT COUNT(*) FROM ebay"))
        count = result.scalar()
        print(f"   ✓ Осталось в ebay: {count}")


def clean_logs():
    """Удаление всех лог-файлов."""
    print("\n📝 Удаление лог-файлов...")

    project_root = Path(__file__).resolve().parent
    log_patterns = [
        "*.log",
        "duplicate_cache.txt",
    ]

    deleted_count = 0
    for pattern in log_patterns:
        for log_file in project_root.glob(pattern):
            if log_file.is_file():
                size = log_file.stat().st_size
                log_file.unlink()
                print(f"   ✓ Удалён: {log_file.name} ({size} байт)")
                deleted_count += 1

    if deleted_count == 0:
        print("   ✓ Лог-файлов не найдено")
    else:
        print(f"   ✓ Всего удалено файлов: {deleted_count}")


async def main():
    """Главная функция."""
    print("=" * 60)
    print("ПОЛНАЯ ОЧИСТКА СИСТЕМЫ")
    print("=" * 60)

    # 1. Redis
    await clean_redis()

    # 2. PostgreSQL
    await clean_database()

    # 3. Лог-файлы
    clean_logs()

    print("\n" + "=" * 60)
    print("✅ ВСЁ ОЧИЩЕНО!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
