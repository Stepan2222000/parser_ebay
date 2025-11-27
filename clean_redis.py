#!/usr/bin/env python3
"""Скрипт для очистки внешнего Redis."""

import asyncio
import os
from pathlib import Path

from redis.asyncio import Redis
from dotenv import load_dotenv

# Загрузка .env
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(env_path, override=True)


async def main():
    """Удаляет все ключи во внешнем Redis."""

    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    password = os.getenv("REDIS_PASSWORD")

    print(f"Подключение к Redis {host}:{port}...")
    r = Redis(host=host, port=port, password=password, decode_responses=True)

    try:
        # Получаем все ключи
        all_keys = await r.keys("*")
        print(f"Найдено ключей: {len(all_keys)}")

        if not all_keys:
            print("Redis уже пуст!")
            return

        # Показываем что будем удалять
        print("\nБудут удалены следующие ключи:")
        for key in sorted(all_keys):
            key_str = key.decode() if isinstance(key, bytes) else key
            key_type = await r.type(key_str)
            print(f"  {key_type:10s} {key_str}")

        # Удаляем все ключи
        print(f"\nУдаление {len(all_keys)} ключей...")
        deleted = await r.delete(*all_keys)
        print(f"✓ Удалено ключей: {deleted}")

        # Проверяем результат
        remaining = await r.dbsize()
        print(f"✓ Осталось ключей: {remaining}")

        if remaining == 0:
            print("\n✅ Redis полностью очищен!")
        else:
            print(f"\n⚠️  Осталось {remaining} ключей")

    finally:
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
