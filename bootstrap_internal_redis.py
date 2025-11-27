from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Iterable

from redis.asyncio import Redis


PROJECT_ROOT = Path(__file__).resolve().parent
PROXIES_FILE = PROJECT_ROOT / "proxies.txt"
DEFAULT_PROXY_COOKIE = "{}"
REDIS_DBS_TO_FLUSH: tuple[int, ...] = (0, 2, 4, 5)


async def _flush_db(host: str, db_index: int) -> None:
    client = Redis(host=host, db=db_index)
    try:
        await client.flushdb()
    finally:
        await client.aclose()


def _load_proxy_servers(path: Path) -> Iterable[str]:
    if not path.exists():
        return []
    servers: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(":")
        if len(parts) < 2:
            continue
        host, port = parts[0], parts[1]
        servers.append(f"{host}:{port}")
    return servers


async def _seed_proxy_cookies(host: str, proxies: Iterable[str]) -> None:
    client = Redis(host=host, db=4)
    try:
        await asyncio.gather(
            *(client.set(server, DEFAULT_PROXY_COOKIE) for server in proxies)
        )
    finally:
        await client.aclose()


async def main() -> None:
    redis_host = os.getenv("INTERNAL_REDIS_HOST", "redis")

    for db_index in REDIS_DBS_TO_FLUSH:
        await _flush_db(redis_host, db_index)

    proxy_servers = list(_load_proxy_servers(PROXIES_FILE))
    if proxy_servers:
        await _seed_proxy_cookies(redis_host, proxy_servers)


if __name__ == "__main__":
    asyncio.run(main())
