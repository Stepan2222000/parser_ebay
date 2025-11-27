import asyncio
import logging
import os
from pathlib import Path
from time import time
from typing import Iterable, Tuple, Optional, Dict

try:
    from . import main as core  # package mode
    from .excel_source import read_excel_queries
    from . import excel_log
    from . import request_log
    from . import price_provider_smart as smart_price
except ImportError:
    import main as core  # script mode
    from excel_source import read_excel_queries
    import excel_log
    import request_log
    import price_provider_smart as smart_price


def _env_flag(name: str, default: str = "0") -> bool:
    value = os.getenv(name, default)
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}

def load_dotenv_if_present(path: Path) -> None:
    """Загружает пары KEY=VALUE из .env, если он существует."""
    if not path.exists():
        return
    try:
        for line in path.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' not in line:
                continue
            k, v = line.split('=', 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and v and k not in os.environ:
                os.environ[k] = v
    except Exception:
        # KISS: не падаем из‑за .env, просто продолжаем
        pass


async def _resolve_limit(query: str, max_price: Optional[float]) -> Optional[float]:
    mp = None if max_price is None else float(max_price)
    if smart_price.enabled():
        try:
            mp = await smart_price.get_limit_for_article(query, mp)
        except Exception as ex:
            logging.warning(f"smart_price failed for {query}: {ex}")
    return mp


async def _enqueue_query(query: str, max_price: Optional[float]) -> Optional[float]:
    mp = await _resolve_limit(query, max_price)
    while True:
        try:
            length = await core.redis.llen("cl_my")
            assert length < 10000, "Очередь переполнена"
            logging.info(f"QUERY SEND (excel) - {query} (max_price={mp})")
            await core.task_collect_loop.kiq(
                query=query,
                proxy=next(core.PROXIES),
                max_price=mp,
            )
            return mp
        except Exception as ex:
            logging.warning(ex, stack_info=True)
            await asyncio.sleep(10)


async def _enqueue_pairs(
    pairs: Iterable[Tuple[str, Optional[float]]],
) -> Dict[str, Optional[float]]:
    """Отправляет набор запросов в очередь `cl_my`."""

    queued: Dict[str, Optional[float]] = {}
    for query, max_price in pairs:
        added = await core.redis.sadd(core.CATALOG_QUEUE_DEDUP_SET, query)
        if not added:
            logging.info(f"QUERY DUPLICATE SKIP (excel) - {query}")
            if excel_log.enabled():
                excel_log.log_event(
                    "excel_duplicate_skip",
                    query=query,
                )
            continue
        mp = await _enqueue_query(query, max_price)
        queued[query] = mp
    return queued


def _decode(value: str | bytes) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return value


async def _requeue_stale(
    base_limits: Dict[str, Optional[float]],
    cached_limits: Dict[str, Optional[float]],
) -> bool:
    """Перекидывает зависшие запросы обратно в очередь, если воркир упал."""

    stale_seconds = float(os.getenv("CATALOG_PROCESSING_STALE", "300"))
    threshold = time() - stale_seconds
    stale_queries = await core.redis.zrangebyscore(
        core.CATALOG_PROCESSING_ZSET,
        "-inf",
        threshold,
    )
    if not stale_queries:
        return False

    requeued_any = False
    for raw in stale_queries:
        query = _decode(raw)
        owner = await core.redis.hget(core.CATALOG_OWNER_HASH, query)
        owner_str = _decode(owner) if owner else None
        if owner_str:
            hb_key = f"hb:{owner_str}"
            try:
                ttl = await core.redis_controller.ttl(hb_key)
            except Exception:
                ttl = None
            else:
                if ttl == -1 or (ttl is not None and ttl > 0):
                    logging.debug(
                        "CATALOG_ONE_SHOT: задача %s остаётся за живым воркером %s",
                        query,
                        owner_str,
                    )
                    await core.redis.zadd(
                        core.CATALOG_PROCESSING_ZSET,
                        {query: time()},
                        xx=True,
                    )
                    continue

        logging.warning("CATALOG: повторная постановка зависшей задачи %s", query)
        await core.redis.zrem(core.CATALOG_PROCESSING_ZSET, query)
        await core.redis.hdel(core.CATALOG_OWNER_HASH, query)
        await core.redis.srem(core.CATALOG_QUEUE_DEDUP_SET, query)

        added = await core.redis.sadd(core.CATALOG_QUEUE_DEDUP_SET, query)
        if not added:
            continue

        original_limit = base_limits.get(query)
        cached_limit = cached_limits.get(query, original_limit)
        cached_limits[query] = await _enqueue_query(query, cached_limit)
        requeued_any = True
    return requeued_any


async def _wait_for_queue_drain(
    base_limits: Dict[str, Optional[float]],
    cached_limits: Dict[str, Optional[float]],
) -> None:
    """Ждёт опустошения очереди каталога и возвращает зависшие задачи."""

    poll_interval = float(os.getenv("CATALOG_ONE_SHOT_POLL", "5"))
    keys = ["cl_my"]

    while True:
        requeued = await _requeue_stale(base_limits, cached_limits)
        lengths = await asyncio.gather(*(core.redis.llen(key) for key in keys))
        pending = {key: length for key, length in zip(keys, lengths) if length}
        if not pending and not requeued:
            processing_left = await core.redis.zcard(core.CATALOG_PROCESSING_ZSET)
            if processing_left == 0:
                logging.info("CATALOG_ONE_SHOT: очередь cl_my пуста, продюсер завершает работу.")
                break
        logging.info(
            "CATALOG_ONE_SHOT: ожидаем очистку очереди %s (повтор через %.1fs)",
            pending,
            poll_interval,
        )
        await asyncio.sleep(poll_interval)


async def main() -> None:
    """Читает Excel и ставит задачи; лимит берём из SMART при включённом флаге."""
    logging.basicConfig(level=logging.DEBUG)

    # Корень проекта внутри контейнера — каталог файла
    project_root = Path(__file__).resolve().parent
    load_dotenv_if_present(project_root / '.env')

    # Настройка расширенного логирования при необходимости
    excel_log.init_from_env(project_root)
    # Включаем лог статусов запросов по флагу в .env
    request_log.init_from_env(project_root)

    # Путь к Excel из .env, по умолчанию — queries.xlsx в корне проекта
    excel_path = os.getenv('EXCEL_PATH', str(project_root / 'queries.xlsx'))
    excel_path = str(Path(excel_path))

    # Инициализация как в основном main.py
    # Инициализация: запуск брокера cl_my; pw_my стартует в хукe startup_cl
    core.DataBase.init()
    await core.broker_cl.startup()

    # Прогреваем cookies для прокси
    for proxy in core.proxies:
        cookie = await core.redis_session.get(proxy['server'])
        if not cookie:
            await core.redis_session.set(proxy['server'], '{}')

    # Парсинг Excel только при старте
    pairs = list(read_excel_queries(excel_path))
    logging.info(f"EXCEL pairs loaded: {len(pairs)}")
    if excel_log.enabled():
        for q, mp in pairs:
            excel_log.log_event(
                "excel_pair",
                query=q,
                max_price=mp,
                excel_path=excel_path,
            )

    base_limits = {query: max_price for query, max_price in pairs}

    one_shot_mode = _env_flag("CATALOG_ONE_SHOT")
    cycle = 0
    while True:
        cycle += 1
        logging.info(f"EXCEL cycle start #{cycle} (pairs={len(pairs)})")
        if excel_log.enabled():
            excel_log.log_event("excel_cycle_start", cycle=cycle, total=len(pairs))

        cached_limits = await _enqueue_pairs(pairs)

        logging.info(f"EXCEL cycle finished #{cycle}")
        if excel_log.enabled():
            excel_log.log_event("excel_cycle_finish", cycle=cycle, total=len(pairs))

        if one_shot_mode:
            logging.info("CATALOG_ONE_SHOT включён — очередь заполнена один раз, ждём завершения каталога.")
            await _wait_for_queue_drain(base_limits, cached_limits)
            logging.info("CATALOG_ONE_SHOT: все задания каталога поставлены, завершаем продюсер.")
            return


if __name__ == '__main__':
    asyncio.run(main())
