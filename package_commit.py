from __future__ import annotations

import asyncio
import logging
import os
from contextvars import ContextVar
from dataclasses import dataclass
from time import monotonic
from typing import Any, List, Optional

from asyncpg.exceptions import UniqueViolationError
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool.impl import NullPool


_logger: logging.Logger = logging.getLogger("package_commit")

_LOG_FILE = os.getenv("PACKAGE_COMMIT_LOG_FILE", "package_commit.log")
# Ставим безопасный дефолт 5 минут: если переменная окружения не задана,
# воркер всё равно не зависнет навсегда на записи в БД.
_FLUSH_TIMEOUT = float(os.getenv("PACKAGE_COMMIT_TIMEOUT", "300"))
_file_logger = logging.getLogger("package_commit.file")
if not _file_logger.handlers:
    _handler = logging.FileHandler(_LOG_FILE, encoding="utf-8")
    _handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    _file_logger.addHandler(_handler)
_file_logger.setLevel(logging.INFO)
_file_logger.propagate = False


def configure_logger(logger: logging.Logger) -> None:
    global _logger
    _logger = logger


class DataBase:
    session_marker: async_sessionmaker

    @classmethod
    def init(cls) -> None:
        engine = create_async_engine(
            'postgresql+asyncpg://admin:Password123@81.30.105.134:5421/eb',
            poolclass=NullPool
        )
        cls.session_marker = async_sessionmaker(bind=engine, expire_on_commit=False)


@dataclass
class _PendingBuilder:
    item: dict[str, Any]
    specifics: List[dict[str, str]]


@dataclass
class _PendingItem:
    item: dict[str, Any]
    specifics: List[dict[str, str]]
    future: asyncio.Future[None]


class PackageCommit:
    _builder_ctx: ContextVar[Optional[_PendingBuilder]] = ContextVar(
        "package_commit_builder", default=None
    )
    _lock: asyncio.Lock = asyncio.Lock()
    _pending: List[_PendingItem] = []
    _flush_task: Optional[asyncio.Task[None]] = None
    _batch_size: int = int(os.getenv("DB_BATCH_SIZE", "10"))
    _flush_delay: float = float(os.getenv("DB_BATCH_DELAY", "0.05"))

    @classmethod
    def reset(cls) -> None:
        cls._builder_ctx.set(None)

    @classmethod
    def insert_into_ebay(
        cls,
        query: str,
        price: float,
        price_without_delivery: float,
        number: str,
        location: str | None,
        condition: str,
        title: str | None,
        delivery_price: float,
        seller: str,
        _cycle: int,
    ) -> int:
        builder = _PendingBuilder(
            item={
                "query": query,
                "price": price,
                "price_without_delivery": price_without_delivery,
                "number": number,
                "location": location,
                "condition": condition,
                "title": title,
                "delivery_price": delivery_price,
                "seller": seller,
                "cycle": _cycle,
            },
            specifics=[],
        )
        cls._builder_ctx.set(builder)
        return 0

    @classmethod
    def insert_into_item_specifics(
        cls, key: str, value: str, insert_into_ebay_index: int
    ) -> None:
        builder = cls._builder_ctx.get()
        if builder is None:
            return
        builder.specifics.append({"key": key, "value": value})

    @classmethod
    async def commit(cls) -> None:
        builder = cls._builder_ctx.get()
        if builder is None:
            return
        try:
            await cls._enqueue(builder.item, builder.specifics)
        finally:
            cls._builder_ctx.set(None)

    @classmethod
    async def _enqueue(cls, item: dict[str, Any], specifics: List[dict[str, str]]) -> None:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()

        pending_batch: Optional[List[_PendingItem]] = None
        async with cls._lock:
            cls._pending.append(_PendingItem(item, specifics, future))
            if len(cls._pending) >= cls._batch_size:
                if cls._flush_task and not cls._flush_task.done():
                    cls._flush_task.cancel()
                pending_batch = cls._drain_pending_locked()
            else:
                if cls._flush_task is None or cls._flush_task.done():
                    cls._flush_task = asyncio.create_task(cls._delayed_flush())

        if pending_batch:
            await cls._flush_batch(pending_batch)

        await future

    @classmethod
    def _drain_pending_locked(cls) -> List[_PendingItem]:
        if not cls._pending:
            cls._flush_task = None
            return []
        batch = cls._pending
        cls._pending = []
        cls._flush_task = None
        return batch

    @classmethod
    async def _delayed_flush(cls) -> None:
        try:
            await asyncio.sleep(max(cls._flush_delay, 0.0))
            async with cls._lock:
                pending = cls._drain_pending_locked()
        except asyncio.CancelledError:
            return
        if pending:
            await cls._flush_batch(pending)

    @classmethod
    async def _flush_batch(cls, items: List[_PendingItem]) -> None:
        if not items:
            return

        batch_items = [pending.item for pending in items]
        specifics_count = sum(len(pending.specifics) for pending in items)
        started = monotonic()
        _file_logger.info(
            "flush_start items=%s specifics=%s",
            len(batch_items),
            specifics_count,
        )

        _logger.info(
            "package commit prepared batch items=%s specifics=%s",
            len(batch_items),
            specifics_count,
        )

        try:
            if _FLUSH_TIMEOUT > 0:
                await asyncio.wait_for(
                    cls._flush_batch_inner(items, batch_items, specifics_count, started),
                    timeout=_FLUSH_TIMEOUT,
                )
            else:
                await cls._flush_batch_inner(items, batch_items, specifics_count, started)
        except asyncio.TimeoutError as exc:
            duration = monotonic() - started
            _file_logger.error(
                "flush_timeout items=%s specifics=%s duration=%.3fs",
                len(batch_items),
                specifics_count,
                duration,
            )
            for pending in items:
                if not pending.future.done():
                    pending.future.set_exception(exc)
            raise

    @classmethod
    async def _flush_batch_inner(
        cls,
        items: List[_PendingItem],
        batch_items: List[dict[str, Any]],
        specifics_count: int,
        started: float,
    ) -> None:
        try:
            async with DataBase.session_marker() as session:
                try:
                    insert_stmt = text(
                        """
                        insert into ebay (
                            query, price, price_without_delivery,
                            number, location, condition,
                            title, delivery_price, seller, cycle
                        ) values (
                            :query, :price, :price_without_delivery,
                            :number, :location, :condition,
                            :title, :delivery_price, :seller, :cycle
                        ) returning (id)
                        """
                    )

                    ids: list[int] = []
                    if len(batch_items) == 1:
                        single_result = await session.execute(insert_stmt, batch_items[0])
                        ids.append(single_result.scalar_one())
                    else:
                        for payload in batch_items:
                            single_result = await session.execute(insert_stmt, payload)
                            ids.append(single_result.scalar_one())
                    specifics_payload: List[dict[str, Any]] = []
                    for ebay_id, pending in zip(ids, items):
                        for spec in pending.specifics:
                            specifics_payload.append(
                                {"key": spec["key"], "value": spec["value"], "ebay_id": ebay_id}
                            )
                    if specifics_payload:
                        await session.execute(
                            text(
                                """
                                insert into item_specifics (key, value, ebay_id)
                                values (:key, :value, :ebay_id)
                                """
                            ),
                            specifics_payload,
                        )
                    await session.commit()
                except IntegrityError:
                    await session.rollback()
                    raise
        except IntegrityError as exc:
            _file_logger.warning(
                "flush_error integrity items=%s specifics=%s err=%s",
                len(batch_items),
                specifics_count,
                exc,
            )
            await cls._handle_integrity_error(items, exc)
            return
        except Exception as exc:
            _file_logger.error(
                "flush_error items=%s specifics=%s err=%s",
                len(batch_items),
                specifics_count,
                exc,
            )
            for pending in items:
                if not pending.future.done():
                    pending.future.set_exception(exc)
            raise
        else:
            _logger.info(
                "package commit stored batch items=%s specifics=%s",
                len(batch_items),
                specifics_count,
            )
            duration = monotonic() - started
            _file_logger.info(
                "flush_ok items=%s specifics=%s duration=%.3fs",
                len(batch_items),
                specifics_count,
                duration,
            )
            for pending in items:
                if not pending.future.done():
                    pending.future.set_result(None)

    @classmethod
    async def _handle_integrity_error(
        cls, items: List[_PendingItem], original_exc: IntegrityError
    ) -> None:
        _logger.warning(
            "package commit bulk failed with IntegrityError: %s", original_exc
        )
        for pending in items:
            try:
                async with DataBase.session_marker() as session:
                    try:
                        result = await session.execute(
                            text(
                                """
                                insert into ebay (
                                    query, price, price_without_delivery,
                                    number, location, condition,
                                    title, delivery_price, seller, cycle
                                ) values (
                                    :query, :price, :price_without_delivery,
                                    :number, :location, :condition,
                                    :title, :delivery_price, :seller, :cycle
                                ) returning (id)
                                """
                            ),
                            [pending.item],
                        )
                        inserted_ids = result.all()
                        if inserted_ids:
                            ebay_id = inserted_ids[0][0]
                            if pending.specifics:
                                specifics_payload = [
                                    {"key": spec["key"], "value": spec["value"], "ebay_id": ebay_id}
                                    for spec in pending.specifics
                                ]
                                await session.execute(
                                    text(
                                        """
                                        insert into item_specifics (key, value, ebay_id)
                                        values (:key, :value, :ebay_id)
                                        """
                                    ),
                                    specifics_payload,
                                )
                        await session.commit()
                    except IntegrityError as inner_exc:
                        await session.rollback()
                        raise inner_exc
            except IntegrityError as inner_exc:
                if isinstance(getattr(inner_exc, "orig", None), UniqueViolationError):
                    if not pending.future.done():
                        pending.future.set_exception(inner_exc)
                    continue
                if not pending.future.done():
                    pending.future.set_exception(inner_exc)
                raise
            except Exception as inner_exc:
                if not pending.future.done():
                    pending.future.set_exception(inner_exc)
                raise
            else:
                if not pending.future.done():
                    pending.future.set_result(None)
