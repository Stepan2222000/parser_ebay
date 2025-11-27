import io
import os  # Что: доступ к переменным окружения; Зачем: читать BLOCKED_SELLERS
import logging
import re
import sys
import uuid
from asyncio import run, sleep, Task, create_task, CancelledError, get_running_loop
from pathlib import Path
from functools import wraps
from itertools import count, cycle
from json import dumps, loads
from time import time
from typing import Callable, Any, Optional, Self

from dotenv import load_dotenv  # Что: загрузка .env; Зачем: получить REDIS_* и прочие настройки

env_path = Path("/root/new_service/ebay_my_news_done_without_del/.env")
if not env_path.exists():
    env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(env_path, override=True)  # Что: жёстко читаем .env проекта; Зачем: гарантировать USE_SMART_PRICE

TRACK_LOG_FILE = os.getenv(
    "TRACK_LOG_FILE",
    str(Path(__file__).resolve().parent / "tracking_debug.log"),
)
TRACK_LOGGER = logging.getLogger("tracking_debug")
if not TRACK_LOGGER.handlers:
    _tracking_handler = logging.FileHandler(TRACK_LOG_FILE, encoding="utf-8")
    _tracking_handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    TRACK_LOGGER.addHandler(_tracking_handler)
TRACK_LOGGER.setLevel(logging.INFO)
TRACK_LOGGER.propagate = False

from aiohttp import ClientResponse, ClientSession, TCPConnector
from bs4 import BeautifulSoup
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from asyncpg.exceptions import UniqueViolationError
from taskiq import TaskiqEvents
from taskiq_redis import ListQueueBroker
from duplicate_guard import DuplicateGuard
try:
    from .blocked_title_filter import check_title, log_block, passes_whitelist  # package mode
except Exception:  # pragma: no cover - top-level script execution
    from blocked_title_filter import check_title, log_block, passes_whitelist  # type: ignore
from package_commit import (
    DataBase,
    PackageCommit,
    configure_logger as configure_package_commit_logger,
)
try:
    from . import duplicate_cache  # type: ignore
except Exception:
    import duplicate_cache  # type: ignore

configure_package_commit_logger(TRACK_LOGGER)

try:
    from . import excel_log  # when imported as package
except ImportError:  # when run as top-level module (taskiq worker main:broker_*)
    import excel_log  # type: ignore
try:
    from . import request_log  # when imported as package
except ImportError:
    import request_log  # type: ignore
from undetected_playwright.async_api import async_playwright, \
    ProxySettings, Playwright, Browser, Route, Request
try:
    # optional import; used only for pushing to Redis Streams
    from .redis_stream_producer import push_product  # when imported as package
except Exception:
    from redis_stream_producer import push_product  # type: ignore
try:
    from .xpath_debug import record_xpath_value  # type: ignore
    from .html_text_logger import log_html_text  # type: ignore
except Exception:
    from xpath_debug import record_xpath_value  # type: ignore
    from html_text_logger import log_html_text  # type: ignore
try:
    from .catalog_less_match_guard import should_stop  # package mode
except Exception:
    from catalog_less_match_guard import should_stop  # type: ignore
try:
    from .heartbeat import start as start_heartbeat  # package mode
except Exception:
    from heartbeat import start as start_heartbeat  # type: ignore

try:
    # Что: модуль переобновления по изменению заголовка
    # Зачем: вынос основной логики в отдельный файл (KISS)
    from .title_recheck import (
        enabled as recheck_titles_enabled,
        recheck_and_delete_if_changed,
    )  # package mode
except Exception:  # when run as top-level module
    try:
        from title_recheck import (
            enabled as recheck_titles_enabled,
            recheck_and_delete_if_changed,
        )  # type: ignore
    except Exception:  # pragma: no cover - запасной вариант
        recheck_titles_enabled = lambda: False  # type: ignore
        async def recheck_and_delete_if_changed(*args, **kwargs):  # type: ignore
            return []


broker_cl = ListQueueBroker("redis://redis", queue_name="cl_my")
broker_pw = ListQueueBroker("redis://redis", queue_name="pw_my")
redis = Redis(host="redis")
redis_controller = Redis(host='redis', db=2)
redis_session = Redis(host='redis', db=4)
redis_guard = Redis(host='redis', db=5)  # Что: отдельная БД для замков; Зачем: не смешивать с cookies
duplicate_guard = DuplicateGuard(redis_guard)
_heartbeat_task: Optional[Task] = None

CATALOG_OWNER_HASH = "cl_my:owners"

_WORKER_BASE_ID = os.getenv("HEARTBEAT_WORKER_ID", "cl_main")
WORKER_ID = f"{_WORKER_BASE_ID}:{os.getpid()}:{uuid.uuid4().hex[:8]}"

CODE_DELIVERY = "90066"
PROCENT_DELIVERY = 6.
MAX_PRICE = 1000000
CATALOG_QUEUE_DEDUP_SET = "cl_my:dedupe_queries"
CATALOG_PROCESSING_ZSET = "cl_my:processing"

_DELAY_DEFAULT = 0.9125 / 100
_DELAY_STEP = 0.1825 / 100
_DELAY_MIN = 0.1825 / 100
_DELAY_MAX = 3.75 / 100
_DELAY_NO_PROXY = 0.45625 / 100
PROXY_HEALTHCHECK_URL = "http://httpbin.org/ip"

# Что: набор заблокированных продавцов из окружения (через запятую)
# Зачем: быстрое точное сравнение ника без лишней логики
BLOCKED_SELLERS: set[str] = {
    s.strip() for s in (os.getenv("BLOCKED_SELLERS") or "").split(",") if s.strip()
}

# Что: отдельный логгер для фикса дубликатов
# Зачем: анализировать повторные попытки добавления товаров
DUPLICATE_LOG_FILE = os.getenv("DUPLICATE_LOG_FILE", "duplicates.log")
DUPLICATE_LOGGER = logging.getLogger("duplicate_items")
if not DUPLICATE_LOGGER.handlers:
    _duplicate_handler = logging.FileHandler(DUPLICATE_LOG_FILE, encoding="utf-8")
    _duplicate_handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    DUPLICATE_LOGGER.addHandler(_duplicate_handler)
DUPLICATE_LOGGER.setLevel(logging.INFO)
DUPLICATE_LOGGER.propagate = False

# Что: управление файлом asyncio_guard
# Зачем: дать возможность выключить отдельный лог без правки кода
def _env_flag(name: str, default: str = "1") -> bool:
    value = os.getenv(name, default)
    return value.strip().lower() in {"1", "true", "yes", "on"}


ASYNCIO_GUARD_LOG_ENABLED = _env_flag("ASYNCIO_GUARD_LOG", "1")
ASYNCIO_GUARD_LOG_PATH = os.getenv("ASYNCIO_GUARD_LOG_FILE", "asyncio_guard.log")

# Что: флаги для проверки и логирования изменения заголовков из каталога
# Зачем: включать/выключать функциональность через .env, не трогая код

CATALOG_ONE_SHOT = _env_flag("CATALOG_ONE_SHOT", "0")
CATALOG_DUPLICATE_CACHE = _env_flag("CATALOG_DUPLICATE_CACHE", "1")


class TypeErrorExitHandler(logging.Handler):
    """Отслеживает TypeError из asyncio и защищает воркеры от известных гонок."""

    _SUPPRESSED_SIGNATURE = "TypeError: 'NoneType' object is not callable"

    def __init__(self) -> None:
        super().__init__()
        self._suppressed_reported = False  # Что: флаг единовременного уведомления; Зачем: не заспамить лог
        # Что: отдельный логгер для долгосрочного контроля гонок
        # Зачем: иметь историю без смешения с основными логами
        self._guard_logger = logging.getLogger("asyncio_guard")
        self._guard_logger_enabled = ASYNCIO_GUARD_LOG_ENABLED
        if self._guard_logger_enabled and not self._guard_logger.handlers:
            handler = logging.FileHandler(ASYNCIO_GUARD_LOG_PATH, encoding="utf-8")
            handler.setFormatter(logging.Formatter(
                fmt="%(asctime)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            ))
            self._guard_logger.addHandler(handler)
        self._guard_logger.setLevel(logging.WARNING)
        self._guard_logger.propagate = False

    def emit(self, record: logging.LogRecord) -> None:
        """Перехватывает события asyncio и гасит безобидную гонку, остальные эскалирует."""
        if "asyncio" not in record.name:
            return

        message = record.getMessage()
        # Что: узнаём «гонку» read_ready_cb → None в Python 3.12
        # Зачем: не убивать воркер из-за штатного сбоя транспорта
        if self._SUPPRESSED_SIGNATURE in message:
            if self._guard_logger_enabled and not self._suppressed_reported:
                self._guard_logger.warning(
                    "Обнаружена гонка asyncio (_read_ready_cb). Воркер продолжает работу."
                )
                self._suppressed_reported = True
            return

        # Что: любые другие TypeError внутри asyncio всё ещё считаем критичными
        # Зачем: сохранить прежнюю защиту от реальных сбоев
        logging.warning("TypeError в asyncio: %s", message)
        sys.exit(0)

logging.root.addHandler(TypeErrorExitHandler())


def retry(
        exception: type[Exception | tuple[Exception, ...]],
        *, tries: Optional[int]=7,
        delay=0, skip: type[Exception | tuple[Exception, ...]] = None
) -> Callable:
    def decorator(func) -> Callable:
        @wraps(func)
        async def __wrapper(*args, **kwargs) -> Any:
            _ex: Exception = ...
            for attempt in \
                    range(tries) if tries else count(0):
                try:
                    return await func(*args, **kwargs)
                except BaseException as ex:
                    if skip and isinstance(ex, skip):
                        logging.info(ex)
                        break
                    elif isinstance(ex, exception) and (not tries or attempt < tries):
                        _ex = ex
                        logging.warning(ex)
                        await sleep(delay)
                    else:
                        raise ex
            else:
                TRACK_LOGGER.error("retry exhausted for %s: %r", func.__name__, _ex)
            return None
        return __wrapper
    return decorator



class Webdriver:
    pw: Playwright
    browser: Browser
    mapped: dict[str, Task] = {}

    def __init__(self, proxy: ProxySettings) -> None:
        self.proxy = proxy

    async def __call__(self, url: str) -> None:
        server = self.proxy['server']
        task_execute = self.mapped.get(server)
        if (not task_execute) or task_execute.done():
            self.mapped[server] = create_task(self.execute(url))

    async def redirect(self, route: Route, request: Request) -> None:
        headers = {
            **request.headers,
            "Access-Control-Allow-Origin": "*"
        }
        await route.continue_(headers=headers)

    async def abort(self, route: Route) -> None:
        await route.abort()

    async def execute(self, url: str) -> None:
        await redis_session.delete(self.proxy['server'])
        logging.info(self.proxy['server'])
        browser = await Webdriver.pw.firefox.launch_persistent_context(
            "",
            channel="firefox",
            headless=True,
            firefox_user_prefs={
                "network.http.max-persistent-connections-per-server": 1
            },
            proxy=self.proxy
        )
        cookies = {}
        try:
            page = browser.pages[0]
            await page.route(re.compile(".warm$"), self.redirect)
            await page.route(re.compile(".css$"), lambda route: route.abort())
            await page.route(re.compile("/image/"), lambda route: route.abort())
            await page.route(re.compile(".png"), lambda route: route.abort())
            async with page:
                try:
                    resp = await page.goto(url, wait_until="domcontentloaded", timeout=180_000)
                    status = resp.status if resp else "NONE"
                    try:
                        if request_log.enabled():
                            request_log.log_playwright(status, url, self.proxy['server'])
                    except Exception:
                        pass
                except Exception as ex:
                    try:
                        if request_log.enabled():
                            request_log.log_playwright_error(url, ex, self.proxy['server'])
                    except Exception:
                        pass
                    raise
                await sleep(1)
                while "www.ebay.com/splashui/challenge" in page.url:
                    await sleep(1)
                cookies = await page.context.cookies()
                await redis_session.set(self.proxy['server'], dumps(cookies))
        except:
            await redis_session.set(self.proxy['server'], dumps(cookies))
            raise
        finally:
            await browser.close()


@broker_pw.task("task_execute")
async def task_execute(url: str, proxy: ProxySettings) -> None:
    await Webdriver(proxy)(url)


@broker_pw.on_event(TaskiqEvents.WORKER_STARTUP)
async def startup(*args, **kw) -> None:
    try:
        request_log.init_from_env(Path(__file__).resolve().parent)
    except Exception:
        pass
    Webdriver.pw = await async_playwright().start()


@broker_pw.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def shutdown(*args, **kw) -> None:
    await Webdriver.pw.stop()


def load_proxies() -> list[ProxySettings]:
    with open('proxies.txt') as file:
        proxies = []
        for line in file.read().split('\n'):
            if line:
                host, port, username, password = line.split(':')
                proxies.append(ProxySettings(
                    server=f"{host}:{port}",
                    username=username,
                    password=password
                ))
    return proxies

proxies = load_proxies()
PROXIES = cycle(proxies)


class Delay:
    def __init__(self, proxy: ProxySettings = None) -> None:
        self.proxy = None
        if proxy:
            self.proxy = proxy['server']

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, e, *args, **kw) -> None:
        if self.proxy:
            _sleep = await redis_controller.get(self.proxy)
            _sleep = float(_sleep or _DELAY_DEFAULT)
            if e is None:
                if _sleep > _DELAY_MIN:
                    _sleep = max(_DELAY_MIN, _sleep - _DELAY_STEP)
            else:
                if _sleep < _DELAY_MAX:
                    _sleep = min(_DELAY_MAX, _sleep + _DELAY_STEP)
            await redis_controller.set(self.proxy, _sleep)
            logging.info(f'For {self.proxy!r} wait {_sleep}s')
            await sleep(_sleep)
        else:
            await sleep(_DELAY_NO_PROXY)


async def log_proxy_health(session: ClientSession, proxy: Optional[ProxySettings]) -> None:
    """Проверяет прокси запросом к httpbin и логирует статус."""
    proxy_id = proxy['server'] if proxy else None
    try:
        async with session.get(PROXY_HEALTHCHECK_URL) as health_response:
            status = health_response.status
            if request_log.enabled():
                request_log.log_http(status, PROXY_HEALTHCHECK_URL, proxy_id)
            if status != 200:
                logging.warning(
                    "Proxy %s health-check status=%s",
                    proxy_id,
                    status,
                )
    except Exception as ex:
        if request_log.enabled():
            request_log.log_http_error(PROXY_HEALTHCHECK_URL, ex, proxy_id)
        raise


class Timer:
    def __init__(self, target: str) -> None:
        self.target = target

    def __enter__(self) -> None:
        self.start_time = time()

    def __exit__(self, *args, **kw) -> None:
        self.end_time = time()
        self.difference = self.end_time - self.start_time
        logging.debug(f"[{self.target}] {self.difference}s")


def get_element(func: Callable) -> Optional[Any]:
    try:
        return func()
    except Exception:
        return None

def extract_catalog_seller(li: Any) -> Optional[str]:
    """Возвращает ник продавца из карточки каталога (или None)."""

    # Что: пробуем блок с бейджем продавца (seller/store info)
    # Зачем: вариант, когда ник и рейтинг в одном span
    el = li.select_one(
        ".s-card__program-badge-container--sellerOrStoreInfo .su-styled-text"
    )
    if not el:
        # что: запасной вариант — блок вторичных атрибутов
        el = li.select_one(
            ".su-card-container__attributes__secondary .su-styled-text.primary.large"
        )
    if not el:
        return None
    text = el.get_text(" ", strip=True)
    if not text:
        return None
    match = re.match(r"([A-Za-z0-9._-]+)", text)
    return match.group(1) if match else None


def extract_catalog_title(li: Any) -> Optional[str]:
    """Возвращает title из карточки каталога без нормализации.

    Назначение: нужен для строгого сравнения с тем, что уже в БД, чтобы
    определить устаревшие лоты и инициировать перепарс.
    """
    # Что: классический селектор eBay списка
    el = li.select_one(".s-item__title")
    if not el:
        # Что: альтернативные варианты разметки (новые карточки)
        el = li.select_one(".s-card__title, [role='heading']")
    if not el:
        # Что: запасной путь — aria-label у ссылки
        a = li.find("a")
        if a and a.get("aria-label"):
            return a.get("aria-label").strip()
        return None
    txt = el.get_text(" ", strip=True)
    return txt if txt else None

class _HtmlParse:
    def __init__(self, _text: str):
        self.text = _text
        self.html = BeautifulSoup(_text, 'lxml')


class HtmlParse1(_HtmlParse):
    @property
    def mpn(self) -> Optional[str]:
        return get_element(
            lambda: self.html
            .find(string=re.compile(r"(manufacturer part number|mpn|MPN)"))
            .find_next("span")
            .text.strip()
        )

    @property
    def delivery(self) -> Optional[str]:
        return get_element(
            lambda: self.html
            .find(string="Shipping:")
            .find_next("span")
            .text.strip()
        )

    @property
    def price_without_delivery(self) -> Optional[str]:
        return get_element(
            lambda: self.html
            .find("div", {"class": "x-price-primary"})
            .text.strip()
        )

    @property
    def location(self) -> Optional[str]:
        return get_element(
            lambda: self.html
            .find(string=re.compile("Located in"))
            .text.strip()
        )

    @property
    def brand(self) -> Optional[str]:
        return get_element(
            lambda: [*filter(
                lambda _: _.find(string=re.compile('^(Brand|Hersteller)', re.IGNORECASE)),
                self.html.find_all("dt")
            )][0].span.find_next("span").text.strip()
        )

    @property
    def condition(self) -> Optional[str]:
        return get_element(
            lambda: self.html
            .find(string=re.compile('Condition', re.IGNORECASE))
            .find_next("span")
            .text.strip()
        )

    @property
    def title(self) -> Optional[str]:
        return get_element(
            lambda: self.html.find("h1").text.strip()
        )

    @property
    def seller(self) -> Optional[str]:
        return get_element(
            lambda: self.html
            .find("div", {"class": "x-sellercard-atf__info__about-seller"})
            .find('span')
            .text.strip()
        )

    @property
    def description_href(self) -> Optional[str]:
        return get_element(
            lambda: self.html.find("iframe", {"id": "desc_ifr"})["src"]
        )


class HtmlParse2(HtmlParse1):
    @property
    def price_without_delivery(self) -> str:
        return get_element(
            lambda: re.search(r'"price":\["(.*?)"', self.text)
        ) or super().price_without_delivery


class HtmlParse(HtmlParse2):
    pass


async def set_zip_code(session: ClientSession, item_number: str) -> None:
    logging.debug(f"Устанавливаем почтовый индекс - \"{CODE_DELIVERY}\"")
    async with session.get(
            f"https://www.ebay.com/itemmodules/{item_number}"
            "?module_groups=GET_RATES_MODAL&co=0&isGetRates=1"
            "&rt=nc&quantity=&shipToCountryCode=USA"
            f"&shippingZipCode={CODE_DELIVERY}"
    ) as response:
        ...


class PardonOurInterruption(Exception):
    def __str__(self) -> str:
        return "Pardon Our Interruption"


async def check_block(response: ClientResponse, proxy: ProxySettings) -> None:
    if "<title>Pardon Our Interruption...</title>" in await response.text():
        await task_execute.kiq(str(response.url), proxy)
        raise PardonOurInterruption()


class Cookies:
    _previous_cookie: dict[str, dict] = {}

    @classmethod
    async def get_cookies(cls, proxy: ProxySettings) -> tuple[dict, bool]:
        data = await redis_session.get(proxy['server'])
        if data is None:
            await sleep(0.1)
            return await cls.get_cookies(proxy)
        cookies = {
            _['name']: _['value']
            for _ in loads(data)
        }
        logging.debug(cookies)
        previous_cookies = cls._previous_cookie.get(proxy['server'])
        сhanged = previous_cookies != cookies
        cls._previous_cookie[proxy['server']] = cookies
        if сhanged:
            logging.debug("cookies modified")
        return cookies, сhanged


@retry(BaseException, tries=3)
async def product(session: ClientSession, item_id: str, query: str, this_cycle: int, proxy: ProxySettings) -> None:
    item_key = str(item_id)  # Что: ключ замка в Redis; Зачем: не повторять сетевой запрос
    if duplicate_cache.enabled():
        if await duplicate_cache.contains(item_key):
            duplicate_cache.log_skip(item_key, query)
            if excel_log.enabled():
                excel_log.log_event(
                    "duplicate_cache_skip",
                    item_id=item_key,
                    query=query,
                    stage='duplicate_cache',
                )
            logging.info("Skip item %s from duplicate cache", item_key)
            return
    if not await duplicate_guard.acquire(item_key, query=query):
        if excel_log.enabled():
            excel_log.log_event(
                'product_skip',
                item_id=item_id,
                query=query,
                stage='duplicate_guard',
                reason='замок уже занят',
            )
        return
    TRACK_LOGGER.info("product start item=%s query=%s cycle=%s", item_id, query, this_cycle)
    try:
        async with Delay(proxy):
            url = f"https://www.ebay.com/itm/{item_id}"

            with Timer("product redis get cookies"):
                cookies, сhanged = await Cookies.get_cookies(proxy)
            if сhanged:
                with Timer("set zip code"):
                    await set_zip_code(session, item_id)
                    session.cookie_jar.update_cookies(cookies)

            with Timer("product request"):
                try:
                    await log_proxy_health(session, proxy)
                    async with session.get(url) as response:
                        _text = await response.text()
                        # ВРЕМЕННО: логируем полный текст страницы
                        try:
                            page_text = BeautifulSoup(_text, "lxml").get_text(" ", strip=True)
                        except Exception:
                            page_text = _text
                        log_html_text(item_id, page_text)
                        await check_block(response, proxy)
                        try:
                            if request_log.enabled():
                                request_log.log_http(response.status, str(response.url), proxy['server'])
                        except Exception:
                            pass
                except Exception as ex:
                    try:
                        if request_log.enabled():
                            request_log.log_http_error(url, ex, proxy['server'])
                    except Exception:
                        pass
                    TRACK_LOGGER.exception(
                        "product request failed item=%s query=%s", item_id, query
                    )
                    raise

            if "<span class=\"ux-textspans\">CURRENTLY SOLD OUT</span>" in _text:
                raise AssertionError("Сайт - 'CURRENTLY SOLD OUT'")

            html = HtmlParse(_text)

            # ВРЕМЕННО
            xpath_value: Optional[str] = None
            try:
                xpath_target = html.html.select_one(
                    "html > body > div:nth-of-type(2) > main > div:nth-of-type(1) > "
                    "div:nth-of-type(1) > div:nth-of-type(4) > div > div > div:nth-of-type(2) > "
                    "div > div:nth-of-type(1) > div:nth-of-type(3) > div:nth-of-type(1) > "
                    "div > div > span"
                )
                if xpath_target:
                    text_value = xpath_target.get_text(strip=True)
                    xpath_value = text_value or None
            except Exception:
                xpath_value = None
            record_xpath_value(item_id, xpath_value)
            # ВРЕМЕННО (конец кода)

            try:
                __delivery_raw = html.delivery or ""
                if __delivery_raw and "free" not in __delivery_raw.lower():
                    matches = [chunk for chunk in re.findall(r"[.\d]+", __delivery_raw) if chunk.strip('.')]
                    __delivery = float(matches[0]) if matches else 0.0
                else:
                    __delivery = 0.0
                __pwd_raw = html.price_without_delivery or ""
                __pwd_matches = re.findall(r"[.\d]+", __pwd_raw)
                assert __pwd_matches, "price_without_delivery not found"
                __price_without_delivery = float(__pwd_matches[0])
                __price = float(__delivery) + __price_without_delivery * \
                          (1 + float(PROCENT_DELIVERY) / 100)
                __title = html.title
            except Exception:
                TRACK_LOGGER.exception(
                    "product parse failed item=%s query=%s", item_id, query
                )
                raise

            with Timer("product insert"):
                _index = PackageCommit.insert_into_ebay(
                    query=query,
                    price=__price,
                    price_without_delivery=__price_without_delivery,
                    number=item_id,
                    location=html.location[:510] if html.location else None,
                    condition=html.condition,
                    title=__title[:255] if __title else None,
                    delivery_price=__delivery,
                    seller=html.seller,
                    _cycle=this_cycle
                )
                # Collect item specifics to DB and for stream payload
                inner_payload: dict[str, Any] = {
                    "query": query,
                    "condition": html.condition,
                    "Brand": html.brand,
                    "title": __title,
                    "location": html.location,
                    "price": str(__price),
                    "price_without_delivery": str(__price_without_delivery),
                    "seller": html.seller,
                }
                if html.html.find('dl'):
                    for item in html.html.find_all('dl'):
                        dt = item.find('dt')
                        dd = item.find('dd')
                        if dt and dd:
                            _k = dt.text.strip()[:120]
                            _v = dd.text.strip()[:1280]
                            # DB insert
                            PackageCommit.insert_into_item_specifics(
                                key=_k,
                                value=_v,
                                insert_into_ebay_index=_index
                            )
                            # Include in payload (keys 1:1, no normalization)
                            inner_payload[_k] = _v
                try:
                    await PackageCommit.commit()
                    if duplicate_cache.enabled():
                        await duplicate_cache.record_seen(item_key, query=query)
                except IntegrityError as exc:  # Что: отлавливаем дубликат
                    # Зачем: не запускать retry, если товар уже обработан
                    if isinstance(getattr(exc, "orig", None), UniqueViolationError):
                        PackageCommit.reset()
                        if excel_log.enabled():
                            excel_log.log_event(
                                'product_skip',
                                item_id=item_id,
                                query=query,
                                stage='db_unique_violation',
                                reason='уже есть в базе',
                            )
                        DUPLICATE_LOGGER.info(
                            "duplicate item %s skipped query=%s", item_id, query
                        )
                        logging.info("Skip duplicate item %s", item_id)
                        return
                    raise
            TRACK_LOGGER.info("product committed item=%s query=%s", item_id, query)
        # Push to Redis Stream (KISS): one XADD per product
        try:
            await push_product(item_id, inner_payload)
        except Exception as ex:
            # Do not break the main flow; log and continue
            logging.warning(f"XADD failed for {item_id}: {ex}")
        logging.info(f"Товар {item_id!r} добавлен.")
    finally:
        await duplicate_guard.release(item_key)

# rm -r /tmp/playwright_firefoxdev_profile*
@retry(BaseException)
async def catalog_request(session: ClientSession, query: str, p: int, proxy: ProxySettings) -> str:
    async with Delay(proxy):
        logging.info(f"QUERY - {query!r} [{p}]")
        url = f"https://www.ebay.com/sch/i.html?_nkw={query}" \
              f"&_sacat=0&_from=R40&rt=nc&LH_ItemCondition=3" \
              f"&_ipg=240&_pgn={p}"
        with Timer("catalog redis get cookies"):
            cookies, сhanged = await Cookies.get_cookies(proxy)
            if сhanged:
                session.cookie_jar.update_cookies(cookies)
        with Timer("catalog request"):
            try:
                await log_proxy_health(session, proxy)
                async with session.get(url) as response:
                    __text = await response.text()
                    await check_block(response, proxy)
                    try:
                        if request_log.enabled():
                            request_log.log_http(response.status, str(response.url), proxy['server'])
                    except Exception:
                        pass
            except Exception as ex:
                try:
                    if request_log.enabled():
                        request_log.log_http_error(url, ex, proxy['server'])
                except Exception:
                    pass
                await task_execute.kiq(url, proxy)
                raise
    return __text


@retry(Exception)
async def catalog(session: ClientSession, query: str, proxy: ProxySettings, max_price: Optional[float] = None) -> tuple[list[str], int]:
    async with DataBase.session_marker() as session_maker:
        with Timer("sql insert count"):
            await session_maker.execute(text("""
                insert into count(query, value) values (:query, 1)
                on conflict (query) do update
                set value = count.value + 1
            """), {'query': query})
            await session_maker.commit()
        with Timer("sql select count"):
            this_cycle_exec = await session_maker.execute(text("""
                select value from count where query = :query
            """), {'query': query})
        this_cycle = this_cycle_exec.first()[0]
        links = []  # Что: список href прошедших фильтр лотов
        log_verbose = excel_log.enabled()
        decisions: dict[str, dict[str, Any]] = {}  # Что: кэш решений по каждому item; Зачем: позже собрать единый лог
        included_titles: dict[int, Optional[str]] = {}  # Что: id->title для проверки изменений
        seen_sellers_global: set[str] = set()
        for p in count(1):
            __text = await catalog_request(session, query, p, proxy)
            __html = BeautifulSoup(__text, "lxml")
            if not __html.find("h1"):
                break
            __count = __html.find("h1").find("span").text
            logging.debug(f"Количество в каталоге - \"{__count}\"")
            __links = []
            items = __html.find_all('li', {'id': re.compile(r'item')})
            scanned = 0
            seen_titles_in_page: set[str] = set()
            for li in items:
                if should_stop(li):
                    break
                href_el = li.find('a')
                if not href_el:
                    continue
                href = href_el.get('href')
                if not href:
                    continue
                scanned += 1
                # Что: извлекаем ник продавца из карточки каталога
                # Зачем: фильтровать лоты по BLOCKED_SELLERS до перехода в товар
                seller_nick = extract_catalog_seller(li)
                title_text = extract_catalog_title(li)
                match = re.search(r"/itm/(\d+)", href)
                item_id = match.group(1) if match else None
                blocked_title_word: Optional[str] = None
                price_value: Optional[float] = None
                price_reason = 'no_max_price'
                decision = 'include'
                duplicate_title = False
                duplicate_seller = False
                if max_price is not None:
                    price_span = li.find('span', {'class': 's-card__price'})
                    if price_span and price_span.text:
                        try:
                            price_value = float(''.join(filter(lambda c: c.isnumeric() or c == '.', price_span.text)))
                        except Exception:
                            price_value = None
                    if price_value is None:
                        decision = 'skip'
                        price_reason = 'price_parse_failed'
                    elif price_value < max_price:
                        decision = 'include'
                        price_reason = 'below_threshold'
                    else:
                        decision = 'skip'
                        price_reason = 'above_threshold'
                # Что: дополнительная проверка на чёрный список продавцов
                # Зачем: просто пропускаем лоты нежелательных никнеймов
                blocked = False
                if decision == 'include' and seller_nick and seller_nick in BLOCKED_SELLERS:
                    decision = 'skip'
                    price_reason = 'blocked_seller'
                    blocked = True
                    logging.info(f"catalog skip by blocked seller: {seller_nick}")
                normalized_seller = seller_nick.casefold() if seller_nick else None
                if decision == 'include' and normalized_seller:
                    if normalized_seller in seen_sellers_global:
                        decision = 'skip'
                        price_reason = 'duplicate_seller'
                        duplicate_seller = True
                        logging.info("catalog skip by duplicate seller: %s", seller_nick)
                if decision == 'include':
                    if not passes_whitelist(title_text):
                        reason = 'white_list_miss'
                        decision = 'skip'
                        price_reason = reason
                        blocked_title_word = reason
                        if item_id:
                            log_block(item_id, reason, title_text or "")
                    else:
                        blocked_by_title, matched_word = check_title(title_text)
                        if blocked_by_title:
                            decision = 'skip'
                            price_reason = 'blocked_title'
                            blocked_title_word = matched_word
                            if item_id and matched_word:
                                log_block(item_id, matched_word, title_text or "")
                        else:
                            normalized_title = title_text.casefold() if title_text else None
                            if normalized_title and normalized_title in seen_titles_in_page:
                                decision = 'skip'
                                price_reason = 'duplicate_title'
                                duplicate_title = True
                                logging.info("catalog skip by duplicate title: %s", title_text)
                            if decision == 'include':
                                if normalized_title:
                                    seen_titles_in_page.add(normalized_title)
                                if normalized_seller:
                                    seen_sellers_global.add(normalized_seller)
                                __links.append(href)  # Что: лот принят после всех проверок
                                if recheck_titles_enabled() and item_id:
                                    included_titles[int(item_id)] = title_text
                if log_verbose:
                    # Логируем причину выбора/отброса
                    key = item_id or href
                    decisions[key] = {
                        "item_id": item_id,
                        "query": query,
                        "page": p,
                        "href": href,
                        "price": price_value,
                        "threshold": max_price,
                        "decision": decision,
                        "reason": price_reason,
                        "seller": seller_nick,
                        "blocked": blocked,
                        "blocked_title": blocked_title_word,
                        "duplicate_title": duplicate_title,
                        "duplicate_seller": duplicate_seller,
                        "title": title_text,
                        "duplicate_cache": False,
                    }
            __page_next = __html.find("a", {"class": "pagination__next"})
            links += __links
            if log_verbose:
                excel_log.log_event(
                    'catalog_page_summary',
                    query=query,
                    page=p,
                    scanned=scanned,
                    accepted=len(__links),
                    threshold=max_price,
                )
            if not __page_next:
                break
        # Что: перед SQL этапом — опционально удаляем изменившиеся объявления
        # Зачем: чтобы они попали далее как «новые» в стандартный pipeline
        if recheck_titles_enabled() and included_titles:
            try:
                await recheck_and_delete_if_changed(included_titles, query)
            except Exception as ex:
                logging.warning("title recheck failed: %s", ex)

        ids = re.findall(r"itm/(.*?)\?", "".join(links))
        result = []
        already_in_db: set[str] = set()
        locked_ids: set[str] = set()
        enqueued_ids: set[str] = set()
        cache_skipped_ids: set[str] = set()
        if ids:
            # Убираем дубли item_id, которые могут повторяться на нескольких карточках
            ids = list(dict.fromkeys(ids))
            with Timer("sql create temp table"):
                await session_maker.execute(text("""
                    create temp table _temp (id bigint)
                """))
            with Timer("sql insert ids to temp"):
                await session_maker.execute(text(
                    """insert into _temp values (:id)"""
                ), [{'id': int(_)} for _ in ids])
            with Timer("sql сomparisons ids"):
                ids_exec = await session_maker.execute(text("""
                    select _temp.id from _temp
                    left join ebay on ebay.number = _temp.id
                    where ebay.number is NULL;
                """))
            with Timer("sql update ebay ids"):
                await session_maker.execute(text("""
                    update ebay set cycle = :this_cycle
                    from _temp where ebay.number = _temp.id 
                """), {"this_cycle": this_cycle})
            with Timer("sql old archiving"):
                await session_maker.execute(text("""
                    select id from ebay 
                    where query = :query and cycle < :this_cycle - 1000000000000000000
                    for update skip locked
                """), {'query': query, 'this_cycle': this_cycle})
                await session_maker.execute(text("""
                    delete from ebay where query = :query and cycle < :this_cycle - 15000000000000
                """), {
                    'query': query,
                    'this_cycle': this_cycle
                })
            with Timer("sql ebay drop temp and commit"):
                await session_maker.execute(text("drop table _temp"))
                await session_maker.commit()
            db_candidates = [str(row[0]) for row in ids_exec.all()]
            already_in_db = {item for item in ids if item not in db_candidates}
            if db_candidates:
                int_candidates = [int(item) for item in db_candidates]
                filtered = await duplicate_guard.filter_unlocked(int_candidates, query=query)
                if CATALOG_DUPLICATE_CACHE and duplicate_cache.enabled():
                    filtered_after_cache: list[int] = []
                    for _id in filtered:
                        key = str(_id)
                        if await duplicate_cache.contains(key):
                            cache_skipped_ids.add(key)
                            logging.info("catalog skip by duplicate cache: %s (query=%s)", key, query)
                            if log_verbose:
                                payload = decisions.get(key)
                                if payload is not None:
                                    payload["decision"] = "skip"
                                    payload["reason"] = "duplicate_cache"
                                    payload["duplicate_cache"] = True
                            continue
                        filtered_after_cache.append(_id)
                    if cache_skipped_ids:
                        logging.info("duplicate cache filtered %s items for query %s", len(cache_skipped_ids), query)
                    filtered = filtered_after_cache
                enqueued_ids = {str(_id) for _id in filtered}
                locked_ids = (set(db_candidates) - enqueued_ids) - cache_skipped_ids
                skipped = len(db_candidates) - len(enqueued_ids)
                if cache_skipped_ids:
                    skipped -= len(cache_skipped_ids)
                    if skipped < 0:
                        skipped = 0
                if skipped:
                    logging.info("duplicate guard filtered %s items for query %s", skipped, query)
                result = [int(_id) for _id in filtered]
            else:
                result = []
        if log_verbose and decisions:
            for key, payload in decisions.items():
                item_id = payload.get("item_id")
                lookup = item_id or key
                decision = payload.get("decision")
                if lookup in cache_skipped_ids:
                    payload["decision"] = "skip"
                    payload["reason"] = "duplicate_cache"
                    payload["db_status"] = "в кеше дублей"
                    payload["final_status"] = "skip"
                    payload["duplicate_cache"] = True
                elif decision != "include":
                    payload["db_status"] = "не проверялось"
                    payload["final_status"] = "skip"
                else:
                    if lookup in already_in_db:
                        payload["db_status"] = "уже в базе"
                        payload["final_status"] = "skip"
                    elif lookup in locked_ids:
                        payload["db_status"] = "занято другим воркером"
                        payload["final_status"] = "skip"
                    elif lookup in enqueued_ids:
                        payload["db_status"] = "новое"
                        payload["final_status"] = "enqueue"
                    else:
                        payload["db_status"] = "статус неизвестен"
                        payload["final_status"] = "skip"
                excel_log.log_event(
                    'catalog_item_decision',
                    item_id=item_id,
                    query=payload.get("query"),
                    page=payload.get("page"),
                    href=payload.get("href"),
                    price=payload.get("price"),
                    threshold=payload.get("threshold"),
                    decision=decision,
                    reason=payload.get("reason"),
                    seller=payload.get("seller"),
                    blocked=payload.get("blocked"),
                    blocked_title=payload.get("blocked_title"),
                    title=payload.get("title"),
                    db_status=payload.get("db_status"),
                    final_status=payload.get("final_status"),
                    duplicate_cache=payload.get("duplicate_cache"),
                )

        logging.info(f"Ids count - \"{len(result)}\"")
        return result, this_cycle


class Connector:
    connector: TCPConnector = ...


@broker_cl.task("task_collect_loop")
async def task_collect_loop(query: str, proxy: ProxySettings, max_price: Optional[float] = None) -> None:
    now_ts = time()
    await redis.zadd(CATALOG_PROCESSING_ZSET, {query: now_ts})
    await redis.hset(CATALOG_OWNER_HASH, query, WORKER_ID)
    try:
        async with ClientSession(
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:141.0) Gecko/20100101 Firefox/141.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
                "Sec-GPC": "1",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Priority": "u=0, i"
            },
            proxy=f"http://{proxy['username']}:{proxy['password']}@{proxy['server']}",
            raise_for_status=True,
            # connector=TCPConnector(limit=1)
        ) as session:
            if excel_log.enabled():
                excel_log.log_event(
                    'task_collect_start',
                    query=query,
                    proxy=proxy['server'],
                    threshold=max_price,
                )
            with Timer("catalog"):
                ids, this_cycle = await catalog(session, query, proxy, max_price=max_price)
            TRACK_LOGGER.info(
                "catalog result query=%s new_ids=%s",
                query,
                len(ids),
            )
            if ids:
                # refresh processing timestamp while catalog items are being dispatched
                try:
                    await redis.zadd(
                        CATALOG_PROCESSING_ZSET,
                        {query: time()},
                        xx=True,
                    )
                    await redis.hset(CATALOG_OWNER_HASH, query, WORKER_ID)
                except Exception:
                    pass
                anchor = ids[0]
                try:
                    TRACK_LOGGER.info(
                        "set_zip_code start item=%s query=%s proxy=%s",
                        anchor,
                        query,
                        proxy['server'],
                    )
                    await set_zip_code(session, anchor)
                    TRACK_LOGGER.info(
                        "set_zip_code ok item=%s query=%s proxy=%s",
                        anchor,
                        query,
                        proxy['server'],
                    )
                except Exception:
                    TRACK_LOGGER.exception(
                        "set_zip_code failed item=%s query=%s proxy=%s",
                        anchor,
                        query,
                        proxy['server'],
                    )
                    raise
                for _id in ids:
                    TRACK_LOGGER.info(
                        "product dispatch item=%s query=%s cycle=%s",
                        _id,
                        query,
                        this_cycle,
                    )
                    with Timer("product iteration"):
                        await product(session, _id, query, this_cycle, proxy)
    finally:
        try:
            await redis.srem(CATALOG_QUEUE_DEDUP_SET, query)
        except Exception as exc:
            logging.warning("Failed to release catalog dedupe key for %s: %s", query, exc)
        try:
            await redis.zrem(CATALOG_PROCESSING_ZSET, query)
        except Exception as exc:
            logging.warning("Failed to clear processing mark for %s: %s", query, exc)
        try:
            await redis.hdel(CATALOG_OWNER_HASH, query)
        except Exception as exc:
            logging.warning("Failed to clear owner mark for %s: %s", query, exc)
        if CATALOG_ONE_SHOT:
            try:
                remaining = await redis.llen("cl_my")
                if remaining == 0:
                    logging.info("CATALOG_ONE_SHOT: очередь cl_my пуста, каталожный воркер завершает работу.")
                    get_running_loop().call_soon(sys.exit, 0)
            except Exception as exc:
                logging.warning("CATALOG_ONE_SHOT: не удалось проверить очередь cl_my: %s", exc)


@broker_cl.on_event(TaskiqEvents.WORKER_STARTUP)
async def startup_cl(*args, **kw) -> None:
    # Включаем подробное логирование в воркере при наличии флага
    try:
        excel_log.init_from_env(Path(__file__).resolve().parent)
    except Exception:
        pass
    try:
        request_log.init_from_env(Path(__file__).resolve().parent)
    except Exception:
        pass
    DataBase.init()
    await broker_pw.startup()
    try:
        await duplicate_cache.bootstrap(Path(__file__).resolve().parent)
    except Exception:
        logging.exception("duplicate cache bootstrap failed")
    global _heartbeat_task
    if _heartbeat_task is None:
        _heartbeat_task = start_heartbeat(redis_controller, worker_id=WORKER_ID)


@broker_cl.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def shutdown_cl(*args, **kw) -> None:
    global _heartbeat_task
    task = _heartbeat_task
    _heartbeat_task = None
    if task is None:
        return
    task.cancel()
    try:
        await task
    except CancelledError:
        pass
    try:
        await redis_controller.delete(f"hb:{WORKER_ID}")
    except Exception:
        pass


async def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    try:
        request_log.init_from_env(Path(__file__).resolve().parent)
    except Exception:
        pass
    DataBase.init()
    await broker_cl.startup()
    for proxy in proxies:
        cookie = await redis_session.get(proxy['server'])
        if not cookie:
            await redis_session.set(proxy['server'], '{}')
    await sleep(10)
    async with DataBase.session_marker() as session_maker:
        while True:
            async with session_maker.begin():
                await session_maker.execute(text("""
                    create temp table values as select value from task order by priority limit 100
                """))
                result = await session_maker.execute(text("""
                    update task set priority = task.priority + 1
                    from values where task.value = values.value
                    returning task.value;
                """))
            for query in result.all():
                while True:
                    try:
                        length = await redis.llen("cl_my")
                        assert length < 10000, "Очередь переполнена"
                        query: str = query[0]
                        await session_maker.commit()
                        logging.info(f"QUERY SEND - {query}")
                        await task_collect_loop.kiq(query=query, proxy=next(PROXIES), max_price=MAX_PRICE)
                        break
                    except Exception as ex:
                        logging.warning(ex, stack_info=True)
                        await sleep(10)


if __name__ == '__main__':
    run(main())
