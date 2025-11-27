from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

_ENABLED = False
_HTTP_LOGGER: Optional[logging.Logger] = None
_PW_LOGGER: Optional[logging.Logger] = None


def _as_bool(v: str | None) -> bool:
    if not v:
        return False
    return v.strip().lower() in {"1", "true", "yes", "on"}


def init_from_env(project_root: Path) -> None:
    """Configure two simple file loggers when DEBUG_REQUESTS=1.

    Files can be overridden via env:
      - REQUEST_LOG_AIOHTTP (default: project_root/requests_aiohttp.log)
      - REQUEST_LOG_PLAYWRIGHT (default: project_root/requests_playwright.log)
    """
    global _ENABLED, _HTTP_LOGGER, _PW_LOGGER
    _ENABLED = _as_bool(os.getenv("DEBUG_REQUESTS"))
    if not _ENABLED:
        _HTTP_LOGGER = None
        _PW_LOGGER = None
        return

    http_path = os.getenv("REQUEST_LOG_AIOHTTP") or str(project_root / "requests_aiohttp.log")
    pw_path = os.getenv("REQUEST_LOG_PLAYWRIGHT") or str(project_root / "requests_playwright.log")

    # AIOHTTP logger
    http_logger = logging.getLogger("requests_aiohttp")
    http_logger.setLevel(logging.INFO)
    # Avoid duplicate handlers when re-init
    http_logger.handlers.clear()
    h1 = logging.FileHandler(http_path, encoding="utf-8")
    h1.setFormatter(logging.Formatter("%(asctime)s %(message)s", "%Y-%m-%d %H:%M:%S"))
    http_logger.addHandler(h1)

    # Playwright logger
    pw_logger = logging.getLogger("requests_playwright")
    pw_logger.setLevel(logging.INFO)
    pw_logger.handlers.clear()
    h2 = logging.FileHandler(pw_path, encoding="utf-8")
    h2.setFormatter(logging.Formatter("%(asctime)s %(message)s", "%Y-%m-%d %H:%M:%S"))
    pw_logger.addHandler(h2)

    _HTTP_LOGGER = http_logger
    _PW_LOGGER = pw_logger


def enabled() -> bool:
    return _ENABLED and _HTTP_LOGGER is not None and _PW_LOGGER is not None


def _append_proxy(message: str, proxy: str | None) -> str:
    if proxy:
        return f"{message} proxy={proxy}"
    return message


def log_http(status: int | str, url: str, proxy: str | None = None) -> None:
    if not (_HTTP_LOGGER and _ENABLED):
        return
    # Minimal line: "<status> <url> [proxy=...]"
    _HTTP_LOGGER.info(_append_proxy(f"{status} {url}", proxy))


def log_http_error(url: str, exc: BaseException, proxy: str | None = None) -> None:
    if not (_HTTP_LOGGER and _ENABLED):
        return
    _HTTP_LOGGER.info(_append_proxy(f"EXC({exc.__class__.__name__}) {url}", proxy))


def log_playwright(status: int | str, url: str, proxy: str | None = None) -> None:
    if not (_PW_LOGGER and _ENABLED):
        return
    _PW_LOGGER.info(_append_proxy(f"{status} {url}", proxy))


def log_playwright_error(url: str, exc: BaseException, proxy: str | None = None) -> None:
    if not (_PW_LOGGER and _ENABLED):
        return
    _PW_LOGGER.info(_append_proxy(f"EXC({exc.__class__.__name__}) {url}", proxy))
