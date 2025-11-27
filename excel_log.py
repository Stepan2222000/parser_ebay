from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

_VERBOSE = False
_LOGGER: logging.Logger | None = None


def _as_bool(value: str | None) -> bool:
    if not value:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def init_from_env(project_root: Path) -> None:
    """Initialize verbose logger from env.

    EXCEL_VERBOSE=1 enables verbose decision logs.
    EXCEL_LOG_FILE=path sets output file (default: project_root/'excel_verbose.log').
    """
    global _VERBOSE, _LOGGER
    if _as_bool(os.getenv("EXCEL_VERBOSE_DISABLE", "1")):
        _VERBOSE = False
        _LOGGER = None
        return
    _VERBOSE = _as_bool(os.getenv("EXCEL_VERBOSE"))
    if not _VERBOSE:
        _LOGGER = None
        return

    log_file = os.getenv("EXCEL_LOG_FILE")
    if not log_file:
        log_file = str(project_root / "excel_verbose.log")

    logger = logging.getLogger("excel_verbose")
    logger.setLevel(logging.INFO)
    # Avoid duplicate handlers when re-init
    logger.handlers.clear()
    handler = logging.FileHandler(log_file, encoding="utf-8")
    handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    logger.addHandler(handler)
    _LOGGER = logger
    logger.info("verbose_logging_enabled file=%s", log_file)


def enabled() -> bool:
    return _VERBOSE and _LOGGER is not None


def log_event(event: str, **fields: Any) -> None:
    if not enabled():
        return
    # Compact key=value pairs on a single line for easy grep
    payload = " ".join(
        f"{k}={repr(v)}" for k, v in fields.items()
    )
    _LOGGER.info("EVT=%s %s", event, payload)
