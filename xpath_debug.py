"""Временный лог значений, извлекаемых по XPath."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

# ВРЕМЕННО
# Что: путь до лог-файла; Зачем: накапливать значения по item_id
_LOG_FILE = Path(__file__).resolve().parent / "xpath_debug.log"


def record_xpath_value(item_id: str, value: Optional[str]) -> None:
    """Добавляет строку `<item_id> - <значение|NONE>` в лог."""

    raw = (value or "").replace("\n", " ").strip()
    text = raw if raw else "NONE"
    line = f"{item_id} - {text}\n"
    try:
        with _LOG_FILE.open("a", encoding="utf-8") as handle:
            handle.write(line)
    except Exception:
        pass


# ВРЕМЕННО (конец кода)
