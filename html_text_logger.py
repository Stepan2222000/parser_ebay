"""Сохраняет текст карточки eBay для временной диагностики."""

from __future__ import annotations

from pathlib import Path

# ВРЕМЕННО
_LOG_FILE = Path(__file__).resolve().parent / "html_text.log"


def log_html_text(item_id: str, text: str) -> None:
    """Пишет строку `<item_id> - текст|NONE` в логовый файл."""

    payload = text.replace("\n", " ").strip()
    snippet = payload[:100] if payload else "NONE"
    line = f"{item_id} - {snippet}\n"
    try:
        with _LOG_FILE.open("a", encoding="utf-8") as handle:
            handle.write(line)
    except Exception:
        pass


# ВРЕМЕННО (конец кода)
