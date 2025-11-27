from __future__ import annotations

import json
import logging
import subprocess
import time
from pathlib import Path
from typing import Dict, List


PROJECT_ROOT = Path(__file__).resolve().parent
FILES_DIR = PROJECT_ROOT / "files"
ENV_FILE = PROJECT_ROOT / ".env"
LOG_FILE = PROJECT_ROOT / "batch_runner.log"

COMPOSE_BIN = ["docker", "compose"]
SERVICE_TO_WATCH = "app-main"
POLL_SECONDS = 10


def setup_logger() -> logging.Logger:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("batch_runner")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)
    logger.propagate = False
    return logger


LOGGER = setup_logger()


def list_excel_files() -> List[Path]:
    if not FILES_DIR.exists():
        raise FileNotFoundError(f"Каталог с Excel не найден: {FILES_DIR}")
    files = sorted(FILES_DIR.glob("*.xlsx"))
    if not files:
        raise FileNotFoundError(f"В каталоге {FILES_DIR} нет файлов *.xlsx")
    return files


def rewrite_env(excel_path: str) -> None:
    lines: List[str] = []
    found = False
    if ENV_FILE.exists():
        content = ENV_FILE.read_text(encoding="utf-8").splitlines()
    else:
        content = []
    for line in content:
        if line.startswith("EXCEL_PATH="):
            lines.append(f"EXCEL_PATH={excel_path}")
            found = True
        else:
            lines.append(line)
    if not found:
        lines.append(f"EXCEL_PATH={excel_path}")
    ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_compose(*args: str) -> subprocess.CompletedProcess[str]:
    command = COMPOSE_BIN + list(args)
    LOGGER.info("Выполняю команду: %s", " ".join(command))
    try:
        result = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            check=True,
            text=True,
            capture_output=True,
        )
        if result.stdout:
            LOGGER.debug("stdout:\n%s", result.stdout.strip())
        if result.stderr:
            LOGGER.debug("stderr:\n%s", result.stderr.strip())
        return result
    except subprocess.CalledProcessError as exc:
        LOGGER.error("Команда завершилась с ошибкой (код %s)", exc.returncode)
        if exc.stdout:
            LOGGER.error("stdout:\n%s", exc.stdout.strip())
        if exc.stderr:
            LOGGER.error("stderr:\n%s", exc.stderr.strip())
        raise


def get_services_state() -> Dict[str, Dict[str, str]]:
    result = subprocess.run(
        COMPOSE_BIN + ["ps", "--format", "json"],
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    raw = result.stdout.strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            items = [data]
        else:
            items = list(data)
    except json.JSONDecodeError as exc:
        items = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except json.JSONDecodeError as inner_exc:
                raise RuntimeError(
                    f"Не удалось распарсить вывод docker compose ps: {inner_exc}\n{result.stdout}"
                ) from inner_exc
    state_map: Dict[str, Dict[str, str]] = {}
    for item in items:
        service = item.get("Service")
        if service:
            state_map[service] = item
    return state_map


def wait_for_service_exit(timeout: int | None = None) -> int:
    LOGGER.info("Ожидаю завершения сервиса %s", SERVICE_TO_WATCH)
    start = time.monotonic()
    while True:
        states = get_services_state()
        service = states.get(SERVICE_TO_WATCH)
        if service:
            state = service.get("State", "")
            exit_code = service.get("ExitCode")
            LOGGER.debug("Состояние %s: %s (exit=%s)", SERVICE_TO_WATCH, state, exit_code)
            if state.lower().startswith("exited"):
                try:
                    return int(exit_code)
                except (TypeError, ValueError):
                    return 0
        else:
            LOGGER.debug("Сервис %s ещё не запущен или уже удалён", SERVICE_TO_WATCH)
        if timeout is not None and time.monotonic() - start > timeout:
            raise TimeoutError(f"Сервис {SERVICE_TO_WATCH} не завершился за {timeout} секунд")
        time.sleep(POLL_SECONDS)


def process_file(path: Path) -> None:
    container_path = f"/opt/app/files/{path.name}"
    LOGGER.info("Начинаю обработку файла %s", path)
    rewrite_env(container_path)
    LOGGER.info("EXCEL_PATH обновлён: %s", container_path)

    # Всегда стараемся стартовать с чистого состояния
    try:
        run_compose("down", "--remove-orphans")
        LOGGER.info("Ожидаю 3 секунды перед запуском контейнеров...")
        time.sleep(3)
    except subprocess.CalledProcessError as exc:
        LOGGER.warning("docker compose down завершился с ошибкой: %s", exc)

    run_compose("up", "-d")
    exit_code = 0
    try:
        exit_code = wait_for_service_exit()
        LOGGER.info("Сервис %s завершился с кодом %s", SERVICE_TO_WATCH, exit_code)
    finally:
        try:
            run_compose("down")
        except subprocess.CalledProcessError as exc:
            LOGGER.warning("docker compose down завершился с ошибкой: %s", exc)
    if exit_code != 0:
        raise RuntimeError(f"Контейнер {SERVICE_TO_WATCH} завершился с кодом {exit_code}")
    LOGGER.info("Обработка файла %s завершена успешно", path)


def main() -> None:
    LOGGER.info("Старт пакетной обработки Excel")
    files = list_excel_files()
    for index, file_path in enumerate(files, start=1):
        LOGGER.info("Файл %s из %s", index, len(files))
        try:
            process_file(file_path)
        except Exception as exc:
            LOGGER.error("Ошибка при обработке %s: %s", file_path, exc, exc_info=True)
            LOGGER.info("Останавливаю пакетную обработку")
            break
    else:
        LOGGER.info("Все файлы обработаны успешно")


if __name__ == "__main__":
    main()
