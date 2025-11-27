# Temporary Logging Additions

Все ниже перечисленные сообщения пишет отдельный логгер `tracking_debug`
в файл `tracking_debug.log` (путь можно переопределить переменной окружения
`TRACK_LOG_FILE`).

## PackageCommit (main.py)
- Logs `package commit prepared batch items=<count> specifics=<count>` before the buffers are reset.
- Logs `package commit stored batch items=<count> specifics=<count>` after `session.commit()`.
- Purpose: confirm that SQL batches are assembled and written to the DB layer.

## retry decorator (main.py)
- On exhausting retries writes `retry exhausted for <func>: <repr(error)>`.
- Purpose: surface silent failures in `product`/`catalog_request` after all attempts.

## product() (main.py)
- Logs `product start item=<id> query=<q> cycle=<cycle>` right after lock acquisition.
- Logs `product committed item=<id> query=<q>` once DB commit succeeds.
- Purpose: trace lifecycle of individual items from queue through DB write.

## task_collect_loop() (main.py)
- Logs `catalog result query=<q> new_ids=<n>` after catalog scan.
- Logs `set_zip_code start/ok` + proxy details и `set_zip_code failed ...` (stack trace при исключении).
- Logs `product dispatch item=<id> query=<q> cycle=<cycle>` before calling `product()` for each item.
- Purpose: ensure every enqueued item is handed off to `product()` and detect failures in `set_zip_code`.

> Эти логи временные и добавлены для расследования пропавших вставок. После устранения причины их нужно убрать, чтобы не зашумлять продакшен-логи.
