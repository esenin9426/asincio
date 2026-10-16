# Урок 09. Блокирующий код: потоки, процессы, debug-режим

## Проблема

Цикл событий однопоточный. Любой вызов, который не возвращает управление
быстро, **останавливает всё**: таймеры опаздывают, сокеты не читаются,
другие задачи стоят.

Блокирующие вызовы: `time.sleep`, `requests`, синхронные драйверы БД,
`open().read()` больших файлов, `subprocess.run`, тяжёлые вычисления
(хэши, сжатие, парсинг, ML), `input()`.

## Решения

### `asyncio.to_thread(fn, *args, **kwargs)` (3.9+)

```python
data = await asyncio.to_thread(requests.get, url)
```

- Выполняет функцию в **пуле потоков по умолчанию** цикла.
- Переносит `contextvars` в поток.
- Подходит для I/O-bound блокирующих библиотек. Для CPU-bound кода
  потоки почти бесполезны из-за GIL (кроме free-threaded сборок 3.13t).

### `loop.run_in_executor(executor, fn, *args)`

```python
loop = asyncio.get_running_loop()
with concurrent.futures.ProcessPoolExecutor() as pool:
    result = await loop.run_in_executor(pool, cpu_heavy, data)
```

- `executor=None` — пул потоков по умолчанию (`loop.set_default_executor`).
- `ThreadPoolExecutor(max_workers=n)` — свой пул, ограничение потоков.
- `ProcessPoolExecutor` — для CPU-bound: настоящая параллельность.
  Функция и аргументы должны быть **picklable** (функции — на уровне модуля).
- kwargs не поддерживаются → `functools.partial`.

### Потоки нельзя отменить

Отмена `await asyncio.to_thread(...)` отменяет только **ожидание**: функция
в потоке продолжает работать до конца. Для настоящей остановки нужна
кооперация — `threading.Event`, который функция периодически проверяет.

### Пул по умолчанию ограничен

`min(32, os.cpu_count() + 4)` потоков. Если 100 задач одновременно вызовут
`to_thread`, лишние встанут в очередь.

## Debug-режим

Включается: `asyncio.run(main(), debug=True)`, `PYTHONASYNCIODEBUG=1`,
`python -X dev`, `loop.set_debug(True)`.

Что даёт:
- лог `Executing <Task ...> took 0.153 seconds` для колбэков/шагов задач
  дольше `loop.slow_callback_duration` (по умолчанию 0.1 с) — **находит
  блокирующий код**;
- предупреждения о не-`await`-нутых корутинах с местом создания;
- проверки потокобезопасности (вызов не из потока цикла → `RuntimeError`);
- трассировки создания задач и хэндлов в логах.

Логи пишутся в логгер `asyncio` (модуль `logging`).

## Задачи

Проверка: `uv run pytest lessons/l09_blocking -v`

1. `run_blocking_concurrently` — `to_thread` для пачки блокирующих вызовов.
2. `count_primes_parallel` — `ProcessPoolExecutor` + `run_in_executor`.
3. `heartbeat_while` — доказать, что цикл не блокируется.
4. `bounded_to_thread` — свой `ThreadPoolExecutor` с лимитом потоков.
5. `to_thread_with_timeout` — кооперативная остановка потока по таймауту.
6. `find_blocking` — поиск блокирующего кода через debug-режим и логгер.

## Вопросы для самопроверки

1. Почему `time.sleep(1)` в корутине — ошибка, а `await asyncio.sleep(1)` — нет?
2. Когда нужен `ProcessPoolExecutor`, а когда достаточно `to_thread`?
3. Что произойдёт с функцией в потоке, если отменить `await to_thread(...)`?
4. Почему функция для `ProcessPoolExecutor` должна быть на уровне модуля?
5. Как найти, какой колбэк блокирует цикл?

Документация: https://docs.python.org/3/library/asyncio-dev.html,
https://docs.python.org/3/library/asyncio-eventloop.html#executing-code-in-thread-or-process-pools
