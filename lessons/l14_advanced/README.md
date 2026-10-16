# Урок 14. Продвинутое: contextvars, обработка ошибок цикла, сигналы, фабрики задач, тестирование

## contextvars — «thread-local» для корутин

Глобальные переменные общие для всех корутин; `threading.local` — тоже
(поток один). Для данных «текущего запроса» (request id, пользователь,
транзакция) — `contextvars.ContextVar`.

```python
import contextvars
request_id = contextvars.ContextVar("request_id", default="-")

async def handle(rid):
    token = request_id.set(rid)
    try:
        await process()          # везде внутри request_id.get() == rid
    finally:
        request_id.reset(token)  # вернуть прежнее значение
```

Правила:
- Каждая **задача** при создании получает **копию** текущего контекста
  (`contextvars.copy_context()`). Изменения внутри задачи не видны снаружи.
- Обычный `await coro()` выполняется в **том же** контексте — изменения
  видны вызывающему (поэтому нужен `reset(token)`).
- `create_task(coro, context=ctx)` (3.11+) — запустить в заданном контексте.
- `loop.call_soon(cb, context=ctx)`, `ctx.run(fn, *args)`.
- `asyncio.to_thread` переносит контекст в поток; `run_in_executor` — нет.

## Обработчик исключений цикла

Ошибки, которым «некуда» пойти: исключение в колбэке `call_soon`,
`Task exception was never retrieved`, ошибки транспортов. По умолчанию
пишутся в лог `asyncio`.

```python
def handler(loop, context):
    # context: {"message": ..., "exception": ..., "future"/"task"/"handle": ...}
    log.error("loop error: %s", context.get("exception") or context["message"])

loop.set_exception_handler(handler)
loop.default_exception_handler(context)   # стандартное поведение
loop.call_exception_handler(context)      # вызвать вручную
```

## Сигналы (Unix)

```python
loop = asyncio.get_running_loop()
stop = asyncio.Event()
loop.add_signal_handler(signal.SIGTERM, stop.set)
await stop.wait()                       # graceful shutdown
loop.remove_signal_handler(signal.SIGTERM)
```

Обработчик — обычный колбэк, вызывается в потоке цикла (безопасно трогать
asyncio-объекты). `asyncio.run` сам обрабатывает `Ctrl+C` (SIGINT):
отменяет главную задачу.

## Фабрики задач и eager tasks

- `loop.set_task_factory(factory)` — `factory(loop, coro, **kwargs) -> Task`.
  Можно подменить создание задач: метрики, трассировка, свой класс задач.
- `asyncio.eager_task_factory` (3.12+): корутина начинает выполняться
  **синхронно внутри `create_task`** до первой реальной приостановки. Если
  она завершилась без ожидания (например, ответ из кэша) — задача уже
  `done()` сразу после создания, без планирования через цикл. Ускоряет код
  с большим числом «быстрых» задач.
  `asyncio.create_eager_task_factory(custom_task_constructor)` — для своих классов.
- В 3.12+ можно передать `loop_factory` в `asyncio.run`/`Runner`, а в
  `Runner` настраивать цикл через `runner.get_loop()`.

## Политики и альтернативные циклы

`uvloop` — быстрая реализация цикла на libuv:
`asyncio.run(main(), loop_factory=uvloop.new_event_loop)`. Политики
(`set_event_loop_policy`) устаревают с 3.14 — используйте `loop_factory`.

## Тестирование асинхронного кода

- `unittest.IsolatedAsyncioTestCase` — встроенный: `async def test_*`,
  `asyncSetUp`, `asyncTearDown`, `addAsyncCleanup`.
- `pytest-asyncio` / `anyio` — плагины для pytest (в курсе — свой
  мини-хук в `conftest.py`, загляните в него).
- Не полагайтесь на точные тайминги: проверяйте порядок событий, а для
  времени — диапазоны.
- Debug-режим (`PYTHONASYNCIODEBUG=1`) в CI ловит незакрытые ресурсы и
  блокировки.

## Задачи

Проверка: `uv run pytest lessons/l14_advanced -v`

1. `log` / `handle_request` — contextvars и `reset(token)`.
2. `run_in_context` — `create_task(..., context=)`.
3. `capture_callback_errors` — свой обработчик исключений цикла.
4. `wait_for_signal` — обработка сигнала.
5. `run_eager` — `eager_task_factory`.
6. `install_task_counter` — своя фабрика задач.

## Вопросы для самопроверки

1. Почему изменение `ContextVar` внутри задачи не видно снаружи, а внутри
   `await coro()` — видно?
2. Куда попадает исключение из колбэка `call_soon`?
3. В каком потоке выполняется обработчик, добавленный `add_signal_handler`?
4. В чём выигрыш `eager_task_factory` и когда он может изменить поведение
   программы?
5. Как подменить реализацию цикла событий на uvloop?

Документация: https://docs.python.org/3/library/contextvars.html,
https://docs.python.org/3/library/asyncio-eventloop.html#error-handling-api
