# Урок 08. Future и низкоуровневый API цикла событий

## Future

`asyncio.Future` — «ячейка для будущего результата». Это низкоуровневый
строительный блок: `Task` — подкласс `Future`, а все `await` в конце концов
ждут какой-нибудь `Future`.

```python
loop = asyncio.get_running_loop()
fut = loop.create_future()          # предпочтительнее, чем asyncio.Future()

loop.call_later(1, fut.set_result, "готово")
print(await fut)                     # "готово"
```

| Метод | |
|---|---|
| `set_result(x)` / `set_exception(e)` | завершить; повторно → `InvalidStateError` |
| `cancel(msg=None)` | отменить |
| `done()`, `cancelled()`, `result()`, `exception()` | состояние |
| `add_done_callback(fn, context=None)` / `remove_done_callback(fn)` | колбэки (вызываются через `call_soon`, не сразу) |
| `get_loop()` | цикл, к которому привязан Future |

**Типичная ловушка:** перед `set_result` проверяйте `if not fut.done()` —
ожидающий мог быть отменён (по таймауту), и тогда Future уже `cancelled`.

Главное применение — **мост от колбэчного API к `await`**.

## Планирование колбэков

| Метод | Когда выполнится |
|---|---|
| `loop.call_soon(cb, *args, context=None)` | на следующей итерации цикла, FIFO |
| `loop.call_later(delay, cb, *args)` | через `delay` секунд |
| `loop.call_at(when, cb, *args)` | в момент `when` по `loop.time()` |
| `loop.call_soon_threadsafe(cb, *args)` | **единственный** безопасный способ из другого потока |

`call_soon` возвращает `Handle`, `call_later/at` — `TimerHandle`
(`cancel()`, `cancelled()`, `when()`). Колбэки — обычные функции, не
корутины; если колбэк бросил исключение, оно уходит в обработчик исключений
цикла (урок 14), а не к вам.

## Потоки и asyncio

asyncio-объекты не потокобезопасны. Правила:

- **Из другого потока в цикл (колбэк):** `loop.call_soon_threadsafe(fn, *args)`.
- **Из другого потока в цикл (корутина):**
  `cf = asyncio.run_coroutine_threadsafe(coro, loop)` → `concurrent.futures.Future`;
  блокирующее `cf.result(timeout)` в том потоке.
- **Из цикла в поток/процесс:** `loop.run_in_executor(executor, fn, *args)`
  или `asyncio.to_thread(fn, *args)` (урок 09).
- **Обёртка `concurrent.futures.Future` для `await`:**
  `asyncio.wrap_future(cf)`.
- Ссылку на цикл получайте в цикле (`get_running_loop()`) и передавайте
  в поток явно.

## Прочее API цикла

- `loop.time()` — часы цикла.
- `loop.is_running()`, `loop.is_closed()`, `loop.stop()`, `loop.close()`.
- `loop.run_until_complete(fut)`, `loop.run_forever()` — для своего
  управления циклом (обычно не нужно — есть `asyncio.run`/`Runner`).
- `loop.shutdown_asyncgens()`, `loop.shutdown_default_executor()`.
- `asyncio.isfuture(obj)`, `asyncio.ensure_future(obj)`.

## Задачи

Проверка: `uv run pytest lessons/l08_futures_loop -v`

1. `callback_to_awaitable` — превратить колбэчный API в `await`.
2. `delayed_calls` — `call_later` + отмена `TimerHandle`.
3. `Debouncer` — дебаунс через перезапуск таймера.
4. `collect_from_thread` — поток → цикл через `call_soon_threadsafe`.
5. `call_async_from_thread` — поток вызывает корутину через
   `run_coroutine_threadsafe`.
6. `await_concurrent` — `wrap_future`.

## Вопросы для самопроверки

1. Чем `Task` отличается от `Future`?
2. Почему нельзя вызвать `fut.set_result` из другого потока напрямую?
3. Что будет, если колбэк, запланированный `call_soon`, бросит исключение?
4. Почему перед `set_result` нужно проверять `fut.done()`?
5. Как дождаться результата корутины из синхронного кода в другом потоке?

Документация: https://docs.python.org/3/library/asyncio-future.html,
https://docs.python.org/3/library/asyncio-eventloop.html
