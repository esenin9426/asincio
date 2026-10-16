# Урок 15. Под капотом: пишем свой цикл событий

Лучший способ понять asyncio — написать игрушечную версию.

## Корутина — это генератор

`await x` ≈ `yield from x.__await__()`. Корутину можно «крутить» вручную:

```python
async def f():
    await Sleep(1)          # Sleep.__await__ делает yield ("sleep", 1)
    return 42

coro = f()
cmd = coro.send(None)       # ("sleep", 1) — корутина приостановилась и «сказала», чего ждёт
try:
    coro.send(None)         # продолжить
except StopIteration as e:
    print(e.value)          # 42 — результат корутины
```

Значение, которое корутина отдаёт через `yield`, видит только **тот, кто
её крутит** — цикл событий. В asyncio это всегда `Future`: задача
(`Task.__step`) получает Future, подписывается на него `add_done_callback`
и продолжит корутину, когда Future завершится.

## Устройство цикла asyncio (упрощённо)

```
while not stopped:
    timeout = 0 if ready else (ближайший таймер - now)
    events = selector.select(timeout)       # epoll/kqueue: ждём I/O или таймер
    for key in events: ready.append(callback для сокета)
    while timers and timers[0].when <= now: ready.append(heappop(timers))
    for handle in ready (сколько было на начало итерации): handle.run()
```

- `ready` — очередь `Handle` (то, что дают `call_soon`).
- `timers` — куча `TimerHandle` (`call_later/call_at`).
- `selector` — `selectors.DefaultSelector` (kqueue на macOS, epoll на Linux).
- `Task.__step` — колбэк, который делает `coro.send(None)` до следующего
  `yield`.
- Отмена = `coro.throw(CancelledError)` на следующем шаге.

Исходники для чтения:
`Lib/asyncio/base_events.py` (`_run_once`), `Lib/asyncio/tasks.py`
(`__step`), `Lib/asyncio/futures.py`, `Lib/asyncio/selector_events.py`.

## Задачи

Проверка: `uv run pytest lessons/l15_under_the_hood -v`

Никакого `asyncio` в решении — только генераторы, `heapq`, `collections`,
`selectors`, `time`.

1. `Sleep`, `ReadReady` — awaitable-«команды» циклу.
2. `MiniTask` — обёртка над корутиной: результат, исключение, ожидание
   другой задачи через `await task`.
3. `MiniLoop` — ready-очередь, куча таймеров, `selectors` для I/O,
   `spawn`, `run_until_complete`, отмена.

## Вопросы для самопроверки

1. Что видит цикл событий, когда корутина делает `await asyncio.sleep(1)`?
2. Как цикл определяет, сколько ждать в `select()`?
3. Как реализована отмена задачи на уровне генератора?
4. Почему колбэки, добавленные во время итерации, выполняются только на
   следующей?
5. Что общего у `Task` и `Future`?
