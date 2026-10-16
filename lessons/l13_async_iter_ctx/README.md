# Урок 13. Асинхронные итераторы, генераторы и контекстные менеджеры

## Асинхронный итератор

Протокол: `__aiter__()` (обычный метод, возвращает итератор) и
`async def __anext__()` (бросает `StopAsyncIteration` в конце).

```python
class Ticker:
    def __init__(self, n): self.i, self.n = 0, n
    def __aiter__(self): return self
    async def __anext__(self):
        if self.i >= self.n:
            raise StopAsyncIteration
        await asyncio.sleep(0.1)
        self.i += 1
        return self.i

async for x in Ticker(3): ...
```

Встроенные `aiter(obj)` и `await anext(it[, default])` (3.10+).

## Асинхронный генератор

`async def` + `yield` = асинхронный генератор — самый простой способ
сделать асинхронный итератор:

```python
async def ticker(n, delay):
    for i in range(n):
        await asyncio.sleep(delay)
        yield i
```

- Асинхронные comprehension: `[x async for x in gen if x % 2]`,
  `[await f(x) for x in items]`.
- **Закрытие.** Если цикл прервался (`break`, исключение), генератор не
  закрывается сразу — его `finally` выполнится когда-нибудь при сборке
  мусора (через хук цикла) или при `asyncio.run` → `shutdown_asyncgens()`.
  Детерминированно закрывайте сами:

  ```python
  from contextlib import aclosing
  async with aclosing(ticker(10, 0.1)) as gen:
      async for x in gen:
          if x == 3: break
  # здесь finally генератора уже выполнен
  ```
- `agen.asend(v)`, `agen.athrow(exc)`, `agen.aclose()` — асинхронные аналоги
  `send/throw/close`.
- Нельзя `yield` внутри `async with TaskGroup()` / `asyncio.timeout()` через
  границу итерации — отмена/таймаут может «вылететь» в чужой код
  потребителя. Держите группы задач внутри генератора, не пересекая `yield`,
  или используйте очередь.

## Асинхронный контекстный менеджер

Протокол: `async def __aenter__(self)` и `async def __aexit__(self, exc_type, exc, tb)`.
`__aexit__` возвращает `True`, чтобы подавить исключение.

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def connection(dsn):
    conn = await connect(dsn)
    try:
        yield conn
    finally:
        await conn.close()
```

## AsyncExitStack

Динамическое число асинхронных ресурсов, корректное закрытие в обратном
порядке даже при ошибке в середине:

```python
from contextlib import AsyncExitStack

async with AsyncExitStack() as stack:
    conns = [await stack.enter_async_context(connection(d)) for d in dsns]
    stack.push_async_callback(log_close)       # произвольная async-функция
    ...
```

`stack.pop_all()` — передать ответственность за закрытие дальше.

## Задачи

Проверка: `uv run pytest lessons/l13_async_iter_ctx -v`

1. `Countdown` — класс асинхронного итератора.
2. `aticker` — асинхронный генератор.
3. `amap`, `afilter`, `atake` — комбинаторы асинхронных итераторов;
   `atake` закрывает источник.
4. `Timer` — асинхронный контекстный менеджер класса.
5. `open_all` — `AsyncExitStack` с откатом при ошибке.
6. `merge` — слияние нескольких асинхронных итераторов по мере готовности.
7. `abatch` — группировка в пачки по размеру ИЛИ по времени.

## Вопросы для самопроверки

1. Почему `__aiter__` — обычный метод, а `__anext__` — корутина?
2. Когда выполнится `finally` асинхронного генератора, если прервать
   `async for` через `break`?
3. Зачем нужен `contextlib.aclosing`?
4. В каком порядке `AsyncExitStack` закрывает ресурсы?
5. Почему опасно делать `yield` внутри `async with asyncio.timeout(...)`?

Документация: https://docs.python.org/3/reference/expressions.html#asynchronous-generator-functions,
https://docs.python.org/3/library/contextlib.html
