# Урок 03. Ожидание группы задач: gather, wait, as_completed, таймауты

## `asyncio.gather(*aws, return_exceptions=False)`

- Принимает корутины/задачи/фьючерсы, корутины оборачивает в задачи.
- Возвращает **список результатов в порядке аргументов** (не завершения).
- `return_exceptions=False`: первое исключение сразу пробрасывается в
  `await gather(...)`, **но остальные задачи продолжают работать** — их
  никто не отменяет! Это частый источник «висящих» задач.
- `return_exceptions=True`: исключения возвращаются в списке как значения.
- Отмена самого `gather` отменяет все его дочерние задачи.

## `asyncio.wait(aws, *, timeout=None, return_when=ALL_COMPLETED)`

- Принимает **итерируемое задач/фьючерсов** (корутины — запрещены с 3.11).
- Возвращает `(done, pending)` — два множества. Исключения не бросает,
  результаты забираются из задач вручную.
- `return_when`: `FIRST_COMPLETED`, `FIRST_EXCEPTION`, `ALL_COMPLETED`.
- По `timeout` просто возвращает, что успело, **ничего не отменяя**.

```python
done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
for t in pending:
    t.cancel()
```

## `asyncio.as_completed(aws, *, timeout=None)`

Итерация по мере завершения:

```python
for fut in asyncio.as_completed(coros):
    result = await fut            # результаты в порядке готовности
```

С 3.13 поддерживает `async for` и тогда отдаёт **исходные задачи**:

```python
async for task in asyncio.as_completed(tasks):
    print(task.get_name(), task.result())
```

## Таймауты

| Инструмент | Поведение |
|---|---|
| `await asyncio.wait_for(aw, timeout)` | отменяет `aw` по таймауту, бросает `TimeoutError` |
| `async with asyncio.timeout(delay):` (3.11+) | отменяет **текущую задачу** при истечении, на выходе превращает отмену в `TimeoutError` |
| `async with asyncio.timeout_at(when):` | то же, но абсолютный дедлайн по `loop.time()` |
| `cm.reschedule(new_when)` / `cm.when()` / `cm.expired()` | перенос и проверка дедлайна |
| `asyncio.wait(..., timeout=)` | **не** отменяет и **не** бросает |

С 3.11 `asyncio.TimeoutError is TimeoutError`.

```python
async with asyncio.timeout(1.5) as cm:
    await step1()
    cm.reschedule(asyncio.get_running_loop().time() + 5)   # продлить
    await step2()
```

## `asyncio.shield(aw)`

Защищает внутреннюю операцию от отмены *внешнего* ожидающего: внешний
`await` получит `CancelledError`, а сама операция продолжит работу.
Подробно — в уроке 04.

## Как выбрать

- Нужны все результаты по порядку → `gather` (или `TaskGroup`, урок 05).
- Нужен первый готовый / контроль над хвостом → `wait(FIRST_COMPLETED)`.
- Обрабатывать результаты по мере готовности → `as_completed`.
- Ограничить время одной операции → `timeout` / `wait_for`.

## Задачи

Проверка: `uv run pytest lessons/l03_combinators -v`

1. `fetch_all` — `gather` с порядком результатов.
2. `fetch_all_safe` — `gather(return_exceptions=True)`, разделить успехи и ошибки.
3. `first_completed` — `wait(FIRST_COMPLETED)` + отмена хвоста (гонка запросов).
4. `in_completion_order` — `as_completed`.
5. `with_timeout` — значение по умолчанию при таймауте.
6. `deadline_batch` — частичные результаты к дедлайну.
7. `staged_deadline` — `timeout` с `reschedule`.

## Вопросы для самопроверки

1. Что случится с остальными задачами `gather`, если одна упала (без `return_exceptions`)?
2. Чем `wait(timeout=...)` отличается от `wait_for`?
3. В каком порядке `gather` возвращает результаты? А `as_completed`?
4. Почему в `asyncio.wait` нельзя передавать корутины?
5. Что делает `asyncio.timeout` с текущей задачей, когда время вышло?

Документация: https://docs.python.org/3/library/asyncio-task.html#running-tasks-concurrently
