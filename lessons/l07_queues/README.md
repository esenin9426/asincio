# Урок 07. Очереди

`asyncio.Queue` — основной способ передавать данные между корутинами
(producer/consumer, пулы воркеров, конвейеры). Не потокобезопасна!
Для потоков см. урок 08 (`call_soon_threadsafe`) или `queue.Queue` +
`to_thread`.

## Виды

| Класс | Порядок выдачи |
|---|---|
| `Queue(maxsize=0)` | FIFO |
| `PriorityQueue` | наименьший элемент первым (`heapq`); элементы — обычно кортежи `(priority, counter, data)` |
| `LifoQueue` | стек |

`maxsize=0` — без ограничения. При `maxsize > 0` `put()` ждёт, пока
освободится место — это и есть **backpressure**: быстрый производитель
притормаживает до скорости потребителя.

## API

| Метод | |
|---|---|
| `await put(x)` / `put_nowait(x)` | `put_nowait` бросает `QueueFull` |
| `await get()` / `get_nowait()` | `get_nowait` бросает `QueueEmpty` |
| `task_done()` | «элемент обработан» — по одному вызову на каждый `get` |
| `await join()` | ждёт, пока все положенные элементы не будут обработаны (`task_done`) |
| `qsize()`, `empty()`, `full()`, `maxsize` | состояние |
| `shutdown(immediate=False)` (3.13+) | закрыть очередь (см. ниже) |

## Пул воркеров — классический шаблон

```python
async def worker(q):
    while True:
        item = await q.get()
        try:
            await handle(item)
        finally:
            q.task_done()

q = asyncio.Queue()
workers = [asyncio.create_task(worker(q)) for _ in range(5)]
for x in data:
    q.put_nowait(x)
await q.join()             # всё обработано
for w in workers:
    w.cancel()             # воркеры висят на get() — отменяем
await asyncio.gather(*workers, return_exceptions=True)
```

Альтернатива отмене — «ядовитая пилюля» (sentinel, например `None`),
по одной на воркера.

## `Queue.shutdown()` (Python 3.13+)

- После `shutdown()` любой `put` бросает `asyncio.QueueShutDown`.
- `shutdown(immediate=False)`: `get` продолжает отдавать оставшиеся
  элементы, а на пустой очереди бросает `QueueShutDown` → воркеры
  завершаются сами, без отмены и sentinel-ов.
- `shutdown(immediate=True)`: очередь сразу очищается (для выброшенных
  элементов `task_done` вызывается автоматически), все ждущие `get`/`put`
  получают `QueueShutDown`.

```python
async def worker(q):
    while True:
        try:
            item = await q.get()
        except asyncio.QueueShutDown:
            return
        ...
```

## PriorityQueue и одинаковые приоритеты

`(1, {"a": 1}) < (1, {"b": 2})` → `TypeError`, потому что при равных
приоритетах сравниваются данные. Добавляйте монотонный счётчик:
`(priority, next(counter), data)` — это ещё и сохраняет FIFO-порядок для
равных приоритетов.

## Задачи

Проверка: `uv run pytest lessons/l07_queues -v`

1. `producer_consumer` — пул воркеров с `join` и отменой.
2. `priority_order` — `PriorityQueue` со стабильным порядком.
3. `pipeline` — двухэтапный конвейер с ограниченными очередями.
4. `WorkerPool` — пул на `Queue.shutdown()`.

## Вопросы для самопроверки

1. Что произойдёт, если забыть вызвать `task_done()`?
2. Как ограничить потребление памяти, если производитель быстрее потребителя?
3. Чем отличается `shutdown(immediate=True)` от `immediate=False`?
4. Почему в `PriorityQueue` нужен счётчик?
5. Как правильно остановить воркеров, ждущих на `get()`?

Документация: https://docs.python.org/3/library/asyncio-queue.html
