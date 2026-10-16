# Урок 06. Примитивы синхронизации

## Зачем, если поток один?

Гонки в asyncio возникают **между точками `await`**. Код
«прочитал → `await` → записал» не атомарен: пока корутина ждёт, другая
успевает изменить то же состояние.

```python
balance = self.balance
await asyncio.sleep(0)      # тут может вклиниться другая корутина
self.balance = balance + 1  # потерянное обновление
```

Примитивы asyncio **не потокобезопасны** — они для корутин одного цикла.
Для потоков — `threading` и `loop.call_soon_threadsafe` (урок 08).

## Обзор

| Примитив | Назначение | API |
|---|---|---|
| `Lock` | взаимное исключение | `async with lock:`, `acquire()`, `release()`, `locked()` |
| `Event` | «флаг»: ждать, пока кто-то выставит | `set()`, `clear()`, `is_set()`, `await wait()` |
| `Condition` | ждать произвольного условия на общем состоянии | `async with cond:`, `await wait()`, `await wait_for(pred)`, `notify(n)`, `notify_all()` |
| `Semaphore(n)` | не более n одновременно | `async with sem:` |
| `BoundedSemaphore(n)` | то же + `ValueError` при лишнем `release()` | |
| `Barrier(n)` (3.11+) | n участников ждут друг друга | `await wait()` → индекс, `reset()`, `abort()`, `parties`, `n_waiting`, `broken` |

Все `acquire/wait` — корутины; `release/set/notify` — обычные методы.
Порядок пробуждения ожидающих — FIFO (честность).

## Lock

```python
lock = asyncio.Lock()
async with lock:
    value = await read()
    await write(value + 1)
```

Lock не реентерабельный: повторный `acquire` в той же задаче — дедлок.

## Event

```python
ready = asyncio.Event()
async def waiter():
    await ready.wait()
...
ready.set()   # будит ВСЕХ ожидающих; пока не clear(), wait() возвращается сразу
```

## Condition

Condition = Lock + очередь ожидающих. `wait()` **отпускает** lock на время
ожидания и снова захватывает перед возвратом. Всегда проверяйте условие в
цикле (или используйте `wait_for(predicate)`):

```python
async with cond:
    await cond.wait_for(lambda: len(items) > 0)
    item = items.pop()
    cond.notify_all()
```

`notify()` можно вызывать только при захваченном lock.

## Semaphore

```python
sem = asyncio.Semaphore(10)
async def fetch(url):
    async with sem:           # не более 10 запросов одновременно
        return await http_get(url)
```

## Barrier

```python
barrier = asyncio.Barrier(3)
async def worker():
    await phase1()
    await barrier.wait()      # ждём, пока все 3 закончат фазу 1
    await phase2()
```

Барьер циклический: после прохода всех участников его можно использовать
снова. `abort()` переводит в состояние broken → ожидающие получают
`BrokenBarrierError`.

## Задачи

Проверка: `uv run pytest lessons/l06_sync -v`

1. `SafeCounter` — исправить гонку через `Lock`.
2. `limited_gather` — `Semaphore`.
3. `Latch` — «защёлка» обратного отсчёта на `Event`.
4. `BoundedBuffer` — ограниченный буфер на `Condition`.
5. `phased_workers` — фазы через `Barrier`.
6. `ConnectionPool` — пул соединений на `BoundedSemaphore`.
7. `RWLock` — блокировка «читатели–писатель» на `Condition` (со звёздочкой).

## Вопросы для самопроверки

1. Где возможны гонки в однопоточном asyncio-коде?
2. Можно ли использовать `asyncio.Lock` из нескольких потоков?
3. Почему `Condition.wait()` нужно вызывать в цикле проверки условия?
4. Чем `BoundedSemaphore` отличается от `Semaphore`?
5. Что произойдёт с ожидающими `Barrier.wait()` после `abort()`?

Документация: https://docs.python.org/3/library/asyncio-sync.html
