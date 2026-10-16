"""Урок 06 — задачи. Проверка: uv run pytest lessons/l06_sync -v"""

import asyncio
import contextlib
from collections.abc import AsyncIterator, Coroutine
from typing import Any, TypeVar

T = TypeVar("T")


class SafeCounter:
    """Задача 1. Счётчик с гонкой — исправьте его.

    Сейчас 100 конкурентных increment() дают значение меньше 100.
    Добавьте asyncio.Lock. Строку `await asyncio.sleep(0)` между чтением
    и записью НЕ удаляйте — она имитирует I/O.
    """

    def __init__(self) -> None:
        self.value = 0

    async def increment(self) -> None:
        current = self.value
        await asyncio.sleep(0)
        self.value = current + 1


async def limited_gather(coros: list[Coroutine[Any, Any, T]], limit: int) -> list[T]:
    """Задача 2.

    Выполните все корутины конкурентно, но так, чтобы одновременно
    выполнялось не более `limit` из них (asyncio.Semaphore).
    Результаты — в исходном порядке.
    """
    raise NotImplementedError


class Latch:
    """Задача 3. Защёлка обратного отсчёта (CountDownLatch).

    - `count_down()` уменьшает счётчик (ниже нуля не опускается).
    - `await wait()` ждёт, пока счётчик станет 0 (если уже 0 — сразу).
    - `count` — текущее значение.
    Используйте asyncio.Event.
    """

    def __init__(self, count: int) -> None:
        raise NotImplementedError

    @property
    def count(self) -> int:
        raise NotImplementedError

    def count_down(self) -> None:
        raise NotImplementedError

    async def wait(self) -> None:
        raise NotImplementedError


class BoundedBuffer:
    """Задача 4. Ограниченный FIFO-буфер на asyncio.Condition (без Queue!).

    - `await put(item)` ждёт, если буфер заполнен (len == capacity).
    - `await get()` ждёт, если буфер пуст; возвращает самый старый элемент.
    - `len(buf)` — текущее число элементов.
    """

    def __init__(self, capacity: int) -> None:
        raise NotImplementedError

    def __len__(self) -> int:
        raise NotImplementedError

    async def put(self, item: Any) -> None:
        raise NotImplementedError

    async def get(self) -> Any:
        raise NotImplementedError


async def phased_workers(n: int, phases: int, log: list[tuple[int, int]]) -> None:
    """Задача 5.

    Запустите n воркеров (id 0..n-1). Каждый воркер для каждой фазы
    p = 0..phases-1: спит `0.01 * ((id + p) % 3)` секунд, добавляет
    в log кортеж (p, id) и ждёт на asyncio.Barrier(n).
    Итог: все записи фазы p в log стоят раньше любой записи фазы p+1.
    Дождитесь завершения всех воркеров.
    """
    raise NotImplementedError


class ConnectionPool:
    """Задача 6. Пул из `size` «соединений» — целых чисел 0..size-1.

    - `await acquire()` → свободный id соединения (наименьший из свободных);
      если свободных нет — ждёт.
    - `release(conn)` → вернуть соединение. Возврат соединения, которое не
      выдано (в т.ч. повторный), — ValueError. Используйте BoundedSemaphore
      как счётчик свободных мест.
    - `connection()` — асинхронный контекстный менеджер:
        async with pool.connection() as conn: ...
    - `in_use` — сколько соединений выдано сейчас.
    """

    def __init__(self, size: int) -> None:
        raise NotImplementedError

    @property
    def in_use(self) -> int:
        raise NotImplementedError

    async def acquire(self) -> int:
        raise NotImplementedError

    def release(self, conn: int) -> None:
        raise NotImplementedError

    @contextlib.asynccontextmanager
    async def connection(self) -> AsyncIterator[int]:
        raise NotImplementedError
        yield


class RWLock:
    """Задача 7 (*). Блокировка «много читателей ИЛИ один писатель».

    - `async with lock.read():` — может быть много читателей одновременно,
      если нет активного писателя.
    - `async with lock.write():` — эксклюзивно: ни читателей, ни других
      писателей.
    - Приоритет писателей: если писатель ждёт, НОВЫЕ читатели не входят
      (иначе писатель может голодать бесконечно).
    Реализуйте на одном asyncio.Condition.
    """

    def __init__(self) -> None:
        raise NotImplementedError

    @contextlib.asynccontextmanager
    async def read(self) -> AsyncIterator[None]:
        raise NotImplementedError
        yield

    @contextlib.asynccontextmanager
    async def write(self) -> AsyncIterator[None]:
        raise NotImplementedError
        yield
