"""Урок 13 — задачи. Проверка: uv run pytest lessons/l13_async_iter_ctx -v"""

import asyncio
import contextlib
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from typing import Any, TypeVar

T = TypeVar("T")
U = TypeVar("U")


class Countdown:
    """Задача 1. Асинхронный итератор (класс, БЕЗ async-генератора):
    `async for x in Countdown(3, 0.01)` отдаёт 3, 2, 1, перед каждым
    значением ожидая delay секунд."""

    def __init__(self, start: int, delay: float) -> None:
        raise NotImplementedError

    def __aiter__(self):
        raise NotImplementedError

    async def __anext__(self) -> int:
        raise NotImplementedError


async def aticker(interval: float, count: int) -> AsyncIterator[float]:
    """Задача 2. Асинхронный генератор: `count` раз, с интервалом
    `interval` отдаёт время `loop.time()` в момент тика. Тики должны идти
    по сетке start + k*interval без накопления дрейфа (если обработка
    потребителем заняла время — следующий сон короче)."""
    raise NotImplementedError
    yield


async def amap(fn: Callable[[T], Awaitable[U]], source: AsyncIterable[T]) -> AsyncIterator[U]:
    """Задача 3a. Для каждого x из source отдаёт `await fn(x)`."""
    raise NotImplementedError
    yield


async def afilter(pred: Callable[[T], bool], source: AsyncIterable[T]) -> AsyncIterator[T]:
    """Задача 3b. Отдаёт элементы source, для которых pred(x) истинно
    (pred — обычная функция)."""
    raise NotImplementedError
    yield


async def atake(n: int, source: AsyncIterator[T]) -> list[T]:
    """Задача 3c. Верните первые n элементов source списком. После этого
    source нужно ЗАКРЫТЬ (aclose), чтобы его finally выполнился сразу."""
    raise NotImplementedError


class Timer:
    """Задача 4. Асинхронный контекстный менеджер (класс):

        async with Timer() as t:
            ...
        t.elapsed   # секунды по loop.time()

    Если внутри блока возникло исключение TimeoutError — подавить его
    и выставить `t.timed_out = True`; прочие исключения не подавлять.
    """

    def __init__(self) -> None:
        self.elapsed: float = 0.0
        self.timed_out: bool = False

    async def __aenter__(self) -> "Timer":
        raise NotImplementedError

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        raise NotImplementedError


@contextlib.asynccontextmanager
async def open_all(
    names: list[str],
    opener: Callable[[str], contextlib.AbstractAsyncContextManager[Any]],
) -> AsyncIterator[list[Any]]:
    """Задача 5.

    Откройте `opener(name)` для каждого имени (это асинхронные контекстные
    менеджеры) через AsyncExitStack и отдайте список полученных ресурсов.
    При выходе — закрыть всё в обратном порядке. Если открытие какого-то
    ресурса упало — уже открытые должны закрыться, исключение
    пробрасывается.
    """
    raise NotImplementedError
    yield


async def merge(*sources: AsyncIterable[T]) -> AsyncIterator[T]:
    """Задача 6.

    Отдавайте элементы из всех источников по мере их готовности
    (источники читаются конкурентно). Завершиться, когда исчерпаны все.
    Если источник упал — пробросить исключение потребителю.
    Если потребитель прекратил итерацию (aclose) — все фоновые задачи
    чтения должны быть отменены.
    Подсказка: по задаче на источник + asyncio.Queue.
    """
    raise NotImplementedError
    yield


async def abatch(
    source: AsyncIterable[T], max_size: int, max_wait: float
) -> AsyncIterator[list[T]]:
    """Задача 7 (*).

    Группирует элементы source в пачки: пачка отдаётся, когда в ней
    max_size элементов ИЛИ прошло max_wait секунд с момента появления
    ПЕРВОГО элемента пачки (что раньше). Остаток в конце тоже отдаётся.
    Пустые пачки не отдаются.
    Подсказка: читайте source в отдельной задаче в очередь, а здесь
    ждите очередь с таймаутом.
    """
    raise NotImplementedError
    yield
