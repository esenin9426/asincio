"""Урок 05 — задачи. Проверка: uv run pytest lessons/l05_taskgroup -v

Во всех задачах используйте asyncio.TaskGroup (не gather).
"""

import asyncio
from collections.abc import Awaitable, Callable, Coroutine, Hashable
from typing import Any, TypeVar

T = TypeVar("T")


async def process_all(items: list[T], handler: Callable[[T], Awaitable[Any]]) -> list[Any]:
    """Задача 1. Конкурентно примените handler к каждому элементу через
    TaskGroup и верните результаты в порядке items. Ошибки не перехватывайте
    (наружу выйдет ExceptionGroup)."""
    raise NotImplementedError


async def collect_errors(
    coros: list[Coroutine[Any, Any, Any]],
) -> tuple[list[ValueError], list[KeyError]]:
    """Задача 2.

    Запустите корутины в TaskGroup. С помощью `except*` соберите все
    ValueError и все KeyError и верните их двумя списками.
    Исключения других типов должны выйти наружу (ExceptionGroup с ними).
    Если ошибок нет — вернуть ([], []).
    """
    raise NotImplementedError


async def group_with_timeout(coros: list[Coroutine[Any, Any, T]], timeout: float) -> list[T]:
    """Задача 3. Выполните все корутины в TaskGroup, но не дольше `timeout`
    секунд. Вернуть результаты по порядку; по таймауту — бросить TimeoutError
    (все задачи группы должны быть отменены)."""
    raise NotImplementedError


async def run_isolated(coros: list[Coroutine[Any, Any, Any]]) -> list[tuple[str, Any]]:
    """Задача 4.

    Выполните корутины в TaskGroup так, чтобы ошибка одной НЕ отменяла
    остальные. Верните список по порядку: ("ok", результат) или
    ("error", исключение). Отмену (CancelledError) не перехватывайте.
    """
    raise NotImplementedError


async def walk_tree(
    root: Hashable, children: Callable[[Hashable], Awaitable[list[Hashable]]]
) -> set[Hashable]:
    """Задача 5.

    Обойдите граф, начиная с root: `await children(node)` возвращает соседей.
    Каждый узел обрабатывайте в отдельной задаче ОДНОЙ TaskGroup; задачи
    сами добавляют в группу задачи для новых узлов. Каждый узел посещается
    (children вызывается) ровно один раз, даже если в графе есть циклы.
    Верните множество всех достижимых узлов.
    """
    raise NotImplementedError


async def search_first(
    items: list[T], predicate: Callable[[T], Awaitable[bool]]
) -> T | None:
    """Задача 6.

    Конкурентно проверьте элементы асинхронным предикатом. Как только
    найден первый подходящий — остановите все остальные проверки
    (бросьте своё исключение из задачи и поймайте его через except*)
    и верните найденный элемент. Ничего не нашли — None.
    """
    raise NotImplementedError
