"""Урок 07 — задачи. Проверка: uv run pytest lessons/l07_queues -v"""

import asyncio
from collections.abc import AsyncIterable, Awaitable, Callable
from typing import Any


async def producer_consumer(
    items: list[Any], n_workers: int, handler: Callable[[Any], Awaitable[Any]]
) -> list[Any]:
    """Задача 1.

    Положите все items в asyncio.Queue, запустите n_workers воркеров,
    каждый берёт элемент, вызывает `await handler(item)` и сохраняет
    результат. Используйте task_done() + join(), затем отмените воркеров
    и дождитесь их. Верните список результатов (в любом порядке).
    Если handler бросил исключение — результат для элемента просто не
    добавляется, воркер продолжает работу.
    """
    raise NotImplementedError


async def priority_order(items: list[tuple[int, Any]]) -> list[Any]:
    """Задача 2.

    items — пары (приоритет, данные); данные могут быть НЕсравнимыми
    (например, dict). Пропустите всё через asyncio.PriorityQueue и
    верните данные в порядке обработки: сначала меньший приоритет,
    при равных приоритетах — в порядке добавления.
    """
    raise NotImplementedError


async def pipeline(
    source: AsyncIterable[Any],
    stage1: Callable[[Any], Awaitable[Any]],
    stage2: Callable[[Any], Awaitable[Any]],
    maxsize: int,
) -> list[Any]:
    """Задача 3. Конвейер: source → stage1 → stage2.

    Три корутины, соединённые очередями asyncio.Queue(maxsize):
      - читатель: перебирает `async for x in source` и кладёт x в q1;
      - этап 1: берёт из q1, кладёт `await stage1(x)` в q2;
      - этап 2: берёт из q2, складывает `await stage2(y)` в результат.
    Каждый этап — ровно одна корутина, порядок элементов сохраняется.
    Конец потока передавайте sentinel-ом. Этапы должны работать
    одновременно (пока этап 2 обрабатывает элемент i, этап 1 уже
    обрабатывает i+1). Верните результаты этапа 2.
    """
    raise NotImplementedError


class WorkerPool:
    """Задача 4. Пул воркеров на Queue.shutdown() (Python 3.13+).

    - `__init__(n_workers, handler)` — сохранить параметры.
    - `start()` — запустить n_workers задач-воркеров.
    - `submit(item)` — положить элемент в очередь без ожидания.
      После закрытия пула должен бросать asyncio.QueueShutDown.
    - `await close(immediate=False)`:
        immediate=False — обработать всё, что уже в очереди, затем
            воркеры завершаются САМИ (по QueueShutDown, без cancel);
        immediate=True — выбросить необработанные элементы; воркеры
            доделывают текущий элемент и завершаются.
      В обоих случаях дождаться завершения воркеров.
    - `results` — список результатов handler(item) в порядке завершения.
    """

    def __init__(self, n_workers: int, handler: Callable[[Any], Awaitable[Any]]) -> None:
        self.results: list[Any] = []
        raise NotImplementedError

    def start(self) -> None:
        raise NotImplementedError

    def submit(self, item: Any) -> None:
        raise NotImplementedError

    async def close(self, immediate: bool = False) -> None:
        raise NotImplementedError
