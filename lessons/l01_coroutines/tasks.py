"""Урок 01 — задачи. Реализуйте функции и запустите:

    uv run pytest lessons/l01_coroutines -v
"""

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any


async def greet(name: str) -> str:
    """Задача 1.

    Уступите цикл один раз (`await asyncio.sleep(0)`) и верните строку
    "Hello, <name>!".
    """
    raise NotImplementedError


async def sequential(delays: list[float]) -> float:
    """Задача 2.

    Последовательно «поспите» каждую задержку из `delays` и верните,
    сколько секунд прошло, измерив время через `loop.time()` текущего цикла.
    """
    raise NotImplementedError


async def countdown(name: str, n: int, log: list[str]) -> None:
    """Задача 3.

    Для i от n до 1 добавляйте в `log` строку f"{name}{i}", а после КАЖДОГО
    добавления уступайте цикл через `asyncio.sleep(0)`.

    Тогда две корутины, запущенные через gather, будут чередоваться:
    ["a2", "b2", "a1", "b1"].
    """
    raise NotImplementedError


class Delayed:
    """Задача 4. Собственный awaitable.

    `await Delayed(0.1, "x")` должен неблокирующе подождать 0.1 c
    и вернуть "x". Реализуйте `__await__` (без `async def __await__`!).
    Объект должен быть awaitable многократно: каждый `await` ждёт заново.
    """

    def __init__(self, delay: float, value: Any) -> None:
        self.delay = delay
        self.value = value

    def __await__(self):
        raise NotImplementedError


def run_sync(coro_fn: Callable[..., Coroutine[Any, Any, Any]], *args: Any) -> Any:
    """Задача 5.

    Выполните `coro_fn(*args)` из синхронного кода, используя
    `asyncio.Runner`, и верните результат.

    Если функцию вызвали, когда в этом потоке уже работает цикл событий,
    бросьте RuntimeError("run_sync() нельзя вызывать из работающего цикла"),
    НЕ создавая объект корутины (иначе будет предупреждение
    "coroutine was never awaited").
    """
    raise NotImplementedError
