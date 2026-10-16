"""Урок 03 — задачи. Проверка: uv run pytest lessons/l03_combinators -v"""

import asyncio
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, TypeVar

T = TypeVar("T")
Fetch = Callable[[str], Awaitable[Any]]


async def fetch_all(urls: list[str], fetch: Fetch) -> list[Any]:
    """Задача 1.

    Конкурентно вызовите `fetch(url)` для всех url и верните результаты
    в порядке `urls`. Если какой-то fetch упал — исключение пробрасывается.
    """
    raise NotImplementedError


async def fetch_all_safe(
    urls: list[str], fetch: Fetch
) -> tuple[dict[str, Any], dict[str, Exception]]:
    """Задача 2.

    Конкурентно вызовите fetch для всех url и верните пару словарей:
    (успешные url -> результат, упавшие url -> исключение).
    Исключения наружу не пробрасывать. Используйте return_exceptions=True.
    """
    raise NotImplementedError


async def first_completed(coros: list[Coroutine[Any, Any, T]]) -> T:
    """Задача 3. «Гонка зеркал».

    Запустите все корутины как задачи, верните результат той, что
    завершилась первой (если первая упала — пробросьте её исключение).
    Все остальные задачи отмените и ДОЖДИТЕСЬ их завершения перед выходом,
    чтобы не оставлять висящих задач.
    """
    raise NotImplementedError


async def in_completion_order(coros: list[Coroutine[Any, Any, T]]) -> list[T]:
    """Задача 4.

    Верните результаты в порядке ЗАВЕРШЕНИЯ (as_completed).
    """
    raise NotImplementedError


async def with_timeout(aw: Awaitable[T], timeout: float, default: T) -> T:
    """Задача 5.

    Верните результат `aw`, а если он не успел за `timeout` секунд —
    `default`. Операция `aw` при таймауте должна быть отменена.
    """
    raise NotImplementedError


async def deadline_batch(
    coros: list[Coroutine[Any, Any, T]], deadline: float
) -> tuple[list[T], int]:
    """Задача 6.

    Запустите все корутины; через `deadline` секунд верните
    (результаты успевших завершиться УСПЕШНО — в любом порядке,
     число не успевших задач).
    Не успевшие задачи отмените и дождитесь. Упавшие задачи (с исключением)
    не попадают ни в результаты, ни в счётчик не успевших.
    Если всё готово раньше дедлайна — вернитесь сразу, не дожидаясь его.
    """
    raise NotImplementedError


async def staged_deadline(
    steps: list[Callable[[], Awaitable[Any]]], budget: float, bonus: float
) -> int:
    """Задача 7.

    Выполняйте шаги `steps` по очереди (каждый — функция без аргументов,
    возвращающая awaitable) под ОДНИМ `asyncio.timeout` с бюджетом `budget`.
    После каждого успешно завершённого шага сдвигайте дедлайн на `bonus`
    секунд вперёд ОТНОСИТЕЛЬНО ТЕКУЩЕГО ДЕДЛАЙНА: новый = cm.when() + bonus
    (через `cm.reschedule(...)`).
    Верните число полностью выполненных шагов. Если время вышло — не
    пробрасывайте TimeoutError, а верните сколько успели.
    """
    raise NotImplementedError
