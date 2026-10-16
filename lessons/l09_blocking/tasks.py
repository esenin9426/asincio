"""Урок 09 — задачи. Проверка: uv run pytest lessons/l09_blocking -v"""

import asyncio
import os
import threading
from collections.abc import Callable, Coroutine
from typing import Any


def count_primes(lo: int, hi: int) -> tuple[int, int]:
    """ГОТОВАЯ функция (не меняйте): CPU-bound подсчёт простых чисел в
    [lo, hi). Возвращает (количество, pid процесса, где выполнялась)."""
    count = 0
    for n in range(max(lo, 2), hi):
        if all(n % d for d in range(2, int(n**0.5) + 1)):
            count += 1
    return count, os.getpid()


async def run_blocking_concurrently(
    fn: Callable[[Any], Any], args_list: list[Any]
) -> list[Any]:
    """Задача 1. Вызовите fn(arg) для каждого arg в отдельном потоке
    (asyncio.to_thread) конкурентно. Результаты — по порядку."""
    raise NotImplementedError


async def count_primes_parallel(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Задача 2. Посчитайте count_primes(lo, hi) для каждого диапазона
    в ProcessPoolExecutor через loop.run_in_executor. Верните результаты
    по порядку. Пул должен закрываться по завершении."""
    raise NotImplementedError


async def heartbeat_while(blocking_fn: Callable[[], Any], interval: float = 0.01) -> int:
    """Задача 3.

    Запустите задачу-«сердцебиение», которая каждые `interval` секунд
    увеличивает счётчик, и одновременно выполните blocking_fn в потоке.
    Когда blocking_fn завершится — остановите сердцебиение и верните
    число тиков. (Если бы blocking_fn выполнялась прямо в цикле, тиков
    было бы ~0.)
    """
    raise NotImplementedError


async def bounded_to_thread(
    fn: Callable[[Any], Any], args_list: list[Any], max_threads: int
) -> list[Any]:
    """Задача 4. Как задача 1, но не более max_threads потоков одновременно:
    используйте собственный ThreadPoolExecutor(max_workers=max_threads)
    и loop.run_in_executor. Пул по умолчанию не трогайте."""
    raise NotImplementedError


async def to_thread_with_timeout(
    fn: Callable[[threading.Event], Any], timeout: float
) -> Any:
    """Задача 5.

    Выполните fn(stop_event) в потоке. Если не уложилась в timeout:
    выставьте stop_event (функция кооперативно проверяет его и выходит),
    дождитесь, пока поток действительно завершится, и бросьте TimeoutError.
    Иначе верните результат.
    """
    raise NotImplementedError


def find_blocking(
    coro_fn: Callable[[], Coroutine[Any, Any, Any]], threshold: float
) -> list[str]:
    """Задача 6. Синхронная функция.

    Запустите coro_fn() в НОВОМ цикле в debug-режиме с
    `loop.slow_callback_duration = threshold`. Перехватите сообщения
    логгера "asyncio" уровня WARNING и выше (свой logging.Handler)
    и верните список их текстов (record.getMessage()).
    После выхода обработчик нужно снять с логгера.
    """
    raise NotImplementedError
