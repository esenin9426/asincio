"""Урок 15 — задачи. Проверка: uv run pytest lessons/l15_under_the_hood -v

НЕ используйте asyncio. Только генераторы, heapq, collections, selectors, time.

Протокол «команд»: awaitable-объекты в __await__ делают `yield` кортежа,
который цикл интерпретирует:
    ("sleep", delay)       — разбудить через delay секунд;
    ("read", sock)         — разбудить, когда sock готов к чтению;
    ("wait", mini_task)    — разбудить, когда mini_task завершится.
"""

import selectors
import socket
from collections.abc import Coroutine
from typing import Any


class CancelledError(Exception):
    """Бросается в корутину при отмене MiniTask."""


class Sleep:
    """Задача 1a. `await Sleep(d)` — yield ("sleep", d); вернуть None."""

    def __init__(self, delay: float) -> None:
        self.delay = delay

    def __await__(self):
        raise NotImplementedError


class ReadReady:
    """Задача 1b. `await ReadReady(sock)` — yield ("read", sock); вернуть None."""

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock

    def __await__(self):
        raise NotImplementedError


class MiniTask:
    """Задача 2. Задача над корутиной.

    - `done()`, `result()` (бросает исключение корутины, если она упала;
      RuntimeError, если ещё не завершена), `cancelled()`.
    - `cancel()` — при следующем шаге в корутину бросается
      CancelledError (coro.throw). Если корутина его не перехватила —
      задача становится cancelled, а result() бросает CancelledError.
    - `await other_task` внутри другой корутины: yield ("wait", self),
      пока задача не завершится, затем вернуть её result().
    Сама корутина продвигается методами цикла (MiniLoop) — как цикл и
    задача поделят обязанности, решайте сами.
    """

    def __init__(self, coro: Coroutine[Any, Any, Any], loop: "MiniLoop") -> None:
        raise NotImplementedError

    def done(self) -> bool:
        raise NotImplementedError

    def cancelled(self) -> bool:
        raise NotImplementedError

    def result(self) -> Any:
        raise NotImplementedError

    def cancel(self) -> None:
        raise NotImplementedError

    def __await__(self):
        raise NotImplementedError


class MiniLoop:
    """Задача 3. Цикл событий.

    - `time()` — монотонные часы.
    - `spawn(coro) -> MiniTask` — запланировать корутину (в ready-очередь).
    - `run_until_complete(coro)` — spawn + крутить цикл, пока эта задача не
      завершится; вернуть её result(). Цикл должен:
        * выполнять ready-задачи (FIFO),
        * будить спящие задачи по куче таймеров,
        * ждать I/O через selectors.DefaultSelector с таймаутом до
          ближайшего таймера (а не крутиться вхолостую!),
        * будить задачи, ждущие других задач, когда те завершились.
    """

    def __init__(self) -> None:
        raise NotImplementedError

    def time(self) -> float:
        raise NotImplementedError

    def spawn(self, coro: Coroutine[Any, Any, Any]) -> MiniTask:
        raise NotImplementedError

    def run_until_complete(self, coro: Coroutine[Any, Any, Any]) -> Any:
        raise NotImplementedError
