"""Урок 14 — задачи. Проверка: uv run pytest lessons/l14_advanced -v"""

import asyncio
import collections
import contextvars
from collections.abc import Callable, Coroutine
from typing import Any

request_id: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")


def log(msg: str) -> str:
    """Задача 1a. Верните строку f"[{текущий request_id}] {msg}"."""
    raise NotImplementedError


async def handle_request(rid: str, steps: int, out: list[str]) -> None:
    """Задача 1b.

    Установите request_id = rid, выполните `steps` шагов: на каждом
    `await asyncio.sleep(0)` и `out.append(log(f"step {i}"))`.
    По выходе (в т.ч. при исключении) верните request_id к прежнему
    значению через token — функцию могут вызвать прямым `await`, без
    отдельной задачи.
    """
    raise NotImplementedError


async def run_in_context(
    values: dict[contextvars.ContextVar, Any],
    coro_fn: Callable[[], Coroutine[Any, Any, Any]],
) -> Any:
    """Задача 2.

    Скопируйте текущий контекст, установите в КОПИИ значения переменных
    из `values` (подсказка: ctx.run(var.set, value)) и выполните
    coro_fn() задачей в этом контексте (create_task(..., context=ctx)).
    Верните результат. Контекст вызывающего меняться не должен.
    """
    raise NotImplementedError


async def capture_callback_errors(callbacks: list[Callable[[], Any]]) -> list[BaseException]:
    """Задача 3.

    Временно установите на текущем цикле свой обработчик исключений,
    запланируйте все колбэки через loop.call_soon, дождитесь, пока все
    они выполнятся (успешно или нет), восстановите прежний обработчик и
    верните список исключений из колбэков (в порядке возникновения).
    """
    raise NotImplementedError


async def wait_for_signal(sig: int, timeout: float) -> bool:
    """Задача 4.

    Дождитесь сигнала sig (loop.add_signal_handler) не дольше timeout.
    Верните True, если сигнал пришёл, False — если таймаут.
    Обработчик сигнала нужно снять в любом случае.
    """
    raise NotImplementedError


def run_eager(main: Callable[[], Coroutine[Any, Any, Any]]) -> Any:
    """Задача 5. Синхронная функция: выполните main() в новом цикле,
    у которого установлена фабрика asyncio.eager_task_factory, и верните
    результат (asyncio.Runner или asyncio.run с loop_factory)."""
    raise NotImplementedError


def install_task_counter(loop: asyncio.AbstractEventLoop) -> collections.Counter:
    """Задача 6.

    Установите на loop фабрику задач, которая считает созданные задачи
    по имени функции-корутины (`coro.__qualname__`, если есть, иначе
    type(coro).__name__) и создаёт обычные asyncio.Task, корректно
    передавая все kwargs (name, context, ...). Верните Counter,
    который будет обновляться.
    """
    raise NotImplementedError
