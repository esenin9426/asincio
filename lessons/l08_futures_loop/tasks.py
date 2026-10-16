"""Урок 08 — задачи. Проверка: uv run pytest lessons/l08_futures_loop -v

В этом уроке НЕ используйте asyncio.sleep для ожидания — только Future,
колбэки цикла и его таймеры.
"""

import asyncio
import concurrent.futures
from collections.abc import Callable, Coroutine
from typing import Any

# Колбэк в стиле Node.js: callback(error, result)
NodeCallback = Callable[[BaseException | None, Any], None]


async def callback_to_awaitable(start_op: Callable[[NodeCallback], None]) -> Any:
    """Задача 1.

    `start_op(cb)` запускает операцию и когда-нибудь вызовет
    `cb(error, result)` В ПОТОКЕ ЦИКЛА (через call_soon/call_later).
    Верните result или бросьте error.

    Если ожидающего отменили (например, по таймауту), а колбэк пришёл
    позже — он не должен падать с InvalidStateError.
    """
    raise NotImplementedError


async def delayed_calls(schedule: list[tuple[float, str]], cancel: set[str]) -> list[str]:
    """Задача 2.

    Для каждой пары (delay, name) запланируйте через `loop.call_later`
    добавление name в список. Сразу после планирования отмените таймеры,
    имена которых есть в `cancel`. Дождитесь, пока сработают все
    неотменённые таймеры (через Future, без sleep), и верните имена
    в порядке срабатывания.
    """
    raise NotImplementedError


class Debouncer:
    """Задача 3. Дебаунс.

    `trigger(value)` (обычный метод, не корутина) — перезапускает таймер:
    `fn(value)` вызывается ОДИН раз через `delay` секунд после ПОСЛЕДНЕГО
    trigger и с последним значением. `cancel()` — отменить ожидающий вызов.
    Используйте `loop.call_later` и `TimerHandle.cancel()`.
    """

    def __init__(self, delay: float, fn: Callable[[Any], None]) -> None:
        raise NotImplementedError

    def trigger(self, value: Any) -> None:
        raise NotImplementedError

    def cancel(self) -> None:
        raise NotImplementedError


async def collect_from_thread(n: int) -> list[int]:
    """Задача 4.

    Запустите обычный поток (threading.Thread), который генерирует числа
    0..n-1 (между числами — time.sleep(0.001)) и передаёт их в цикл через
    `loop.call_soon_threadsafe` в asyncio.Queue, а в конце — sentinel None.
    В корутине соберите числа до sentinel и верните список.
    Не блокируйте цикл (никаких thread.join() в корутине).
    """
    raise NotImplementedError


def call_async_from_thread(
    loop: asyncio.AbstractEventLoop,
    coro_fn: Callable[..., Coroutine[Any, Any, Any]],
    *args: Any,
    timeout: float,
) -> Any:
    """Задача 5. Синхронная функция, вызываемая из ДРУГОГО потока.

    Выполните `coro_fn(*args)` в цикле `loop` и дождитесь результата не
    дольше timeout секунд. По таймауту — отмените корутину в цикле и
    бросьте TimeoutError.
    """
    raise NotImplementedError


async def await_concurrent(
    executor: concurrent.futures.Executor, fn: Callable[..., Any], *args: Any
) -> Any:
    """Задача 6. Отправьте fn(*args) в executor через executor.submit
    и дождитесь результата через asyncio.wrap_future."""
    raise NotImplementedError
