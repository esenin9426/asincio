"""Урок 04 — задачи. Проверка: uv run pytest lessons/l04_cancellation -v"""

import asyncio
from collections.abc import Awaitable, Coroutine
from typing import Any


async def cancellable_worker(log: list[str], interval: float = 0.01) -> None:
    """Задача 1.

    Бесконечно: добавлять "tick" в log и ждать `interval`.
    При отмене: добавить "cleanup" и корректно завершиться как ОТМЕНЁННАЯ
    задача (task.cancelled() == True). В лог после "cleanup" ничего
    добавляться не должно.
    """
    raise NotImplementedError


async def cancel_and_wait(task: asyncio.Task) -> bool:
    """Задача 2.

    Отмените `task` и дождитесь её завершения.
    Верните True, если задача в итоге отменена, и False, если она успела
    завершиться сама (успешно) или проигнорировала отмену.
    Если задача завершилась исключением (не отменой) — пробросьте его.

    ВАЖНО: если во время ожидания отменили САМУ корутину cancel_and_wait,
    CancelledError нужно пробросить, а не проглотить
    (подсказка: asyncio.current_task().cancelling()).
    """
    raise NotImplementedError


async def protected_save(save: Coroutine[Any, Any, Any], registry: list[asyncio.Task]) -> Any:
    """Задача 3.

    Выполните корутину сохранения так, чтобы отмена protected_save НЕ
    прерывала её: обёрните в задачу, положите задачу в `registry`
    (чтобы вызывающий мог потом её дождаться) и ждите через shield.
    Верните результат сохранения.
    """
    raise NotImplementedError


async def cancel_reason(task: asyncio.Task, msg: str) -> str | None:
    """Задача 4.

    Отмените задачу с сообщением `msg`, дождитесь и верните сообщение,
    которое пришло в CancelledError (exc.args[0]), или None, если задача
    завершилась не отменой.
    """
    raise NotImplementedError


async def graceful_shutdown(tasks: list[asyncio.Task], grace: float) -> tuple[int, int]:
    """Задача 5.

    Дайте задачам до `grace` секунд, чтобы завершиться самостоятельно.
    Оставшиеся отмените и дождитесь (их ошибки не пробрасывать).
    Верните (сколько завершились сами, сколько пришлось отменить).
    Если все завершились раньше — вернитесь, не дожидаясь `grace`.
    """
    raise NotImplementedError


async def run_ignoring_cancel_for(aw: Awaitable[Any]) -> Any:
    """Задача 6. Критическая секция с отложенной отменой.

    Выполните `aw` до конца, даже если текущую задачу отменяют в процессе.
    Если отмена пришла во время выполнения — после завершения `aw`
    пробросьте CancelledError (отмена не должна потеряться).
    Если отмены не было — верните результат `aw`.

    Подсказка: shield + повторное ожидание внутренней задачи.
    """
    raise NotImplementedError
