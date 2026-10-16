"""Урок 02 — задачи. Проверка: uv run pytest lessons/l02_tasks -v"""

import asyncio
from collections.abc import Coroutine
from typing import Any


async def run_concurrently(delays: list[float]) -> float:
    """Задача 1.

    Для каждой задержки создайте отдельную задачу `asyncio.sleep(delay)`,
    дождитесь их всех (без gather — только create_task + await) и верните
    затраченное время по `loop.time()`. Ожидается ≈ max(delays).
    """
    raise NotImplementedError


async def spawn_named(n: int) -> list[str]:
    """Задача 2.

    Создайте n задач с именами "worker-0" ... "worker-{n-1}".
    Каждая задача должна САМА узнать своё имя через `asyncio.current_task()`
    и вернуть его. Верните список результатов в порядке создания.
    """
    raise NotImplementedError


def describe(task: asyncio.Task) -> str:
    """Задача 3.

    Верните состояние задачи одной строкой:
      "pending"   — ещё не завершена;
      "cancelled" — отменена;
      "failed: <ИмяКлассаИсключения>" — завершилась исключением,
                                         например "failed: ValueError";
      "done: <repr(результата)>"      — успешно, например "done: 42".

    Функция синхронная и не должна бросать исключений.
    """
    raise NotImplementedError


class BackgroundTasks:
    """Задача 4. Безопасный запуск фоновых задач.

    - `spawn(coro)` создаёт задачу, хранит на неё сильную ссылку и
      возвращает её. После завершения задача должна автоматически
      исчезнуть из хранилища (используйте add_done_callback).
    - `len(bg)` — число ещё работающих задач.
    - `failures` — список исключений упавших задач (отменённые задачи не
      считаются упавшими). Заполняется в колбэке завершения, чтобы ошибки
      не терялись с "Task exception was never retrieved".
    - `await bg.wait_all()` — дождаться всех текущих задач, не пробрасывая
      их исключения наружу.
    """

    def __init__(self) -> None:
        self.failures: list[BaseException] = []

    def spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task:
        raise NotImplementedError

    def __len__(self) -> int:
        raise NotImplementedError

    async def wait_all(self) -> None:
        raise NotImplementedError


async def first_done_name(tasks: list[asyncio.Task]) -> str:
    """Задача 5.

    Верните имя задачи, которая завершится первой. Нельзя использовать
    asyncio.wait / as_completed / gather — только `loop.create_future()`
    и `add_done_callback`. Остальные задачи не отменяйте. Перед выходом
    уберите свои колбэки с ещё работающих задач (`remove_done_callback`).
    """
    raise NotImplementedError
