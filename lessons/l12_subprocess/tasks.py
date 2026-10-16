"""Урок 12 — задачи. Проверка: uv run pytest lessons/l12_subprocess -v

Используйте asyncio.create_subprocess_exec (не subprocess.run и не shell).
"""

import asyncio
from collections.abc import AsyncIterator


async def run_command(*args: str) -> tuple[int, str, str]:
    """Задача 1. Запустите процесс, дождитесь завершения и верните
    (returncode, stdout, stderr) — вывод декодирован из UTF-8."""
    raise NotImplementedError


async def run_with_timeout(args: list[str], timeout: float) -> tuple[int, str]:
    """Задача 2. Как run_command, но не дольше timeout.

    Успели — вернуть (returncode, stdout).
    Не успели — убить процесс (kill), дождаться его завершения (без
    зомби) и бросить TimeoutError.
    Если отменили саму корутину — процесс тоже должен быть убит.
    """
    raise NotImplementedError


async def stream_lines(args: list[str]) -> AsyncIterator[str]:
    """Задача 3. Асинхронный генератор: отдаёт строки stdout процесса
    (без "\\n") по мере их появления. После последней строки дождитесь
    завершения процесса; если код возврата не 0 — бросьте
    RuntimeError(f"exit code {rc}").
    Если потребитель прекратил итерацию раньше (aclose) — убейте процесс.
    """
    raise NotImplementedError
    yield


async def run_parallel(commands: list[list[str]], limit: int) -> list[int]:
    """Задача 4. Выполните команды (stdout/stderr → DEVNULL), одновременно
    не более `limit` процессов. Верните коды возврата по порядку."""
    raise NotImplementedError


async def pipe_through(args: list[str], data: bytes) -> bytes:
    """Задача 5. Передайте data в stdin процесса и верните его stdout.
    Учтите, что данные могут быть больше размера буфера пайпа (64 КиБ+)."""
    raise NotImplementedError


async def interactive_session(args: list[str], requests: list[str]) -> list[str]:
    """Задача 6. Диалог с процессом.

    Процесс читает строки из stdin и на каждую отвечает одной строкой
    в stdout. По очереди: отправьте запрос (+ "\\n", drain), прочитайте
    ответ (readline). После всех запросов закройте stdin и дождитесь
    завершения процесса. Верните ответы без "\\n".
    """
    raise NotImplementedError
