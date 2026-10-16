import ast
import inspect
import socket
import threading
import time

import pytest

from lessons.l15_under_the_hood import tasks
from lessons.l15_under_the_hood.tasks import CancelledError, MiniLoop, ReadReady, Sleep


def test_no_asyncio_import():
    tree = ast.parse(inspect.getsource(tasks))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert all(a.name != "asyncio" for a in node.names)
        if isinstance(node, ast.ImportFrom):
            assert node.module != "asyncio"


def test_sleep_command():
    async def f():
        await Sleep(0.5)
        return "ok"

    c = f()
    assert c.send(None) == ("sleep", 0.5)
    with pytest.raises(StopIteration) as ei:
        c.send(None)
    assert ei.value.value == "ok"


def test_run_until_complete_simple():
    async def f(x):
        await Sleep(0)
        return x * 2

    assert MiniLoop().run_until_complete(f(21)) == 42


def test_concurrent_sleeps():
    loop = MiniLoop()
    log = []

    async def worker(name, delay, n):
        for i in range(n):
            await Sleep(delay)
            log.append((name, i))
        return name

    async def main():
        a = loop.spawn(worker("a", 0.05, 3))
        b = loop.spawn(worker("b", 0.07, 2))
        return [await a, await b]

    start = time.monotonic()
    assert loop.run_until_complete(main()) == ["a", "b"]
    elapsed = time.monotonic() - start
    assert 0.14 <= elapsed < 0.25, f"сон должен быть конкурентным: {elapsed:.2f}"
    assert log == [("a", 0), ("b", 0), ("a", 1), ("b", 1), ("a", 2)]


def test_exceptions_propagate_through_await():
    loop = MiniLoop()

    async def bad():
        await Sleep(0.01)
        raise ValueError("boom")

    async def main():
        t = loop.spawn(bad())
        try:
            await t
        except ValueError as e:
            return f"caught {e}"

    assert loop.run_until_complete(main()) == "caught boom"


def test_cancel():
    loop = MiniLoop()
    cleanup = []

    async def forever():
        try:
            while True:
                await Sleep(0.01)
        finally:
            cleanup.append("done")

    async def main():
        t = loop.spawn(forever())
        await Sleep(0.03)
        t.cancel()
        try:
            await t
        except CancelledError:
            return t.cancelled()

    assert loop.run_until_complete(main()) is True
    assert cleanup == ["done"]


def test_io_readiness_without_busy_loop():
    loop = MiniLoop()
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)

    async def reader():
        await ReadReady(a)
        return a.recv(100)

    async def writer():
        await Sleep(0.1)
        b.send(b"hello")

    async def main():
        r = loop.spawn(reader())
        loop.spawn(writer())
        return await r

    cpu_start = time.process_time()
    try:
        assert loop.run_until_complete(main()) == b"hello"
    finally:
        a.close()
        b.close()
    assert time.process_time() - cpu_start < 0.05, "цикл крутится вхолостую вместо select()"


def test_io_from_other_thread():
    loop = MiniLoop()
    a, b = socket.socketpair()
    a.setblocking(False)

    def later():
        time.sleep(0.05)
        b.send(b"x")

    threading.Thread(target=later).start()

    async def main():
        await ReadReady(a)
        return a.recv(10)

    try:
        assert loop.run_until_complete(main()) == b"x"
    finally:
        a.close()
        b.close()
