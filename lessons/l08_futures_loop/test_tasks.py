import asyncio
import concurrent.futures
import threading

import pytest

from lessons.l08_futures_loop.tasks import (
    Debouncer,
    await_concurrent,
    call_async_from_thread,
    callback_to_awaitable,
    collect_from_thread,
    delayed_calls,
)


def now():
    return asyncio.get_running_loop().time()


async def test_callback_to_awaitable_result():
    loop = asyncio.get_running_loop()

    def start_op(cb):
        loop.call_later(0.02, cb, None, "data")

    assert await callback_to_awaitable(start_op) == "data"


async def test_callback_to_awaitable_error():
    loop = asyncio.get_running_loop()

    def start_op(cb):
        loop.call_soon(cb, OSError("disk"), None)

    with pytest.raises(OSError, match="disk"):
        await callback_to_awaitable(start_op)


async def test_callback_to_awaitable_late_callback_after_timeout():
    loop = asyncio.get_running_loop()
    errors = []
    old = loop.get_exception_handler()
    loop.set_exception_handler(lambda l, ctx: errors.append(ctx))
    try:
        def start_op(cb):
            loop.call_later(0.05, cb, None, "late")

        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.01):
                await callback_to_awaitable(start_op)
        await asyncio.sleep(0.08)
    finally:
        loop.set_exception_handler(old)
    assert errors == [], f"поздний колбэк упал: {errors}"


async def test_delayed_calls():
    start = now()
    res = await delayed_calls(
        [(0.06, "c"), (0.02, "a"), (0.04, "b"), (0.03, "x"), (0.5, "slow")],
        cancel={"x", "slow"},
    )
    assert res == ["a", "b", "c"]
    assert 0.05 <= now() - start < 0.15


async def test_delayed_calls_all_cancelled():
    start = now()
    assert await delayed_calls([(1, "a")], cancel={"a"}) == []
    assert now() - start < 0.1


async def test_debouncer():
    calls = []
    d = Debouncer(0.05, calls.append)
    for i in range(5):
        d.trigger(i)
        await asyncio.sleep(0.01)
    assert calls == []
    await asyncio.sleep(0.07)
    assert calls == [4]
    d.trigger("x")
    await asyncio.sleep(0.07)
    assert calls == [4, "x"]


async def test_debouncer_cancel():
    calls = []
    d = Debouncer(0.03, calls.append)
    d.trigger(1)
    d.cancel()
    await asyncio.sleep(0.05)
    assert calls == []


async def test_collect_from_thread():
    ticks = 0
    stop = False

    async def heartbeat():
        nonlocal ticks
        while not stop:
            ticks += 1
            await asyncio.sleep(0.005)

    hb = asyncio.create_task(heartbeat())
    threads_before = threading.active_count()
    res = await collect_from_thread(50)
    stop = True
    await hb
    assert res == list(range(50))
    assert ticks >= 5, "цикл был заблокирован"
    assert threading.active_count() <= threads_before + 1


async def test_call_async_from_thread():
    loop = asyncio.get_running_loop()
    seen_threads = set()

    async def add(a, b):
        seen_threads.add(threading.get_ident())
        await asyncio.sleep(0.01)
        return a + b

    assert await asyncio.to_thread(call_async_from_thread, loop, add, 2, 3, timeout=1) == 5
    assert seen_threads == {threading.get_ident()}, "корутина должна работать в потоке цикла"


async def test_call_async_from_thread_timeout_cancels():
    loop = asyncio.get_running_loop()
    state = {}

    async def slow():
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            state["cancelled"] = True
            raise

    with pytest.raises(TimeoutError):
        await asyncio.to_thread(call_async_from_thread, loop, slow, timeout=0.05)
    await asyncio.sleep(0.02)
    assert state.get("cancelled")


async def test_await_concurrent():
    with concurrent.futures.ThreadPoolExecutor(2) as ex:
        assert await await_concurrent(ex, pow, 2, 10) == 1024

        def boom():
            raise KeyError("k")

        with pytest.raises(KeyError):
            await await_concurrent(ex, boom)
