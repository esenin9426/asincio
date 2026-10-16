import asyncio
import logging
import os
import threading
import time

import pytest

from lessons.l09_blocking.tasks import (
    bounded_to_thread,
    count_primes,
    count_primes_parallel,
    find_blocking,
    heartbeat_while,
    run_blocking_concurrently,
    to_thread_with_timeout,
)


def now():
    return asyncio.get_running_loop().time()


def slow_square(x):
    time.sleep(0.1)
    return x * x


async def test_run_blocking_concurrently():
    start = now()
    assert await run_blocking_concurrently(slow_square, [1, 2, 3, 4, 5]) == [1, 4, 9, 16, 25]
    assert now() - start < 0.3


@pytest.mark.timeout(60)
async def test_count_primes_parallel():
    ranges = [(0, 20_000), (20_000, 40_000), (40_000, 60_000)]
    res = await count_primes_parallel(ranges)
    assert [c for c, _ in res] == [count_primes(lo, hi)[0] for lo, hi in ranges]
    assert all(pid != os.getpid() for _, pid in res), "вычисления должны идти в других процессах"


async def test_heartbeat_while():
    ticks = await heartbeat_while(lambda: time.sleep(0.2))
    assert ticks >= 10


async def test_bounded_to_thread():
    active = 0
    peak = 0
    lock = threading.Lock()

    def work(x):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.03)
        with lock:
            active -= 1
        return x + 1

    start = now()
    assert await bounded_to_thread(work, list(range(8)), 2) == list(range(1, 9))
    assert peak == 2
    assert now() - start < 0.3


async def test_to_thread_with_timeout_ok():
    def quick(stop):
        return "done"

    assert await to_thread_with_timeout(quick, 1) == "done"


async def test_to_thread_with_timeout_stops_thread():
    state = {"iterations": 0, "exited": False}

    def loop_until_stopped(stop: threading.Event):
        while not stop.is_set():
            state["iterations"] += 1
            time.sleep(0.005)
        state["exited"] = True

    start = now()
    with pytest.raises(TimeoutError):
        await to_thread_with_timeout(loop_until_stopped, 0.05)
    assert now() - start < 0.2
    assert state["exited"], "поток должен завершиться до выхода из функции"


def test_find_blocking_detects_time_sleep():
    async def bad():
        await asyncio.sleep(0)
        time.sleep(0.12)

    messages = find_blocking(bad, 0.05)
    assert any("took" in m for m in messages), messages


def test_find_blocking_clean_code():
    async def good():
        await asyncio.sleep(0.12)

    assert find_blocking(good, 0.05) == []


def test_find_blocking_removes_handler():
    before = list(logging.getLogger("asyncio").handlers)

    async def noop():
        pass

    find_blocking(noop, 0.05)
    assert logging.getLogger("asyncio").handlers == before
