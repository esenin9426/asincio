import asyncio
import contextlib
import inspect

import pytest

from lessons.l13_async_iter_ctx.tasks import (
    Countdown,
    Timer,
    abatch,
    afilter,
    aticker,
    amap,
    atake,
    merge,
    open_all,
)


def now():
    return asyncio.get_running_loop().time()


async def gen(items, delay=0.0, log=None):
    try:
        for x in items:
            await asyncio.sleep(delay)
            yield x
    finally:
        if log is not None:
            log.append("closed")


async def test_countdown():
    assert not inspect.isasyncgenfunction(Countdown.__anext__)
    start = now()
    assert [x async for x in Countdown(3, 0.02)] == [3, 2, 1]
    assert now() - start >= 0.055
    assert [x async for x in Countdown(0, 0.02)] == []


async def test_aticker_no_drift():
    assert inspect.isasyncgenfunction(aticker)
    ticks = []
    async for t in aticker(0.05, 5):
        ticks.append(t)
        await asyncio.sleep(0.03)  # «медленный» потребитель
    assert len(ticks) == 5
    total = ticks[-1] - ticks[0]
    assert 0.18 <= total < 0.24, f"дрейф: {total:.3f} вместо 0.2"


async def test_amap_afilter():
    async def double(x):
        await asyncio.sleep(0)
        return x * 2

    res = [x async for x in afilter(lambda v: v % 3 == 0, amap(double, gen(range(10))))]
    assert res == [0, 6, 12, 18]


async def test_atake_closes_source():
    log = []
    src = gen(range(100), log=log)
    assert await atake(3, src) == [0, 1, 2]
    assert log == ["closed"], "источник не закрыт"


async def test_atake_short_source():
    assert await atake(10, gen([1, 2])) == [1, 2]


async def test_timer():
    async with Timer() as t:
        await asyncio.sleep(0.05)
    assert 0.045 <= t.elapsed < 0.15
    assert not t.timed_out

    async with Timer() as t2:
        async with asyncio.timeout(0.02):
            await asyncio.sleep(1)
    assert t2.timed_out
    assert 0.015 <= t2.elapsed < 0.1

    with pytest.raises(KeyError):
        async with Timer():
            raise KeyError


def make_opener(log, fail_on=None):
    @contextlib.asynccontextmanager
    async def opener(name):
        await asyncio.sleep(0)
        if name == fail_on:
            raise ConnectionError(name)
        log.append(f"open {name}")
        try:
            yield name.upper()
        finally:
            await asyncio.sleep(0)
            log.append(f"close {name}")
    return opener


async def test_open_all():
    log = []
    async with open_all(["a", "b", "c"], make_opener(log)) as res:
        assert res == ["A", "B", "C"]
        log.append("body")
    assert log == ["open a", "open b", "open c", "body", "close c", "close b", "close a"]


async def test_open_all_rollback():
    log = []
    with pytest.raises(ConnectionError):
        async with open_all(["a", "b", "c"], make_opener(log, fail_on="c")):
            log.append("body")
    assert log == ["open a", "open b", "close b", "close a"]


async def test_merge_by_readiness():
    start = now()
    res = [x async for x in merge(gen(["a1", "a2"], 0.03), gen(["b1", "b2", "b3"], 0.02))]
    assert sorted(res) == ["a1", "a2", "b1", "b2", "b3"]
    assert res[0] == "b1" and res.index("a1") < res.index("b3")
    assert now() - start < 0.1, "источники должны читаться конкурентно"


async def test_merge_error():
    async def bad():
        yield 1
        raise ValueError("source failed")

    with pytest.raises(ValueError, match="source failed"):
        async for _ in merge(bad(), gen(range(100), 0.01)):
            pass


async def test_merge_aclose_cancels_readers():
    log = []
    before = len(asyncio.all_tasks())
    m = merge(gen(range(1000), 0.01, log), gen(range(1000), 0.01, log))
    await anext(m)
    await m.aclose()
    await asyncio.sleep(0.02)
    assert log == ["closed", "closed"]
    assert len(asyncio.all_tasks()) == before


async def test_abatch_by_size_and_time():
    async def bursty():
        for x in range(5):
            yield x                 # 5 элементов сразу
        await asyncio.sleep(0.15)
        yield 5                     # одиночный — уйдёт по времени
        await asyncio.sleep(0.15)
        yield 6
        yield 7

    stamps = []
    batches = []
    start = now()
    async for b in abatch(bursty(), max_size=2, max_wait=0.05):
        batches.append(b)
        stamps.append(now() - start)
    assert batches == [[0, 1], [2, 3], [4], [5], [6, 7]]
    assert stamps[2] < 0.1, "пачка [4] должна уйти по max_wait, не дожидаясь 5"
    assert 0.15 <= stamps[3] < 0.25
