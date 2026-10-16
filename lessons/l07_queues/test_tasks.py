import asyncio

import pytest

from lessons.l07_queues.tasks import WorkerPool, pipeline, priority_order, producer_consumer


def now():
    return asyncio.get_running_loop().time()


async def test_producer_consumer():
    active = 0
    peak = 0

    async def handler(x):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.02)
        active -= 1
        if x == 7:
            raise ValueError
        return x * 10

    before = len(asyncio.all_tasks())
    start = now()
    res = await producer_consumer(list(range(12)), 4, handler)
    assert sorted(res) == [x * 10 for x in range(12) if x != 7]
    assert peak == 4
    assert now() - start < 0.15
    assert len(asyncio.all_tasks()) == before, "воркеры не остановлены"


async def test_producer_consumer_empty():
    async def handler(x):
        return x

    assert await producer_consumer([], 3, handler) == []


async def test_priority_order_stable_with_uncomparable_data():
    items = [(2, {"n": "b1"}), (1, {"n": "a1"}), (2, {"n": "b2"}), (0, {"n": "z"}), (1, {"n": "a2"})]
    res = await priority_order(items)
    assert [d["n"] for d in res] == ["z", "a1", "a2", "b1", "b2"]


async def test_pipeline_order_and_overlap():
    async def source():
        for i in range(5):
            yield i

    async def s1(x):
        await asyncio.sleep(0.04)
        return x + 1

    async def s2(x):
        await asyncio.sleep(0.04)
        return x * 2

    start = now()
    assert await pipeline(source(), s1, s2, maxsize=2) == [2, 4, 6, 8, 10]
    # последовательно было бы 5 * 0.08 = 0.4, конвейер ≈ 0.24
    assert now() - start < 0.33


async def test_pipeline_backpressure():
    produced = 0
    consumed = 0
    max_gap = 0

    async def source():
        nonlocal produced
        for i in range(30):
            produced += 1
            yield i

    async def s1(x):
        return x

    async def s2(x):
        nonlocal consumed, max_gap
        await asyncio.sleep(0.005)
        consumed += 1
        max_gap = max(max_gap, produced - consumed)
        return x

    assert await pipeline(source(), s1, s2, maxsize=2) == list(range(30))
    assert max_gap <= 8, f"производитель убежал на {max_gap} элементов: нет backpressure"


async def test_pipeline_empty():
    async def source():
        return
        yield

    async def ident(x):
        return x

    assert await pipeline(source(), ident, ident, 1) == []


async def test_worker_pool_graceful_close():
    async def handler(x):
        await asyncio.sleep(0.01)
        return x * 2

    before = len(asyncio.all_tasks())
    pool = WorkerPool(3, handler)
    pool.start()
    for i in range(9):
        pool.submit(i)
    await pool.close()
    assert sorted(pool.results) == [i * 2 for i in range(9)]
    assert len(asyncio.all_tasks()) == before
    with pytest.raises(asyncio.QueueShutDown):
        pool.submit(100)


async def test_worker_pool_workers_exit_without_cancel():
    cancelled = 0

    async def handler(x):
        nonlocal cancelled
        try:
            await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            cancelled += 1
            raise
        return x

    pool = WorkerPool(2, handler)
    pool.start()
    for i in range(4):
        pool.submit(i)
    await pool.close()
    assert cancelled == 0
    assert sorted(pool.results) == [0, 1, 2, 3]


async def test_worker_pool_immediate_close():
    async def handler(x):
        await asyncio.sleep(0.05)
        return x

    pool = WorkerPool(2, handler)
    pool.start()
    for i in range(10):
        pool.submit(i)
    await asyncio.sleep(0.01)  # воркеры взяли 0 и 1
    start = now()
    await pool.close(immediate=True)
    assert now() - start < 0.1
    assert sorted(pool.results) == [0, 1]
