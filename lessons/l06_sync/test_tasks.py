import asyncio
import inspect

import pytest

from lessons.l06_sync.tasks import (
    BoundedBuffer,
    ConnectionPool,
    Latch,
    RWLock,
    SafeCounter,
    limited_gather,
    phased_workers,
)


async def test_safe_counter():
    c = SafeCounter()
    await asyncio.gather(*(c.increment() for _ in range(200)))
    assert c.value == 200
    assert "sleep(0)" in inspect.getsource(SafeCounter.increment), "не удаляйте sleep(0)"


async def test_limited_gather():
    active = 0
    peak = 0

    async def job(i):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.02)
        active -= 1
        return i

    loop = asyncio.get_running_loop()
    start = loop.time()
    assert await limited_gather([job(i) for i in range(10)], 3) == list(range(10))
    assert peak == 3
    assert loop.time() - start < 0.15  # 4 «волны» по 0.02


async def test_latch():
    latch = Latch(3)
    assert latch.count == 3
    woke = []

    async def waiter(i):
        await latch.wait()
        woke.append(i)

    tasks = [asyncio.create_task(waiter(i)) for i in range(3)]
    await asyncio.sleep(0.01)
    latch.count_down()
    latch.count_down()
    await asyncio.sleep(0.01)
    assert woke == []
    latch.count_down()
    await asyncio.gather(*tasks)
    assert sorted(woke) == [0, 1, 2]
    latch.count_down()
    assert latch.count == 0
    await asyncio.wait_for(latch.wait(), 0.1)


async def test_latch_zero():
    await asyncio.wait_for(Latch(0).wait(), 0.1)


async def test_bounded_buffer_fifo_and_blocking():
    buf = BoundedBuffer(2)
    await buf.put(1)
    await buf.put(2)
    assert len(buf) == 2
    put3 = asyncio.create_task(buf.put(3))
    await asyncio.sleep(0.02)
    assert not put3.done(), "put должен ждать при заполненном буфере"
    assert await buf.get() == 1
    await asyncio.wait_for(put3, 0.1)
    assert [await buf.get(), await buf.get()] == [2, 3]
    get_task = asyncio.create_task(buf.get())
    await asyncio.sleep(0.02)
    assert not get_task.done(), "get должен ждать на пустом буфере"
    await buf.put("x")
    assert await asyncio.wait_for(get_task, 0.1) == "x"


async def test_bounded_buffer_many_producers_consumers():
    buf = BoundedBuffer(3)
    got = []

    async def producer(base):
        for i in range(20):
            await buf.put(base + i)
            assert len(buf) <= 3

    async def consumer():
        for _ in range(20):
            got.append(await buf.get())

    await asyncio.gather(producer(0), producer(100), producer(200),
                         consumer(), consumer(), consumer())
    assert sorted(got) == sorted([b + i for b in (0, 100, 200) for i in range(20)])


async def test_phased_workers():
    log = []
    await phased_workers(4, 3, log)
    assert len(log) == 12
    phases = [p for p, _ in log]
    assert phases == sorted(phases), f"фазы перемешаны: {log}"
    for p in range(3):
        assert sorted(i for ph, i in log if ph == p) == [0, 1, 2, 3]


async def test_connection_pool():
    pool = ConnectionPool(2)
    a = await pool.acquire()
    b = await pool.acquire()
    assert {a, b} == {0, 1}
    assert pool.in_use == 2
    waiter = asyncio.create_task(pool.acquire())
    await asyncio.sleep(0.01)
    assert not waiter.done()
    pool.release(a)
    assert await asyncio.wait_for(waiter, 0.1) == a
    pool.release(b)
    with pytest.raises(ValueError):
        pool.release(b)
    with pytest.raises(ValueError):
        pool.release(99)
    assert pool.in_use == 1


async def test_connection_pool_context_manager_limits():
    pool = ConnectionPool(3)
    peak = 0

    async def use():
        nonlocal peak
        async with pool.connection() as conn:
            assert 0 <= conn < 3
            peak = max(peak, pool.in_use)
            await asyncio.sleep(0.01)

    await asyncio.gather(*(use() for _ in range(10)))
    assert peak == 3
    assert pool.in_use == 0


async def test_connection_pool_releases_on_error():
    pool = ConnectionPool(1)
    with pytest.raises(RuntimeError):
        async with pool.connection():
            raise RuntimeError
    assert pool.in_use == 0


async def test_rwlock_readers_share():
    lock = RWLock()
    active = 0
    peak = 0

    async def reader():
        nonlocal active, peak
        async with lock.read():
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.02)
            active -= 1

    await asyncio.gather(*(reader() for _ in range(5)))
    assert peak == 5


async def test_rwlock_writer_exclusive_and_priority():
    lock = RWLock()
    log = []

    async def reader(name, delay=0.0):
        await asyncio.sleep(delay)
        async with lock.read():
            log.append(f"{name}+")
            await asyncio.sleep(0.03)
            log.append(f"{name}-")

    async def writer(name, delay):
        await asyncio.sleep(delay)
        async with lock.write():
            log.append(f"{name}+")
            await asyncio.sleep(0.02)
            log.append(f"{name}-")

    await asyncio.gather(
        reader("r1"),
        writer("w", 0.01),      # пришёл, пока r1 читает → ждёт
        reader("r2", 0.02),     # пришёл после писателя → должен ждать писателя
    )
    assert log.index("w+") > log.index("r1-"), "писатель не ждал читателя"
    assert log.index("r2+") > log.index("w-"), "новый читатель обогнал ждущего писателя"
