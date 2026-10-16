import asyncio
import gc

from lessons.l02_tasks.tasks import (
    BackgroundTasks,
    describe,
    first_done_name,
    run_concurrently,
    spawn_named,
)


async def test_run_concurrently_takes_max():
    elapsed = await run_concurrently([0.1, 0.2, 0.15, 0.2])
    assert 0.19 <= elapsed < 0.3


async def test_run_concurrently_many():
    elapsed = await run_concurrently([0.1] * 200)
    assert elapsed < 0.3


async def test_spawn_named():
    assert await spawn_named(3) == ["worker-0", "worker-1", "worker-2"]
    assert await spawn_named(0) == []


async def test_describe_states():
    async def ok():
        return 42

    async def fail():
        raise ValueError("x")

    async def forever():
        await asyncio.sleep(10)

    t_ok = asyncio.create_task(ok())
    t_fail = asyncio.create_task(fail())
    t_pending = asyncio.create_task(forever())
    t_cancel = asyncio.create_task(forever())
    await asyncio.sleep(0)
    t_cancel.cancel()
    await asyncio.sleep(0)

    assert describe(t_ok) == "done: 42"
    assert describe(t_fail) == "failed: ValueError"
    assert describe(t_pending) == "pending"
    assert describe(t_cancel) == "cancelled"

    t_pending.cancel()
    await asyncio.sleep(0)


async def test_describe_repr():
    async def s():
        return "hi"

    t = asyncio.create_task(s())
    await t
    assert describe(t) == "done: 'hi'"


async def test_background_tasks_keeps_and_releases():
    bg = BackgroundTasks()
    done = []

    async def work(i, d):
        await asyncio.sleep(d)
        done.append(i)

    tasks = [bg.spawn(work(i, 0.05 * (i + 1))) for i in range(3)]
    assert all(isinstance(t, asyncio.Task) for t in tasks)
    del tasks
    gc.collect()
    assert len(bg) == 3
    await asyncio.sleep(0.07)
    assert len(bg) == 2
    await bg.wait_all()
    assert len(bg) == 0
    assert sorted(done) == [0, 1, 2]


async def test_background_tasks_collects_failures():
    bg = BackgroundTasks()

    async def boom():
        await asyncio.sleep(0.01)
        raise KeyError("k")

    async def slow():
        await asyncio.sleep(10)

    bg.spawn(boom())
    t = bg.spawn(slow())
    await asyncio.sleep(0.02)
    t.cancel()
    await bg.wait_all()  # не должно бросить
    assert len(bg) == 0
    assert len(bg.failures) == 1
    assert isinstance(bg.failures[0], KeyError)


async def test_first_done_name():
    async def sleep_then(d):
        await asyncio.sleep(d)

    tasks = [
        asyncio.create_task(sleep_then(0.2), name="slow"),
        asyncio.create_task(sleep_then(0.05), name="fast"),
        asyncio.create_task(sleep_then(0.1), name="mid"),
    ]
    assert await first_done_name(tasks) == "fast"
    assert not tasks[0].done(), "остальные задачи нельзя отменять"
    assert not tasks[0].cancelled()
    await asyncio.gather(*tasks)


async def test_first_done_name_failed_task_counts():
    async def fail():
        await asyncio.sleep(0.01)
        raise RuntimeError

    async def slow():
        await asyncio.sleep(0.1)

    t1 = asyncio.create_task(fail(), name="failing")
    t2 = asyncio.create_task(slow(), name="slow")
    assert await first_done_name([t1, t2]) == "failing"
    await asyncio.gather(t1, t2, return_exceptions=True)


class SpyFuture(asyncio.Future):
    """Future с именем, который считает «живые» колбэки."""

    def __init__(self, name):
        super().__init__()
        self.name = name
        self.active = 0

    def get_name(self):
        return self.name

    def add_done_callback(self, fn, *, context=None):
        self.active += 1
        super().add_done_callback(fn, context=context)

    def remove_done_callback(self, fn):
        removed = super().remove_done_callback(fn)
        self.active -= removed
        return removed


async def test_first_done_name_removes_callbacks():
    loop = asyncio.get_running_loop()
    fast, slow = SpyFuture("fast"), SpyFuture("slow")
    loop.call_later(0.01, fast.set_result, None)
    assert await first_done_name([fast, slow]) == "fast"
    assert slow.active == 0, "колбэк на незавершённой задаче не снят"
    slow.cancel()
