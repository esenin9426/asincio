import asyncio

import pytest

from lessons.l04_cancellation.tasks import (
    cancel_and_wait,
    cancel_reason,
    cancellable_worker,
    graceful_shutdown,
    protected_save,
    run_ignoring_cancel_for,
)


async def sleeper(d, result=None):
    await asyncio.sleep(d)
    return result


async def test_cancellable_worker():
    log = []
    t = asyncio.create_task(cancellable_worker(log))
    await asyncio.sleep(0.055)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    assert t.cancelled()
    assert log.count("tick") >= 3
    assert log[-1] == "cleanup"
    await asyncio.sleep(0.03)
    assert log[-1] == "cleanup"


async def test_cancel_and_wait_cancelled():
    t = asyncio.create_task(sleeper(1))
    await asyncio.sleep(0)
    assert await cancel_and_wait(t) is True
    assert t.cancelled()


async def test_cancel_and_wait_already_done():
    t = asyncio.create_task(sleeper(0, 5))
    await asyncio.sleep(0.01)
    assert await cancel_and_wait(t) is False


async def test_cancel_and_wait_swallowing_task():
    async def stubborn():
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            return "ignored"

    t = asyncio.create_task(stubborn())
    await asyncio.sleep(0)
    assert await cancel_and_wait(t) is False


async def test_cancel_and_wait_propagates_error():
    async def fails_on_cancel():
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            raise ValueError("cleanup failed")

    t = asyncio.create_task(fails_on_cancel())
    await asyncio.sleep(0)
    with pytest.raises(ValueError):
        await cancel_and_wait(t)


async def test_cancel_and_wait_does_not_swallow_own_cancellation():
    async def slow_to_die():
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            await asyncio.sleep(0.1)
            raise

    victim = asyncio.create_task(slow_to_die())
    await asyncio.sleep(0)
    waiter = asyncio.create_task(cancel_and_wait(victim))
    await asyncio.sleep(0.02)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert waiter.cancelled(), "собственную отмену нельзя проглатывать"
    await asyncio.gather(victim, return_exceptions=True)


async def test_protected_save_normal():
    registry = []
    assert await protected_save(sleeper(0.01, "saved"), registry) == "saved"
    assert len(registry) == 1


async def test_protected_save_survives_cancel():
    registry = []
    state = {}

    async def save():
        await asyncio.sleep(0.1)
        state["saved"] = True
        return "ok"

    outer = asyncio.create_task(protected_save(save(), registry))
    await asyncio.sleep(0.02)
    outer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await outer
    assert not state, "внешний ждущий отменён сразу"
    assert len(registry) == 1
    assert await registry[0] == "ok"
    assert state["saved"]


async def test_cancel_reason():
    t = asyncio.create_task(sleeper(1))
    await asyncio.sleep(0)
    assert await cancel_reason(t, "shutdown") == "shutdown"

    done = asyncio.create_task(sleeper(0))
    await asyncio.sleep(0.01)
    assert await cancel_reason(done, "late") is None


async def test_graceful_shutdown():
    cleaned = []

    async def job(i, d):
        try:
            await asyncio.sleep(d)
        except asyncio.CancelledError:
            cleaned.append(i)
            raise

    tasks = [asyncio.create_task(job(i, d)) for i, d in enumerate([0.01, 0.02, 1, 2])]
    start = asyncio.get_running_loop().time()
    assert await graceful_shutdown(tasks, grace=0.1) == (2, 2)
    assert 0.09 <= asyncio.get_running_loop().time() - start < 0.3
    assert sorted(cleaned) == [2, 3]
    assert all(t.done() for t in tasks)


async def test_graceful_shutdown_early_exit_and_errors():
    async def fail():
        raise RuntimeError

    tasks = [asyncio.create_task(sleeper(0.01)), asyncio.create_task(fail())]
    start = asyncio.get_running_loop().time()
    assert await graceful_shutdown(tasks, grace=5) == (2, 0)
    assert asyncio.get_running_loop().time() - start < 0.1


async def test_run_ignoring_cancel_for_completes_then_cancels():
    state = {}

    async def critical():
        await asyncio.sleep(0.1)
        state["done"] = True
        return 1

    t = asyncio.create_task(run_ignoring_cancel_for(critical()))
    await asyncio.sleep(0.02)
    t.cancel()
    await asyncio.sleep(0.03)
    assert not t.done(), "задача должна дождаться критической секции"
    with pytest.raises(asyncio.CancelledError):
        await t
    assert state.get("done")
    assert t.cancelled()


async def test_run_ignoring_cancel_for_no_cancel():
    assert await run_ignoring_cancel_for(sleeper(0.01, 7)) == 7
