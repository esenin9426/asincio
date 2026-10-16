import asyncio

import pytest

from lessons.l03_combinators.tasks import (
    deadline_batch,
    fetch_all,
    fetch_all_safe,
    first_completed,
    in_completion_order,
    staged_deadline,
    with_timeout,
)

DELAYS = {"a": 0.15, "b": 0.05, "c": 0.1}


async def fake_fetch(url):
    await asyncio.sleep(DELAYS.get(url, 0.05))
    if url.startswith("bad"):
        raise ConnectionError(url)
    return url.upper()


def elapsed_since(start):
    return asyncio.get_running_loop().time() - start


async def test_fetch_all_order_and_concurrency():
    start = asyncio.get_running_loop().time()
    assert await fetch_all(["a", "b", "c"], fake_fetch) == ["A", "B", "C"]
    assert elapsed_since(start) < 0.25


async def test_fetch_all_raises():
    with pytest.raises(ConnectionError):
        await fetch_all(["a", "bad1"], fake_fetch)


async def test_fetch_all_safe():
    ok, errors = await fetch_all_safe(["a", "bad1", "b", "bad2"], fake_fetch)
    assert ok == {"a": "A", "b": "B"}
    assert set(errors) == {"bad1", "bad2"}
    assert all(isinstance(e, ConnectionError) for e in errors.values())


async def test_first_completed_returns_fastest_and_cancels_rest():
    cancelled = []

    async def mirror(name, delay):
        try:
            await asyncio.sleep(delay)
            return name
        except asyncio.CancelledError:
            cancelled.append(name)
            raise

    before = len(asyncio.all_tasks())
    start = asyncio.get_running_loop().time()
    result = await first_completed([mirror("m1", 0.3), mirror("m2", 0.05), mirror("m3", 0.2)])
    assert result == "m2"
    assert elapsed_since(start) < 0.15
    assert sorted(cancelled) == ["m1", "m3"], "проигравшие задачи нужно отменить"
    assert len(asyncio.all_tasks()) == before, "остались висящие задачи"


async def test_first_completed_propagates_error():
    async def fail():
        await asyncio.sleep(0.01)
        raise ValueError("first")

    with pytest.raises(ValueError, match="first"):
        await first_completed([fail(), asyncio.sleep(0.2)])


async def test_in_completion_order():
    async def val(v, d):
        await asyncio.sleep(d)
        return v

    res = await in_completion_order([val(1, 0.15), val(2, 0.05), val(3, 0.1)])
    assert res == [2, 3, 1]


async def test_with_timeout_success():
    assert await with_timeout(asyncio.sleep(0.01, "ok"), 1, "default") == "ok"


async def test_with_timeout_returns_default_and_cancels():
    state = {}

    async def slow():
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            state["cancelled"] = True
            raise

    start = asyncio.get_running_loop().time()
    assert await with_timeout(slow(), 0.05, "default") == "default"
    assert elapsed_since(start) < 0.2
    assert state.get("cancelled"), "операция должна быть отменена"


async def test_deadline_batch_partial():
    async def val(v, d, fail=False):
        await asyncio.sleep(d)
        if fail:
            raise RuntimeError
        return v

    before = len(asyncio.all_tasks())
    start = asyncio.get_running_loop().time()
    results, pending = await deadline_batch(
        [val(1, 0.01), val(2, 0.02), val(3, 1), val(4, 1), val(5, 0.01, fail=True)],
        deadline=0.1,
    )
    assert sorted(results) == [1, 2]
    assert pending == 2
    assert 0.09 <= elapsed_since(start) < 0.3
    assert len(asyncio.all_tasks()) == before


async def test_deadline_batch_returns_early():
    start = asyncio.get_running_loop().time()
    results, pending = await deadline_batch([asyncio.sleep(0.01, "x")], deadline=5)
    assert results == ["x"] and pending == 0
    assert elapsed_since(start) < 0.1


async def test_staged_deadline_bonus_extends():
    steps = [lambda: asyncio.sleep(0.06) for _ in range(4)]
    # без бонуса за 0.1 с успеем 1 шаг; с бонусом 0.06 успеем все 4
    assert await staged_deadline(steps, budget=0.1, bonus=0.06) == 4


async def test_staged_deadline_runs_out():
    steps = [lambda: asyncio.sleep(0.05)] * 2 + [lambda: asyncio.sleep(1)] * 2
    start = asyncio.get_running_loop().time()
    assert await staged_deadline(steps, budget=0.08, bonus=0.03) == 2
    assert elapsed_since(start) < 0.3


async def test_staged_deadline_does_not_swallow_outer_cancel():
    steps = [lambda: asyncio.sleep(1)]
    t = asyncio.create_task(staged_deadline(steps, budget=5, bonus=0))
    await asyncio.sleep(0.02)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
