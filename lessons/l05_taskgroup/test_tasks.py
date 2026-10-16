import asyncio

import pytest

from lessons.l05_taskgroup.tasks import (
    collect_errors,
    group_with_timeout,
    process_all,
    run_isolated,
    search_first,
    walk_tree,
)


async def val(v, d=0.0, exc=None):
    await asyncio.sleep(d)
    if exc is not None:
        raise exc
    return v


def now():
    return asyncio.get_running_loop().time()


async def test_process_all_order_and_concurrency():
    async def double(x):
        await asyncio.sleep(0.1 - x * 0.02)
        return x * 2

    start = now()
    assert await process_all([1, 2, 3, 4], double) == [2, 4, 6, 8]
    assert now() - start < 0.2


async def test_process_all_cancels_on_failure():
    cancelled = []

    async def handler(x):
        try:
            await asyncio.sleep(0.01 if x == 0 else 1)
            if x == 0:
                raise ValueError("bad")
        except asyncio.CancelledError:
            cancelled.append(x)
            raise

    start = now()
    with pytest.raises(ExceptionGroup) as ei:
        await process_all([0, 1, 2], handler)
    assert ei.group_contains(ValueError)
    assert sorted(cancelled) == [1, 2]
    assert now() - start < 0.3


async def test_collect_errors_sorted_by_type():
    values, keys = await collect_errors([
        val(None, 0, ValueError("v1")),
        val(None, 0, KeyError("k1")),
        val(None, 0, ValueError("v2")),
        val(1),
    ])
    assert sorted(str(e) for e in values) == ["v1", "v2"]
    assert [e.args[0] for e in keys] == ["k1"]


async def test_collect_errors_no_errors():
    assert await collect_errors([val(1), val(2)]) == ([], [])


async def test_collect_errors_other_types_propagate():
    with pytest.raises(ExceptionGroup) as ei:
        await collect_errors([val(None, 0, ValueError()), val(None, 0, TypeError("t"))])
    assert ei.group_contains(TypeError)
    assert not ei.group_contains(ValueError)


async def test_group_with_timeout_ok():
    assert await group_with_timeout([val(1, 0.01), val(2, 0.02)], 1) == [1, 2]


async def test_group_with_timeout_expires():
    cancelled = []

    async def slow(i):
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            cancelled.append(i)
            raise

    start = now()
    with pytest.raises(TimeoutError):
        await group_with_timeout([slow(1), slow(2)], 0.05)
    assert now() - start < 0.2
    assert sorted(cancelled) == [1, 2]


async def test_run_isolated():
    start = now()
    res = await run_isolated([val(1, 0.1), val(None, 0.01, KeyError("x")), val(3, 0.1)])
    assert now() - start < 0.2
    assert res[0] == ("ok", 1)
    assert res[1][0] == "error" and isinstance(res[1][1], KeyError)
    assert res[2] == ("ok", 3)


async def test_run_isolated_does_not_swallow_cancel():
    t = asyncio.create_task(run_isolated([val(1, 1)]))
    await asyncio.sleep(0.01)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t


async def test_walk_tree_with_cycles_and_concurrency():
    graph = {
        "a": ["b", "c"],
        "b": ["d", "a"],
        "c": ["d", "e"],
        "d": ["a"],
        "e": [],
        "z": ["a"],  # недостижим
    }
    calls = []
    active = 0
    max_active = 0

    async def children(node):
        nonlocal active, max_active
        calls.append(node)
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.05)
        active -= 1
        return graph[node]

    start = now()
    assert await walk_tree("a", children) == {"a", "b", "c", "d", "e"}
    assert sorted(calls) == ["a", "b", "c", "d", "e"], "каждый узел — ровно один раз"
    assert max_active >= 2, "узлы одного уровня должны обрабатываться конкурентно"
    assert now() - start < 0.25


async def test_search_first_stops_others():
    checked = []
    cancelled = []

    async def is_target(x):
        try:
            await asyncio.sleep(0.02 * x)
            checked.append(x)
            return x == 3
        except asyncio.CancelledError:
            cancelled.append(x)
            raise

    start = now()
    assert await search_first([5, 4, 3, 2, 1, 10, 20], is_target) == 3
    assert now() - start < 0.15
    assert sorted(cancelled) == [4, 5, 10, 20]


async def test_search_first_none():
    async def never(x):
        await asyncio.sleep(0)
        return False

    assert await search_first([1, 2, 3], never) is None
    assert await search_first([], never) is None
