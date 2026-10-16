import asyncio
import inspect
import warnings

import pytest

from lessons.l01_coroutines.tasks import (
    Delayed,
    countdown,
    greet,
    run_sync,
    sequential,
)


# --- greet -----------------------------------------------------------------

def test_greet_is_coroutine_function():
    assert inspect.iscoroutinefunction(greet)


async def test_greet_result():
    assert await greet("Ann") == "Hello, Ann!"


async def test_greet_yields_to_loop():
    log = []

    async def other():
        log.append("other")

    t = asyncio.create_task(other())
    result = await greet("x")
    log.append("greet-done")
    await t
    assert result == "Hello, x!"
    assert log == ["other", "greet-done"], "greet должна уступить цикл через sleep(0)"


# --- sequential ------------------------------------------------------------

async def test_sequential_sums_delays():
    elapsed = await sequential([0.05, 0.05, 0.1])
    assert 0.19 <= elapsed < 0.4


async def test_sequential_empty():
    assert await sequential([]) < 0.05


# --- countdown -------------------------------------------------------------

async def test_countdown_single():
    log = []
    await countdown("a", 3, log)
    assert log == ["a3", "a2", "a1"]


async def test_countdown_interleaves():
    log = []
    await asyncio.gather(countdown("a", 3, log), countdown("b", 3, log))
    assert log == ["a3", "b3", "a2", "b2", "a1", "b1"]


# --- Delayed ---------------------------------------------------------------

async def test_delayed_returns_value():
    loop = asyncio.get_running_loop()
    start = loop.time()
    assert await Delayed(0.1, "x") == "x"
    assert 0.09 <= loop.time() - start < 0.3


async def test_delayed_is_not_coroutine():
    d = Delayed(0, 1)
    assert not inspect.iscoroutine(d)
    assert inspect.isawaitable(d)


async def test_delayed_reusable_and_concurrent():
    d = Delayed(0.1, 7)
    assert await d == 7
    loop = asyncio.get_running_loop()
    start = loop.time()
    results = await asyncio.gather(Delayed(0.1, 1), Delayed(0.1, 2), Delayed(0.1, 3))
    assert results == [1, 2, 3]
    assert loop.time() - start < 0.25, "Delayed не должен блокировать цикл"


# --- run_sync --------------------------------------------------------------

async def _add(a, b):
    await asyncio.sleep(0)
    return a + b


def test_run_sync_from_sync_code():
    assert run_sync(_add, 2, 3) == 5


def test_run_sync_propagates_exceptions():
    async def boom():
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        run_sync(boom)


async def test_run_sync_inside_loop_raises():
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        with pytest.raises(RuntimeError, match="run_sync"):
            run_sync(_add, 1, 2)
