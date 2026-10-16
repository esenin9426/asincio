import asyncio
import contextvars
import os
import signal

import pytest

from lessons.l14_advanced.tasks import (
    capture_callback_errors,
    handle_request,
    install_task_counter,
    log,
    request_id,
    run_eager,
    run_in_context,
    wait_for_signal,
)


async def test_log_default():
    assert log("hi") == "[-] hi"


async def test_handle_request_isolated_between_tasks():
    out = []
    await asyncio.gather(handle_request("A", 2, out), handle_request("B", 2, out))
    assert sorted(out) == ["[A] step 0", "[A] step 1", "[B] step 0", "[B] step 1"]
    assert out[0].startswith("[A]") and out[1].startswith("[B]"), "шаги должны чередоваться"


async def test_handle_request_resets_after_direct_await():
    out = []
    token = request_id.set("outer")
    try:
        await handle_request("inner", 1, out)
        assert request_id.get() == "outer", "после прямого await значение не восстановлено"
        assert out == ["[inner] step 0"]
    finally:
        request_id.reset(token)


user = contextvars.ContextVar("user", default=None)


async def test_run_in_context():
    async def read_both():
        await asyncio.sleep(0)
        return user.get(), request_id.get()

    assert await run_in_context({user: "alice", request_id: "r1"}, read_both) == ("alice", "r1")
    assert user.get() is None and request_id.get() == "-"


async def test_capture_callback_errors():
    loop = asyncio.get_running_loop()
    sentinel = object()

    def my_handler(l, ctx):
        pass

    my_handler.marker = sentinel
    loop.set_exception_handler(my_handler)
    try:
        ran = []
        errs = await capture_callback_errors([
            lambda: ran.append(1),
            lambda: 1 / 0,
            lambda: ran.append(2),
            lambda: {}["missing"],
        ])
        assert ran == [1, 2]
        assert [type(e) for e in errs] == [ZeroDivisionError, KeyError]
        assert loop.get_exception_handler() is my_handler, "обработчик не восстановлен"
    finally:
        loop.set_exception_handler(None)


async def test_wait_for_signal():
    loop = asyncio.get_running_loop()
    loop.call_later(0.05, os.kill, os.getpid(), signal.SIGUSR1)
    assert await wait_for_signal(signal.SIGUSR1, 1) is True
    assert loop.remove_signal_handler(signal.SIGUSR1) is False, "обработчик не снят"


async def test_wait_for_signal_timeout():
    loop = asyncio.get_running_loop()
    assert await wait_for_signal(signal.SIGUSR2, 0.05) is False
    assert loop.remove_signal_handler(signal.SIGUSR2) is False


def test_run_eager():
    async def main():
        async def instant():
            return 1

        t = asyncio.create_task(instant())
        return t.done(), asyncio.get_running_loop().get_task_factory()

    done_immediately, factory = run_eager(main)
    assert done_immediately is True
    assert factory is asyncio.eager_task_factory


async def test_install_task_counter():
    loop = asyncio.get_running_loop()
    try:
        counter = install_task_counter(loop)

        async def alpha():
            await asyncio.sleep(0)
            return asyncio.current_task().get_name()

        async def beta():
            return 2

        ctx = contextvars.copy_context()
        t = asyncio.create_task(alpha(), name="named-alpha", context=ctx)
        assert await t == "named-alpha"
        assert isinstance(t, asyncio.Task)
        await asyncio.gather(alpha(), beta(), beta())
        assert counter["test_install_task_counter.<locals>.alpha"] == 2
        assert counter["test_install_task_counter.<locals>.beta"] == 2
    finally:
        loop.set_task_factory(None)
