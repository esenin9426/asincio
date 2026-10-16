"""Примеры к уроку 14. Запуск: uv run python -m lessons.l14_advanced.examples"""

import asyncio
import contextvars
import os
import signal

request_id = contextvars.ContextVar("request_id", default="-")


def log(msg):
    print(f"   [{request_id.get()}] {msg}")


async def handler(rid):
    request_id.set(rid)        # внутри задачи — своя копия контекста
    log("начал")
    await asyncio.sleep(0.01)
    log("закончил")


async def example_contextvars():
    print("1. contextvars: у каждой задачи своя копия")
    await asyncio.gather(handler("A"), handler("B"))
    log("в главной задаче значение не изменилось")


async def example_exception_handler():
    print("2. Обработчик исключений цикла")
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(lambda l, ctx: print("   handler:", repr(ctx.get("exception"))))
    loop.call_soon(lambda: 1 / 0)
    await asyncio.sleep(0.01)
    loop.set_exception_handler(None)


async def example_signal():
    print("3. Сигналы")
    loop = asyncio.get_running_loop()
    got = asyncio.Event()
    loop.add_signal_handler(signal.SIGUSR1, got.set)
    loop.call_later(0.05, os.kill, os.getpid(), signal.SIGUSR1)
    await got.wait()
    loop.remove_signal_handler(signal.SIGUSR1)
    print("   получен SIGUSR1")


async def example_eager():
    print("4. Eager tasks")
    loop = asyncio.get_running_loop()

    async def cached():
        return "из кэша"

    t = asyncio.create_task(cached())
    print("   обычная задача done сразу:", t.done())
    await t
    loop.set_task_factory(asyncio.eager_task_factory)
    t = asyncio.create_task(cached())
    print("   eager-задача done сразу:", t.done(), t.result())
    loop.set_task_factory(None)


async def main():
    await example_contextvars()
    await example_exception_handler()
    await example_signal()
    await example_eager()


if __name__ == "__main__":
    asyncio.run(main())
