"""Примеры к уроку 03. Запуск: uv run python -m lessons.l03_combinators.examples"""

import asyncio
import random


async def fetch(name: str, delay: float, fail: bool = False) -> str:
    await asyncio.sleep(delay)
    if fail:
        raise ConnectionError(name)
    return f"<{name}>"


async def example_gather():
    print("1. gather: порядок аргументов, исключения")
    print("  ", await asyncio.gather(fetch("a", 0.2), fetch("b", 0.1)))
    print("  ", await asyncio.gather(fetch("a", 0.1), fetch("b", 0.05, fail=True),
                                     return_exceptions=True))


async def example_wait():
    print("2. wait(FIRST_COMPLETED) + отмена остальных")
    tasks = [asyncio.create_task(fetch(f"mirror{i}", random.uniform(0.05, 0.3)))
             for i in range(3)]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    print("   победитель:", done.pop().result(), "| отменяем:", len(pending))
    for t in pending:
        t.cancel()
    await asyncio.gather(*pending, return_exceptions=True)


async def example_as_completed():
    print("3. as_completed")
    coros = [fetch(n, d) for n, d in [("slow", 0.3), ("fast", 0.1), ("mid", 0.2)]]
    for fut in asyncio.as_completed(coros):
        print("  ", await fut)


async def example_timeouts():
    print("4. Таймауты")
    try:
        await asyncio.wait_for(fetch("x", 1), timeout=0.1)
    except TimeoutError:
        print("   wait_for: TimeoutError")
    try:
        async with asyncio.timeout(0.1):
            await fetch("y", 1)
    except TimeoutError:
        print("   timeout(): TimeoutError")
    t = asyncio.create_task(fetch("z", 0.3))
    done, pending = await asyncio.wait([t], timeout=0.1)
    print("   wait(timeout): done =", len(done), "pending =", len(pending),
          "(задача не отменена:", not t.cancelled(), ")")
    await t


async def main():
    await example_gather()
    await example_wait()
    await example_as_completed()
    await example_timeouts()


if __name__ == "__main__":
    asyncio.run(main())
