"""Примеры к уроку 13. Запуск: uv run python -m lessons.l13_async_iter_ctx.examples"""

import asyncio
from contextlib import AsyncExitStack, aclosing, asynccontextmanager


async def numbers(n):
    try:
        for i in range(n):
            await asyncio.sleep(0.01)
            yield i
    finally:
        print("   генератор закрыт")


async def example_generator():
    print("1. Асинхронный генератор и comprehension")
    print("  ", [x * x async for x in numbers(5) if x % 2 == 0])


async def example_aclosing():
    print("2. aclosing: finally выполнится сразу после break")
    async with aclosing(numbers(100)) as gen:
        async for x in gen:
            if x == 2:
                break
    print("   после блока")


@asynccontextmanager
async def resource(name):
    print(f"   открыт {name}")
    await asyncio.sleep(0)
    try:
        yield name.upper()
    finally:
        print(f"   закрыт {name}")


async def example_exit_stack():
    print("3. AsyncExitStack")
    async with AsyncExitStack() as stack:
        res = [await stack.enter_async_context(resource(n)) for n in ["db", "cache", "queue"]]
        print("   используем", res)


async def main():
    await example_generator()
    await example_aclosing()
    await example_exit_stack()


if __name__ == "__main__":
    asyncio.run(main())
