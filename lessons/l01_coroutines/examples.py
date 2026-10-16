"""Примеры к уроку 01. Запуск: uv run python -m lessons.l01_coroutines.examples"""

import asyncio
import inspect


async def hello(name: str) -> str:
    print(f"  [{name}] начало")
    await asyncio.sleep(0.1)
    print(f"  [{name}] конец")
    return name.upper()


def example_coroutine_object():
    print("1. Вызов функции-корутины не выполняет её тело")
    coro = hello("lazy")
    print("  тип:", type(coro), "| iscoroutine:", inspect.iscoroutine(coro))
    print("  результат asyncio.run:", asyncio.run(coro))


async def example_sequential():
    print("2. Последовательные await складывают время")
    loop = asyncio.get_running_loop()
    start = loop.time()
    await hello("a")
    await hello("b")
    print(f"  прошло {loop.time() - start:.2f} c (≈0.2)")


async def worker(name: str, steps: int):
    for i in range(steps):
        print(f"  {name}: шаг {i}")
        await asyncio.sleep(0)  # уступаем цикл


async def example_sleep_zero():
    print("3. sleep(0) даёт поработать другим (gather запускает обе корутины)")
    await asyncio.gather(worker("A", 3), worker("B", 3))


class Answer:
    """Собственный awaitable: делегируем ожидание корутине sleep."""

    def __await__(self):
        yield from asyncio.sleep(0.05).__await__()
        return 42


async def example_awaitable():
    print("4. Свой awaitable:", await Answer())


def example_runner():
    print("5. Runner: один цикл на несколько вызовов")
    with asyncio.Runner() as runner:
        loop1 = runner.run(get_loop())
        loop2 = runner.run(get_loop())
        print("  тот же цикл:", loop1 is loop2)
    print("  новый цикл у каждого asyncio.run:",
          asyncio.run(get_loop()) is not asyncio.run(get_loop()))


async def get_loop():
    return asyncio.get_running_loop()


if __name__ == "__main__":
    example_coroutine_object()
    asyncio.run(example_sequential())
    asyncio.run(example_sleep_zero())
    asyncio.run(example_awaitable())
    example_runner()
