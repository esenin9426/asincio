"""Примеры к уроку 09. Запуск: uv run python -m lessons.l09_blocking.examples"""

import asyncio
import concurrent.futures
import hashlib
import logging
import time


async def heartbeat(stop: asyncio.Event):
    n = 0
    while not stop.is_set():
        n += 1
        await asyncio.sleep(0.01)
    return n


async def measure(name, coro_fn):
    stop = asyncio.Event()
    hb = asyncio.create_task(heartbeat(stop))
    start = time.perf_counter()
    await coro_fn()
    stop.set()
    print(f"   {name}: {time.perf_counter() - start:.2f} c, тиков сердцебиения: {await hb}")


async def blocking_wrong():
    time.sleep(0.3)


async def blocking_right():
    await asyncio.to_thread(time.sleep, 0.3)


def cpu_heavy(n: int) -> str:
    data = b"x"
    for _ in range(n):
        data = hashlib.sha256(data).digest()
    return data.hex()[:8]


async def example_blocking():
    print("1. time.sleep блокирует цикл, to_thread — нет")
    await measure("time.sleep в корутине", blocking_wrong)
    await measure("to_thread(time.sleep)", blocking_right)


async def example_processes():
    print("2. CPU-bound: процессы")
    loop = asyncio.get_running_loop()
    with concurrent.futures.ProcessPoolExecutor() as pool:
        start = time.perf_counter()
        results = await asyncio.gather(
            *(loop.run_in_executor(pool, cpu_heavy, 300_000) for _ in range(4))
        )
        print(f"   {results} за {time.perf_counter() - start:.2f} c")


def example_debug():
    print("3. Debug-режим ловит блокировку")
    logging.basicConfig(level=logging.WARNING, format="   [%(name)s] %(message)s")

    async def main():
        time.sleep(0.15)

    asyncio.run(main(), debug=True)


if __name__ == "__main__":
    asyncio.run(example_blocking())
    asyncio.run(example_processes())
    example_debug()
