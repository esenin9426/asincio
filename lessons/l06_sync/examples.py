"""Примеры к уроку 06. Запуск: uv run python -m lessons.l06_sync.examples"""

import asyncio


async def example_race():
    print("1. Гонка без блокировки и с ней")
    state = {"n": 0}

    async def unsafe():
        v = state["n"]
        await asyncio.sleep(0)
        state["n"] = v + 1

    await asyncio.gather(*(unsafe() for _ in range(100)))
    print("   без Lock:", state["n"])

    state["n"] = 0
    lock = asyncio.Lock()

    async def safe():
        async with lock:
            v = state["n"]
            await asyncio.sleep(0)
            state["n"] = v + 1

    await asyncio.gather(*(safe() for _ in range(100)))
    print("   с Lock:", state["n"])


async def example_event():
    print("2. Event")
    ready = asyncio.Event()

    async def waiter(i):
        await ready.wait()
        print(f"   waiter {i} проснулся")

    tasks = [asyncio.create_task(waiter(i)) for i in range(3)]
    await asyncio.sleep(0.05)
    print("   set()")
    ready.set()
    await asyncio.gather(*tasks)


async def example_condition():
    print("3. Condition: потребитель ждёт данных")
    cond = asyncio.Condition()
    items: list[int] = []

    async def consumer():
        async with cond:
            await cond.wait_for(lambda: items)
            print("   получил", items.pop())

    async def producer():
        await asyncio.sleep(0.05)
        async with cond:
            items.append(42)
            cond.notify()

    await asyncio.gather(consumer(), producer())


async def example_semaphore():
    print("4. Semaphore(2)")
    sem = asyncio.Semaphore(2)

    async def job(i):
        async with sem:
            print(f"   job {i} старт")
            await asyncio.sleep(0.05)

    await asyncio.gather(*(job(i) for i in range(5)))


async def example_barrier():
    print("5. Barrier")
    barrier = asyncio.Barrier(3)

    async def worker(i):
        await asyncio.sleep(0.01 * i)
        print(f"   {i} дошёл до барьера")
        idx = await barrier.wait()
        print(f"   {i} прошёл (индекс {idx})")

    await asyncio.gather(*(worker(i) for i in range(3)))


async def main():
    await example_race()
    await example_event()
    await example_condition()
    await example_semaphore()
    await example_barrier()


if __name__ == "__main__":
    asyncio.run(main())
