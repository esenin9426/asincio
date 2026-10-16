"""Примеры к уроку 07. Запуск: uv run python -m lessons.l07_queues.examples"""

import asyncio
import itertools
import random


async def example_worker_pool():
    print("1. Пул воркеров: join + cancel")
    q: asyncio.Queue[int] = asyncio.Queue()

    async def worker(name):
        while True:
            item = await q.get()
            try:
                await asyncio.sleep(random.uniform(0.01, 0.05))
                print(f"   {name} обработал {item}")
            finally:
                q.task_done()

    workers = [asyncio.create_task(worker(f"w{i}")) for i in range(3)]
    for i in range(6):
        q.put_nowait(i)
    await q.join()
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)


async def example_backpressure():
    print("2. Backpressure: maxsize=2")
    q: asyncio.Queue[int] = asyncio.Queue(maxsize=2)

    async def producer():
        for i in range(5):
            await q.put(i)
            print(f"   положил {i}, в очереди {q.qsize()}")

    async def consumer():
        for _ in range(5):
            await asyncio.sleep(0.05)
            print(f"   взял {await q.get()}")

    await asyncio.gather(producer(), consumer())


async def example_priority():
    print("3. PriorityQueue со счётчиком")
    q: asyncio.PriorityQueue = asyncio.PriorityQueue()
    counter = itertools.count()
    for prio, data in [(2, {"task": "b"}), (1, {"task": "a"}), (2, {"task": "c"})]:
        q.put_nowait((prio, next(counter), data))
    while not q.empty():
        print("  ", q.get_nowait())


async def example_shutdown():
    print("4. Queue.shutdown() (3.13+)")
    q: asyncio.Queue[int] = asyncio.Queue()

    async def worker():
        while True:
            try:
                item = await q.get()
            except asyncio.QueueShutDown:
                print("   очередь закрыта, воркер выходит")
                return
            print("   обработал", item)
            q.task_done()

    w = asyncio.create_task(worker())
    for i in range(3):
        q.put_nowait(i)
    q.shutdown()
    await w


async def main():
    await example_worker_pool()
    await example_backpressure()
    await example_priority()
    await example_shutdown()


if __name__ == "__main__":
    asyncio.run(main())
