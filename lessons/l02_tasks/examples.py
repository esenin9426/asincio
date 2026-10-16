"""Примеры к уроку 02. Запуск: uv run python -m lessons.l02_tasks.examples"""

import asyncio


async def job(name: str, delay: float) -> str:
    print(f"  {name}: старт")
    await asyncio.sleep(delay)
    print(f"  {name}: финиш")
    return name


async def example_create_task():
    print("1. Задачи выполняются конкурентно")
    loop = asyncio.get_running_loop()
    start = loop.time()
    t1 = asyncio.create_task(job("A", 0.2), name="job-A")
    t2 = asyncio.create_task(job("B", 0.1), name="job-B")
    print("  задачи созданы, но ещё не стартовали")
    print("  результаты:", await t1, await t2)
    print(f"  прошло {loop.time() - start:.2f} c (≈0.2, а не 0.3)")


async def example_states():
    print("2. Состояния задачи")
    t = asyncio.create_task(job("C", 0.05))
    print("  done сразу после создания:", t.done())
    await t
    print("  done после await:", t.done(), "| result:", t.result())

    async def fail():
        raise ValueError("oops")

    bad = asyncio.create_task(fail())
    await asyncio.sleep(0)
    print("  упавшая задача:", repr(bad.exception()))


async def example_callbacks_and_refs():
    print("3. Фоновые задачи с сильными ссылками и колбэками")
    background: set[asyncio.Task] = set()
    for i in range(3):
        t = asyncio.create_task(job(f"bg{i}", 0.05 * (i + 1)))
        background.add(t)
        t.add_done_callback(background.discard)
        t.add_done_callback(lambda t: print(f"  callback: {t.get_name()} -> {t.result()}"))
    while background:
        print("  активных:", len(background))
        await asyncio.sleep(0.05)


async def example_introspection():
    print("4. current_task и all_tasks")
    print("  текущая задача:", asyncio.current_task().get_name())
    t = asyncio.create_task(asyncio.sleep(0.1), name="sleeper")
    print("  все задачи:", sorted(x.get_name() for x in asyncio.all_tasks()))
    await t


async def main():
    await example_create_task()
    await example_states()
    await example_callbacks_and_refs()
    await example_introspection()


if __name__ == "__main__":
    asyncio.run(main())
