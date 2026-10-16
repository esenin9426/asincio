"""Примеры к уроку 05. Запуск: uv run python -m lessons.l05_taskgroup.examples"""

import asyncio


async def job(name: str, delay: float, fail: type[Exception] | None = None):
    try:
        await asyncio.sleep(delay)
        if fail:
            raise fail(name)
        print(f"  {name}: готово")
        return name
    except asyncio.CancelledError:
        print(f"  {name}: отменён группой")
        raise


async def example_ok():
    print("1. Все задачи успешны")
    async with asyncio.TaskGroup() as tg:
        tasks = [tg.create_task(job(f"t{i}", 0.05 * i)) for i in range(3)]
    print("  результаты:", [t.result() for t in tasks])


async def example_fail():
    print("2. Одна упала — остальные отменены")
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(job("slow", 1))
            tg.create_task(job("bad", 0.05, ValueError))
    except* ValueError as eg:
        print("  поймали:", eg.exceptions)


async def example_multi():
    print("3. Несколько ошибок разных типов")
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(job("v", 0, ValueError))
            tg.create_task(job("k", 0, KeyError))
            tg.create_task(job("t", 0, TypeError))
    except* (ValueError, KeyError) as eg:
        print("  обработали:", [type(e).__name__ for e in eg.exceptions])
    except* TypeError as eg:
        print("  отдельно TypeError:", eg.exceptions)


async def example_dynamic():
    print("4. Задачи добавляют задачи")

    async def spread(tg: asyncio.TaskGroup, depth: int, name: str):
        print(f"  {'  ' * depth}{name}")
        if depth < 2:
            for i in range(2):
                tg.create_task(spread(tg, depth + 1, f"{name}.{i}"))

    async with asyncio.TaskGroup() as tg:
        tg.create_task(spread(tg, 0, "root"))


async def main():
    await example_ok()
    await example_fail()
    await example_multi()
    await example_dynamic()


if __name__ == "__main__":
    asyncio.run(main())
