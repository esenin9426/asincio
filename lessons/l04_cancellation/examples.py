"""Примеры к уроку 04. Запуск: uv run python -m lessons.l04_cancellation.examples"""

import asyncio


async def worker(name: str):
    try:
        while True:
            print(f"  {name}: работаю")
            await asyncio.sleep(0.05)
    except asyncio.CancelledError as e:
        print(f"  {name}: отменён, причина={e.args}, прибираюсь...")
        await asyncio.sleep(0.01)
        raise
    finally:
        print(f"  {name}: finally")


async def example_cancel():
    print("1. Отмена с сообщением")
    t = asyncio.create_task(worker("w"))
    await asyncio.sleep(0.12)
    t.cancel("пора домой")
    try:
        await t
    except asyncio.CancelledError:
        print("  cancelled():", t.cancelled())


async def example_swallow():
    print("2. Проглоченная отмена (антипаттерн)")

    async def bad():
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            return "я не отменился"

    t = asyncio.create_task(bad())
    await asyncio.sleep(0)
    t.cancel()
    print("  результат:", await t, "| cancelled():", t.cancelled())


async def example_shield():
    print("3. shield")

    async def save():
        await asyncio.sleep(0.1)
        print("  сохранение завершено")

    inner = asyncio.create_task(save())

    async def outer():
        await asyncio.shield(inner)

    o = asyncio.create_task(outer())
    await asyncio.sleep(0.02)
    o.cancel()
    await asyncio.gather(o, return_exceptions=True)
    print("  внешняя отменена:", o.cancelled(), "| внутренняя ещё работает:", not inner.done())
    await inner


async def example_cancelling():
    print("4. cancelling(): timeout не глотает внешнюю отмену")

    async def inner():
        async with asyncio.timeout(10):
            await asyncio.sleep(1)

    t = asyncio.create_task(inner())
    await asyncio.sleep(0.01)
    t.cancel()
    res = await asyncio.gather(t, return_exceptions=True)
    print("  результат:", res)


async def main():
    await example_cancel()
    await example_swallow()
    await example_shield()
    await example_cancelling()


if __name__ == "__main__":
    asyncio.run(main())
