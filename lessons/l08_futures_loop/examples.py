"""Примеры к уроку 08. Запуск: uv run python -m lessons.l08_futures_loop.examples"""

import asyncio
import concurrent.futures
import threading
import time


async def example_future():
    print("1. Future вручную")
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    fut.add_done_callback(lambda f: print("   колбэк: результат", f.result()))
    loop.call_later(0.05, fut.set_result, 42)
    print("   await:", await fut)


async def example_scheduling():
    print("2. call_soon / call_later / call_at")
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    log = []
    loop.call_later(0.02, log.append, "later 0.02")
    loop.call_at(loop.time() + 0.01, log.append, "at +0.01")
    loop.call_soon(log.append, "soon")
    h = loop.call_later(0.015, log.append, "отменённый")
    h.cancel()
    loop.call_later(0.03, done.set_result, None)
    await done
    print("  ", log)


def legacy_api(x, callback):
    """Старый колбэчный API, работающий в своём потоке."""
    def work():
        time.sleep(0.05)
        callback(x * 2)
    threading.Thread(target=work).start()


async def example_bridge():
    print("3. Мост: колбэк из другого потока → await")
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    legacy_api(21, lambda r: loop.call_soon_threadsafe(fut.set_result, r))
    print("   результат:", await fut)


async def example_threadsafe():
    print("4. run_coroutine_threadsafe и wrap_future")
    loop = asyncio.get_running_loop()

    async def coro(x):
        await asyncio.sleep(0.01)
        return x + 1

    def in_thread():
        cf = asyncio.run_coroutine_threadsafe(coro(1), loop)
        return cf.result(timeout=1)

    print("   из потока:", await asyncio.to_thread(in_thread))
    with concurrent.futures.ThreadPoolExecutor() as ex:
        cf = ex.submit(sum, [1, 2, 3])
        print("   wrap_future:", await asyncio.wrap_future(cf))


async def main():
    await example_future()
    await example_scheduling()
    await example_bridge()
    await example_threadsafe()


if __name__ == "__main__":
    asyncio.run(main())
