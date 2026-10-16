"""Примеры к уроку 15. Запуск: uv run python -m lessons.l15_under_the_hood.examples

Крошечный «цикл» без asyncio: корутины отдают команды через yield.
"""

import heapq
import time


class Pause:
    def __init__(self, delay):
        self.delay = delay

    def __await__(self):
        yield self.delay        # «разбуди меня через delay секунд»


async def worker(name, n, delay):
    for i in range(n):
        print(f"   {name}: шаг {i}")
        await Pause(delay)
    return f"{name} готов"


def run(*coros):
    timers = [(0.0, i, c) for i, c in enumerate(coros)]   # (когда, порядок, корутина)
    results = {}
    start = time.monotonic()
    while timers:
        when, i, coro = heapq.heappop(timers)
        time.sleep(max(0.0, when - (time.monotonic() - start)))
        try:
            delay = coro.send(None)
        except StopIteration as e:
            results[i] = e.value
            continue
        heapq.heappush(timers, (time.monotonic() - start + delay, i, coro))
    return [results[i] for i in range(len(coros))]


if __name__ == "__main__":
    print("Корутина вручную:")
    c = worker("manual", 2, 0.1)
    print("   send →", c.send(None))
    print("   send →", c.send(None))
    try:
        c.send(None)
    except StopIteration as e:
        print("   StopIteration.value =", e.value)

    print("Мини-цикл:")
    print("  ", run(worker("A", 3, 0.1), worker("B", 2, 0.15)))
