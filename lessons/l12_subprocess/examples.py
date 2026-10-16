"""Примеры к уроку 12. Запуск: uv run python -m lessons.l12_subprocess.examples"""

import asyncio
import sys
import time

PY = sys.executable


async def example_communicate():
    print("1. communicate")
    proc = await asyncio.create_subprocess_exec(
        PY, "-c", "import sys; print('out'); print('err', file=sys.stderr); sys.exit(3)",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    print(f"   rc={proc.returncode} out={out!r} err={err!r}")


async def example_parallel():
    print("2. Три процесса по 0.3 с параллельно")
    start = time.perf_counter()
    procs = [await asyncio.create_subprocess_exec(PY, "-c", "import time; time.sleep(0.3)")
             for _ in range(3)]
    await asyncio.gather(*(p.wait() for p in procs))
    print(f"   {time.perf_counter() - start:.2f} c")


async def example_streaming():
    print("3. Построчное чтение вывода")
    code = "import time\nfor i in range(3):\n    print('tick', i, flush=True); time.sleep(0.1)"
    proc = await asyncio.create_subprocess_exec(PY, "-c", code, stdout=asyncio.subprocess.PIPE)
    while line := await proc.stdout.readline():
        print("   получено:", line.decode().strip())
    await proc.wait()


async def example_timeout():
    print("4. Таймаут и kill")
    proc = await asyncio.create_subprocess_exec(PY, "-c", "import time; time.sleep(10)")
    try:
        await asyncio.wait_for(proc.wait(), 0.2)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        print("   убит, returncode =", proc.returncode)


async def main():
    await example_communicate()
    await example_parallel()
    await example_streaming()
    await example_timeout()


if __name__ == "__main__":
    asyncio.run(main())
