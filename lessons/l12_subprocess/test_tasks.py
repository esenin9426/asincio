import asyncio
import os
import sys

import pytest

from lessons.l12_subprocess.tasks import (
    interactive_session,
    pipe_through,
    run_command,
    run_parallel,
    run_with_timeout,
    stream_lines,
)

PY = sys.executable


def now():
    return asyncio.get_running_loop().time()


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


async def test_run_command():
    code = "import sys; print('привет'); print('oops', file=sys.stderr); sys.exit(2)"
    rc, out, err = await run_command(PY, "-c", code)
    assert (rc, out.strip(), err.strip()) == (2, "привет", "oops")


async def test_run_with_timeout_ok():
    assert await run_with_timeout([PY, "-c", "print(42)"], 5) == (0, "42\n")


async def test_run_with_timeout_kills():
    code = "import os, time; print(os.getpid(), flush=True); time.sleep(30)"
    start = now()
    with pytest.raises(TimeoutError):
        await run_with_timeout([PY, "-c", code], 0.5)
    assert now() - start < 2


async def test_run_with_timeout_kills_on_cancel(tmp_path):
    pidfile = tmp_path / "pid"
    code = f"import os, time; open({str(pidfile)!r}, 'w').write(str(os.getpid())); time.sleep(30)"
    t = asyncio.create_task(run_with_timeout([PY, "-c", code], 30))
    for _ in range(100):
        await asyncio.sleep(0.05)
        if pidfile.exists() and pidfile.read_text():
            break
    pid = int(pidfile.read_text())
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    await asyncio.sleep(0.1)
    assert not pid_alive(pid), "процесс остался жить после отмены"


async def test_stream_lines_live():
    code = "import time\nfor i in range(3):\n    print(f'line{i}', flush=True)\n    time.sleep(0.2)"
    times = []
    lines = []
    start = now()
    async for line in stream_lines([PY, "-c", code]):
        lines.append(line)
        times.append(now() - start)
    assert lines == ["line0", "line1", "line2"]
    assert times[1] - times[0] > 0.1, "строки должны приходить по мере появления"


async def test_stream_lines_error_code():
    with pytest.raises(RuntimeError, match="exit code 5"):
        async for _ in stream_lines([PY, "-c", "print('x'); raise SystemExit(5)"]):
            pass


async def test_stream_lines_early_close_kills():
    code = "import os, time\nprint(os.getpid(), flush=True)\nwhile True:\n    print('x', flush=True); time.sleep(0.05)"
    gen = stream_lines([PY, "-c", code])
    pid = int(await anext(gen))
    await anext(gen)
    await gen.aclose()
    await asyncio.sleep(0.1)
    assert not pid_alive(pid)


async def test_run_parallel():
    cmd = [PY, "-c", "import time, sys; time.sleep(0.3); sys.exit(int(sys.argv[1]))"]
    start = now()
    codes = await run_parallel([cmd + [str(i)] for i in range(6)], limit=3)
    elapsed = now() - start
    assert codes == list(range(6))
    assert 0.55 < elapsed < 1.5, f"ожидалось 2 «волны» по 0.3 с, прошло {elapsed:.2f}"


async def test_pipe_through_large():
    data = os.urandom(1_000_000)
    code = "import sys; sys.stdout.buffer.write(sys.stdin.buffer.read()[::-1])"
    assert await pipe_through([PY, "-c", code], data) == data[::-1]


async def test_interactive_session():
    code = (
        "import sys\n"
        "for line in sys.stdin:\n"
        "    print(line.strip()[::-1].upper(), flush=True)\n"
    )
    res = await interactive_session([PY, "-u", "-c", code], ["abc", "hello", ""])
    assert res == ["CBA", "OLLEH", ""]
