import asyncio
import contextlib
import json
from datetime import UTC, datetime

import pytest

from dbtest import ch_sandbox
from projects.p02_collector.collector import SCHEMA, Collector, parse_event

TS = 1767268800.25  # 2026-01-01 12:00:00.250 UTC


def now():
    return asyncio.get_running_loop().time()


def ev(i, **over):
    data = {"ts": TS + i, "user_id": i % 100, "kind": ("click", "buy")[i % 2], "amount": i % 10}
    data.update(over)
    return (json.dumps(data) + "\n").encode()


@contextlib.asynccontextmanager
async def sandbox(config):
    async with ch_sandbox(config) as (client, _):
        for stmt in SCHEMA:
            await client.command(stmt)
        yield client


async def count(client):
    return await client.command("SELECT count() FROM events")


class InsertSpy:
    def __init__(self, client, delay=0.0, fail_first=0):
        self.sizes = []
        self.delay = delay
        self.fail_first = fail_first
        self._original = client.insert
        client.insert = self

    async def __call__(self, table, data, *args, **kwargs):
        await asyncio.sleep(self.delay)
        if self.fail_first > 0:
            self.fail_first -= 1
            raise ConnectionError("clickhouse is down")
        self.sizes.append(len(data))
        return await self._original(table, data, *args, **kwargs)


@contextlib.asynccontextmanager
async def connect(port):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        yield reader, writer
    finally:
        writer.close()
        with contextlib.suppress(ConnectionError):
            await writer.wait_closed()


async def request(reader, writer, line: bytes) -> bytes:
    writer.write(line)
    await writer.drain()
    return await reader.readline()


# --- Часть 1 ------------------------------------------------------------------

def test_parse_event_ok():
    row = parse_event(b'  {"ts": 1767268800.25, "user_id": 7, "kind": "click", "amount": 3, "x": 1}\r\n')
    assert row == (datetime(2026, 1, 1, 12, 0, 0, 250000, tzinfo=UTC), 7, "click", 3)
    assert parse_event(b'{"ts": 0, "user_id": 0, "kind": "k", "amount": 4294967295}')[3] == 2**32 - 1


@pytest.mark.parametrize("line", [
    b"not json",
    b"\xff\xfe",
    b"[1, 2]",
    b'{"user_id": 1, "kind": "a", "amount": 1}',
    b'{"ts": "now", "user_id": 1, "kind": "a", "amount": 1}',
    b'{"ts": true, "user_id": 1, "kind": "a", "amount": 1}',
    b'{"ts": 1, "user_id": -1, "kind": "a", "amount": 1}',
    b'{"ts": 1, "user_id": 4294967296, "kind": "a", "amount": 1}',
    b'{"ts": 1, "user_id": 1.5, "kind": "a", "amount": 1}',
    b'{"ts": 1, "user_id": false, "kind": "a", "amount": 1}',
    b'{"ts": 1, "user_id": 1, "kind": "", "amount": 1}',
    b'{"ts": 1, "user_id": 1, "kind": 5, "amount": 1}',
    b'{"ts": 1, "user_id": 1, "kind": "' + b"k" * 33 + b'", "amount": 1}',
    b'{"ts": 1, "user_id": 1, "kind": "a", "amount": "1"}',
])
def test_parse_event_rejects(line):
    with pytest.raises(ValueError):
        parse_event(line)


# --- Часть 2 ------------------------------------------------------------------

@pytest.mark.timeout(20)
async def test_many_clients_all_written(ch_config):
    async with sandbox(ch_config) as client:
        collector = Collector(client, batch_size=500, flush_interval=0.2)
        port = await collector.start()

        async def producer(k):
            async with connect(port) as (reader, writer):
                for i in range(300):
                    writer.write(ev(k * 300 + i))
                await writer.drain()
                replies = [await reader.readline() for _ in range(300)]
                assert replies == [b"OK\n"] * 300

        await asyncio.gather(*(producer(k) for k in range(20)))
        await collector.aclose()
        assert await count(client) == 6000
        assert await client.command("SELECT uniqExact(ts) FROM events") == 6000
        assert collector.stats["accepted"] == collector.stats["written"] == 6000
        assert collector.stats["dropped"] == collector.stats["rejected"] == 0
        assert collector.stats["connections"] == 0
        first = (await client.query("SELECT ts FROM events ORDER BY ts LIMIT 1")).first_row[0]
        assert first == datetime(2026, 1, 1, 12, 0, 0, 250000)


async def test_errors_and_ping(ch_config):
    async with sandbox(ch_config) as client:
        collector = Collector(client, flush_interval=0.05, max_line=200)
        port = await collector.start()
        async with connect(port) as (reader, writer):
            assert await request(reader, writer, b"PING\n") == b"PONG\n"
            reply = await request(reader, writer, b"garbage\n")
            assert reply.startswith(b"ERR ") and reply.endswith(b"\n")
            assert await request(reader, writer, ev(1)) == b"OK\n", "после ошибки соединение живо"
            assert await request(reader, writer, b"{" + b" " * 500 + b"}\n") == b"ERR line too long\n"
            assert await reader.read() == b"", "после слишком длинной строки соединение закрывается"
        await collector.aclose()
        assert collector.stats["rejected"] == 2
        assert collector.stats["accepted"] == 1
        assert await count(client) == 1


async def test_flush_by_size_and_time(ch_config):
    async with sandbox(ch_config) as client:
        spy = InsertSpy(client)
        collector = Collector(client, batch_size=1000, flush_interval=0.3)
        port = await collector.start()
        async with connect(port) as (reader, writer):
            for i in range(2500):
                writer.write(ev(i))
            await writer.drain()
            for _ in range(2500):
                assert await reader.readline() == b"OK\n"
            await asyncio.sleep(0.1)
            assert spy.sizes == [1000, 1000], "полные пачки пишутся сразу"
            await asyncio.sleep(0.4)
            assert spy.sizes == [1000, 1000, 500], "неполная пачка — по таймеру"
            assert await count(client) == 2500
        await collector.aclose()
        assert spy.sizes == [1000, 1000, 500]
        assert collector.stats["batches"] == 3


async def test_backpressure(ch_config):
    async with sandbox(ch_config) as client:
        InsertSpy(client, delay=0.2)
        collector = Collector(client, batch_size=10, flush_interval=0.05, queue_size=10)
        port = await collector.start()
        async with connect(port) as (reader, writer):
            for i in range(100):
                writer.write(ev(i))
            await writer.drain()
            oks = 0
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(0.3):
                    while await reader.readline() == b"OK\n":
                        oks += 1
            # за 0.3 c: ~2 пачки записано + 10 в очереди + 10 в текущей пачке
            assert oks <= 50, f"очередь не ограничивает приём: подтверждено {oks} из 100"
            while oks < 100:
                assert await reader.readline() == b"OK\n"
                oks += 1
        await collector.aclose()
        assert await count(client) == 100


async def test_insert_retries(ch_config):
    async with sandbox(ch_config) as client:
        spy = InsertSpy(client, fail_first=2)
        collector = Collector(client, batch_size=50, flush_interval=0.05, insert_retries=3)
        port = await collector.start()
        async with connect(port) as (reader, writer):
            for i in range(50):
                assert await request(reader, writer, ev(i)) == b"OK\n"
        await collector.aclose()
        assert await count(client) == 50
        assert spy.sizes == [50]
        assert collector.stats["dropped"] == 0


async def test_insert_gives_up_and_continues(ch_config):
    async with sandbox(ch_config) as client:
        spy = InsertSpy(client, fail_first=3)
        collector = Collector(client, batch_size=5, flush_interval=0.05, insert_retries=2)
        port = await collector.start()
        async with connect(port) as (reader, writer):
            for i in range(5):
                assert await request(reader, writer, ev(i)) == b"OK\n"
            await asyncio.sleep(0.5)
            for i in range(5, 10):
                assert await request(reader, writer, ev(i)) == b"OK\n"
        await collector.aclose()
        assert collector.stats["dropped"] == 5
        assert collector.stats["written"] == 5
        assert await count(client) == 5
        assert spy.sizes == [5]


async def test_graceful_shutdown(ch_config):
    async with sandbox(ch_config) as client:
        collector = Collector(client, batch_size=10_000, flush_interval=60)
        port = await collector.start()
        idle = [await asyncio.open_connection("127.0.0.1", port) for _ in range(5)]
        async with connect(port) as (reader, writer):
            for i in range(20):
                assert await request(reader, writer, ev(i)) == b"OK\n"
            await asyncio.sleep(0.05)
            assert collector.stats["connections"] == 6
            assert await count(client) == 0, "пачка ещё не набралась"
            start = now()
            await collector.aclose()
            assert now() - start < 1, "aclose не должен ждать flush_interval"
            assert await count(client) == 20, "при остановке остаток записывается"
            assert await asyncio.wait_for(reader.read(), 1) == b"", "клиенты получают EOF"
        for r, w in idle:
            assert await asyncio.wait_for(r.read(), 1) == b""
            w.close()
        assert collector.stats["connections"] == 0
        with pytest.raises(OSError):
            await asyncio.open_connection("127.0.0.1", port)
        await collector.aclose()
