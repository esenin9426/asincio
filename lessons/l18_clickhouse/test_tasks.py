import asyncio
import contextlib
from datetime import UTC, datetime, timedelta

import pytest
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

from dbtest import ch_sandbox, unique
from lessons.l18_clickhouse.tasks import (
    SCHEMA,
    BatchWriter,
    dashboard,
    idempotent_insert,
    insert_events,
    iter_user_events,
    query_or_kill,
    top_users,
)

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def now():
    return asyncio.get_running_loop().time()


@contextlib.asynccontextmanager
async def sandbox(config):
    async with ch_sandbox(config) as (client, database):
        for stmt in SCHEMA:
            await client.command(stmt)
        yield client


def make_rows(n, users=10, kinds=("click", "buy")):
    return [
        (T0 + timedelta(seconds=i), i % users, kinds[i % len(kinds)], i % 100)
        for i in range(n)
    ]


async def count(client):
    return await client.command("SELECT count() FROM events")


class InsertSpy:
    """Считает вызовы client.insert."""

    def __init__(self, client):
        self.calls = []
        self._original = client.insert
        client.insert = self

    async def __call__(self, table, data, *args, **kwargs):
        self.calls.append(len(data))
        return await self._original(table, data, *args, **kwargs)


async def test_insert_events_single_call(ch_config):
    async with sandbox(ch_config) as client:
        spy = InsertSpy(client)
        rows = make_rows(10_000)
        assert await insert_events(client, rows) == 10_000
        assert spy.calls == [10_000], "нужна одна вставка всей пачкой"
        assert await count(client) == 10_000
        assert await client.command("SELECT sum(amount) FROM events") == sum(r[3] for r in rows)
        assert await insert_events(client, []) == 0
        assert spy.calls == [10_000], "пустую пачку отправлять не нужно"


async def test_batch_writer_flushes_by_size(ch_config):
    async with sandbox(ch_config) as client:
        spy = InsertSpy(client)
        writer = BatchWriter(client, batch_size=1000, flush_interval=60)
        async with writer:
            for row in make_rows(2500):
                await writer.add(row)
            assert spy.calls == [1000, 1000], "по 1000 строк, как только набралась пачка"
            assert await count(client) == 2000
        assert spy.calls == [1000, 1000, 500], "остаток сбрасывается при закрытии"
        assert writer.flushes == 3
        assert await count(client) == 2500
        with pytest.raises(RuntimeError):
            await writer.add(make_rows(1)[0])
        await writer.aclose()
        assert writer.flushes == 3


async def test_batch_writer_flushes_by_time(ch_config):
    async with sandbox(ch_config) as client:
        spy = InsertSpy(client)
        async with BatchWriter(client, batch_size=1000, flush_interval=0.1) as writer:
            for row in make_rows(10):
                await writer.add(row)
            await asyncio.sleep(0.35)
            assert await count(client) == 10, "буфер должен сброситься по таймеру"
            assert spy.calls == [10], "пустой буфер сбрасывать не нужно"
        assert writer.flushes == 1
        await asyncio.sleep(0.25)
        assert spy.calls == [10], "после закрытия фоновая задача должна остановиться"


async def test_batch_writer_concurrent_producers(ch_config):
    async with sandbox(ch_config) as client:
        rows = make_rows(5000, users=50)

        async def producer(writer, part):
            for i, row in enumerate(part):
                await writer.add(row)
                if i % 100 == 0:
                    await asyncio.sleep(0.01)

        async with BatchWriter(client, batch_size=300, flush_interval=0.05) as writer:
            async with asyncio.TaskGroup() as tg:
                for k in range(10):
                    tg.create_task(producer(writer, rows[k::10]))
        assert await count(client) == 5000, "строки потерялись или задвоились"
        assert await client.command("SELECT uniqExact(ts) FROM events") == 5000


async def test_top_users(ch_config):
    async with sandbox(ch_config) as client:
        rows = [
            (T0, 1, "buy", 10),
            (T0 + timedelta(hours=1), 1, "buy", 5),
            (T0 + timedelta(hours=1), 2, "buy", 15),
            (T0 + timedelta(hours=2), 3, "buy", 7),
            (T0 + timedelta(hours=2), 3, "click", 1000),
            (T0 - timedelta(days=1), 3, "buy", 1000),
            (T0 + timedelta(hours=3), 4, "buy", 15),
        ]
        await client.insert("events", rows, column_names=["ts", "user_id", "kind", "amount"])
        assert await top_users(client, "buy", T0, 10) == [(1, 15), (2, 15), (4, 15), (3, 7)]
        assert await top_users(client, "buy", T0, 2) == [(1, 15), (2, 15)]
        assert await top_users(client, "buy", T0 + timedelta(hours=1), 10) == [(2, 15), (4, 15), (3, 7), (1, 5)]
        assert await top_users(client, "click", T0, 10) == [(3, 1000)]
        assert await top_users(client, "x' OR '1'='1", T0, 10) == [], "SQL-инъекция!"


async def test_iter_user_events_rebatches(ch_config):
    async with sandbox(ch_config) as client:
        rows = [(T0 + timedelta(seconds=i), 7 if i % 3 else 8, "click", i) for i in range(30_000)]
        await client.insert("events", rows, column_names=["ts", "user_id", "kind", "amount"])
        expected = [(r[0].replace(tzinfo=None), r[2], r[3]) for r in rows if r[1] == 7]
        batches = [b async for b in iter_user_events(client, 7, 333)]
        assert all(len(b) == 333 for b in batches[:-1])
        assert 0 < len(batches[-1]) <= 333
        assert [tuple(e) for b in batches for e in b] == expected
        assert [b async for b in iter_user_events(client, 999, 10)] == []


async def test_iter_user_events_uses_stream(ch_config):
    async with sandbox(ch_config) as client:
        await insert_events(client, make_rows(1000, users=1))
        streamed = 0
        original = client.query_row_block_stream

        async def spy(*args, **kwargs):
            nonlocal streamed
            streamed += 1
            return await original(*args, **kwargs)

        client.query_row_block_stream = spy
        gen = iter_user_events(client, 0, 10)
        first = await anext(gen)
        await gen.aclose()
        assert len(first) == 10
        assert streamed == 1, "читать нужно потоково: query_row_block_stream"
        assert await asyncio.wait_for(client.command("SELECT 1"), 2) == 1


async def test_dashboard_concurrent(ch_config):
    async with sandbox(ch_config) as client:
        await insert_events(client, make_rows(100))
        queries = {
            "total": "SELECT (SELECT count() FROM events) + sleep(0.3)",
            "buyers": "SELECT (SELECT uniqExact(user_id) FROM events WHERE kind = 'buy') + sleep(0.3)",
            "slow": "SELECT 42 + sleep(0.3)",
            "one": "SELECT 1 + sleep(0.3)",
        }
        start = now()
        result = await dashboard(client, queries)
        elapsed = now() - start
        assert result == {"total": 100, "buyers": 5, "slow": 42, "one": 1}
        assert elapsed < 0.8, f"запросы должны идти конкурентно, прошло {elapsed:.2f} с"


async def test_dashboard_failure(ch_config):
    async with sandbox(ch_config) as client:
        start = now()
        with pytest.raises((DatabaseError, ExceptionGroup)):
            await dashboard(client, {
                "slow": "SELECT sleep(2)",
                "bad": "SELECT * FROM no_such_table",
            })
        assert now() - start < 1.5, "остальные запросы нужно отменить"


async def test_query_or_kill(ch_config):
    async with sandbox(ch_config) as client:
        assert await query_or_kill(client, "SELECT 1 + 1", 2) == [(2,)]
        marker = unique("m")
        sql = f"SELECT sleep(1) AS {marker} FROM numbers(30) SETTINGS max_block_size = 1"
        start = now()
        assert await query_or_kill(client, sql, 0.3) is None
        assert now() - start < 2.5
        running = await client.command(
            "SELECT count() FROM system.processes "
            "WHERE query LIKE {p:String} AND query NOT LIKE '%system.processes%'",
            parameters={"p": f"%{marker}%"},
        )
        assert running == 0, "запрос продолжает выполняться на сервере"
        assert await client.command("SELECT 3") == 3


async def test_idempotent_insert_dedup(ch_config):
    async with sandbox(ch_config) as client:
        rows = make_rows(100)
        await idempotent_insert(client, rows, "batch-1")
        await idempotent_insert(client, rows, "batch-1")
        assert await count(client) == 100, "повтор с тем же токеном не должен дублировать"
        await idempotent_insert(client, rows, "batch-2")
        assert await count(client) == 200


async def test_idempotent_insert_retries(ch_config):
    async with sandbox(ch_config) as client:
        original = client.insert
        attempts = 0

        async def lost_response(*args, **kwargs):
            # первая попытка доходит до сервера, но ответ «теряется»
            nonlocal attempts
            attempts += 1
            result = await original(*args, **kwargs)
            if attempts == 1:
                raise OperationalError("connection reset")
            return result

        client.insert = lost_response
        await idempotent_insert(client, make_rows(50), "tok")
        assert attempts == 2
        assert await count(client) == 50

        async def always_down(*args, **kwargs):
            nonlocal attempts
            attempts += 1
            raise OperationalError("server down")

        attempts = 0
        client.insert = always_down
        with pytest.raises(OperationalError):
            await idempotent_insert(client, make_rows(5), "tok2")
        assert attempts == 3
