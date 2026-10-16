import asyncio
import random

import asyncpg
import pytest

from dbtest import pg_sandbox
from lessons.l16_postgres.tasks import (
    SCHEMA_SQL,
    InsufficientFunds,
    bulk_insert_events,
    create_accounts,
    get_balance,
    iter_events,
    query_with_timeout,
    run_limited,
    transfer,
    wait_notifications,
)


def now():
    return asyncio.get_running_loop().time()


async def test_create_accounts_and_balance(pg_dsn):
    async with pg_sandbox(pg_dsn, SCHEMA_SQL) as (pool, _):
        ids = await create_accounts(pool, {"alice": 100, "bob": 50, "o'hara": 7})
        assert set(ids) == {"alice", "bob", "o'hara"}
        rows = await pool.fetch("SELECT id, owner, balance FROM accounts")
        assert {r["owner"]: (r["id"], r["balance"]) for r in rows} == {
            o: (ids[o], b) for o, b in {"alice": 100, "bob": 50, "o'hara": 7}.items()
        }
        assert await get_balance(pool, "alice") == 100
        assert await get_balance(pool, "o'hara") == 7
        assert await get_balance(pool, "nobody") is None
        assert await get_balance(pool, "x' OR '1'='1") is None, "SQL-инъекция!"


async def test_transfer_basic(pg_dsn):
    async with pg_sandbox(pg_dsn, SCHEMA_SQL) as (pool, _):
        ids = await create_accounts(pool, {"a": 100, "b": 0})
        await transfer(pool, ids["a"], ids["b"], 30)
        assert await get_balance(pool, "a") == 70
        assert await get_balance(pool, "b") == 30
        events = await pool.fetch("SELECT account_id, kind, amount FROM events ORDER BY id")
        assert [tuple(e) for e in events] == [(ids["a"], "debit", 30), (ids["b"], "credit", 30)]

        with pytest.raises(InsufficientFunds):
            await transfer(pool, ids["b"], ids["a"], 31)
        assert await get_balance(pool, "b") == 30
        assert await pool.fetchval("SELECT count(*) FROM events") == 2


@pytest.mark.timeout(30)
async def test_transfer_concurrent_no_deadlocks(pg_dsn):
    async with pg_sandbox(pg_dsn, SCHEMA_SQL, max_size=10) as (pool, _):
        ids = list((await create_accounts(pool, {f"u{i}": 1000 for i in range(5)})).values())
        rnd = random.Random(42)
        pairs = []
        for _ in range(200):
            a, b = rnd.sample(ids, 2)
            pairs.append((a, b, rnd.randint(1, 300)))

        async def safe_transfer(a, b, amt):
            try:
                await transfer(pool, a, b, amt)
                return True
            except InsufficientFunds:
                return False

        results = await asyncio.gather(*(safe_transfer(*p) for p in pairs))
        assert await pool.fetchval("SELECT sum(balance) FROM accounts") == 5000
        assert await pool.fetchval("SELECT min(balance) FROM accounts") >= 0
        assert await pool.fetchval("SELECT count(*) FROM events") == 2 * sum(results)


async def test_bulk_insert_events_copy(pg_dsn):
    async with pg_sandbox(pg_dsn, SCHEMA_SQL) as (pool, _):
        rows = [(i % 10, "credit" if i % 2 else "debit", i) for i in range(100_000)]
        start = now()
        assert await bulk_insert_events(pool, rows) == 100_000
        assert now() - start < 5
        assert await pool.fetchval("SELECT sum(amount) FROM events") == sum(range(100_000))


async def test_iter_events_batches(pg_dsn):
    async with pg_sandbox(pg_dsn, SCHEMA_SQL, max_size=2) as (pool, _):
        await bulk_insert_events(pool, [(1, "credit", i) for i in range(25)] + [(2, "debit", 1)] * 5)
        batches = [b async for b in iter_events(pool, 1, 10)]
        assert [len(b) for b in batches] == [10, 10, 5]
        flat = [e for b in batches for e in b]
        assert [e["amount"] for e in flat] == list(range(25))
        assert set(flat[0]) == {"id", "kind", "amount"}


async def test_iter_events_releases_connection_on_break(pg_dsn):
    async with pg_sandbox(pg_dsn, SCHEMA_SQL, min_size=1, max_size=1) as (pool, _):
        await bulk_insert_events(pool, [(1, "credit", i) for i in range(100)])
        gen = iter_events(pool, 1, 10)
        await anext(gen)
        await gen.aclose()
        # пул из одного соединения: если оно не вернулось — здесь зависнем
        assert await asyncio.wait_for(pool.fetchval("SELECT 1"), 2) == 1


async def test_wait_notifications(pg_dsn):
    async with pg_sandbox(pg_dsn) as (pool, schema):
        channel = f"ch_{schema}"
        ready = asyncio.Event()
        task = asyncio.create_task(wait_notifications(pg_dsn, channel, 3, ready, 5))
        await asyncio.wait_for(ready.wait(), 5)
        for msg in ["one", "два", "three", "four"]:
            await pool.execute("SELECT pg_notify($1, $2)", channel, msg)
        assert await task == ["one", "два", "three"]


async def test_wait_notifications_timeout(pg_dsn):
    async with pg_sandbox(pg_dsn) as (_, schema):
        ready = asyncio.Event()
        with pytest.raises(TimeoutError):
            await wait_notifications(pg_dsn, f"silent_{schema}", 1, ready, 0.2)
        assert ready.is_set()


async def test_query_with_timeout_cancels_on_server(pg_dsn):
    async with pg_sandbox(pg_dsn, max_size=2) as (pool, schema):
        await pool.execute(f"SET application_name = '{schema}'")
        assert await query_with_timeout(pool, "SELECT 1 AS x", 1) is not None
        start = now()
        assert await query_with_timeout(pool, "SELECT pg_sleep(30)", 0.2) is None
        assert now() - start < 2
        await asyncio.sleep(0.2)
        active = await pool.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE query = 'SELECT pg_sleep(30)' AND state = 'active'"
        )
        assert active == 0, "запрос продолжает выполняться на сервере"
        assert await pool.fetchval("SELECT 2") == 2


async def test_run_limited(pg_dsn):
    async with pg_sandbox(pg_dsn, max_size=10) as (pool, _):
        queries = [f"SELECT {i} FROM pg_sleep(0.2)" for i in range(6)]
        start = now()
        assert await run_limited(pool, queries, 3) == list(range(6))
        elapsed = now() - start
        assert 0.38 <= elapsed < 0.8, f"ожидались 2 волны по 0.2 с, прошло {elapsed:.2f}"
