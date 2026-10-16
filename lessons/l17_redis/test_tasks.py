import asyncio
import random

import pytest
import redis.asyncio as redis

from dbtest import redis_sandbox
from lessons.l17_redis.tasks import (
    Cache,
    allow_request,
    collect_messages,
    incr_all,
    locked_increment,
    process_stream,
    reclaim_stale,
    transfer_points,
)


async def test_cache_single_flight(redis_url):
    async with redis_sandbox(redis_url) as (r, p):
        calls = 0

        async def loader():
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.05)
            return "value"

        cache = Cache(r, ttl=30)
        results = await asyncio.gather(*(cache.get_or_load(p + "k", loader) for _ in range(20)))
        assert results == ["value"] * 20
        assert calls == 1, f"loader вызван {calls} раз"
        assert 0 < await r.ttl(p + "k") <= 30
        assert await cache.get_or_load(p + "k", loader) == "value"
        assert calls == 1


async def test_cache_failure_not_cached(redis_url):
    async with redis_sandbox(redis_url) as (r, p):
        attempts = 0

        async def flaky():
            nonlocal attempts
            attempts += 1
            await asyncio.sleep(0.01)
            if attempts == 1:
                raise ConnectionError("backend down")
            return "ok"

        cache = Cache(r, ttl=30)
        res = await asyncio.gather(*(cache.get_or_load(p + "k", flaky) for _ in range(5)),
                                   return_exceptions=True)
        assert all(isinstance(x, ConnectionError) for x in res)
        assert await r.exists(p + "k") == 0
        assert await cache.get_or_load(p + "k", flaky) == "ok"
        assert attempts == 2


async def test_incr_all_uses_pipeline(redis_url):
    async with redis_sandbox(redis_url) as (r, p):
        keys = [f"{p}c{i % 50}" for i in range(500)]
        direct_calls = 0
        original = r.execute_command

        async def spy(*args, **kwargs):
            nonlocal direct_calls
            direct_calls += 1
            return await original(*args, **kwargs)

        r.execute_command = spy
        res = await incr_all(r, keys)
        r.execute_command = original
        assert res == [i // 50 + 1 for i in range(500)]
        assert direct_calls == 0, "команды должны идти через pipeline, а не по одной"


async def test_allow_request_exact_under_concurrency(redis_url):
    async with redis_sandbox(redis_url) as (r, p):
        key = p + "rl"
        res = await asyncio.gather(*(allow_request(r, key, 5, 300) for _ in range(30)))
        assert sum(res) == 5
        assert await r.zcard(key) == 5, "отклонённые запросы не должны записываться"
        assert 0 < await r.pttl(key) <= 300 + 50
        assert not await allow_request(r, key, 5, 300)
        await asyncio.sleep(0.35)
        assert await allow_request(r, key, 5, 300), "окно должно сдвинуться"


async def test_transfer_points(redis_url):
    async with redis_sandbox(redis_url) as (r, p):
        await r.set(p + "a", 100)
        assert await transfer_points(r, p + "a", p + "b", 40) is True
        assert (await r.get(p + "a"), await r.get(p + "b")) == ("60", "40")
        assert await transfer_points(r, p + "b", p + "a", 41) is False
        assert await r.get(p + "b") == "40"


async def test_transfer_points_concurrent(redis_url):
    async with redis_sandbox(redis_url) as (r, p):
        accounts = [f"{p}acc{i}" for i in range(4)]
        for a in accounts:
            await r.set(a, 100)
        rnd = random.Random(1)
        moves = [(*rnd.sample(accounts, 2), rnd.randint(1, 60)) for _ in range(100)]
        await asyncio.gather(*(transfer_points(r, *m) for m in moves))
        values = [int(await r.get(a)) for a in accounts]
        assert sum(values) == 400
        assert min(values) >= 0


async def test_collect_messages(redis_url):
    async with redis_sandbox(redis_url) as (r, p):
        ready = asyncio.Event()
        task = asyncio.create_task(collect_messages(r, p + "chan", 3, ready, 5))
        await asyncio.wait_for(ready.wait(), 5)
        for m in ["a", "б", "c", "d"]:
            await r.publish(p + "chan", m)
        assert await task == ["a", "б", "c"]
        with pytest.raises(TimeoutError):
            await collect_messages(r, p + "silent", 1, asyncio.Event(), 0.2)


@pytest.mark.timeout(20)
async def test_process_stream_exactly_once(redis_url):
    async with redis_sandbox(redis_url) as (r, p):
        stream = p + "stream"
        async with r.pipeline(transaction=False) as pipe:
            for i in range(300):
                pipe.xadd(stream, {"n": str(i)})
            await pipe.execute()
        seen = []
        consumers = set()

        async def handler(consumer, fields):
            consumers.add(consumer)
            seen.append(int(fields["n"]))
            await asyncio.sleep(0.001)

        assert await process_stream(r, stream, "g", 4, handler) == 300
        assert sorted(seen) == list(range(300)), "каждое сообщение ровно один раз"
        assert len(consumers) >= 2, "работать должны несколько потребителей"
        assert (await r.xpending(stream, "g"))["pending"] == 0
        # повторный запуск: группа уже есть, новых сообщений нет
        assert await process_stream(r, stream, "g", 2, handler) == 0


async def test_reclaim_stale(redis_url):
    async with redis_sandbox(redis_url) as (r, p):
        stream = p + "s"
        ids = [await r.xadd(stream, {"n": str(i)}) for i in range(150)]
        await r.xgroup_create(stream, "g", id="0")
        await r.xreadgroup("g", "dead-worker", {stream: ">"}, count=1000)
        assert await reclaim_stale(r, stream, "g", "rescuer", 10_000) == []
        await asyncio.sleep(0.06)
        assert await reclaim_stale(r, stream, "g", "rescuer", 50) == ids
        assert (await r.xpending(stream, "g"))["pending"] == 0


async def test_locked_increment_across_clients(redis_url):
    async with redis_sandbox(redis_url) as (r, p):
        clients = [redis.from_url(redis_url, decode_responses=True) for _ in range(5)]
        try:
            await asyncio.gather(*(
                locked_increment(clients[i % 5], p + "lock", p + "counter") for i in range(50)
            ))
        finally:
            for c in clients:
                await c.aclose()
        assert await r.get(p + "counter") == "50"
        assert await r.exists(p + "lock") == 0
