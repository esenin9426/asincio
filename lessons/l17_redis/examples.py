"""Примеры к уроку 17. Нужна БД: docker compose up -d --wait
Запуск: uv run python -m lessons.l17_redis.examples
"""

import asyncio
import time

import redis.asyncio as redis

from dbtest import REDIS_URL


async def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    try:
        print("1. SET/GET с TTL")
        await r.set("demo:key", "hello", ex=10)
        print("  ", await r.get("demo:key"), "| ttl:", await r.ttl("demo:key"))

        print("2. 2000 INCR: по одному vs пайплайн")
        start = time.perf_counter()
        for _ in range(2000):
            await r.incr("demo:counter")
        t1 = time.perf_counter() - start
        start = time.perf_counter()
        async with r.pipeline(transaction=False) as pipe:
            for _ in range(2000):
                pipe.incr("demo:counter")
            await pipe.execute()
        print(f"   по одному {t1:.3f} c, пайплайн {time.perf_counter() - start:.3f} c")

        print("3. Pub/Sub")
        pubsub = r.pubsub()
        await pubsub.subscribe("demo:chan")
        await r.publish("demo:chan", "привет")
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
        while msg is None:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
        print("   получено:", msg["data"])
        await pubsub.aclose()

        print("4. Streams")
        await r.delete("demo:stream")
        for i in range(3):
            await r.xadd("demo:stream", {"n": i})
        await r.xgroup_create("demo:stream", "g", id="0")
        resp = await r.xreadgroup("g", "c1", {"demo:stream": ">"}, count=10)
        for _, messages in resp:
            for msg_id, fields in messages:
                print("  ", msg_id, fields)
                await r.xack("demo:stream", "g", msg_id)
    finally:
        await r.delete("demo:key", "demo:counter", "demo:stream")
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
