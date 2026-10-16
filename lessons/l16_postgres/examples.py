"""Примеры к уроку 16. Нужна БД: docker compose up -d --wait
Запуск: uv run python -m lessons.l16_postgres.examples
"""

import asyncio
import time

import asyncpg

from dbtest import PG_DSN


async def main():
    pool = await asyncpg.create_pool(PG_DSN, min_size=1, max_size=5)
    try:
        print("1. Простые запросы")
        print("   версия:", await pool.fetchval("SHOW server_version"))
        row = await pool.fetchrow("SELECT $1::int + $2::int AS s, now() AS t", 2, 3)
        print("   ", dict(row))

        print("2. Пять запросов pg_sleep(0.2) конкурентно через пул")
        start = time.perf_counter()
        await asyncio.gather(*(pool.execute("SELECT pg_sleep(0.2)") for _ in range(5)))
        print(f"   {time.perf_counter() - start:.2f} c (≈0.2, пул на 5 соединений)")

        print("3. Транзакция и откат")
        async with pool.acquire() as conn:
            await conn.execute("CREATE TEMP TABLE demo(x int)")
            try:
                async with conn.transaction():
                    await conn.execute("INSERT INTO demo VALUES (1)")
                    raise RuntimeError("что-то пошло не так")
            except RuntimeError:
                pass
            print("   строк после отката:", await conn.fetchval("SELECT count(*) FROM demo"))

        print("4. Таймаут отменяет запрос на сервере")
        try:
            await pool.execute("SELECT pg_sleep(10)", timeout=0.3)
        except TimeoutError:
            print("   TimeoutError, соединение живо:", await pool.fetchval("SELECT 1"))

        print("5. LISTEN/NOTIFY")
        listener = await asyncpg.connect(PG_DSN)
        got = asyncio.get_running_loop().create_future()
        await listener.add_listener("demo", lambda c, pid, ch, payload: got.set_result(payload))
        await pool.execute("SELECT pg_notify('demo', 'привет')")
        print("   получено:", await asyncio.wait_for(got, 1))
        await listener.close()
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
