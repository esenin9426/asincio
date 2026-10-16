"""Примеры к уроку 18. Нужна БД: docker compose up -d --wait
Запуск: uv run python -m lessons.l18_clickhouse.examples
"""

import asyncio
import time
import uuid
from datetime import UTC, datetime, timedelta

import clickhouse_connect

from dbtest import CH_CONFIG


async def main():
    client = await clickhouse_connect.get_async_client(**CH_CONFIG)
    table = f"demo_{uuid.uuid4().hex[:8]}"
    try:
        print("1. Простые запросы")
        print("   версия:", await client.command("SELECT version()"))
        res = await client.query("SELECT number, number * number AS sq FROM numbers(3)")
        print("   колонки:", res.column_names, "строки:", res.result_rows)

        print("2. Четыре запроса sleep(0.3) конкурентно через один клиент")
        start = time.perf_counter()
        await asyncio.gather(*(client.command("SELECT sleep(0.3)") for _ in range(4)))
        print(f"   {time.perf_counter() - start:.2f} c (≈0.3)")

        print("3. Вставка пачкой и агрегат")
        await client.command(
            f"CREATE TABLE {table} (ts DateTime('UTC'), kind String, amount UInt32) "
            "ENGINE = MergeTree ORDER BY ts"
        )
        t0 = datetime(2026, 1, 1, tzinfo=UTC)
        rows = [(t0 + timedelta(minutes=i), ("click", "buy")[i % 2], i) for i in range(100_000)]
        start = time.perf_counter()
        await client.insert(table, rows, column_names=["ts", "kind", "amount"])
        print(f"   100 000 строк за {time.perf_counter() - start:.2f} c")
        res = await client.query(
            "SELECT kind, count(), sum(amount) FROM {t:Identifier} "
            "WHERE amount >= {min:UInt32} GROUP BY kind ORDER BY kind",
            parameters={"t": table, "min": 50_000},
        )
        print("   ", res.result_rows)

        print("4. Потоковое чтение блоками")
        stream = await client.query_row_block_stream(
            "SELECT number FROM numbers(25)", settings={"max_block_size": 10}
        )
        async with stream:
            print("   размеры блоков:", [len(block) async for block in stream])

        print("5. Клиентский таймаут НЕ останавливает запрос на сервере")
        qid = uuid.uuid4().hex
        try:
            async with asyncio.timeout(0.3):
                await client.query(
                    "SELECT sleep(1) FROM numbers(10) SETTINGS max_block_size = 1",
                    settings={"query_id": qid},
                )
        except TimeoutError:
            running = await client.command(
                "SELECT count() FROM system.processes WHERE query_id = {id:String}",
                parameters={"id": qid},
            )
            print("   TimeoutError; запрос всё ещё на сервере:", running == 1)
            await client.command("KILL QUERY WHERE query_id = {id:String} SYNC", parameters={"id": qid})
            print("   после KILL QUERY ... SYNC — снят")

        print("6. Серверный таймаут: max_execution_time")
        try:
            await client.query("SELECT sleep(2)", settings={"max_execution_time": 0.5})
        except clickhouse_connect.driver.exceptions.DatabaseError as e:
            print("   ", str(e).split("\n")[0][:110], "…")
    finally:
        await client.command(f"DROP TABLE IF EXISTS {table}")
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
