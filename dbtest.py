"""Настройки подключения к БД и изоляция тестов (уроки 16–18, проекты).

Значения по умолчанию соответствуют docker-compose.yml; переопределяются
переменными окружения PG_DSN, REDIS_URL, CH_HOST, CH_PORT, CH_USER,
CH_PASSWORD.

Каждый тест работает в своей «песочнице»:
  - PostgreSQL — отдельная схема (search_path), удаляется после теста;
  - Redis      — уникальный префикс ключей, ключи удаляются после теста;
  - ClickHouse — отдельная база данных, удаляется после теста.
"""

import contextlib
import os
import uuid
from collections.abc import AsyncIterator

PG_DSN = os.environ.get("PG_DSN", "postgresql://course:course@127.0.0.1:55432/course")
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:56379/0")
CH_CONFIG = {
    "host": os.environ.get("CH_HOST", "127.0.0.1"),
    "port": int(os.environ.get("CH_PORT", "58123")),
    "username": os.environ.get("CH_USER", "course"),
    "password": os.environ.get("CH_PASSWORD", "course"),
}


def unique(prefix: str = "t") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


@contextlib.asynccontextmanager
async def pg_sandbox(dsn: str, schema_sql: str | None = None, **pool_kwargs) -> AsyncIterator:
    """Пул asyncpg, привязанный к временной схеме. Отдаёт (pool, dsn_with_schema)."""
    import asyncpg

    schema = unique("s")
    admin = await asyncpg.connect(dsn)
    await admin.execute(f"CREATE SCHEMA {schema}")
    pool_kwargs.setdefault("min_size", 1)
    pool_kwargs.setdefault("max_size", 10)
    pool = await asyncpg.create_pool(dsn, server_settings={"search_path": schema}, **pool_kwargs)
    try:
        if schema_sql:
            await pool.execute(schema_sql)
        yield pool, schema
    finally:
        pool.terminate()
        await admin.execute(f"DROP SCHEMA {schema} CASCADE")
        await admin.close()


@contextlib.asynccontextmanager
async def redis_sandbox(url: str) -> AsyncIterator:
    """Клиент redis.asyncio и уникальный префикс ключей. Отдаёт (client, prefix)."""
    import redis.asyncio as aioredis

    prefix = unique("t") + ":"
    client = aioredis.from_url(url, decode_responses=True)
    try:
        yield client, prefix
    finally:
        keys = [k async for k in client.scan_iter(match=prefix + "*")]
        if keys:
            await client.delete(*keys)
        await client.aclose()


@contextlib.asynccontextmanager
async def ch_sandbox(config: dict) -> AsyncIterator:
    """Асинхронный клиент clickhouse-connect на временной базе. Отдаёт (client, database)."""
    import clickhouse_connect

    database = unique("db")
    admin = await clickhouse_connect.get_async_client(**config)
    await admin.command(f"CREATE DATABASE {database}")
    client = await clickhouse_connect.get_async_client(**config, database=database)
    try:
        yield client, database
    finally:
        await client.close()
        await admin.command(f"DROP DATABASE IF EXISTS {database}")
        await admin.close()
