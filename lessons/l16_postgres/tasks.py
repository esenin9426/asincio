"""Урок 16 — задачи. Нужна БД: docker compose up -d --wait
Проверка: uv run pytest lessons/l16_postgres -v

Таблицы создаются тестами по SCHEMA_SQL во временной схеме (search_path
уже настроен) — обращайтесь к ним просто по именам accounts / events.
"""

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import asyncpg

SCHEMA_SQL = """
CREATE TABLE accounts (
    id      serial PRIMARY KEY,
    owner   text   NOT NULL UNIQUE,
    balance bigint NOT NULL CHECK (balance >= 0)
);
CREATE TABLE events (
    id         bigserial   PRIMARY KEY,
    account_id int         NOT NULL,
    kind       text        NOT NULL,
    amount     bigint      NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
"""


class InsufficientFunds(Exception):
    pass


async def create_accounts(pool: asyncpg.Pool, balances: dict[str, int]) -> dict[str, int]:
    """Задача 1.

    Создайте счета {owner: начальный баланс} ОДНИМ запросом
    (INSERT ... SELECT * FROM unnest($1::text[], $2::bigint[]) RETURNING ...)
    и верните {owner: id}.
    """
    raise NotImplementedError


async def get_balance(pool: asyncpg.Pool, owner: str) -> int | None:
    """Задача 2. Баланс владельца или None, если счёта нет.
    Только параметризованный запрос ($1)!"""
    raise NotImplementedError


async def transfer(pool: asyncpg.Pool, src_id: int, dst_id: int, amount: int) -> None:
    """Задача 3.

    В одной транзакции:
      - заблокировать ОБЕ строки счетов (SELECT ... FOR UPDATE) в
        фиксированном порядке (по id), чтобы конкурентные встречные
        переводы не давали дедлок;
      - если на src недостаточно средств — InsufficientFunds (транзакция
        откатывается, ничего не меняется);
      - списать с src, зачислить на dst;
      - записать в events две строки: (src, 'debit', amount) и
        (dst, 'credit', amount).
    """
    raise NotImplementedError


async def bulk_insert_events(pool: asyncpg.Pool, rows: list[tuple[int, str, int]]) -> int:
    """Задача 4. Загрузите строки (account_id, kind, amount) в events
    через COPY (copy_records_to_table). Верните число загруженных строк."""
    raise NotImplementedError


async def iter_events(
    pool: asyncpg.Pool, account_id: int, batch_size: int
) -> AsyncIterator[list[dict[str, Any]]]:
    """Задача 5. Асинхронный генератор.

    Читайте события счёта (ORDER BY id) серверным курсором внутри
    транзакции и отдавайте пачками по batch_size (последняя может быть
    меньше) — списками dict с ключами id, kind, amount.
    Соединение должно вернуться в пул, даже если потребитель прервал
    итерацию.
    """
    raise NotImplementedError
    yield


async def wait_notifications(
    dsn: str, channel: str, count: int, ready: asyncio.Event, timeout: float
) -> list[str]:
    """Задача 6.

    Откройте ОТДЕЛЬНОЕ соединение, подпишитесь на channel (add_listener),
    выставьте ready, соберите payload-ы первых `count` уведомлений
    (не дольше timeout, иначе TimeoutError). Обязательно снимите
    слушатель и закройте соединение.
    """
    raise NotImplementedError


async def query_with_timeout(pool: asyncpg.Pool, sql: str, timeout: float) -> list[Any] | None:
    """Задача 7. Выполните fetch(sql). Если не уложился в timeout —
    верните None. Запрос на сервере должен быть отменён, а пул остаться
    рабочим."""
    raise NotImplementedError


async def run_limited(pool: asyncpg.Pool, queries: list[str], limit: int) -> list[Any]:
    """Задача 8. Выполните запросы (fetchval) конкурентно, но не более
    `limit` одновременно (размер пула может быть больше). Результаты —
    по порядку."""
    raise NotImplementedError
