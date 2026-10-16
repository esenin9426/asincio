"""Урок 18 — задачи. Нужна БД: docker compose up -d --wait
Проверка: uv run pytest lessons/l18_clickhouse -v

Тесты создают таблицы по SCHEMA во временной базе (клиент уже к ней
привязан) — обращайтесь к таблице просто по имени events.
ClickHouse выполняет один SQL-оператор за запрос, поэтому SCHEMA — список.

Время: на вход — aware-datetime в UTC (naive-datetime при вставке драйвер
считает ЛОКАЛЬНЫМ временем!), на выходе драйвер отдаёт naive-datetime в UTC.
"""

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from typing import Any

from clickhouse_connect.driver.asyncclient import AsyncClient

SCHEMA = [
    """
    CREATE TABLE events (
        ts      DateTime('UTC'),
        user_id UInt32,
        kind    LowCardinality(String),
        amount  UInt32
    )
    ENGINE = MergeTree
    ORDER BY (kind, user_id, ts)
    SETTINGS non_replicated_deduplication_window = 100
    """,
]

COLUMNS = ["ts", "user_id", "kind", "amount"]

Row = tuple[datetime, int, str, int]


async def insert_events(client: AsyncClient, rows: Sequence[Row]) -> int:
    """Задача 1. Вставьте строки (ts, user_id, kind, amount) в events ОДНИМ
    вызовом client.insert (с явным column_names). Верните число строк.
    Пустой список — ничего не отправлять, вернуть 0."""
    raise NotImplementedError


class BatchWriter:
    """Задача 2. Буферизованная запись — главный паттерн вставки в ClickHouse.

    ClickHouse плохо переносит много мелких INSERT (каждый создаёт «кусок»
    на диске), поэтому строки копят в памяти и пишут пачками.

      - `await writer.add(row)` кладёт строку в буфер; как только в буфере
        `batch_size` строк — сбрасывает их одной вставкой (insert_events);
      - фоновая задача сбрасывает непустой буфер каждые `flush_interval`
        секунд, даже если пачка не набралась (её запускает `start()`);
      - `await writer.aclose()` останавливает фоновую задачу и сбрасывает
        остаток; после закрытия `add` бросает RuntimeError; повторный
        `aclose` ничего не делает;
      - сбросы не должны идти параллельно друг с другом, и ни одна строка
        не должна потеряться или записаться дважды;
      - `flushes` — число выполненных вставок (непустых);
      - класс — асинхронный контекстный менеджер: `__aenter__` вызывает
        start() и возвращает self, `__aexit__` — aclose().
    """

    def __init__(self, client: AsyncClient, batch_size: int, flush_interval: float) -> None:
        raise NotImplementedError

    def start(self) -> None:
        raise NotImplementedError

    async def add(self, row: Row) -> None:
        raise NotImplementedError

    async def flush(self) -> None:
        raise NotImplementedError

    async def aclose(self) -> None:
        raise NotImplementedError

    async def __aenter__(self) -> "BatchWriter":
        raise NotImplementedError

    async def __aexit__(self, *exc: object) -> None:
        raise NotImplementedError


async def top_users(
    client: AsyncClient, kind: str, since: datetime, limit: int
) -> list[tuple[int, int]]:
    """Задача 3. Пользователи с наибольшей суммой amount по событиям вида
    `kind` с момента `since` (включительно): список (user_id, total),
    сортировка по total убыванию, при равенстве — по user_id, не больше
    `limit` штук. Все значения — только через серверные параметры
    ({name:Type}), никаких f-строк."""
    raise NotImplementedError


async def iter_user_events(
    client: AsyncClient, user_id: int, batch_size: int
) -> AsyncIterator[list[tuple[datetime, str, int]]]:
    """Задача 4. Асинхронный генератор.

    Читайте события пользователя (ts, kind, amount) в порядке ts потоково
    (query_row_block_stream, а не query — результат может не влезть в
    память) и отдавайте пачками ровно по batch_size строк (последняя может
    быть меньше) — независимо от того, какими блоками их прислал сервер.
    Поток должен закрываться, даже если потребитель прервал итерацию.
    """
    raise NotImplementedError
    yield


async def dashboard(client: AsyncClient, queries: dict[str, str]) -> dict[str, Any]:
    """Задача 5. Выполните все запросы КОНКУРЕНТНО (каждый возвращает одно
    значение — используйте client.command) и верните {имя: значение}.
    Если хоть один упал — исключение пробрасывается, остальные отменяются."""
    raise NotImplementedError


async def query_or_kill(client: AsyncClient, sql: str, timeout: float) -> list[tuple] | None:
    """Задача 6. Выполните query(sql) и верните result_rows. Если не уложился
    в timeout — верните None.

    Подвох: отмена asyncio-задачи лишь рвёт HTTP-запрос — сам запрос
    продолжает работать на сервере. Поэтому задайте запросу свой
    уникальный query_id (через settings) и при таймауте убейте его:
    `KILL QUERY WHERE query_id = {id:String} SYNC`.
    """
    raise NotImplementedError


async def idempotent_insert(client: AsyncClient, rows: Sequence[Row], token: str) -> None:
    """Задача 7. Вставка, которую можно безопасно повторять (например, после
    сетевой ошибки, когда неизвестно, дошла ли вставка).

    Вставьте rows в events с настройкой insert_deduplication_token=token:
    повторная вставка с тем же токеном сервер проигнорирует (таблица
    создана с non_replicated_deduplication_window). При ошибке
    соединения/сервера (clickhouse_connect.driver.exceptions.OperationalError)
    повторите до 3 попыток с паузой 0.05 с, затем пробросьте исключение.
    """
    raise NotImplementedError
