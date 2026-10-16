# Урок 16. PostgreSQL из asyncio: asyncpg

Подготовка: `docker compose up -d --wait` (PostgreSQL на `127.0.0.1:55432`,
пользователь/пароль/база — `course`). Без запущенной БД тесты урока
пропускаются (skip).

## Драйверы

| Библиотека | Особенности |
|---|---|
| **asyncpg** | самый быстрый, бинарный протокол, свой API (`$1`-параметры). Используем в курсе |
| psycopg 3 (`psycopg.AsyncConnection`) | DB-API-подобный, `%s`-параметры, sync и async в одном пакете |
| SQLAlchemy 2.0 async | ORM/Core поверх asyncpg/psycopg: `create_async_engine`, `AsyncSession` |

## Соединение и пул

```python
import asyncpg

conn = await asyncpg.connect("postgresql://course:course@127.0.0.1:55432/course")
await conn.close()

pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
async with pool.acquire() as conn:      # взять соединение и вернуть в пул
    ...
await pool.fetch("SELECT 1")            # сокращения: pool.fetch/fetchrow/fetchval/execute
await pool.close()
```

**Одно соединение = одна операция за раз.** Два конкурентных запроса через
одно соединение → `InterfaceError: another operation is in progress`.
Конкурентность — через пул: каждая задача берёт своё соединение. Размер
пула ограничивает параллельность запросов к БД (лишние задачи ждут в
`acquire()` — это естественный семафор).

## Запросы

| Метод | Возвращает |
|---|---|
| `execute(sql, *args)` | статус-строку (`"INSERT 0 1"`) |
| `executemany(sql, args_list)` | — (батч в одной транзакции) |
| `fetch(sql, *args)` | `list[Record]` |
| `fetchrow(sql, *args)` | `Record \| None` |
| `fetchval(sql, *args, column=0)` | одно значение |

- Параметры — `$1, $2, …`; **никогда** не подставляйте значения через
  f-строки (SQL-инъекции). Имена таблиц/колонок параметризовать нельзя.
- `Record` ведёт себя как кортеж и как словарь: `r["id"]`, `r[0]`, `dict(r)`.
- Массивы Python ↔ массивы PostgreSQL: вставка пачки одним запросом —
  `INSERT ... SELECT * FROM unnest($1::text[], $2::int[]) RETURNING id`.
- Для каждого запроса asyncpg готовит и кэширует prepared statement
  (`statement_cache_size`). С PgBouncer в transaction-режиме кэш нужно
  отключать: `statement_cache_size=0`.

## Транзакции

```python
async with conn.transaction():                       # BEGIN ... COMMIT / ROLLBACK при исключении
    await conn.execute("UPDATE ...")
    async with conn.transaction():                   # вложенная → SAVEPOINT
        ...
async with conn.transaction(isolation="serializable"):
    ...
```

- Конкурентные изменения одних строк → блокировки. `SELECT ... FOR UPDATE`
  блокирует строки до конца транзакции.
- **Дедлоки**: две транзакции блокируют строки A,B и B,A. Лечится
  захватом блокировок в одном порядке (например, `ORDER BY id`).
- `serializable` может кидать `SerializationError` — транзакцию нужно
  повторить.

## Курсоры, COPY, LISTEN/NOTIFY

```python
async with conn.transaction():                       # курсор живёт только в транзакции
    async for record in conn.cursor("SELECT * FROM big", prefetch=500):
        ...
    cur = await conn.cursor("SELECT ...")
    batch = await cur.fetch(1000)

await conn.copy_records_to_table("events", records=rows, columns=[...])  # COPY — в разы быстрее INSERT

await conn.add_listener("channel", callback)         # callback(conn, pid, channel, payload) — обычная функция!
await conn.execute("NOTIFY channel, 'hello'")        # или SELECT pg_notify($1, $2)
await conn.remove_listener("channel", callback)
```

LISTEN требует **выделенного** соединения, живущего всё время прослушивания
(не возвращайте его в пул).

## Таймауты и отмена

- `await conn.fetch(sql, timeout=1.0)` — по таймауту asyncpg отправляет
  серверу **отмену запроса** и бросает `TimeoutError`; соединение
  остаётся рабочим.
- Отмена задачи (`task.cancel()`) во время запроса делает то же самое.
- `command_timeout=` в `connect/create_pool` — таймаут по умолчанию.
- Серверный предохранитель: `SET statement_timeout = '5s'`.

## Задачи

Проверка: `uv run pytest lessons/l16_postgres -v`

Схема (`SCHEMA_SQL` в `tasks.py`) создаётся тестами во временной схеме.

1. `create_accounts` — пакетная вставка одним запросом через `unnest`.
2. `get_balance` — параметризованный запрос.
3. `transfer` — транзакция, `FOR UPDATE`, защита от дедлоков.
4. `bulk_insert_events` — `COPY`.
5. `iter_events` — серверный курсор, выдача пачками.
6. `wait_notifications` — LISTEN/NOTIFY.
7. `query_with_timeout` — таймаут с отменой запроса на сервере.
8. `run_limited` — конкурентные запросы с ограничением.

## Вопросы для самопроверки

1. Почему нельзя выполнять два запроса одновременно через одно соединение?
2. Что произойдёт с запросом на сервере, если отменить задачу, которая его ждёт?
3. Как избежать дедлока при переводе денег между двумя счетами?
4. Почему LISTEN нельзя делать на соединении из пула «на один запрос»?
5. Чем COPY выгоднее `executemany` для больших объёмов?

Документация: https://magicstack.github.io/asyncpg/current/
