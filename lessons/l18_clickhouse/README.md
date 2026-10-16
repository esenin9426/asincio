# Урок 18. ClickHouse из asyncio: clickhouse-connect

Подготовка: `docker compose up -d --wait` (ClickHouse, HTTP-интерфейс на
`127.0.0.1:58123`, пользователь/пароль/база — `course`). Без запущенной БД
тесты урока пропускаются (skip).

## Чем ClickHouse отличается от PostgreSQL

| | PostgreSQL | ClickHouse |
|---|---|---|
| Хранение | строки | **столбцы** |
| Нагрузка | OLTP: много мелких чтений/записей, транзакции | OLAP: агрегаты по миллионам строк |
| Вставка | по одной строке — нормально | **пачками** по тысячам строк |
| UPDATE/DELETE | обычные операции | тяжёлые «мутации», избегать |
| Транзакции | есть | нет (атомарна отдельная вставка) |

Из этого следует главный практический вывод: асинхронный код,
пишущий в ClickHouse, должен **копить строки в буфере и вставлять
пачками** (задача 2).

## Клиент

`clickhouse-connect` работает по HTTP; асинхронный клиент написан на
aiohttp и держит пул HTTP-соединений.

```python
import clickhouse_connect

client = await clickhouse_connect.get_async_client(
    host="127.0.0.1", port=58123, username="course", password="course",
    database="course",
)
print(await client.command("SELECT version()"))
await client.close()
```

- **Один клиент можно использовать из многих задач** — каждый запрос
  идёт своим HTTP-запросом. В отличие от asyncpg, пул «брать» не нужно.
- Исключение — **сессии** (`session_id`): ClickHouse не даёт выполнять
  два запроса в одной сессии одновременно (`SESSION_IS_LOCKED`).
  Асинхронный клиент по умолчанию работает без сессии. Если она нужна
  (временные таблицы, `SET`), заведите отдельный клиент на каждую
  конкурентную задачу.
- Ограничить число одновременных соединений можно параметром
  `connector_limit` или своим `Semaphore`.

## Запросы

| Метод | Для чего | Возвращает |
|---|---|---|
| `command(sql)` | DDL, `KILL`, запросы с одним значением | значение / список / `QuerySummary` |
| `query(sql)` | обычный SELECT | `QueryResult`: `.result_rows`, `.column_names`, `.first_row` |
| `query_row_block_stream(sql)` | большие выборки потоком | `StreamContext` — блоки строк |
| `query_rows_stream(sql)` | то же, по одной строке | `StreamContext` |
| `insert(table, rows, column_names=[...])` | вставка пачки | `QuerySummary` |

### Параметры

Параметры подставляет **сервер**. Тип указывается прямо в тексте запроса:

```python
await client.query(
    "SELECT user_id, sum(amount) FROM events "
    "WHERE kind = {kind:String} AND ts >= {since:DateTime} "
    "GROUP BY user_id LIMIT {n:UInt32}",
    parameters={"kind": "buy", "since": since, "n": 10},
)
```

Как и в asyncpg, **никаких f-строк** для значений. Имя таблицы тоже можно
передать параметром: `{t:Identifier}`.

### Потоковое чтение

```python
stream = await client.query_row_block_stream("SELECT ... ", settings={"max_block_size": 10_000})
async with stream:                  # закрывает HTTP-ответ даже при break/исключении
    async for block in stream:      # block — список кортежей
        ...
```

Размер блока определяет сервер: `max_block_size` — это лишь подсказка.
Если нужны пачки фиксированного размера, соберите их сами (задача 4).

### Настройки запроса

Любую настройку ClickHouse можно передать в `settings=`:
`max_execution_time` (серверный таймаут, в секундах), `query_id`,
`max_block_size`, `async_insert`, `insert_deduplication_token`…

## Время и часовые пояса

Частый источник ошибок:

- колонку объявляйте с явным поясом: `DateTime('UTC')`;
- **при вставке naive-`datetime` считается локальным временем машины
  клиента** — передавайте aware-значения (`datetime(..., tzinfo=UTC)`);
- при чтении из UTC-колонки драйвер по умолчанию отдаёт naive-`datetime`
  в UTC (`tz_mode="naive_utc"`); `tz_mode="aware"` — aware-значения.

## Отмена и таймауты

```python
async with asyncio.timeout(1):
    await client.query("SELECT ... тяжёлый запрос")
```

Отмена задачи **рвёт HTTP-соединение, но запрос на сервере продолжает
работать** и занимает CPU, пока не закончится сам. Надёжные варианты:

1. Серверный лимит: `settings={"max_execution_time": 1}` — сервер сам
   прервёт запрос с ошибкой `TIMEOUT_EXCEEDED`.
2. Свой `query_id` и `KILL QUERY WHERE query_id = {id:String} SYNC`
   после клиентского таймаута (задача 6). Посмотреть, что сейчас
   выполняется: `SELECT query_id, elapsed, query FROM system.processes`.

## Вставка: пачки, async_insert, дедупликация

- Каждый `INSERT` создаёт на диске новый «кусок» (part), куски потом
  сливаются в фоне. Тысячи мелких вставок в секунду → `TOO_MANY_PARTS`.
  Решения: буфер на клиенте (`BatchWriter`, задача 2) или серверный
  буфер — `settings={"async_insert": 1, "wait_for_async_insert": 1}`.
- После сетевой ошибки неизвестно, записалась ли пачка. Повторять
  вставку безопасно, только если она **идемпотентна**: для таблиц
  `Replicated*` (а для обычного MergeTree — с настройкой
  `non_replicated_deduplication_window`) сервер отбрасывает повтор блока
  с тем же содержимым или с тем же `insert_deduplication_token`
  (задача 7).

## Задачи

Проверка: `uv run pytest lessons/l18_clickhouse -v`

1. `insert_events` — вставка пачки одним запросом.
2. `BatchWriter` — буферизованная запись: по размеру, по таймеру, при закрытии.
3. `top_users` — агрегирующий запрос с серверными параметрами.
4. `iter_user_events` — потоковое чтение и перенарезка на пачки.
5. `dashboard` — несколько запросов конкурентно через один клиент.
6. `query_or_kill` — таймаут с отменой запроса на сервере.
7. `idempotent_insert` — повторы с дедупликацией.

## Вопросы для самопроверки

1. Почему вставлять в ClickHouse по одной строке плохо, а в PostgreSQL —
   терпимо?
2. Почему после `asyncio.timeout` запрос всё ещё виден в
   `system.processes`? Что с этим сделать?
3. Можно ли выполнять запросы конкурентно через один `AsyncClient`?
   А если у клиента задан `session_id`?
4. Что вставится, если передать `datetime(2026, 1, 1, 12, 0)` (без
   tzinfo) на машине в часовом поясе UTC+3?
5. Почему повтор вставки без токена дедупликации может задвоить данные?
6. Что потеряется при падении процесса, если в `BatchWriter` большой
   `flush_interval`? Как с этим живут в продакшене?

Документация: https://clickhouse.com/docs/integrations/python
