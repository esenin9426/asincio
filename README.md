# Курс по asyncio

Пошаговый практический курс по асинхронному программированию на Python:
от первой корутины до собственного цикла событий и работы с PostgreSQL,
Redis и ClickHouse. Каждый урок — это теория, запускаемые примеры и набор
задач, которые проверяются автотестами.

## Цель

- Понять, **как устроен** asyncio: корутины, задачи, фьючерсы, цикл событий.
- Научиться писать **корректный** конкурентный код: отмена, таймауты,
  структурированная конкурентность, синхронизация, отсутствие «висящих»
  задач и блокировок цикла.
- Освоить сетевой и системный слой: streams, транспорты/протоколы,
  подпроцессы, потоки и процессы.
- Применить всё это к реальным хранилищам: asyncpg, redis.asyncio,
  clickhouse-connect.

Код пишется без готовых async-фреймворков — только стандартная библиотека
и драйверы БД, чтобы было видно, что происходит под капотом.

## Требования

- Python **3.13+**
- [uv](https://docs.astral.sh/uv/) — менеджер окружения и зависимостей
- Docker + Docker Compose — только для уроков 16–18 и проекта 2 (базы данных)

## Быстрый старт

```bash
# 1. Установить зависимости (создаст .venv)
uv sync

# 2. Прочитать теорию урока
open lessons/l01_coroutines/README.md      # или любым редактором

# 3. Запустить примеры урока
uv run python -m lessons.l01_coroutines.examples

# 4. Решить задачи в lessons/l01_coroutines/tasks.py
#    (заменить `raise NotImplementedError` на реализацию)

# 5. Проверить решение
uv run pytest lessons/l01_coroutines -v
```

## Как устроен урок

```
lessons/lNN_<тема>/
├── README.md      # теория, шпаргалка по API, подводные камни, вопросы для самопроверки
├── examples.py    # готовые примеры: uv run python -m lessons.lNN_<тема>.examples
├── tasks.py       # задачи: функции/классы с описанием в docstring и NotImplementedError
└── test_tasks.py  # тесты, проверяющие решения
```

Порядок работы:

1. Прочитать `README.md` урока.
2. Запустить и поковырять `examples.py`.
3. По очереди реализовать задачи в `tasks.py` — условие каждой написано в
   её docstring («Задача 1», «Задача 2», …).
4. Прогнать тесты урока; добиться, чтобы всё было зелёным.
5. Ответить себе на «Вопросы для самопроверки» в конце `README.md`.

Отдельную задачу можно проверить через `-k`:

```bash
uv run pytest lessons/l03_combinators -v -k fetch_all
```

## Программа курса

| №  | Урок | Темы | Задачи |
|----|------|------|--------|
| 01 | [Корутины и цикл событий](lessons/l01_coroutines/README.md) | `async def`/`await`, awaitable-объекты, `asyncio.run`, `Runner` | `greet`, `sequential`, `countdown`, `Delayed`, `run_sync` |
| 02 | [Задачи (Task)](lessons/l02_tasks/README.md) | `create_task`, имена, состояние задач, фоновые задачи | `run_concurrently`, `spawn_named`, `describe`, `BackgroundTasks`, `first_done_name` |
| 03 | [gather, wait, as_completed, таймауты](lessons/l03_combinators/README.md) | комбинаторы ожидания, `timeout`, `wait_for`, дедлайны | `fetch_all`, `fetch_all_safe`, `first_completed`, `in_completion_order`, `with_timeout`, `deadline_batch`, `staged_deadline` |
| 04 | [Отмена](lessons/l04_cancellation/README.md) | `cancel`, `CancelledError`, `shield`, graceful shutdown | `cancellable_worker`, `cancel_and_wait`, `protected_save`, `cancel_reason`, `graceful_shutdown`, `run_ignoring_cancel_for` |
| 05 | [TaskGroup и ExceptionGroup](lessons/l05_taskgroup/README.md) | структурированная конкурентность, `except*` | `process_all`, `collect_errors`, `group_with_timeout`, `run_isolated`, `walk_tree`, `search_first` |
| 06 | [Примитивы синхронизации](lessons/l06_sync/README.md) | `Lock`, `Semaphore`, `Event`, `Condition`, `Barrier` | `SafeCounter`, `limited_gather`, `Latch`, `BoundedBuffer`, `phased_workers`, `ConnectionPool`, `RWLock` |
| 07 | [Очереди](lessons/l07_queues/README.md) | `Queue`, `PriorityQueue`, producer/consumer, конвейеры | `producer_consumer`, `priority_order`, `pipeline`, `WorkerPool` |
| 08 | [Future и низкоуровневый API цикла](lessons/l08_futures_loop/README.md) | `Future`, `call_soon`/`call_later`, связь с потоками | `callback_to_awaitable`, `delayed_calls`, `Debouncer`, `collect_from_thread`, `call_async_from_thread`, `await_concurrent` |
| 09 | [Блокирующий код](lessons/l09_blocking/README.md) | `to_thread`, `run_in_executor`, процессы, debug-режим | `run_blocking_concurrently`, `count_primes_parallel`, `heartbeat_while`, `bounded_to_thread`, `to_thread_with_timeout`, `find_blocking` |
| 10 | [Streams: TCP](lessons/l10_streams/README.md) | `open_connection`, `start_server`, фрейминг, `drain` | `start_upper_server`, `fetch_line`, `send_msg`/`recv_msg`, `TrackedServer`, `request_with_timeout`, `http_get` |
| 11 | [Транспорты и протоколы](lessons/l11_protocols/README.md) | `Protocol`, `DatagramProtocol`, UDP, backpressure | `LineReverseProtocol`, `start_stats_server`, `protocol_client`, `UdpAckProtocol`, `udp_request`, `BackpressureProtocol` |
| 12 | [Подпроцессы](lessons/l12_subprocess/README.md) | `create_subprocess_exec`, пайпы, таймауты | `run_command`, `run_with_timeout`, `stream_lines`, `run_parallel`, `pipe_through`, `interactive_session` |
| 13 | [Асинхронные итераторы, генераторы, контекстные менеджеры](lessons/l13_async_iter_ctx/README.md) | `__aiter__`/`__anext__`, async-генераторы, `asynccontextmanager`, `AsyncExitStack` | `Countdown`, `aticker`, `amap`, `afilter`, `atake`, `Timer`, `open_all`, `merge`, `abatch` |
| 14 | [Продвинутое](lessons/l14_advanced/README.md) | `contextvars`, обработчик ошибок цикла, сигналы, eager-задачи, фабрики задач | `handle_request`, `run_in_context`, `capture_callback_errors`, `wait_for_signal`, `run_eager`, `install_task_counter` |
| 15 | [Под капотом: свой цикл событий](lessons/l15_under_the_hood/README.md) | корутина как генератор, планировщик, I/O-готовность | `Sleep`, `ReadReady`, `MiniTask`, `MiniLoop` |
| 16 | [PostgreSQL: asyncpg](lessons/l16_postgres/README.md) | пул, транзакции, `COPY`, курсоры, `LISTEN/NOTIFY` | `create_accounts`, `get_balance`, `transfer`, `bulk_insert_events`, `iter_events`, `wait_notifications`, `query_with_timeout`, `run_limited` |
| 17 | [Redis: redis.asyncio](lessons/l17_redis/README.md) | кэш, pipeline, rate limiting, pub/sub, streams, блокировки | `Cache`, `incr_all`, `allow_request`, `transfer_points`, `collect_messages`, `process_stream`, `reclaim_stale`, `locked_increment` |
| 18 | [ClickHouse: clickhouse-connect](lessons/l18_clickhouse/README.md) | вставка пачками, серверные параметры, потоковое чтение, `KILL QUERY`, дедупликация | `insert_events`, `BatchWriter`, `top_users`, `iter_user_events`, `dashboard`, `query_or_kill`, `idempotent_insert` |

## Итоговые проекты

Проекты в `projects/` объединяют темы нескольких уроков. Устроены так же,
как уроки: `README.md` с ТЗ, модуль с заготовками (`NotImplementedError`)
и тесты.

| № | Проект | Темы | БД |
|---|--------|------|----|
| 1 | [Асинхронный веб-краулер](projects/p01_crawler/README.md) | aiohttp, таймауты и повторы с backoff, лимиты на хост, обход по уровням в `TaskGroup`, отмена | не нужна |
| 2 | [Сборщик событий: TCP → очередь → ClickHouse](projects/p02_collector/README.md) | streams-сервер, backpressure, писатель с пачками и таймером, повторы, graceful shutdown | ClickHouse |

```bash
uv run pytest projects/p01_crawler -v
uv run pytest projects/p02_collector -v
```

## Базы данных (уроки 16–18, проект 2)

Сервисы описаны в `docker-compose.yml`:

| Сервис     | Образ                              | Адрес              | Логин / пароль / БД |
|------------|------------------------------------|--------------------|---------------------|
| PostgreSQL | `postgres:17`                      | `127.0.0.1:55432`  | `course` / `course` / `course` |
| Redis      | `redis:7.4`                        | `127.0.0.1:56379`  | — |
| ClickHouse | `clickhouse/clickhouse-server:25.3`| `127.0.0.1:58123` (HTTP) | `course` / `course` / `course` |

Порты нестандартные, чтобы не конфликтовать с локально установленными
серверами.

```bash
docker compose up -d --wait   # запустить и дождаться healthcheck
docker compose down           # остановить
docker compose down -v        # остановить и удалить данные
```

Если база не запущена, соответствующие тесты **пропускаются** (skip) с
подсказкой, а не падают.

Адреса можно переопределить переменными окружения: `PG_DSN`, `REDIS_URL`,
`CH_HOST`, `CH_PORT`, `CH_USER`, `CH_PASSWORD`.

## Тестовая инфраструктура

- **`conftest.py`** — запускает `async def test_...` через `asyncio.run()`
  без сторонних плагинов (pytest-asyncio не нужен). У каждого async-теста
  есть лимит времени: 10 секунд по умолчанию, свой — через
  `@pytest.mark.timeout(N)`. Если решение зависло или работает
  последовательно вместо конкурентного, тест падает с понятным
  сообщением. На случай полного зависания цикла включён `faulthandler`,
  который завершит процесс с дампом стеков.
- **`dbtest.py`** — настройки подключения и «песочницы» для изоляции
  тестов друг от друга:
  - PostgreSQL — временная схема (`search_path`), удаляется после теста;
  - Redis — уникальный префикс ключей, ключи удаляются после теста;
  - ClickHouse — временная база данных, удаляется после теста.
- Фикстуры `pg_dsn`, `redis_url`, `ch_config` проверяют доступность
  сервиса и выдают параметры подключения.

## Полезные команды

```bash
uv run pytest                          # все тесты курса
uv run pytest lessons/l05_taskgroup    # тесты одного урока
uv run pytest -x --lf                  # только упавшие в прошлый раз, до первой ошибки
uv run python -m lessons.l15_under_the_hood.examples   # примеры урока
```

## Структура репозитория

```
.
├── README.md            # этот файл
├── pyproject.toml       # метаданные проекта, dev-зависимости, настройки pytest
├── uv.lock              # зафиксированные версии зависимостей
├── docker-compose.yml   # PostgreSQL, Redis, ClickHouse для уроков 16–18
├── conftest.py          # запуск async-тестов, таймауты, фикстуры БД
├── dbtest.py            # параметры подключения и песочницы для тестов с БД
├── lessons/             # уроки 01–18
└── projects/            # итоговые проекты
```

## Документация

- [asyncio — официальная документация](https://docs.python.org/3/library/asyncio.html)
- [asyncpg](https://magicstack.github.io/asyncpg/)
- [redis-py asyncio](https://redis.readthedocs.io/en/stable/examples/asyncio_examples.html)
- [clickhouse-connect](https://clickhouse.com/docs/integrations/python)
