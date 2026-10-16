# Урок 17. Redis из asyncio: redis.asyncio

Подготовка: `docker compose up -d --wait` (Redis на `127.0.0.1:56379`).

## Клиент

Пакет `redis` (redis-py) содержит асинхронный клиент `redis.asyncio`
(бывший aioredis).

```python
import redis.asyncio as redis

r = redis.from_url("redis://127.0.0.1:56379/0", decode_responses=True)
await r.set("key", "value", ex=60)       # TTL 60 c
print(await r.get("key"))
await r.aclose()
```

- Клиент держит **пул соединений** и безопасен для конкурентного
  использования из многих задач.
- `decode_responses=True` — получать `str` вместо `bytes`.
- Блокирующие команды (`BLPOP`, `XREAD BLOCK`, pub/sub) занимают
  соединение — пул должен быть достаточно большим (`max_connections`).

## Пайплайны и транзакции

Каждая команда — сетевой round trip. **Пайплайн** отправляет пачку
команд за один раз:

```python
async with r.pipeline(transaction=False) as pipe:
    for k in keys:
        pipe.incr(k)
    results = await pipe.execute()
```

`transaction=True` (по умолчанию) оборачивает пачку в `MULTI/EXEC` —
атомарно. Оптимистическая блокировка — `WATCH`:

```python
async with r.pipeline() as pipe:
    while True:
        try:
            await pipe.watch("balance")             # немедленный режим
            bal = int(await pipe.get("balance"))
            pipe.multi()                            # буферный режим
            pipe.set("balance", bal - 10)
            await pipe.execute()                    # WatchError, если ключ изменили
            break
        except redis.WatchError:
            continue
```

## Lua-скрипты

Атомарная логика на стороне сервера, один round trip:

```python
script = r.register_script("return redis.call('INCRBY', KEYS[1], ARGV[1])")
await script(keys=["counter"], args=[5])
```

## Pub/Sub

```python
pubsub = r.pubsub()
await pubsub.subscribe("news")
async for msg in pubsub.listen():         # {"type": "message", "channel": ..., "data": ...}
    ...
# или: await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
await pubsub.aclose()
```

Pub/Sub — «fire-and-forget»: кто не подписан в момент публикации, сообщение
не получит. Для надёжной доставки — Streams.

## Streams и группы потребителей

```python
await r.xadd("events", {"type": "click"})
await r.xgroup_create("events", "workers", id="0", mkstream=True)
resp = await r.xreadgroup("workers", "worker-1", {"events": ">"}, count=10, block=1000)
# resp: [[stream, [(id, fields), ...]]]
await r.xack("events", "workers", msg_id)
await r.xautoclaim("events", "workers", "worker-2", min_idle_time=30000)  # забрать «зависшие»
```

Сообщение, выданное потребителю, остаётся в PEL (pending entries list),
пока его не подтвердят `XACK`. Упавший потребитель → сообщения можно
переназначить через `XAUTOCLAIM`/`XCLAIM`.

## Распределённая блокировка

```python
async with r.lock("resource", timeout=10, blocking_timeout=5):
    ...
```

Это `SET NX PX` + Lua для безопасного снятия. `timeout` — защита от
вечной блокировки упавшим процессом.

## Задачи

Проверка: `uv run pytest lessons/l17_redis -v`

Все ключи в тестах начинаются с уникального префикса — функции получают
полные имена ключей.

1. `Cache` — cache-aside с TTL и защитой от «stampede» (single-flight).
2. `incr_all` — пайплайн.
3. `allow_request` — rate limiter «скользящее окно» на Lua.
4. `transfer_points` — WATCH/MULTI с повторами.
5. `collect_messages` — pub/sub.
6. `process_stream` — группа потребителей Streams.
7. `reclaim_stale` — `XAUTOCLAIM` зависших сообщений.
8. `locked_increment` — распределённая блокировка.

## Вопросы для самопроверки

1. Почему 1000 команд в пайплайне быстрее 1000 отдельных `await`?
2. Чем `MULTI/EXEC` отличается от Lua-скрипта?
3. Что произойдёт с сообщением Streams, если потребитель упал до `XACK`?
4. Почему pub/sub не подходит для очереди задач?
5. Что такое cache stampede и как его предотвратить внутри одного процесса?

Документация: https://redis.readthedocs.io/en/stable/examples/asyncio_examples.html
