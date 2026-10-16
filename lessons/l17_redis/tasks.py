"""Урок 17 — задачи. Нужна БД: docker compose up -d --wait
Проверка: uv run pytest lessons/l17_redis -v

Клиенты созданы с decode_responses=True (строки, а не байты).
"""

import asyncio
from collections.abc import Awaitable, Callable

import redis.asyncio as redis


class Cache:
    """Задача 1. Cache-aside с защитой от «stampede».

    `await cache.get_or_load(key, loader)`:
      - если ключ есть в Redis — вернуть значение;
      - иначе вызвать `await loader()` (возвращает str), сохранить с TTL
        `ttl` секунд и вернуть.
    Если несколько корутин ЭТОГО процесса одновременно запросили один
    отсутствующий ключ — loader вызывается ОДИН раз, остальные ждут его
    результат (single-flight через Future/Task). Если loader упал —
    исключение получают все ожидающие, в Redis ничего не пишется,
    следующий вызов пробует снова.
    """

    def __init__(self, r: redis.Redis, ttl: int) -> None:
        raise NotImplementedError

    async def get_or_load(self, key: str, loader: Callable[[], Awaitable[str]]) -> str:
        raise NotImplementedError


async def incr_all(r: redis.Redis, keys: list[str]) -> list[int]:
    """Задача 2. Увеличьте каждый ключ на 1 ОДНИМ пайплайном (без
    транзакции) и верните новые значения по порядку."""
    raise NotImplementedError


async def allow_request(r: redis.Redis, key: str, limit: int, window_ms: int) -> bool:
    """Задача 3. Rate limiter «скользящее окно».

    Разрешить запрос, если за последние window_ms миллисекунд было
    разрешено меньше `limit` запросов. Храните метки разрешённых запросов
    в sorted set `key`. Проверка и запись — атомарно, в Lua-скрипте
    (r.register_script); отклонённые запросы НЕ записываются.
    Время берите на сервере (redis.call('TIME')). У ключа должен быть TTL,
    чтобы он не жил вечно. Члены sorted set должны быть уникальны.
    """
    raise NotImplementedError


async def transfer_points(r: redis.Redis, src: str, dst: str, amount: int) -> bool:
    """Задача 4. Перевод «баллов» между ключами-счётчиками (целые числа
    в строках; отсутствующий ключ = 0) через WATCH/MULTI/EXEC.
    Недостаточно на src — вернуть False, ничего не меняя.
    При конфликте (WatchError) — повторить. Успех — True."""
    raise NotImplementedError


async def collect_messages(
    r: redis.Redis, channel: str, count: int, ready: asyncio.Event, timeout: float
) -> list[str]:
    """Задача 5. Подпишитесь на channel, выставьте ready ПОСЛЕ того как
    подписка подтверждена, соберите data первых count сообщений (не
    дольше timeout, иначе TimeoutError). Подписку закройте в любом случае."""
    raise NotImplementedError


async def process_stream(
    r: redis.Redis,
    stream: str,
    group: str,
    n_consumers: int,
    handler: Callable[[str, dict[str, str]], Awaitable[None]],
) -> int:
    """Задача 6. Группа потребителей.

    Создайте группу (с начала потока, MKSTREAM; если группа уже есть —
    не падать). Запустите n_consumers корутин-потребителей с именами
    "consumer-0".."consumer-{n-1}". Каждая читает XREADGROUP пачками
    (count=10, block=100 мс), для каждого сообщения вызывает
    `await handler(consumer_name, fields)` и делает XACK.
    Потребитель завершается, когда чтение вернуло пустой ответ.
    Верните общее число обработанных сообщений.
    """
    raise NotImplementedError


async def reclaim_stale(
    r: redis.Redis, stream: str, group: str, consumer: str, min_idle_ms: int
) -> list[str]:
    """Задача 7. Заберите на `consumer` все сообщения группы, которые
    висят неподтверждёнными дольше min_idle_ms (XAUTOCLAIM, с учётом
    курсора — их может быть много), подтвердите их (XACK) и верните
    их id по возрастанию."""
    raise NotImplementedError


async def locked_increment(r: redis.Redis, lock_name: str, counter_key: str) -> None:
    """Задача 8. Под распределённой блокировкой (r.lock, timeout=5,
    blocking_timeout=5) выполните НЕатомарное увеличение:
    прочитать counter_key (нет ключа = 0), `await asyncio.sleep(0.001)`,
    записать значение + 1."""
    raise NotImplementedError
