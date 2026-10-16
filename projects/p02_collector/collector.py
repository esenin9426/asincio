"""Проект 2. Сборщик событий: TCP → очередь → ClickHouse.
Нужна БД: docker compose up -d --wait
Проверка: uv run pytest projects/p02_collector -v

Подробное ТЗ — в README.md рядом.
"""

import asyncio
from datetime import datetime

from clickhouse_connect.driver.asyncclient import AsyncClient

SCHEMA = [
    """
    CREATE TABLE events (
        ts      DateTime64(3, 'UTC'),
        user_id UInt32,
        kind    LowCardinality(String),
        amount  UInt32
    )
    ENGINE = MergeTree
    ORDER BY (kind, ts)
    """,
]

COLUMNS = ["ts", "user_id", "kind", "amount"]

Row = tuple[datetime, int, str, int]


# --- Часть 1. Протокол (синхронный код) ---------------------------------------

def parse_event(line: bytes) -> Row:
    """Разберите строку протокола — JSON-объект в UTF-8:

        {"ts": 1767268800.25, "user_id": 7, "kind": "click", "amount": 3}

    Верните (ts как aware-datetime в UTC, user_id, kind, amount).
    Пробелы и перевод строки по краям допустимы. Лишние поля игнорируются.
    При любой ошибке — ValueError с понятным сообщением:
      - не UTF-8, не JSON или не объект;
      - нет обязательного поля;
      - ts — не число (int/float), а bool — не число;
      - user_id — не int или вне [0, 2**32);
      - amount — не int или вне [0, 2**32);
      - kind — не строка, пустая или длиннее 32 символов.
    """
    raise NotImplementedError


# --- Часть 2. Сервер ----------------------------------------------------------

class Collector:
    """Принимает события по TCP и пачками пишет их в таблицу events.

    Протокол построчный. На каждую строку клиента сервер отвечает строкой:
      - корректное событие → b"OK\\n" — ПОСЛЕ того, как оно положено в
        очередь (подтверждённое событие обязано оказаться в БД);
      - ошибка разбора → b"ERR <сообщение>\\n", соединение продолжает работу;
      - строка длиннее max_line байт → b"ERR line too long\\n", и
        соединение закрывается;
      - b"PING" → b"PONG\\n".

    Устройство:
      - очередь asyncio.Queue(maxsize=queue_size) между соединениями и
        писателем. Если она полна, обработчик соединения ждёт в put() и
        перестаёт читать сокет — так медленная БД притормаживает клиентов
        (backpressure), а память не растёт;
      - одна задача-писатель забирает события из очереди и вставляет пачку,
        когда набралось batch_size событий ИЛИ прошло flush_interval секунд
        с момента, как в пачку попало первое событие;
      - неудачную вставку (любое Exception) писатель повторяет до
        insert_retries раз с паузой 0.05 * 2 ** k; если не вышло —
        события пачки считаются потерянными (stats["dropped"]), а писатель
        продолжает работу.

    stats — словарь со счётчиками:
      connections — открытых сейчас соединений,
      accepted    — событий подтверждено (OK),
      rejected    — строк с ERR,
      written     — событий записано в БД,
      batches     — успешных вставок,
      dropped     — событий потеряно после всех повторов.
    """

    def __init__(
        self,
        client: AsyncClient,
        *,
        batch_size: int = 1000,
        flush_interval: float = 1.0,
        queue_size: int = 10_000,
        max_line: int = 4096,
        insert_retries: int = 3,
    ) -> None:
        raise NotImplementedError

    async def start(self, host: str = "127.0.0.1", port: int = 0) -> int:
        """Запустить сервер и писателя. Вернуть фактический порт."""
        raise NotImplementedError

    async def aclose(self) -> None:
        """Корректная остановка:
        1. перестать принимать новые соединения;
        2. закрыть открытые соединения и дождаться завершения их обработчиков
           (клиенты получат EOF);
        3. записать в БД всё, что осталось в очереди и в текущей пачке;
        4. остановить писателя.
        Повторный вызов ничего не делает.
        """
        raise NotImplementedError

    async def __aenter__(self) -> "Collector":
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()
