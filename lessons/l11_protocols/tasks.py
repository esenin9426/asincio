"""Урок 11 — задачи. Проверка: uv run pytest lessons/l11_protocols -v

Используйте только транспорты/протоколы (loop.create_server,
loop.create_connection, loop.create_datagram_endpoint), без Streams.
"""

import asyncio
from collections.abc import Awaitable, Callable


class LineReverseProtocol(asyncio.Protocol):
    """Задача 1.

    - При подключении отправить b"READY\\n".
    - Данные приходят произвольными кусками. Для каждой полной строки
      (оканчивается b"\\n") отправить её байты в обратном порядке + b"\\n".
      Например b"abc\\n" → b"cba\\n".
    - Неполный хвост хранить в буфере до следующего data_received.
    - На EOF от клиента: если в буфере остался неполный хвост — обработать
      его как строку; затем закрыть соединение.
    """


def start_stats_server(stats: dict[str, int]) -> Awaitable[asyncio.Server]:
    """Задача 2.

    Верните корутину/awaitable, которая запускает сервер на 127.0.0.1:0
    через loop.create_server. Все соединения обновляют ОБЩИЙ словарь stats:
      stats["connections"] += 1 при подключении,
      stats["bytes"]       += len(data) на каждый кусок данных,
      stats["closed"]      += 1 при отключении.
    (Ключи изначально могут отсутствовать.) Сервер ничего не отвечает.
    Подсказка: можно просто объявить эту функцию как `async def`.
    """
    raise NotImplementedError


async def protocol_client(host: str, port: int, lines: list[str]) -> list[str]:
    """Задача 3.

    Клиент для LineReverseProtocol на loop.create_connection.
    Отправьте каждую строку из lines + "\\n", соберите ответы до тех пор,
    пока не получите по ответу на каждую строку (приветствие READY не
    считается), закройте транспорт и верните ответы без "\\n".
    Ожидание завершения — через Future, который протокол выставляет сам.
    """
    raise NotImplementedError


class UdpAckProtocol(asyncio.DatagramProtocol):
    """Задача 4a. UDP-сервер: на каждую датаграмму data отвечать
    b"ACK:" + data отправителю. Датаграммы, начинающиеся с b"DROP",
    игнорировать (имитация потери пакета)."""


async def udp_request(host: str, port: int, data: bytes, timeout: float) -> bytes:
    """Задача 4b. Отправьте одну датаграмму и дождитесь ответа не дольше
    timeout (иначе TimeoutError). Транспорт закрывается в любом случае."""
    raise NotImplementedError


class BackpressureProtocol(asyncio.Protocol):
    """Задача 5. Сервер с медленной асинхронной обработкой строк.

    - `__init__(handler, max_pending)`: handler — `async def handler(line:
      bytes) -> bytes`.
    - Каждая полная строка (без b"\\n") попадает во внутреннюю очередь.
      ОДНА фоновая задача на соединение по очереди вызывает
      `await handler(line)` и отправляет результат + b"\\n". Порядок ответов
      совпадает с порядком строк.
    - Если в очереди стало >= max_pending строк — `transport.pause_reading()`
      и `self.paused_count += 1`; когда очередь опустела до
      < max_pending // 2 — `transport.resume_reading()`.
    - При потере соединения фоновая задача отменяется.
    """

    def __init__(self, handler: Callable[[bytes], Awaitable[bytes]], max_pending: int) -> None:
        self.paused_count = 0
        raise NotImplementedError
