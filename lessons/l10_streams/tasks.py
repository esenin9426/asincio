"""Урок 10 — задачи. Проверка: uv run pytest lessons/l10_streams -v"""

import asyncio


async def start_upper_server(host: str = "127.0.0.1", port: int = 0) -> asyncio.Server:
    """Задача 1.

    Запустите TCP-сервер (asyncio.start_server) и верните объект Server.
    Протокол построчный (UTF-8, строки оканчиваются b"\\n"):
      - на каждую строку отвечать этой же строкой в ВЕРХНЕМ регистре
        (с b"\\n" на конце);
      - на строку "QUIT" ответить "BYE\\n" и закрыть соединение;
      - при закрытии соединения клиентом — корректно закрыть writer.
    Сервер должен обслуживать много клиентов одновременно.
    """
    raise NotImplementedError


async def fetch_line(host: str, port: int, message: str) -> str:
    """Задача 2. Подключитесь, отправьте message + "\\n", прочитайте одну
    строку ответа, корректно закройте соединение (close + wait_closed)
    и верните ответ без завершающего перевода строки."""
    raise NotImplementedError


async def send_msg(writer: asyncio.StreamWriter, payload: bytes) -> None:
    """Задача 3a. Отправьте сообщение: 4 байта длины (big-endian, unsigned)
    + payload. Не забудьте про drain()."""
    raise NotImplementedError


async def recv_msg(reader: asyncio.StreamReader) -> bytes | None:
    """Задача 3b. Прочитайте одно сообщение формата из send_msg.
    Если соединение закрыто ЧИСТО (до начала заголовка) — верните None.
    Если оборвалось посреди сообщения — пробросьте IncompleteReadError.
    """
    raise NotImplementedError


class TrackedServer:
    """Задача 4. Сервер с учётом подключений.

    - `await start()` — запустить сервер на 127.0.0.1 и свободном порту;
      `port` — фактический порт.
    - Каждому клиенту при подключении отправляется "HELLO <n>\\n", где n —
      порядковый номер подключения (с 1). Дальше сервер просто читает
      и игнорирует данные до EOF.
    - `active` — число открытых сейчас соединений (уменьшается, когда
      клиент отключился).
    - `await stop()` — перестать принимать подключения, отправить всем
      открытым клиентам "SHUTDOWN\\n", закрыть их соединения, дождаться
      завершения всех обработчиков. После stop() `active == 0`, и на
      сервере не остаётся висящих задач.
    """

    def __init__(self) -> None:
        self.port: int = 0
        raise NotImplementedError

    @property
    def active(self) -> int:
        raise NotImplementedError

    async def start(self) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        raise NotImplementedError


async def request_with_timeout(host: str, port: int, payload: bytes, timeout: float) -> bytes:
    """Задача 5. Подключитесь, отправьте payload, закройте запись
    (write_eof) и прочитайте ответ до EOF. Всё целиком — не дольше timeout,
    иначе TimeoutError. Соединение закрывается в любом случае."""
    raise NotImplementedError


async def http_get(host: str, port: int, path: str) -> tuple[int, dict[str, str], bytes]:
    """Задача 6. Минимальный HTTP/1.1 GET-клиент.

    Отправьте:
        GET <path> HTTP/1.1\\r\\n
        Host: <host>:<port>\\r\\n
        Connection: close\\r\\n
        \\r\\n
    Разберите ответ: статус-строку ("HTTP/1.1 200 OK"), заголовки до пустой
    строки (имена приведите к нижнему регистру), тело: если есть
    Content-Length — ровно столько байт (readexactly), иначе — до EOF.
    Верните (status_code, headers, body).
    """
    raise NotImplementedError
