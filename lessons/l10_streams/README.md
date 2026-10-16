# Урок 10. Streams: TCP-клиенты и серверы

Streams — высокоуровневый сетевой API asyncio поверх транспортов/протоколов
(урок 11). Пара объектов: `StreamReader` (чтение) и `StreamWriter` (запись).

## Клиент

```python
reader, writer = await asyncio.open_connection("example.com", 80)
writer.write(b"GET / HTTP/1.0\r\nHost: example.com\r\n\r\n")
await writer.drain()                  # дождаться, пока буфер отправки разгрузится
data = await reader.read()            # до EOF
writer.close()
await writer.wait_closed()
```

Параметры: `ssl=` (TLS), `limit=` (размер буфера строки, по умолчанию 64 КиБ),
`local_addr=`, `happy_eyeballs_delay=`. Для Unix-сокетов —
`open_unix_connection` / `start_unix_server`.

## Сервер

```python
async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    try:
        while line := await reader.readline():
            writer.write(line.upper())
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()

server = await asyncio.start_server(handle, "127.0.0.1", 0)  # 0 — любой свободный порт
port = server.sockets[0].getsockname()[1]
async with server:
    await server.serve_forever()
```

На каждое подключение создаётся **отдельная задача** с `handle`.

## StreamReader

| Метод | Поведение |
|---|---|
| `read(n=-1)` | до `n` байт (что есть); `-1` — до EOF; `b""` — EOF |
| `readline()` | до `\n` включительно; на EOF — остаток или `b""` |
| `readexactly(n)` | ровно `n` байт, иначе `IncompleteReadError` (`.partial`) |
| `readuntil(sep=b"\n")` | до разделителя; `LimitOverrunError` при превышении `limit` |
| `at_eof()` | буфер пуст и получен EOF |

## StreamWriter

| Метод | |
|---|---|
| `write(data)`, `writelines(list)` | **не** корутины: кладут в буфер |
| `await drain()` | ждать, пока буфер ниже «высокой отметки» — **flow control**, без него можно забить память |
| `write_eof()` | полузакрытие (отправить FIN) |
| `close()` + `await wait_closed()` | закрыть соединение |
| `is_closing()` | |
| `get_extra_info("peername" / "sockname" / "socket" / "ssl_object")` | |
| `await start_tls(ssl_context)` (3.11+) | апгрейд соединения до TLS |

## Server

`server.close()` — перестать принимать новые соединения;
`await server.wait_closed()` — дождаться закрытия; `server.sockets`;
`server.is_serving()`; `async with server:`.
В 3.13 добавлены `server.close_clients()` и `server.abort_clients()`.

## Протоколы поверх TCP — framing

TCP — поток байтов без границ сообщений. Один `write` может прийти частями,
несколько — склеенными. Способы разметки:
1. Разделитель (`\n`) → `readline` / `readuntil`.
2. Префикс длины (`struct.pack("!I", len(payload))`) → `readexactly`.
3. Заголовки с длиной (HTTP `Content-Length`).

## Задачи

Проверка: `uv run pytest lessons/l10_streams -v`

1. `start_upper_server` — построчный сервер с командой `QUIT`.
2. `fetch_line` — клиент, корректно закрывающий соединение.
3. `send_msg` / `recv_msg` — framing с префиксом длины.
4. `TrackedServer` — учёт подключений и корректная остановка.
5. `request_with_timeout` — таймаут на сетевой запрос.
6. `http_get` — минимальный HTTP/1.1-клиент на сырых сокетах.

## Вопросы для самопроверки

1. Почему `write()` — не корутина, а `drain()` — корутина?
2. Что вернёт `readline()` при закрытии соединения?
3. Почему нельзя считать, что один `read(1024)` получит одно сообщение?
4. Что делают `server.close()` и `wait_closed()`? Закрывают ли они
   существующие соединения?
5. Как узнать порт, если сервер запущен на порту 0?

Документация: https://docs.python.org/3/library/asyncio-stream.html
