# Урок 11. Транспорты и протоколы (низкоуровневая сеть)

Streams (урок 10) построены поверх этого слоя. Протоколы быстрее (нет
промежуточного буфера и корутины на каждое чтение) и используются в
высокопроизводительных серверах (uvloop, aiohttp, asyncpg).

- **Транспорт** — «как передать байты» (TCP-сокет, UDP, pipe, SSL). Его
  создаёт цикл.
- **Протокол** — «что делать с байтами». Пишете вы: класс с колбэками.
  Колбэки — **обычные методы, не корутины**. Нужна асинхронная работа —
  создавайте задачи.

## asyncio.Protocol (TCP)

```python
class Echo(asyncio.Protocol):
    def connection_made(self, transport):          # 1 раз
        self.transport = transport
    def data_received(self, data: bytes):          # 0..N раз, данные — произвольные куски!
        self.transport.write(data)
    def eof_received(self):                        # клиент сделал write_eof
        return False                               # False → транспорт закроется
    def connection_lost(self, exc):                # 1 раз; exc=None при нормальном закрытии
        ...

loop = asyncio.get_running_loop()
server = await loop.create_server(lambda: Echo(), "127.0.0.1", 0)
transport, protocol = await loop.create_connection(lambda: Client(), host, port)
```

Фабрика протокола вызывается на **каждое** соединение — передавайте общее
состояние через замыкание: `lambda: Echo(shared_state)`.

## Транспорт

| Метод | |
|---|---|
| `write(data)`, `writelines()`, `write_eof()`, `can_write_eof()` | отправка |
| `close()` | закрыть после отправки буфера; `abort()` — немедленно |
| `pause_reading()`, `resume_reading()`, `is_reading()` | **flow control на чтение** |
| `get_write_buffer_size()`, `set_write_buffer_limits(high, low)` | буфер записи |
| `get_extra_info(name)` | `peername`, `socket`, `sslcontext`... |
| `is_closing()` | |

## Flow control

- **Запись:** когда буфер записи выше high-water mark, цикл вызывает у
  протокола `pause_writing()`; когда опустится ниже low — `resume_writing()`.
  (`StreamWriter.drain()` построен ровно на этом.)
- **Чтение:** если вы не успеваете обрабатывать входящие данные — вызовите
  `transport.pause_reading()`, цикл перестанет читать сокет, буферы ОС
  заполнятся и TCP притормозит отправителя. Потом `resume_reading()`.

## UDP: DatagramProtocol

```python
class Udp(asyncio.DatagramProtocol):
    def connection_made(self, transport): self.transport = transport
    def datagram_received(self, data, addr): self.transport.sendto(data, addr)
    def error_received(self, exc): ...

transport, proto = await loop.create_datagram_endpoint(
    lambda: Udp(), local_addr=("127.0.0.1", 0))          # сервер
transport, proto = await loop.create_datagram_endpoint(
    lambda: Client(), remote_addr=("127.0.0.1", port))   # клиент: sendto(data) без addr
```

UDP не гарантирует доставку и порядок — таймауты и повторы на вас.

## BufferedProtocol

`asyncio.BufferedProtocol` — вариант без лишних копий: цикл просит у вас
буфер (`get_buffer(sizehint)`), читает в него и сообщает
`buffer_updated(nbytes)`. Для очень высоких нагрузок.

## Другие транспорты

`loop.connect_read_pipe` / `connect_write_pipe`, `loop.subprocess_exec`
(урок 12 — высокоуровневая обёртка), `loop.start_tls`, `loop.sock_*`
(`sock_recv`, `sock_sendall`, `sock_accept`, `sock_connect`) — работа
напрямую с неблокирующими сокетами.

## Задачи

Проверка: `uv run pytest lessons/l11_protocols -v`

1. `LineReverseProtocol` — буферизация и разбор строк в `data_received`.
2. `start_stats_server` — общее состояние через фабрику протоколов.
3. `protocol_client` — клиент на протоколе + Future для результата.
4. `UdpAckProtocol` и `udp_request` — UDP-сервер и клиент с таймаутом.
5. `BackpressureProtocol` — `pause_reading`/`resume_reading` + задача-обработчик.

## Вопросы для самопроверки

1. Почему методы протокола не могут быть корутинами? Как выполнить
   асинхронную работу при получении данных?
2. Гарантирует ли один `transport.write(b"hello\n")` у клиента один
   `data_received(b"hello\n")` на сервере?
3. Чем `close()` отличается от `abort()`?
4. Как устроен `StreamWriter.drain()` с точки зрения протокола?
5. Как сервер притормозит отправителя, если не успевает обрабатывать данные?

Документация: https://docs.python.org/3/library/asyncio-protocol.html
