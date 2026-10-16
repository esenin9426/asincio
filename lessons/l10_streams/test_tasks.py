import asyncio

import pytest

from lessons.l10_streams.tasks import (
    TrackedServer,
    fetch_line,
    http_get,
    recv_msg,
    request_with_timeout,
    send_msg,
    start_upper_server,
)


def port_of(server: asyncio.Server) -> int:
    return server.sockets[0].getsockname()[1]


async def test_upper_server():
    server = await start_upper_server()
    assert isinstance(server, asyncio.Server)
    async with server:
        r, w = await asyncio.open_connection("127.0.0.1", port_of(server))
        for msg in ["hello", "Мир", "a b c"]:
            w.write(msg.encode() + b"\n")
            await w.drain()
            assert await r.readline() == msg.upper().encode() + b"\n"
        w.write(b"QUIT\n")
        await w.drain()
        assert await r.readline() == b"BYE\n"
        assert await asyncio.wait_for(r.read(), 1) == b"", "сервер должен закрыть соединение"
        w.close()
        await w.wait_closed()


async def test_upper_server_many_clients():
    server = await start_upper_server()
    async with server:
        port = port_of(server)

        async def client(i):
            r, w = await asyncio.open_connection("127.0.0.1", port)
            w.write(f"client{i}\n".encode())
            await w.drain()
            await asyncio.sleep(0.05)  # держим соединение открытым
            line = await r.readline()
            w.close()
            await w.wait_closed()
            return line

        res = await asyncio.gather(*(client(i) for i in range(20)))
        assert res == [f"CLIENT{i}\n".encode() for i in range(20)]


async def test_fetch_line():
    server = await start_upper_server()
    async with server:
        assert await fetch_line("127.0.0.1", port_of(server), "ping") == "PING"


async def test_framing_roundtrip():
    received = []
    done = asyncio.Event()

    async def handler(r, w):
        while (msg := await recv_msg(r)) is not None:
            received.append(msg)
        done.set()
        w.close()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    async with server:
        r, w = await asyncio.open_connection("127.0.0.1", port_of(server))
        msgs = [b"", b"a", b"x" * 300_000, "привет".encode()]
        for m in msgs:
            await send_msg(w, m)
        w.close()
        await w.wait_closed()
        await asyncio.wait_for(done.wait(), 2)
    assert received == msgs


async def test_recv_msg_truncated():
    reader = asyncio.StreamReader()
    reader.feed_data(b"\x00\x00\x00\x0aabc")
    reader.feed_eof()
    with pytest.raises(asyncio.IncompleteReadError):
        await recv_msg(reader)


async def test_recv_msg_clean_eof():
    reader = asyncio.StreamReader()
    reader.feed_data(b"\x00\x00\x00\x02hi")
    reader.feed_eof()
    assert await recv_msg(reader) == b"hi"
    assert await recv_msg(reader) is None


async def test_tracked_server():
    srv = TrackedServer()
    await srv.start()
    assert srv.port > 0
    clients = []
    for i in range(1, 4):
        r, w = await asyncio.open_connection("127.0.0.1", srv.port)
        assert await r.readline() == f"HELLO {i}\n".encode()
        clients.append((r, w))
    assert srv.active == 3

    r0, w0 = clients.pop(0)
    w0.close()
    await w0.wait_closed()
    await asyncio.sleep(0.05)
    assert srv.active == 2

    before = len(asyncio.all_tasks())
    await asyncio.wait_for(srv.stop(), 2)
    assert srv.active == 0
    for r, w in clients:
        assert await asyncio.wait_for(r.read(), 1) == b"SHUTDOWN\n"
        w.close()
        await w.wait_closed()
    assert len(asyncio.all_tasks()) <= before
    with pytest.raises(OSError):
        await asyncio.open_connection("127.0.0.1", srv.port)


async def test_request_with_timeout():
    async def echo_all(r, w):
        data = await r.read()
        w.write(data[::-1])
        await w.drain()
        w.close()

    async def hang(r, w):
        await asyncio.sleep(10)

    s1 = await asyncio.start_server(echo_all, "127.0.0.1", 0)
    s2 = await asyncio.start_server(hang, "127.0.0.1", 0)
    async with s1, s2:
        assert await request_with_timeout("127.0.0.1", port_of(s1), b"abc", 1) == b"cba"
        start = asyncio.get_running_loop().time()
        with pytest.raises(TimeoutError):
            await request_with_timeout("127.0.0.1", port_of(s2), b"abc", 0.1)
        assert asyncio.get_running_loop().time() - start < 0.5
        s2.close_clients()


async def fake_http(r, w):
    request = b""
    while not request.endswith(b"\r\n\r\n"):
        chunk = await r.read(1024)
        if not chunk:
            break
        request += chunk
    first = request.split(b"\r\n")[0].decode()
    path = first.split()[1]
    host_ok = b"Host: 127.0.0.1:" in request
    if path == "/hello" and host_ok:
        body = "Привет, asyncio!".encode()
        w.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                + f"Content-Length: {len(body)}\r\n\r\n".encode())
        # тело частями, чтобы проверить корректное дочитывание
        w.write(body[:5])
        await w.drain()
        await asyncio.sleep(0.02)
        w.write(body[5:] + b"EXTRA-GARBAGE")
    elif path == "/eof":
        w.write(b"HTTP/1.1 200 OK\r\nX-Custom: Yes\r\n\r\nuntil-eof")
    else:
        w.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
    await w.drain()
    w.close()


async def test_http_get():
    server = await asyncio.start_server(fake_http, "127.0.0.1", 0)
    async with server:
        port = port_of(server)
        status, headers, body = await http_get("127.0.0.1", port, "/hello")
        assert status == 200
        assert headers["content-type"] == "text/plain"
        assert body.decode() == "Привет, asyncio!"

        status, headers, body = await http_get("127.0.0.1", port, "/eof")
        assert (status, headers["x-custom"], body) == (200, "Yes", b"until-eof")

        status, _, body = await http_get("127.0.0.1", port, "/missing")
        assert (status, body) == (404, b"")
