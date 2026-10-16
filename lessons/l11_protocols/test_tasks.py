import asyncio
import contextlib

import pytest

from lessons.l11_protocols.tasks import (
    BackpressureProtocol,
    LineReverseProtocol,
    UdpAckProtocol,
    protocol_client,
    start_stats_server,
    udp_request,
)

pytestmark = pytest.mark.timeout(5)


def port_of(server):
    return server.sockets[0].getsockname()[1]


@contextlib.asynccontextmanager
async def serving(server):
    """Как `async with server`, но гарантированно рвёт клиентские соединения,
    чтобы незаконченное решение не подвешивало wait_closed()."""
    try:
        yield server
    finally:
        server.close()
        server.abort_clients()
        await server.wait_closed()


async def reverse_server():
    loop = asyncio.get_running_loop()
    return await loop.create_server(LineReverseProtocol, "127.0.0.1", 0)


async def test_line_reverse_fragmented():
    server = await reverse_server()
    async with serving(server):
        r, w = await asyncio.open_connection("127.0.0.1", port_of(server))
        assert await r.readline() == b"READY\n"
        for chunk in [b"ab", b"c\nhel", b"lo\n", b"x\ny\n"]:
            w.write(chunk)
            await w.drain()
            await asyncio.sleep(0.01)
        lines = [await r.readline() for _ in range(4)]
        assert lines == [b"cba\n", b"olleh\n", b"x\n", b"y\n"]
        w.write(b"tail")
        w.write_eof()
        assert await asyncio.wait_for(r.read(), 1) == b"liat\n"
        w.close()


async def test_stats_server():
    stats = {}
    server = await start_stats_server(stats)
    async with serving(server):
        port = port_of(server)
        for payload in [b"12345", b"abc", b""]:
            _, w = await asyncio.open_connection("127.0.0.1", port)
            w.write(payload)
            await w.drain()
            w.close()
            await w.wait_closed()
        await asyncio.sleep(0.05)
    assert stats == {"connections": 3, "bytes": 8, "closed": 3}


async def test_protocol_client():
    server = await reverse_server()
    async with serving(server):
        res = await protocol_client("127.0.0.1", port_of(server), ["abc", "12345", "x"])
    assert res == ["cba", "54321", "x"]


async def test_protocol_client_many_lines():
    server = await reverse_server()
    lines = [f"line-{i}" * 50 for i in range(200)]
    async with serving(server):
        res = await asyncio.wait_for(protocol_client("127.0.0.1", port_of(server), lines), 3)
    assert res == [l[::-1] for l in lines]


async def test_udp():
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(UdpAckProtocol, local_addr=("127.0.0.1", 0))
    port = transport.get_extra_info("sockname")[1]
    try:
        assert await udp_request("127.0.0.1", port, b"hello", 1) == b"ACK:hello"
        results = await asyncio.gather(*(udp_request("127.0.0.1", port, f"m{i}".encode(), 1)
                                         for i in range(10)))
        assert results == [f"ACK:m{i}".encode() for i in range(10)]
        start = loop.time()
        with pytest.raises(TimeoutError):
            await udp_request("127.0.0.1", port, b"DROP me", 0.1)
        assert loop.time() - start < 0.4
    finally:
        transport.close()


async def test_backpressure_protocol():
    loop = asyncio.get_running_loop()
    protocols = []

    async def slow_upper(line: bytes) -> bytes:
        await asyncio.sleep(0.001)
        return line.upper()

    def factory():
        p = BackpressureProtocol(slow_upper, max_pending=10)
        protocols.append(p)
        return p

    server = await loop.create_server(factory, "127.0.0.1", 0)
    async with serving(server):
        r, w = await asyncio.open_connection("127.0.0.1", port_of(server))
        n = 300
        lines = [f"msg{i}".encode() for i in range(n)]

        async def writer():
            for i in range(0, n, 50):
                w.write(b"\n".join(lines[i:i + 50]) + b"\n")
                await w.drain()
                await asyncio.sleep(0)

        async def reader():
            return [(await r.readline()).rstrip(b"\n") for _ in range(n)]

        _, answers = await asyncio.gather(writer(), reader())
        assert answers == [l.upper() for l in lines]
        assert protocols[0].paused_count > 0, "чтение ни разу не приостанавливалось"
        w.close()
        await w.wait_closed()
        await asyncio.sleep(0.02)
