"""Примеры к уроку 11. Запуск: uv run python -m lessons.l11_protocols.examples"""

import asyncio


class EchoServer(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport
        print("   [server] connection_made", transport.get_extra_info("peername"))

    def data_received(self, data):
        print(f"   [server] data_received {data!r}")
        self.transport.write(data)

    def eof_received(self):
        print("   [server] eof_received")
        return False

    def connection_lost(self, exc):
        print("   [server] connection_lost", exc)


class EchoClient(asyncio.Protocol):
    def __init__(self, message: bytes, done: asyncio.Future):
        self.message = message
        self.done = done
        self.received = b""

    def connection_made(self, transport):
        transport.write(self.message)
        transport.write_eof()

    def data_received(self, data):
        self.received += data

    def connection_lost(self, exc):
        if not self.done.done():
            self.done.set_result(self.received)


async def example_tcp():
    print("1. TCP: протоколы сервера и клиента")
    loop = asyncio.get_running_loop()
    server = await loop.create_server(EchoServer, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    async with server:
        done = loop.create_future()
        await loop.create_connection(lambda: EchoClient(b"hello", done), "127.0.0.1", port)
        print("   [client] ответ:", await done)


class UdpEcho(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        print(f"   [udp server] {data!r} от {addr}")
        self.transport.sendto(data.upper(), addr)


class UdpClient(asyncio.DatagramProtocol):
    def __init__(self, fut):
        self.fut = fut

    def datagram_received(self, data, addr):
        self.fut.set_result(data)


async def example_udp():
    print("2. UDP")
    loop = asyncio.get_running_loop()
    server_tr, _ = await loop.create_datagram_endpoint(UdpEcho, local_addr=("127.0.0.1", 0))
    port = server_tr.get_extra_info("sockname")[1]
    fut = loop.create_future()
    client_tr, _ = await loop.create_datagram_endpoint(
        lambda: UdpClient(fut), remote_addr=("127.0.0.1", port))
    client_tr.sendto(b"ping")
    print("   [udp client] ответ:", await asyncio.wait_for(fut, 1))
    client_tr.close()
    server_tr.close()


async def main():
    await example_tcp()
    await example_udp()


if __name__ == "__main__":
    asyncio.run(main())
