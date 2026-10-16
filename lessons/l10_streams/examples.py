"""Примеры к уроку 10. Запуск: uv run python -m lessons.l10_streams.examples"""

import asyncio
import struct


async def echo_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    print(f"   [server] подключился {peer}")
    try:
        while line := await reader.readline():
            writer.write(b"echo: " + line)
            await writer.drain()
    finally:
        print(f"   [server] отключился {peer}")
        writer.close()
        await writer.wait_closed()


async def example_echo():
    print("1. Эхо-сервер и клиент")
    server = await asyncio.start_server(echo_handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    async with server:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        for msg in ["hello", "world"]:
            writer.write(msg.encode() + b"\n")
            await writer.drain()
            print("   [client]", (await reader.readline()).decode().strip())
        writer.close()
        await writer.wait_closed()
        await asyncio.sleep(0.01)


async def example_framing():
    print("2. Префикс длины")

    async def handler(reader, writer):
        header = await reader.readexactly(4)
        (size,) = struct.unpack("!I", header)
        payload = await reader.readexactly(size)
        print(f"   [server] получил {size} байт: {payload[:20]!r}...")
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    async with server:
        _, writer = await asyncio.open_connection("127.0.0.1", port)
        payload = b"x" * 100_000
        writer.write(struct.pack("!I", len(payload)))
        writer.write(payload)  # придёт частями, но readexactly соберёт
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        await asyncio.sleep(0.05)


async def main():
    await example_echo()
    await example_framing()


if __name__ == "__main__":
    asyncio.run(main())
