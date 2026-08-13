"""M1 transport surface: streams, protocols, backpressure, readiness,
sock_*, signals, TLS (sslproto path)."""

import asyncio
import os
import signal
import socket
import sys

import pytest

import cadeloop


@pytest.fixture()
def loop():
    lp = cadeloop.new_event_loop()
    asyncio.set_event_loop(lp)
    yield lp
    asyncio.set_event_loop(None)
    if not lp.is_closed():
        lp.close()


async def _echo_server(loop):
    async def handler(reader, writer):
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
        writer.close()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()


# --------------------------------------------------------------------- #
# streams                                                               #
# --------------------------------------------------------------------- #


def test_streams_echo_roundtrip(loop):
    async def main():
        server, addr = await _echo_server(loop)
        reader, writer = await asyncio.open_connection(*addr)
        for size in (1, 100, 4096, 70000):
            payload = os.urandom(size)
            writer.write(payload)
            await writer.drain()
            got = await reader.readexactly(size)
            assert got == payload
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_large_transfer_10mb(loop):
    """Exercises partial writes, gather flush, and backpressure."""

    async def main():
        server, addr = await _echo_server(loop)
        reader, writer = await asyncio.open_connection(*addr)
        chunk = os.urandom(256 * 1024)
        total = 40  # 10 MiB
        received = bytearray()

        async def drain_reader():
            while len(received) < total * len(chunk):
                received.extend(await reader.read(1 << 20))

        consumer = loop.create_task(drain_reader())
        for _ in range(total):
            writer.write(chunk)
            await writer.drain()
        await consumer
        assert len(received) == total * len(chunk)
        assert bytes(received) == chunk * total
        writer.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_many_concurrent_connections(loop):
    async def main():
        server, addr = await _echo_server(loop)

        async def one(i):
            reader, writer = await asyncio.open_connection(*addr)
            msg = f"conn-{i}".encode() * 50
            writer.write(msg)
            await writer.drain()
            assert await reader.readexactly(len(msg)) == msg
            writer.close()
            await writer.wait_closed()

        await asyncio.gather(*[one(i) for i in range(50)])
        assert loop.stats()["connections_accepted"] == 50
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


# --------------------------------------------------------------------- #
# protocol-level semantics                                              #
# --------------------------------------------------------------------- #


class Recorder(asyncio.Protocol):
    def __init__(self, done):
        self.events = []
        self.transport = None
        self.done = done

    def connection_made(self, transport):
        self.transport = transport
        self.events.append("made")

    def data_received(self, data):
        self.events.append(("data", data))

    def eof_received(self):
        self.events.append("eof")
        return False  # let transport close

    def connection_lost(self, exc):
        self.events.append(("lost", exc))
        if not self.done.done():
            self.done.set_result(None)


def test_protocol_event_ordering_and_half_close(loop):
    async def main():
        done = loop.create_future()
        server_protos = []

        def server_factory():
            p = Recorder(done)
            server_protos.append(p)
            return p

        server = await loop.create_server(server_factory, "127.0.0.1", 0)
        addr = server.sockets[0].getsockname()

        reader, writer = await asyncio.open_connection(*addr)
        writer.write(b"payload")
        writer.write_eof()  # half-close: server sees data then EOF
        await writer.drain()
        await done
        p = server_protos[0]
        assert p.events[0] == "made"
        payload = b"".join(e[1] for e in p.events if isinstance(e, tuple) and e[0] == "data")
        assert payload == b"payload"
        assert "eof" in p.events
        assert p.events[-1][0] == "lost" and p.events[-1][1] is None
        writer.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


class BufferedRecorder(asyncio.BufferedProtocol):
    def __init__(self, done):
        self.buf = bytearray(1 << 16)
        self.received = bytearray()
        self.done = done
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def get_buffer(self, sizehint):
        return memoryview(self.buf)

    def buffer_updated(self, nbytes):
        self.received.extend(self.buf[:nbytes])

    def eof_received(self):
        return False

    def connection_lost(self, exc):
        if not self.done.done():
            self.done.set_result(bytes(self.received))


def test_buffered_protocol_receive_path(loop):
    async def main():
        done = loop.create_future()
        server = await loop.create_server(lambda: BufferedRecorder(done), "127.0.0.1", 0)
        addr = server.sockets[0].getsockname()
        payload = os.urandom(200_000)
        reader, writer = await asyncio.open_connection(*addr)
        writer.write(payload)
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        received = await done
        assert received == payload
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_write_backpressure_pause_resume(loop):
    class SlowConsumer(asyncio.Protocol):
        def connection_made(self, transport):
            transport.pause_reading()  # do not consume; fill peer's buffers
            self.transport = transport

    class Producer(asyncio.Protocol):
        def __init__(self):
            self.paused = False
            self.pause_count = 0
            self.resume_count = 0

        def connection_made(self, transport):
            self.transport = transport

        def pause_writing(self):
            self.paused = True
            self.pause_count += 1

        def resume_writing(self):
            self.paused = False
            self.resume_count += 1

        def connection_lost(self, exc):
            pass

    async def main():
        consumers = []

        def consumer_factory():
            p = SlowConsumer()
            consumers.append(p)
            return p

        server = await loop.create_server(consumer_factory, "127.0.0.1", 0)
        addr = server.sockets[0].getsockname()
        transport, producer = await loop.create_connection(Producer, *addr)
        transport.set_write_buffer_limits(high=32 * 1024, low=8 * 1024)

        chunk = b"x" * 8192
        while not producer.paused:
            transport.write(chunk)
        assert producer.pause_count == 1
        assert transport.get_write_buffer_size() > 32 * 1024

        # Let the consumer drain: resume must follow.
        consumers[0].transport.resume_reading()
        for _ in range(400):
            if producer.resume_count:
                break
            await asyncio.sleep(0.01)
        assert producer.resume_count == 1
        transport.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_abort_delivers_connection_lost(loop):
    async def main():
        done = loop.create_future()
        server = await loop.create_server(lambda: Recorder(done), "127.0.0.1", 0)
        addr = server.sockets[0].getsockname()
        transport, _proto = await loop.create_connection(
            lambda: Recorder(loop.create_future()), *addr
        )
        assert not transport.is_closing()
        transport.abort()
        assert transport.is_closing()
        await done  # server side saw the close
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_get_extra_info(loop):
    async def main():
        server, addr = await _echo_server(loop)
        transport, _ = await loop.create_connection(
            lambda: Recorder(loop.create_future()), *addr
        )
        peer = transport.get_extra_info("peername")
        name = transport.get_extra_info("sockname")
        assert peer == tuple(addr)
        assert name[0] == "127.0.0.1" and name[1] > 0
        assert transport.get_extra_info("nope", "dflt") == "dflt"
        transport.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_connection_refused(loop):
    async def main():
        # Grab a port that is certainly closed.
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()
        with pytest.raises(ConnectionRefusedError):
            await loop.create_connection(asyncio.Protocol, "127.0.0.1", port)

    loop.run_until_complete(main())


def test_server_start_serving_deferred(loop):
    async def main():
        connected = []

        server = await loop.create_server(
            lambda: Recorder(loop.create_future()), "127.0.0.1", 0, start_serving=False
        )
        addr = server.sockets[0].getsockname()
        assert not server.is_serving()

        # Port is bound but not accepting into protocols yet; a connect
        # completes at TCP level (backlog) without a protocol being made.
        s = socket.create_connection(addr, timeout=2)
        await asyncio.sleep(0.05)
        assert not connected
        await server.start_serving()
        assert server.is_serving()
        s.sendall(b"x")
        await asyncio.sleep(0.1)
        s.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


# --------------------------------------------------------------------- #
# readiness + sock_* + signals                                          #
# --------------------------------------------------------------------- #


def test_add_reader_writer_socketpair(loop):
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    got = []

    def on_readable():
        try:
            got.append(a.recv(100))
        except BlockingIOError:
            return
        loop.remove_reader(a.fileno())
        loop.stop()

    loop.add_reader(a.fileno(), on_readable)
    loop.call_later(0.02, b.send, b"ping")
    loop.call_later(2, loop.stop)  # safety
    loop.run_forever()
    assert got == [b"ping"]

    writable = []
    loop.add_writer(a.fileno(), lambda: (writable.append(True), loop.stop()))
    loop.call_later(2, loop.stop)
    loop.run_forever()
    assert writable
    assert loop.remove_writer(a.fileno()) is True
    assert loop.remove_writer(a.fileno()) is False
    a.close()
    b.close()


def test_sock_functions(loop):
    async def main():
        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(8)
        listener.setblocking(False)
        addr = listener.getsockname()

        client = socket.socket()
        client.setblocking(False)
        await loop.sock_connect(client, addr)
        server_side, _peer = await loop.sock_accept(listener)

        await loop.sock_sendall(client, b"abcdef" * 1000)
        received = bytearray()
        while len(received) < 6000:
            received.extend(await loop.sock_recv(server_side, 65536))
        assert bytes(received) == b"abcdef" * 1000

        buf = bytearray(16)
        await loop.sock_sendall(server_side, b"into-buffer!")
        n = await loop.sock_recv_into(client, buf)
        assert buf[:n] == b"into-buffer!"

        for s in (client, server_side, listener):
            s.close()

    loop.run_until_complete(main())


def test_sock_sendfile_fallback(loop, tmp_path):
    async def main():
        payload = os.urandom(150_000)
        f = tmp_path / "blob.bin"
        f.write_bytes(payload)

        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.setblocking(False)
        addr = listener.getsockname()
        client = socket.socket()
        client.setblocking(False)
        await loop.sock_connect(client, addr)
        server_side, _ = await loop.sock_accept(listener)

        with open(f, "rb") as fh:
            sent = await loop.sock_sendfile(client, fh)
        assert sent == len(payload)
        received = bytearray()
        while len(received) < len(payload):
            received.extend(await loop.sock_recv(server_side, 65536))
        assert bytes(received) == payload
        for s in (client, server_side, listener):
            s.close()

    loop.run_until_complete(main())


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signal path")
def test_signal_handler(loop):
    hits = []

    def on_signal():
        hits.append(1)
        loop.stop()

    loop.add_signal_handler(signal.SIGUSR1, on_signal)
    loop.call_later(0.05, os.kill, os.getpid(), signal.SIGUSR1)
    loop.call_later(5, loop.stop)  # safety
    loop.run_forever()
    assert hits == [1]
    assert loop.remove_signal_handler(signal.SIGUSR1) is True
    assert loop.remove_signal_handler(signal.SIGUSR1) is False


# --------------------------------------------------------------------- #
# TLS via sslproto (R-059 compatibility path)                           #
# --------------------------------------------------------------------- #


def test_tls_echo(loop):
    trustme = pytest.importorskip("trustme")
    import ssl

    ca = trustme.CA()
    server_cert = ca.issue_cert("localhost", "127.0.0.1")

    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_cert.configure_cert(server_ctx)
    client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ca.configure_trust(client_ctx)

    async def main():
        async def handler(reader, writer):
            data = await reader.readexactly(11)
            writer.write(data[::-1])
            await writer.drain()
            writer.close()

        server = await asyncio.start_server(handler, "127.0.0.1", 0, ssl=server_ctx)
        addr = server.sockets[0].getsockname()
        reader, writer = await asyncio.open_connection(
            addr[0], addr[1], ssl=client_ctx, server_hostname="localhost"
        )
        assert writer.get_extra_info("sslcontext") is client_ctx
        writer.write(b"hello world")
        await writer.drain()
        assert await reader.readexactly(11) == b"dlrow olleh"
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_loop_close_with_live_connections():
    """R-122: close() with pending ops cancels and drains cleanly."""
    loop = cadeloop.new_event_loop()

    async def main():
        server, addr = await _echo_server(loop)
        reader, writer = await asyncio.open_connection(*addr)
        writer.write(b"in flight")
        return server, writer

    server, writer = loop.run_until_complete(main())
    assert loop.stats()["connections"] > 0
    loop.close()  # must not hang or leak
    assert loop.is_closed()
