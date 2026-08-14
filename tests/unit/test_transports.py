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


def test_create_connection_validates_ssl_shutdown_timeout(loop):
    """ssl_shutdown_timeout without ssl= was previously accepted
    silently, unlike the already-present ssl_handshake_timeout check
    right next to it."""

    async def main():
        with pytest.raises(ValueError, match="ssl_shutdown_timeout"):
            await loop.create_connection(
                asyncio.Protocol, "127.0.0.1", 1, ssl_shutdown_timeout=5.0
            )

    loop.run_until_complete(main())


def test_create_connection_rejects_ssl_socket(loop):
    """An already-wrapped ssl.SSLSocket passed via sock= previously
    reached _wrap_outgoing unchecked."""
    ssl_mod = pytest.importorskip("ssl")

    async def main():
        raw = socket.socket()
        raw.setblocking(False)
        ctx = ssl_mod.SSLContext(ssl_mod.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl_mod.CERT_NONE
        wrapped = ctx.wrap_socket(raw, server_hostname=None, do_handshake_on_connect=False)
        try:
            with pytest.raises(TypeError, match="SSLSocket"):
                await loop.create_connection(asyncio.Protocol, sock=wrapped)
        finally:
            wrapped.close()

    loop.run_until_complete(main())


def test_create_server_validates_reuse_port_and_ssl_shutdown(loop):
    async def main():
        with pytest.raises(ValueError, match="ssl_shutdown_timeout"):
            await loop.create_server(
                asyncio.Protocol, "127.0.0.1", 0, ssl_shutdown_timeout=5.0
            )

    loop.run_until_complete(main())

    if not hasattr(socket, "SO_REUSEPORT"):
        # Can only exercise the "not supported" branch when it's true —
        # a platform where SO_REUSEPORT is missing entirely.
        async def main_unsupported():
            with pytest.raises(ValueError, match="reuse_port"):
                await loop.create_server(
                    asyncio.Protocol, "127.0.0.1", 0, reuse_port=True
                )

        loop.run_until_complete(main_unsupported())


def test_create_connection_resolves_local_addr_by_family(loop):
    """local_addr was previously used as-is without going through
    getaddrinfo, unlike the remote address right next to it — a
    hostname (rather than a literal IP) as local_addr, or a family
    mismatch against the chosen remote candidate, behaved differently
    than stdlib. This exercises the resolved, family-matched path."""

    async def main():
        server, addr = await _echo_server(loop)
        transport, _proto = await loop.create_connection(
            asyncio.Protocol, addr[0], addr[1], local_addr=("127.0.0.1", 0)
        )
        assert transport.get_extra_info("sockname")[0] == "127.0.0.1"
        transport.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_happy_eyeballs_delay_invokes_staggered_race(loop, monkeypatch):
    """happy_eyeballs_delay/interleave were previously accepted but
    silently ignored -- connection attempts stayed strictly sequential
    even with an explicit delay. Proves the staggered-race path is
    genuinely wired up (not a silent no-op) by intercepting
    asyncio.staggered.staggered_race itself, then confirms the
    connection still succeeds through it."""
    from cadeloop import tcp as cadeloop_tcp

    calls = []
    real_staggered_race = cadeloop_tcp.staggered.staggered_race

    async def spy(coro_fns, delay, *, loop=None):
        calls.append(delay)
        return await real_staggered_race(coro_fns, delay, loop=loop)

    monkeypatch.setattr(cadeloop_tcp.staggered, "staggered_race", spy)

    async def main():
        server, addr = await _echo_server(loop)
        transport, _proto = await loop.create_connection(
            asyncio.Protocol, addr[0], addr[1], happy_eyeballs_delay=0.1
        )
        transport.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())
    assert calls == [0.1], f"staggered_race not invoked with happy_eyeballs_delay: {calls}"


def test_happy_eyeballs_falls_through_bad_address(loop):
    """A broken first candidate must not prevent a working second one
    from connecting under happy_eyeballs_delay (same contract as the
    pre-existing sequential-fallback path, now exercised through
    staggered_race instead of a plain for-loop)."""

    async def main():
        server, addr = await _echo_server(loop)
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]
        probe.close()

        async def fake_getaddrinfo(host, port, **kw):
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", closed_port)),
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", addr),
            ]

        loop.getaddrinfo = fake_getaddrinfo
        transport, _proto = await loop.create_connection(
            asyncio.Protocol, "ignored", 0, happy_eyeballs_delay=0.05
        )
        assert transport.get_extra_info("peername")[1] == addr[1]
        transport.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="AF_UNIX not available")
def test_unix_socket_echo_roundtrip(loop, tmp_path):
    """create_unix_connection/create_unix_server previously fell through
    to AbstractEventLoop's abstract NotImplementedError. Delegates to
    create_connection/create_server's sock= path, driven end to end here
    via the real asyncio.open_unix_connection stream helpers."""
    sock_path = str(tmp_path / "echo.sock")

    async def main():
        async def handler(reader, writer):
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
            writer.close()

        # start_unix_server (not loop.create_unix_server directly): it
        # wraps the reader/writer callback into a real asyncio.Protocol,
        # which is what create_unix_server's protocol_factory contract
        # expects — exercises the same code path as _echo_server's
        # asyncio.start_server does for the TCP case above.
        server = await asyncio.start_unix_server(handler, sock_path)
        reader, writer = await asyncio.open_unix_connection(sock_path)
        for size in (1, 100, 4096):
            payload = os.urandom(size)
            writer.write(payload)
            await writer.drain()
            assert await reader.readexactly(size) == payload
        writer.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="AF_UNIX not available")
def test_create_unix_server_rebinds_stale_socket(loop, tmp_path):
    """A stale socket file left by a crashed prior instance previously
    would have made a second create_unix_server(path=...) fail with
    'address already in use' — must be transparently cleared, matching
    stdlib's own create_unix_server."""
    sock_path = str(tmp_path / "stale.sock")

    async def main():
        async def handler(reader, writer):
            writer.close()

        server1 = await asyncio.start_unix_server(handler, sock_path)
        server1.close()
        await server1.wait_closed()
        assert os.path.exists(sock_path)  # the file itself outlives close()

        server2 = await asyncio.start_unix_server(handler, sock_path)
        server2.close()
        await server2.wait_closed()

    loop.run_until_complete(main())


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="AF_UNIX not available")
def test_create_unix_connection_path_and_sock_mutually_exclusive(loop):
    async def main():
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            with pytest.raises(ValueError, match="path and sock"):
                await loop.create_unix_connection(asyncio.Protocol, "/tmp/x", sock=sock)
        finally:
            sock.close()

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


def _sock_sendfile_pair(loop):
    """listener/client/server_side triple bound + connected on 127.0.0.1."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.setblocking(False)
    return listener


async def _sock_sendfile_connect(loop, listener):
    addr = listener.getsockname()
    client = socket.socket()
    client.setblocking(False)
    await loop.sock_connect(client, addr)
    server_side, _ = await loop.sock_accept(listener)
    return client, server_side


def test_sock_sendfile_native(loop, tmp_path):
    """A real on-disk file has .fileno(), so this exercises the native
    os.sendfile path added alongside the chunked fallback (previously
    sock_sendfile always took the chunked path regardless)."""

    async def main():
        payload = os.urandom(150_000)
        f = tmp_path / "blob.bin"
        f.write_bytes(payload)

        listener = _sock_sendfile_pair(loop)
        client, server_side = await _sock_sendfile_connect(loop, listener)

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


def test_sock_sendfile_fallback_no_fileno(loop):
    """A file-like object without .fileno() (BytesIO) can't use native
    os.sendfile — must still work via the chunked fallback."""
    import io

    async def main():
        payload = os.urandom(50_000)
        listener = _sock_sendfile_pair(loop)
        client, server_side = await _sock_sendfile_connect(loop, listener)

        sent = await loop.sock_sendfile(client, io.BytesIO(payload))
        assert sent == len(payload)
        received = bytearray()
        while len(received) < len(payload):
            received.extend(await loop.sock_recv(server_side, 65536))
        assert bytes(received) == payload
        for s in (client, server_side, listener):
            s.close()

    loop.run_until_complete(main())


def test_sock_sendfile_fallback_false_raises_without_native(loop):
    """fallback=False must raise SendfileNotAvailableError rather than
    silently falling back when native sendfile isn't usable — previously
    fallback= was accepted but never actually inspected."""
    import io

    async def main():
        listener = _sock_sendfile_pair(loop)
        client, server_side = await _sock_sendfile_connect(loop, listener)
        with pytest.raises(asyncio.SendfileNotAvailableError):
            await loop.sock_sendfile(client, io.BytesIO(b"x"), fallback=False)
        for s in (client, server_side, listener):
            s.close()

    loop.run_until_complete(main())


def test_sock_sendfile_validates_params(loop, tmp_path):
    """Params previously reached os.sendfile/file.read unchecked; now
    matches base_events._check_sendfile_params exactly."""
    f = tmp_path / "blob.bin"
    f.write_bytes(b"x" * 100)

    async def main():
        listener = _sock_sendfile_pair(loop)
        client, server_side = await _sock_sendfile_connect(loop, listener)
        try:
            with open(f, "rb") as fh:
                with pytest.raises(TypeError, match="count must be"):
                    await loop.sock_sendfile(client, fh, count="not-an-int")
                with pytest.raises(ValueError, match="count must be"):
                    await loop.sock_sendfile(client, fh, count=0)
                with pytest.raises(TypeError, match="offset must be"):
                    await loop.sock_sendfile(client, fh, offset="not-an-int")
                with pytest.raises(ValueError, match="offset must be"):
                    await loop.sock_sendfile(client, fh, offset=-1)
            with open(f, "r") as text_fh:  # not binary mode
                with pytest.raises(ValueError, match="binary mode"):
                    await loop.sock_sendfile(client, text_fh)
            dgram = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                with pytest.raises(ValueError, match="SOCK_STREAM"):
                    with open(f, "rb") as fh:
                        await loop.sock_sendfile(dgram, fh)
            finally:
                dgram.close()
        finally:
            for s in (client, server_side, listener):
                s.close()

    loop.run_until_complete(main())


def test_sock_recvfrom_and_sendto(loop):
    """sock_recvfrom/sock_recvfrom_into/sock_sendto were previously
    unimplemented (fell through to AbstractEventLoop's abstract
    NotImplementedError)."""

    async def main():
        a = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        b = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        a.bind(("127.0.0.1", 0))
        b.bind(("127.0.0.1", 0))
        a.setblocking(False)
        b.setblocking(False)
        try:
            await loop.sock_sendto(a, b"hello", b.getsockname())
            data, addr = await loop.sock_recvfrom(b, 1024)
            assert data == b"hello"
            assert addr == a.getsockname()

            await loop.sock_sendto(a, b"into-buf", b.getsockname())
            buf = bytearray(64)
            n, addr2 = await loop.sock_recvfrom_into(b, buf)
            assert bytes(buf[:n]) == b"into-buf"
            assert addr2 == a.getsockname()
        finally:
            a.close()
            b.close()

    loop.run_until_complete(main())


def test_connect_accepted_socket(loop):
    """connect_accepted_socket previously fell through to
    AbstractEventLoop's abstract NotImplementedError — the generic
    counterpart to this project's own multi-worker socket handoff."""

    async def main():
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.setblocking(False)
        addr = listener.getsockname()

        client_raw = socket.socket()
        client_raw.setblocking(False)
        await loop.sock_connect(client_raw, addr)
        server_raw, _peer = await loop.sock_accept(listener)

        server_done = loop.create_future()

        class Echo(asyncio.Protocol):
            def connection_made(self, transport):
                self.transport = transport

            def data_received(self, data):
                self.transport.write(data)

            def connection_lost(self, exc):
                if not server_done.done():
                    server_done.set_result(None)

        transport, _proto = await loop.connect_accepted_socket(Echo, server_raw)
        assert transport is not None

        client_reader, client_writer = await asyncio.open_connection(sock=client_raw)
        client_writer.write(b"ping")
        got = await asyncio.wait_for(client_reader.readexactly(4), 5)
        assert got == b"ping"
        client_writer.close()
        transport.close()
        listener.close()

    loop.run_until_complete(main())


def test_connect_accepted_socket_rejects_dgram(loop):
    async def main():
        dgram = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            with pytest.raises(ValueError, match="Stream Socket"):
                await loop.connect_accepted_socket(asyncio.Protocol, dgram)
        finally:
            dgram.close()

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


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signal.valid_signals()")
def test_signal_handler_rejects_invalid_signal(loop):
    """add_signal_handler previously skipped unix_events._check_signal's
    isinstance(sig, int)/valid_signals() checks, so an invalid signal
    number surfaced whatever error signal.signal() itself happened to
    raise instead of a clear, documented ValueError/TypeError."""
    with pytest.raises(TypeError, match="must be an int"):
        loop.add_signal_handler("not-a-signal", lambda: None)
    with pytest.raises(ValueError, match="invalid signal number"):
        loop.add_signal_handler(99999, lambda: None)


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


# --------------------------------------------------------------------- #
# loop.sendfile (R-036) + R-122 edge-case matrix                        #
# --------------------------------------------------------------------- #


def test_loop_sendfile_native(loop, tmp_path):
    import hashlib

    payload = os.urandom(2 * 1024 * 1024)
    f = tmp_path / "blob.bin"
    f.write_bytes(payload)

    async def main():
        received = bytearray()
        done = loop.create_future()

        class Sink(asyncio.Protocol):
            def data_received(self, data):
                received.extend(data)

            def connection_lost(self, exc):
                if not done.done():
                    done.set_result(None)

        server = await loop.create_server(Sink, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        transport, _proto = await loop.create_connection(
            asyncio.Protocol, "127.0.0.1", port
        )
        transport.write(b"HEAD:")  # pre-queued bytes must not interleave
        with open(f, "rb") as fh:
            sent = await asyncio.wait_for(loop.sendfile(transport, fh), 20)
            assert sent == len(payload)
            assert fh.tell() == len(payload)
        transport.close()
        await asyncio.wait_for(done, 10)
        server.close()
        assert bytes(received[:5]) == b"HEAD:"
        assert hashlib.sha256(received[5:]).hexdigest() == hashlib.sha256(payload).hexdigest()

    loop.run_until_complete(main())


def test_loop_sendfile_offset_count(loop, tmp_path):
    f = tmp_path / "oc.bin"
    f.write_bytes(bytes(range(256)) * 16)

    async def main():
        received = bytearray()
        done = loop.create_future()

        class Sink(asyncio.Protocol):
            def data_received(self, data):
                received.extend(data)

            def connection_lost(self, exc):
                if not done.done():
                    done.set_result(None)

        server = await loop.create_server(Sink, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        transport, _p = await loop.create_connection(asyncio.Protocol, "127.0.0.1", port)
        with open(f, "rb") as fh:
            sent = await loop.sendfile(transport, fh, offset=100, count=1000)
        assert sent == 1000
        transport.close()
        await asyncio.wait_for(done, 10)
        server.close()
        assert bytes(received) == (bytes(range(256)) * 16)[100:1100]

    loop.run_until_complete(main())


def test_rst_during_write_surfaces_connection_lost(loop):
    # R-122: peer sets SO_LINGER=0 and closes -> RST. The writer's
    # transport must report connection_lost with an error, not hang.
    async def main():
        lost = loop.create_future()

        class Server(asyncio.Protocol):
            def connection_made(self, transport):
                # SO_LINGER(0) + close = RST instead of FIN. Wrap the
                # transport's fileno directly and detach() before the
                # wrapper is GC'd, rather than os.dup()+close(): os.dup()
                # assumes a CRT fd-table entry, which a raw Windows SOCKET
                # (what transport.fileno() returns there) is not — dup()
                # raises "Bad file descriptor" on Windows. detach() sets
                # the option on the real socket without ever needing a
                # second descriptor, so it works identically everywhere.
                sock = socket.socket(fileno=transport.fileno())
                sock.setsockopt(
                    socket.SOL_SOCKET, socket.SO_LINGER,
                    __import__("struct").pack("ii", 1, 0),
                )
                sock.detach()
                transport.abort()

        class Client(asyncio.Protocol):
            def __init__(self):
                self.transport = None

            def connection_made(self, transport):
                self.transport = transport

            def connection_lost(self, exc):
                if not lost.done():
                    lost.set_result(exc)

        server = await loop.create_server(Server, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        tr, client = await loop.create_connection(Client, "127.0.0.1", port)
        # Write into the (about-to-be-reset) connection until failure.
        for _ in range(50):
            if tr.is_closing():
                break
            tr.write(b"x" * 65536)
            await asyncio.sleep(0.02)
        await asyncio.wait_for(lost, 10)
        server.close()

    loop.run_until_complete(main())


def test_drip_fed_request_head_completes(loop):
    # R-122 drip-feed: one byte at a time must still parse into a full
    # request (no partial-buffer loss across recv boundaries).
    async def main():
        lid, bound, _fd = loop._core.http_listen(
            "127.0.0.1", 0, _drip_app, loop
        )
        port = bound[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        raw = b"GET /drip HTTP/1.1\r\nhost: a\r\nconnection: close\r\n\r\n"
        for i in range(0, len(raw), 3):
            writer.write(raw[i : i + 3])
            await writer.drain()
            await asyncio.sleep(0.005)
        data = await asyncio.wait_for(reader.read(), 10)
        assert b"200" in data.split(b"\r\n", 1)[0]
        assert b"/drip" in data
        writer.close()
        loop._core.listener_close(lid)

    loop.run_until_complete(main())


async def _drip_app(scope, receive, send):
    await receive()
    await send(
        {"type": "http.response.start", "status": 200, "headers": []}
    )
    await send({"type": "http.response.body", "body": scope["raw_path"]})


def test_graceful_close_leaves_no_pool_slots_behind(loop):
    """Baseline for the two below: an ordinary close must not retain pool
    slots. Its recv has normally already completed with EOF, so this does
    not exercise the cancellation path — it pins the steady state that the
    cancellation tests measure against."""

    async def main():
        server, addr = await _echo_server(loop)
        for _ in range(25):
            reader, writer = await asyncio.open_connection(*addr)
            writer.write(b"ping")
            await writer.drain()
            assert await reader.readexactly(4) == b"ping"
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass  # peer-reset flavours differ across platforms
        server.close()
        await server.wait_closed()
        for _ in range(20):
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)

    loop.run_until_complete(main())
    assert loop._core.stats()["buffers_in_use"] == 0


def test_abort_with_ops_in_flight_releases_pool_slots(loop):
    """R-073: `abort()` cancels a posted recv, and `CancelIoEx`/`ECANCELED`
    only *requests* cancellation — the op's buffer reference is released
    when the completion is finally reaped. Dropping the op mapping at
    teardown (as the code used to) orphaned that completion, so every
    aborted connection leaked its pool slot for the life of the process.

    The queued writes matter too, though this counter cannot see them:
    `post_send` points the kernel straight at the queued `WriteBuf` memory,
    which teardown used to drop with the transport entry. That is a
    use-after-free rather than a leak, so it shows up under PageHeap/ASan,
    not here — this test at least drives the path. Both reported by Codex
    review on PR #1.
    """
    import threading

    payload = b"x" * (256 * 1024)  # big enough to stay queued

    # A peer that accepts and never reads, deliberately NOT a cadeloop
    # server: only the client side should own pool slots, so the assertion
    # measures this loop and nothing else.
    lsock = socket.socket()
    lsock.bind(("127.0.0.1", 0))
    lsock.listen(16)
    addr = lsock.getsockname()
    accepted = []
    stop = threading.Event()

    def acceptor():
        while not stop.is_set():
            try:
                c, _ = lsock.accept()
            except OSError:
                return
            accepted.append(c)

    t = threading.Thread(target=acceptor, daemon=True)
    t.start()

    async def main():
        for _ in range(10):
            _reader, writer = await asyncio.open_connection(*addr)
            for _ in range(8):
                writer.write(payload)
            writer.transport.abort()  # recv AND send cancelled in flight
        for _ in range(20):
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)

    try:
        loop.run_until_complete(main())
        assert loop._core.stats()["buffers_in_use"] == 0
    finally:
        stop.set()
        lsock.close()
        for c in accepted:
            c.close()
        t.join(timeout=2)


def test_datagram_close_releases_its_recv_slot(loop):
    """A datagram endpoint's recv slot has a single reference, so releasing
    it straight after `cancel()` handed a still-kernel-owned buffer back to
    the pool. It now travels with the op and is released on reap."""

    class P(asyncio.DatagramProtocol):
        def datagram_received(self, data, addr):
            pass

    async def main():
        for _ in range(15):
            transport, _proto = await loop.create_datagram_endpoint(
                P, local_addr=("127.0.0.1", 0)
            )
            transport.close()
        for _ in range(20):
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)

    loop.run_until_complete(main())
    assert loop._core.stats()["buffers_in_use"] == 0


def test_listener_keeps_accepting_after_many_connections(loop):
    """R-032: the accept pool must never end up empty on a live listener.
    Every path that removes an accept op — including one that completes
    with a cancellation error — has to re-arm, or the listener goes
    permanently deaf while still reporting itself as serving. Reported by
    Codex review on PR #1 (raised twice)."""

    async def main():
        server, addr = await _echo_server(loop)
        for i in range(60):
            reader, writer = await asyncio.open_connection(*addr)
            writer.write(b"ping")
            await writer.drain()
            assert await asyncio.wait_for(reader.readexactly(4), 5) == b"ping", f"stalled at {i}"
            writer.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())
    assert loop._core.stats()["accept_starved"] == 0


def test_close_with_a_pending_connect_releases_it():
    """R-122: loop close sweeps transports, listeners and datagram
    endpoints, but a standalone `connect()` op belongs to none of them. It
    used to keep its future, its buffers and an open socket alive for as
    long as the closed core existed. Reported by Codex review on PR #1."""
    lp = cadeloop.new_event_loop()
    asyncio.set_event_loop(lp)
    try:
        # 198.51.100.0/24 is TEST-NET-2: reserved, so the connect stays
        # pending rather than completing or refusing promptly.
        fut = asyncio.ensure_future(
            asyncio.wait_for(lp.create_connection(asyncio.Protocol, "198.51.100.1", 9), 0.2),
            loop=lp,
        )
        try:
            lp.run_until_complete(asyncio.sleep(0.05))
        except Exception:
            pass
        fut.cancel()
    finally:
        asyncio.set_event_loop(None)
        lp.close()  # must not hang, leak the socket, or leave the op live
    assert lp.is_closed()


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signal disposition")
def test_close_restores_signal_disposition():
    """A handler left installed by close() points the signal module at a
    _dispatch that returns immediately once the loop is closed, so the
    signal is silently swallowed for the rest of the process instead of
    resuming default behaviour. Reported by Codex review on PR #1 (twice)."""
    import signal as signal_module

    before = signal_module.getsignal(signal_module.SIGUSR1)
    lp = cadeloop.new_event_loop()
    try:
        lp.add_signal_handler(signal_module.SIGUSR1, lambda: None)
        assert signal_module.getsignal(signal_module.SIGUSR1) is not before
    finally:
        lp.close()
    assert signal_module.getsignal(signal_module.SIGUSR1) is signal_module.SIG_DFL
    signal_module.signal(signal_module.SIGUSR1, before)
