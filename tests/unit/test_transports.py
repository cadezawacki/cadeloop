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


def test_sendfile_fallback_honours_offset_zero(loop, tmp_path):
    """The seek was guarded by `if offset:`, so the default offset=0 sent
    from the file's CURRENT position on any fallback path (BytesIO, SSL,
    Windows) while the native path sent from byte zero — the same call
    returning different bytes depending on which path ran. Reported by
    Codex review on PR #1 (twice)."""
    payload = b"HEADER" + bytes((i % 251) for i in range(1000))
    path = tmp_path / "payload.bin"
    path.write_bytes(payload)

    received = []

    async def main():
        async def handler(reader, writer):
            received.append(await reader.read(-1))
            writer.close()

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        addr = server.sockets[0].getsockname()
        _reader, writer = await asyncio.open_connection(*addr)
        with open(path, "rb") as fh:
            fh.read(6)  # advance past HEADER, as a prior read would
            assert fh.tell() == 6
            # offset=0 must mean byte zero, not "wherever the file is".
            await loop._sendfile_fallback(writer.transport, fh, 0, None)
        writer.close()
        await asyncio.sleep(0.1)
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())
    assert received, "handler never ran"
    assert received[0] == payload, (
        f"sent {len(received[0])} bytes starting {received[0][:8]!r}; "
        f"expected the whole {len(payload)}-byte file from byte zero"
    )


def test_write_after_write_eof_raises(loop):
    """The asyncio write-transport contract raises here. Silently
    succeeding let protocol code believe output was queued when it can
    never reach the peer — undetected truncation. A CLOSED transport still
    drops silently, which is also the contract. Reported by Codex review
    on PR #1 (twice)."""

    async def main():
        server, addr = await _echo_server(loop)
        _reader, writer = await asyncio.open_connection(*addr)
        t = writer.transport
        t.write(b"before")
        t.write_eof()
        with pytest.raises(RuntimeError, match="write_eof"):
            t.write(b"after")
        t.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_set_write_buffer_limits_derives_high_from_low(loop):
    """asyncio defines high as 4 * low when only low is supplied; a fixed
    64 KiB rejected low > 64 KiB outright and gave a different pause
    threshold from every other loop. Reported by Codex review on PR #1."""

    async def main():
        server, addr = await _echo_server(loop)
        _reader, writer = await asyncio.open_connection(*addr)
        t = writer.transport
        t.set_write_buffer_limits(low=256 * 1024)  # would have raised
        assert t.get_write_buffer_limits() == (256 * 1024, 4 * 256 * 1024)
        t.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


@pytest.mark.skipif(sys.platform == "win32", reason="fd counting is POSIX-only")
def test_failing_protocol_factory_does_not_leak_the_connected_socket(loop):
    """Between a successful connect and the transport that would own it,
    the descriptor has no owner. A protocol_factory that raises therefore
    leaked one connected socket per failure -- invisible until the process
    ran out of them."""
    import errno

    def open_fds():
        return len(os.listdir("/proc/self/fd"))

    async def main():
        server, addr = await _echo_server(loop)
        try:

            def boom():
                raise RuntimeError("no protocol for you")

            for _ in range(3):  # warm any lazily-created fds first
                with pytest.raises(RuntimeError, match="no protocol"):
                    await loop.create_connection(boom, *addr)
            before = open_fds()
            for _ in range(20):
                with pytest.raises(RuntimeError, match="no protocol"):
                    await loop.create_connection(boom, *addr)
            leaked = open_fds() - before
        finally:
            server.close()
            await server.wait_closed()
        assert leaked == 0, f"{leaked} descriptors leaked across 20 failed connects"

    loop.run_until_complete(main())


def test_get_extra_info_socket_is_live_and_non_owning(loop):
    """Libraries reach for the socket through the standard transport API
    to read the family or set an option such as keepalive; returning None
    for a live connection made them fail or silently skip that setup.

    The object must not be a second owner of the engine's descriptor: the
    engine closes that one at teardown, and a second close could land on
    a number the OS has since handed to an unrelated connection."""

    class P(asyncio.Protocol):
        pass

    async def main():
        server, addr = await _echo_server(loop)
        try:
            transport, _ = await loop.create_connection(P, *addr)
            sock = transport.get_extra_info("socket")
            assert sock is not None, 'get_extra_info("socket") returned None'
            assert sock.family == socket.AF_INET
            assert sock.type == socket.SOCK_STREAM
            assert sock.getpeername()[:2] == tuple(addr[:2])
            # Standard use: read and set an option through the wrapper.
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 1
            # Stable across calls -- not a fresh dup each time.
            assert transport.get_extra_info("socket") is sock
            # The transport's close must be final even though this
            # object holds a duplicate. A duplicate is another OWNER of the
            # connection: leaving it open would deny the peer its EOF and
            # keep the connection allocated for as long as the application
            # happened to hold the object.
            transport.close()
            await asyncio.sleep(0.05)
            assert sock.fileno() == -1, "the transport's close left its socket open"
            with pytest.raises(OSError):
                sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE)
        finally:
            server.close()
            await server.wait_closed()

    loop.run_until_complete(main())


def test_ipv6_addresses_keep_flowinfo_and_scope_id(loop):
    """Python represents an IPv6 address as (host, port, flowinfo,
    scope_id). Flattening it to two elements loses the interface scope,
    and a link-local peer's address then cannot be passed back to
    sendto()."""

    class P(asyncio.Protocol):
        pass

    async def main():
        server = await asyncio.start_server(lambda r, w: None, "::1", 0)
        try:
            addr = server.sockets[0].getsockname()
            transport, _ = await loop.create_connection(P, "::1", addr[1])
            peer = transport.get_extra_info("peername")
            name = transport.get_extra_info("sockname")
            assert len(peer) == 4, f"IPv6 peername lost its scope fields: {peer!r}"
            assert len(name) == 4, f"IPv6 sockname lost its scope fields: {name!r}"
            assert peer[1] == addr[1]
            transport.close()
            await asyncio.sleep(0.05)
        finally:
            server.close()
            await server.wait_closed()

    try:
        loop.run_until_complete(main())
    except OSError as exc:  # no IPv6 loopback on this host
        pytest.skip(f"IPv6 unavailable: {exc}")


def test_datagram_sendto_round_trips_an_ipv6_address(loop):
    """The four-element form has to be accepted on the way back out --
    it is exactly what datagram_received hands the application."""

    got = []

    class Echo(asyncio.DatagramProtocol):
        def datagram_received(self, data, addr):
            got.append((data, addr))

    async def main():
        t1, _ = await loop.create_datagram_endpoint(Echo, local_addr=("::1", 0))
        t2, _ = await loop.create_datagram_endpoint(Echo, local_addr=("::1", 0))
        try:
            dest = t1.get_extra_info("sockname")
            assert len(dest) == 4, dest
            t2.sendto(b"ping", dest)  # 4-tuple straight back into sendto
            for _ in range(50):
                await asyncio.sleep(0.01)
                if got:
                    break
            assert got, "no datagram arrived at the 4-tuple address"
            assert got[0][0] == b"ping"
            assert len(got[0][1]) == 4, got[0][1]
        finally:
            t1.close()
            t2.close()
            await asyncio.sleep(0.05)

    try:
        loop.run_until_complete(main())
    except OSError as exc:
        pytest.skip(f"IPv6 unavailable: {exc}")


def test_sendto_rejects_a_malformed_address_tuple(loop):
    """The 4-element form is IPv6-only, and neither form may be a
    different length -- worth checking on a host without IPv6, where the
    round-trip test above can only skip."""

    class P(asyncio.DatagramProtocol):
        pass

    async def main():
        transport, _ = await loop.create_datagram_endpoint(P, local_addr=("127.0.0.1", 0))
        try:
            with pytest.raises(ValueError, match="IPv6-only"):
                transport.sendto(b"x", ("127.0.0.1", 9, 0, 0))
            with pytest.raises(ValueError, match="2 elements"):
                transport.sendto(b"x", ("127.0.0.1", 9, 0))
            with pytest.raises(ValueError, match="invalid IP address"):
                transport.sendto(b"x", ("not-an-ip", 9))
        finally:
            transport.close()
            await asyncio.sleep(0.05)

    loop.run_until_complete(main())


def test_create_server_rejects_a_datagram_socket(loop):
    """A SOCK_DGRAM sock= was detached and registered as a listener;
    accept() then failed forever while the listener rearmed after each
    failure, so the caller got an apparently serving Server that only
    logged accept errors."""
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.bind(("127.0.0.1", 0))
    try:
        with pytest.raises(ValueError, match="stream socket"):
            loop.run_until_complete(
                loop.create_server(asyncio.Protocol, sock=udp)
            )
        # Rejected before ownership transferred, so it is still ours.
        assert udp.fileno() != -1
    finally:
        udp.close()


def test_data_received_exception_closes_the_transport(loop):
    """The next receive is already posted by the time the callback runs,
    so reporting the exception and carrying on kept feeding bytes to a
    protocol whose state may be inconsistent. The stdlib's socket
    transport treats this as fatal."""
    lost = []
    seen = []

    class Boom(asyncio.Protocol):
        def data_received(self, data):
            seen.append(data)
            raise RuntimeError("protocol is confused now")

        def connection_lost(self, exc):
            lost.append(exc)

    reported = []
    loop.set_exception_handler(lambda lp, ctx: reported.append(ctx.get("message", "")))

    async def main():
        server, addr = await _echo_server(loop)
        try:
            transport, _ = await loop.create_connection(Boom, *addr)
            transport.write(b"first")
            for _ in range(100):
                await asyncio.sleep(0.02)
                if lost:
                    break
            assert seen, "the protocol never received anything"
            assert lost, "connection_lost never ran; the transport stayed open"
            assert transport.is_closing()
            assert any("protocol callback" in m for m in reported), reported
        finally:
            server.close()
            await server.wait_closed()

    loop.run_until_complete(main())


def test_pause_reading_withholds_the_in_flight_read(loop):
    """A protocol pauses to bound its own memory. The read already in
    flight when it paused was delivered anyway, handing it another full
    buffer past the limit that prompted the pause.

    The count is asserted against the chunk that TRIGGERED the pause, not
    against a count sampled afterwards -- the extra delivery lands in the
    same tick, so sampling later sees it as the baseline and the test
    proves nothing. (It did, until this was checked against the unfixed
    build.)"""
    total = 512 * 1024
    chunks = []

    class Pauser(asyncio.Protocol):
        def connection_made(self, transport):
            self.transport = transport

        def data_received(self, data):
            chunks.append(data)
            if len(chunks) == 1:
                self.transport.pause_reading()

    async def main():
        server, addr = await _echo_server(loop)
        try:
            transport, _ = await loop.create_connection(Pauser, *addr)
            transport.write(b"z" * total)
            for _ in range(100):
                await asyncio.sleep(0.02)
                if chunks:
                    break
            assert chunks, "nothing was received at all"
            assert not transport.is_reading()
            # Exactly the chunk that caused the pause -- no more, however
            # long the peer keeps sending.
            await asyncio.sleep(0.5)
            assert len(chunks) == 1, (
                f"{len(chunks) - 1} chunk(s) delivered to a paused protocol"
            )
            transport.resume_reading()
            for _ in range(200):
                await asyncio.sleep(0.02)
                if sum(len(c) for c in chunks) >= total:
                    break
            # Every byte arrives exactly once: the held read is delivered
            # on resume, ahead of the next one, and nothing is duplicated.
            assert sum(len(c) for c in chunks) == total, (
                f"{sum(len(c) for c in chunks)} of {total} bytes"
            )
            assert b"".join(chunks) == b"z" * total
            transport.close()
            await asyncio.sleep(0.05)
        finally:
            server.close()
            await server.wait_closed()

    loop.run_until_complete(main())


def test_create_server_starts_listening_on_a_bound_socket(loop):
    """stdlib create_server() accepts a bound-but-not-listening socket and
    calls listen() itself. Without that, native accepts were posted
    against a socket in no state to accept them: every post failed, the
    listener rearmed after each failure, and the caller got a Server that
    looked healthy and could never take a connection."""
    bound = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    bound.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    bound.bind(("127.0.0.1", 0))  # deliberately NOT listening
    addr = bound.getsockname()
    got = []

    class P(asyncio.Protocol):
        def data_received(self, data):
            got.append(data)

    async def main():
        server = await loop.create_server(P, sock=bound)
        try:
            r, w = await asyncio.open_connection(*addr)
            w.write(b"hello")
            await w.drain()
            for _ in range(100):
                await asyncio.sleep(0.02)
                if got:
                    break
            assert got == [b"hello"], got
            w.close()
        finally:
            server.close()
            await server.wait_closed()

    loop.run_until_complete(main())


def test_tfo_and_loopback_fast_path_reach_the_core(loop_factory=None):
    """Both were exposed in Config, documented under R-038, and read by
    nothing -- `tfo=True` was a silent no-op and the default-on loopback
    fast path was never applied."""
    # The observable contract at this layer is that the core accepts them
    # and a listener still comes up; the options themselves are
    # kernel-side and platform-specific.
    lp = cadeloop.Loop(tfo=False, loopback_fast_path=True)
    try:
        assert lp._core is not None
    finally:
        lp.close()
    cfg = cadeloop.Config(tfo=True, loopback_fast_path=False)
    assert cfg.tfo is True and cfg.loopback_fast_path is False


def test_create_connection_rejects_a_datagram_socket(loop):
    """A datagram sock= got the native STREAM transport, which applies
    byte-stream flow control and EOF semantics to packet I/O."""
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.bind(("127.0.0.1", 0))
    try:
        with pytest.raises(ValueError, match="stream socket"):
            loop.run_until_complete(loop.create_connection(asyncio.Protocol, sock=udp))
        assert udp.fileno() != -1, "rejected before ownership transferred"
    finally:
        udp.close()


@pytest.mark.skipif(
    not hasattr(os, "sendfile"),
    reason="the native sendfile path is only reachable where os.sendfile exists",
)
def test_sendfile_reports_position_after_an_interrupted_transfer(loop, monkeypatch):
    """sendfile leaves the file positioned after the bytes actually sent.
    `os.sendfile` takes an explicit offset and does NOT advance the file
    position itself, so the seek is the only thing that establishes it --
    and doing it solely on the success path left tell() at the original
    offset after a transfer that had already put bytes on the wire, so a
    caller retrying from the reported position sent them twice.

    The failure is injected rather than raced: a cancel loses to loopback
    every time, and a test that cannot reach the path it names proves
    nothing."""
    import tempfile

    payload = b"abcdefghij" * 4096
    sent_before_failure = 1024

    with tempfile.TemporaryFile() as fh:
        fh.write(payload)
        fh.flush()
        fh.seek(0)
        a, b = socket.socketpair()
        real_sendfile = os.sendfile
        calls = []

        def flaky(out_fd, in_fd, offset, blocksize, *args):
            calls.append(1)
            if len(calls) > 1:
                raise BrokenPipeError(32, "peer went away mid-transfer")
            return real_sendfile(out_fd, in_fd, offset, sent_before_failure)

        monkeypatch.setattr(os, "sendfile", flaky)
        try:
            with pytest.raises(BrokenPipeError):
                loop.run_until_complete(loop._sendfile_native_fd(a.fileno(), fh, 0, None))
            assert fh.tell() == sent_before_failure, (
                f"tell() reports {fh.tell()} after {sent_before_failure} bytes were sent"
            )
        finally:
            a.close()
            b.close()


@pytest.mark.skipif(sys.platform == "win32", reason="AF_UNIX transports are POSIX-only (R-057)")
def test_unix_transport_reports_its_addresses(loop, tmp_path):
    """netsys::peername/sockname only parsed Internet sockaddrs, so an
    AF_UNIX connection's addresses failed to parse and were dropped:
    get_extra_info returned None on a live transport, leaving no way to
    learn the socket path."""
    path = str(tmp_path / "s.sock")
    got = {}

    class P(asyncio.Protocol):
        def connection_made(self, transport):
            got["server_peer"] = transport.get_extra_info("peername")
            got["server_sock"] = transport.get_extra_info("sockname")

    async def main():
        server = await loop.create_unix_server(P, path)
        try:
            transport, _ = await loop.create_unix_connection(asyncio.Protocol, path)
            try:
                assert transport.get_extra_info("sockname") is not None
                assert transport.get_extra_info("peername") == path, (
                    transport.get_extra_info("peername")
                )
                for _ in range(50):
                    await asyncio.sleep(0.02)
                    if got:
                        break
                assert got.get("server_sock") == path, got
            finally:
                transport.close()
                await asyncio.sleep(0.05)
        finally:
            server.close()
            await server.wait_closed()

    loop.run_until_complete(main())


def test_sendfile_fallback_reports_position_after_a_failed_write(loop):
    """The same contract on the path Windows and every SSL transport
    actually take. Here the reads advance the position on their own, so
    it only diverges when the transfer stops between a read and its
    write -- and then tell() is ahead of what reached the peer, which is
    the direction that makes a retrying caller SKIP bytes."""
    import io

    payload = b"abcdefghij" * 4096
    fh = io.BytesIO(payload)

    class FailingTransport:
        def __init__(self):
            self.written = 0
            self.writes = 0

        def write(self, data):
            self.writes += 1
            if self.writes > 1:
                raise ConnectionResetError("peer went away")
            self.written += len(data)

        def get_write_buffer_size(self):
            return 0

    t = FailingTransport()
    with pytest.raises(ConnectionResetError):
        loop.run_until_complete(loop._sendfile_fallback(t, fh, 0, None))
    assert fh.tell() == t.written, (
        f"tell() reports {fh.tell()} but only {t.written} bytes reached the transport"
    )


def test_close_wakes_serve_forever(loop):
    """The future serve_forever parked on was anonymous, so close() could
    not reach it: a server closed by another task left the coroutine
    pending for good, even after wait_closed() had returned, and the
    caller had to separately cancel a server it had already closed."""

    async def main():
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
        task = loop.create_task(server.serve_forever())
        await asyncio.sleep(0.05)
        assert not task.done()
        server.close()
        await server.wait_closed()
        # The point: no explicit task.cancel() here.
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5.0)

    loop.run_until_complete(main())


def test_close_reaps_the_ops_it_cancels(loop):
    """close() cancels every in-flight op, and R-073 forbids freeing a
    cancelled op's buffers until its completion comes back -- the kernel
    may still be writing into a receive slot or reading from a send
    buffer. So teardown parks them in `reap_guards` and leaves the release
    to the next dispatch. A closed loop has no next dispatch: without a
    reap at close, whatever those ops held stayed resident for as long as
    anything referenced the dead loop.

    The datagram endpoint is the portable case -- a UDP endpoint always
    has a receive posted, so closing the loop always cancels one and
    always parks its slot. The queued-send case below only arises where
    the platform actually parks a partial write.
    """

    async def main():
        await loop.create_datagram_endpoint(asyncio.DatagramProtocol, local_addr=("127.0.0.1", 0))
        await asyncio.sleep(0)

    loop.run_until_complete(main())
    assert loop.stats()["ops_by_target"]["dgram"] >= 1, (
        "no datagram op outstanding; the test is not exercising the guard"
    )
    loop.close()
    assert loop.stats()["unreaped_ops"] == 0, (
        "close() left a receive slot pinned to a cancelled op it never reaped"
    )


def test_close_reaps_the_buffers_of_the_sends_it_cancels(loop):
    """The stream half of the same discipline: a send cancelled at close
    still owns its write queue until the completion is dequeued.

    Whether a send can be made to park with a remainder is a platform
    property -- IOCP takes a large WSASend whole where writev on a socket
    with a small peer receive buffer stops short -- so the precondition is
    a skip, not a failure. The guard itself is covered portably above.
    """
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # A small receive buffer is what makes the send stop short with a
    # remainder still queued; on a default-sized loopback socket the whole
    # payload is absorbed and there is nothing left in flight to cancel.
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    peer = None
    transport = None
    parked = False
    try:

        async def main():
            nonlocal peer, transport, parked
            transport, _ = await loop.create_connection(asyncio.Protocol, *srv.getsockname())
            peer, _ = srv.accept()
            transport.write(b"x" * (8 * 1024 * 1024))
            for _ in range(20):
                await asyncio.sleep(0.01)
                if transport.get_write_buffer_size() > 0 and loop.stats()["ops_by_target"]["send"]:
                    parked = True
                    return

        loop.run_until_complete(main())
        if not parked:
            pytest.skip("this platform did not park a send with a queued remainder")
        loop.close()
        assert loop.stats()["unreaped_ops"] == 0, (
            "close() left write buffers pinned to cancelled ops it never reaped"
        )
    finally:
        if peer is not None:
            peer.close()
        srv.close()


def test_a_failed_ssl_protocol_build_does_not_leak_the_connection(loop):
    """`_make_ssl_context` returns anything that is not a bool unchanged,
    so `ssl="bad"` reaches `_make_ssl_protocol` and raises inside
    wrap_bio. The protocol was built outside the rollback try, so that
    raised past the only code that would have closed the descriptor --
    one connected socket leaked per failure, invisible until the process
    ran out. Reported by Codex on PR #1."""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    # Every attempt completes a connection the server never accepts, so
    # the backlog must outlast the run or the connects themselves block.
    srv.listen(128)
    try:

        async def attempt():
            with pytest.raises(BaseException):
                await loop.create_connection(asyncio.Protocol, *srv.getsockname(), ssl="bad")

        before = _open_fd_count()
        for _ in range(12):
            loop.run_until_complete(attempt())
        after = _open_fd_count()
        # A few descriptors of slack for unrelated churn; the bug leaked
        # one per attempt, so 12 attempts moved this by 12.
        assert after - before < 6, (
            f"open descriptors grew from {before} to {after} over 12 failed attempts"
        )
    finally:
        srv.close()


def _open_fd_count():
    """Open descriptors for this process, or handles on Windows."""
    if hasattr(os, "listdir") and os.path.isdir("/proc/self/fd"):
        return len(os.listdir("/proc/self/fd"))
    import gc as _gc

    return len([o for o in _gc.get_objects() if isinstance(o, socket.socket)])
