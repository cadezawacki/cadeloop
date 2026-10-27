"""Behaviour cadeloop must share with the stdlib asyncio loop.

Found by differential testing against CPython 3.11's selector loop (and
uvloop as a second opinion). Each test states the asyncio behaviour; the
`loop` fixture runs it on cadeloop AND on asyncio's own loop, so a test
that only passes on cadeloop is a test of the wrong behaviour. Tests that
need cadeloop internals use `cade_loop` instead.
"""

import asyncio
import errno
import os
import socket
import ssl
import struct
import time

import cadeloop
import pytest


def _new_loop(kind):
    if kind == "cadeloop":
        return cadeloop.new_event_loop()
    return asyncio.SelectorEventLoop()


@pytest.fixture(params=["cadeloop", "asyncio"])
def loop(request):
    lp = _new_loop(request.param)
    asyncio.set_event_loop(lp)
    yield lp
    asyncio.set_event_loop(None)
    if not lp.is_closed():
        lp.close()


@pytest.fixture()
def cade_loop():
    lp = cadeloop.new_event_loop()
    asyncio.set_event_loop(lp)
    yield lp
    asyncio.set_event_loop(None)
    if not lp.is_closed():
        lp.close()


def _tls_contexts():
    trustme = pytest.importorskip("trustme")
    ca = trustme.CA()
    cert = ca.issue_cert("localhost", "127.0.0.1")
    server_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    cert.configure_cert(server_ctx)
    client_ctx = ssl.create_default_context()
    ca.configure_trust(client_ctx)
    return server_ctx, client_ctx


def _quiet(loop):
    """Swallow the handshake-failure reports the server side logs."""
    seen = []
    loop.set_exception_handler(lambda _loop, ctx: seen.append(ctx))
    return seen


def _closed_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# --------------------------------------------------------------------- #
# 1. TLS error paths (sslproto over the native transport)               #
# --------------------------------------------------------------------- #


def test_tls_untrusted_certificate_fails_the_connect(loop):
    """A certificate the client does not trust raises
    SSLCertVerificationError. It used to *return* `(None, protocol)`:
    sslproto reports a failed handshake through `transport._force_close`,
    which the native transport lacked."""
    server_ctx, _ = _tls_contexts()
    _quiet(loop)

    async def main():
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0, ssl=server_ctx)
        port = server.sockets[0].getsockname()[1]
        try:
            with pytest.raises(ssl.SSLCertVerificationError):
                await asyncio.wait_for(
                    loop.create_connection(
                        asyncio.Protocol,
                        "127.0.0.1",
                        port,
                        ssl=ssl.create_default_context(),
                        server_hostname="localhost",
                    ),
                    10,
                )
            with pytest.raises(ssl.SSLCertVerificationError):
                await asyncio.wait_for(
                    asyncio.open_connection(
                        "127.0.0.1",
                        port,
                        ssl=ssl.create_default_context(),
                        server_hostname="localhost",
                    ),
                    10,
                )
        finally:
            server.close()
            await server.wait_closed()

    loop.run_until_complete(main())


def test_tls_corrupted_record_raises_ssl_error(loop):
    """A corrupted record mid-stream is an SSLError on read, not EOF."""
    server_ctx, client_ctx = _tls_contexts()
    _quiet(loop)

    async def handler(reader, writer):
        writer.write(b"A\n")
        await writer.drain()
        await reader.readexactly(1)
        # Application-data record header + garbage, under the TLS layer.
        fd = writer.get_extra_info("socket").fileno()
        os.write(fd, b"\x17\x03\x03\x00\x20" + b"\x00" * 32)
        await asyncio.sleep(0.5)
        writer.close()

    async def main():
        server = await asyncio.start_server(handler, "127.0.0.1", 0, ssl=server_ctx)
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", port, ssl=client_ctx, server_hostname="localhost"
        )
        assert await reader.readline() == b"A\n"
        writer.write(b"B")
        with pytest.raises(ssl.SSLError):
            await asyncio.wait_for(reader.readline(), 5)
        writer.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_tls_client_handshake_timeout_fires(loop):
    """ssl_handshake_timeout against a server that never speaks TLS."""
    _, client_ctx = _tls_contexts()
    _quiet(loop)

    async def main():
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        started = time.monotonic()
        with pytest.raises(ConnectionAbortedError):
            await asyncio.wait_for(
                loop.create_connection(
                    asyncio.Protocol,
                    "127.0.0.1",
                    port,
                    ssl=client_ctx,
                    server_hostname="localhost",
                    ssl_handshake_timeout=0.3,
                ),
                10,
            )
        assert time.monotonic() - started < 5
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_tls_server_drops_a_client_that_never_handshakes(loop):
    """Server-side ssl_handshake_timeout: a client that connects and stays
    silent is dropped. Before, it was kept forever -- every idle TCP
    connection to a TLS server stayed open."""
    server_ctx, _ = _tls_contexts()
    _quiet(loop)

    async def main():
        server = await asyncio.start_server(
            lambda r, w: None, "127.0.0.1", 0, ssl=server_ctx, ssl_handshake_timeout=0.3
        )
        client = socket.create_connection(server.sockets[0].getsockname())
        client.setblocking(False)
        try:
            deadline = time.monotonic() + 5
            while True:
                try:
                    if client.recv(1) == b"":
                        break
                except BlockingIOError:
                    pass
                except ConnectionResetError:
                    break
                assert time.monotonic() < deadline, "silent TLS client was never dropped"
                await asyncio.sleep(0.02)
        finally:
            client.close()
            server.close()
            await server.wait_closed()

    loop.run_until_complete(main())


def test_tls_shutdown_timeout_closes_the_connection(loop):
    """close() on a TLS transport whose peer never answers close_notify
    ends in connection_lost once ssl_shutdown_timeout expires."""
    import threading

    server_ctx, client_ctx = _tls_contexts()
    _quiet(loop)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    release = threading.Event()

    def serve():
        conn, _ = listener.accept()
        tls = server_ctx.wrap_socket(conn, server_side=True)  # handshake, then silence
        release.wait(10)
        tls.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()

    async def main():
        lost = loop.create_future()

        class P(asyncio.Protocol):
            def connection_lost(self, exc):
                lost.set_result(exc)

        transport, _ = await loop.create_connection(
            P,
            *listener.getsockname(),
            ssl=client_ctx,
            server_hostname="localhost",
            ssl_shutdown_timeout=0.3,
        )
        transport.close()
        await asyncio.wait_for(lost, 5)

    try:
        loop.run_until_complete(main())
    finally:
        release.set()
        thread.join(5)
        listener.close()


def test_tls_abort_calls_connection_lost(loop):
    """abort() on a TLS transport tears it down and reaches
    connection_lost; it raised AttributeError and left it open."""
    server_ctx, client_ctx = _tls_contexts()
    _quiet(loop)

    async def main():
        server = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0, ssl=server_ctx)
        lost = loop.create_future()

        class P(asyncio.Protocol):
            def connection_lost(self, exc):
                lost.set_result(exc)

        transport, _ = await loop.create_connection(
            P,
            "127.0.0.1",
            server.sockets[0].getsockname()[1],
            ssl=client_ctx,
            server_hostname="localhost",
        )
        transport.abort()
        assert await asyncio.wait_for(lost, 5) is None
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_force_close_hands_its_exception_to_connection_lost(cade_loop):
    """The private `_force_close(exc)` that sslproto calls: connection_lost
    receives `exc` itself."""
    loop = cade_loop

    async def main():
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
        lost = loop.create_future()

        class P(asyncio.Protocol):
            def connection_lost(self, exc):
                lost.set_result(exc)

        transport, _ = await loop.create_connection(
            P, "127.0.0.1", server.sockets[0].getsockname()[1]
        )
        boom = ConnectionAbortedError("boom")
        transport._force_close(boom)
        assert transport.is_closing()
        assert await asyncio.wait_for(lost, 5) is boom
        transport._force_close(ValueError("late"))  # already gone: a no-op
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_start_tls_validates_its_arguments(loop):
    """start_tls(transport, protocol, None) is a TypeError up front. It
    used to hang: SSLProtocol built a default client context for it."""

    async def main():
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
        transport, protocol = await loop.create_connection(
            asyncio.Protocol, "127.0.0.1", server.sockets[0].getsockname()[1]
        )
        with pytest.raises(TypeError, match="sslcontext is expected to be an instance"):
            await asyncio.wait_for(loop.start_tls(transport, protocol, None), 5)
        transport.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


# --------------------------------------------------------------------- #
# 2. OSError carries errno and asyncio's wording                        #
# --------------------------------------------------------------------- #


def test_connect_refused_has_errno_and_names_the_address(loop):
    port = _closed_port()

    async def main():
        with pytest.raises(ConnectionRefusedError) as info:
            await loop.create_connection(asyncio.Protocol, "127.0.0.1", port)
        assert info.value.errno == errno.ECONNREFUSED
        assert info.value.strerror == f"Connect call failed ('127.0.0.1', {port})"

    loop.run_until_complete(main())


def test_bind_in_use_has_errno_and_names_the_address(loop):
    holder = socket.socket()
    holder.bind(("127.0.0.1", 0))
    holder.listen()
    port = holder.getsockname()[1]

    async def main():
        with pytest.raises(OSError) as info:
            await loop.create_server(asyncio.Protocol, "127.0.0.1", port, reuse_address=False)
        assert info.value.errno == errno.EADDRINUSE
        assert info.value.strerror == (
            f"error while attempting to bind on address ('127.0.0.1', {port}): "
            "address already in use"
        )

    try:
        loop.run_until_complete(main())
    finally:
        holder.close()


def test_local_addr_bind_failure_names_the_local_address(loop):
    holder = socket.socket()
    holder.bind(("127.0.0.1", 0))
    holder.listen()
    taken = holder.getsockname()

    async def main():
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        with pytest.raises(OSError) as info:
            await loop.create_connection(
                asyncio.Protocol, "127.0.0.1", port, local_addr=taken
            )
        assert info.value.errno == errno.EADDRINUSE
        assert info.value.strerror == (
            f"error while attempting to bind on address {taken!r}: address already in use"
        )
        server.close()
        await server.wait_closed()

    try:
        loop.run_until_complete(main())
    finally:
        holder.close()


def test_peer_reset_reaches_connection_lost_with_errno(loop):
    async def main():
        accepted = loop.create_future()

        class Server(asyncio.Protocol):
            def connection_made(self, transport):
                accepted.set_result(transport)

        lost = loop.create_future()

        class Client(asyncio.Protocol):
            def connection_lost(self, exc):
                lost.set_result(exc)

        server = await loop.create_server(Server, "127.0.0.1", 0)
        transport, _ = await loop.create_connection(
            Client, "127.0.0.1", server.sockets[0].getsockname()[1]
        )
        peer = await accepted
        # SO_LINGER(on, 0): close() sends RST instead of FIN.
        peer.get_extra_info("socket").setsockopt(
            socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
        )
        peer.abort()
        exc = await asyncio.wait_for(lost, 5)
        assert isinstance(exc, ConnectionResetError)
        assert exc.errno == errno.ECONNRESET
        assert exc.strerror == os.strerror(errno.ECONNRESET)
        transport.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_native_errors_keep_errno(cade_loop):
    """Native entry points raise OSError(errno, strerror), so the subclass
    and errno are what the socket module would give."""
    loop = cade_loop
    holder = socket.socket()
    holder.bind(("127.0.0.1", 0))
    port = holder.getsockname()[1]
    try:
        with pytest.raises(OSError) as info:
            loop._core.tcp_listen("127.0.0.1", port, asyncio.Protocol, 100, False)
        assert info.value.errno == errno.EADDRINUSE
        assert "os error" not in str(info.value)
    finally:
        holder.close()


# --------------------------------------------------------------------- #
# 3. create_server's address-family rules                               #
# --------------------------------------------------------------------- #


def test_wildcard_server_serves(loop):
    """create_server(host=None, port) binds every family the host has. On
    a host without IPv6 (this CI's containers) it used to fail outright,
    because the EAFNOSUPPORT skip compared an errno that was None."""

    async def main():
        server = await loop.create_server(asyncio.Protocol, None, 0)
        assert server.sockets
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_create_server_needs_an_address_or_a_socket(loop):
    async def main():
        with pytest.raises(ValueError, match="Neither host/port nor sock were specified"):
            await loop.create_server(asyncio.Protocol)

    loop.run_until_complete(main())


def test_create_server_skips_addresses_that_are_not_available(loop):
    """EADDRNOTAVAIL on one resolved address skips it (bpo-30945)."""
    real_gai = loop.getaddrinfo

    async def fake_gai(host, port, *, family=0, type=0, proto=0, flags=0):
        return [
            # TEST-NET-1: never configured on a host, so bind() fails with
            # EADDRNOTAVAIL.
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.2.1", port)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port)),
        ]

    async def main():
        loop.getaddrinfo = fake_gai
        try:
            server = await loop.create_server(asyncio.Protocol, "example.invalid", 0)
        finally:
            loop.getaddrinfo = real_gai
        assert [s.getsockname()[0] for s in server.sockets] == ["127.0.0.1"]
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_create_server_with_nothing_bindable_names_the_addresses(loop):
    real_gai = loop.getaddrinfo

    async def fake_gai(host, port, *, family=0, type=0, proto=0, flags=0):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.2.1", port))]

    async def main():
        loop.getaddrinfo = fake_gai
        try:
            with pytest.raises(OSError, match=r"could not bind on any address out of \[\('192.0.2.1', 0\)\]"):
                await loop.create_server(asyncio.Protocol, "example.invalid", 0)
        finally:
            loop.getaddrinfo = real_gai

    loop.run_until_complete(main())


def test_create_server_skips_a_family_without_sockets(cade_loop):
    """The core's skip_unavailable reports a family it cannot make a
    socket for as None, and create_server serves on the rest."""
    loop = cade_loop
    real_core = loop._core

    class CoreProxy:
        def __getattr__(self, name):
            return getattr(real_core, name)

        def tcp_listen(self, ip, port, *args):
            assert args[-1] is True  # skip_unavailable
            if ":" in ip:
                return None
            return real_core.tcp_listen(ip, port, *args)

    async def fake_gai(host, port, *, family=0, type=0, proto=0, flags=0):
        return [
            (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::1", port, 0, 0)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port)),
        ]

    loop.getaddrinfo = fake_gai
    loop._core = CoreProxy()
    try:

        async def main():
            server = await loop.create_server(asyncio.Protocol, "localhost", 0)
            assert [s.getsockname()[0] for s in server.sockets] == ["127.0.0.1"]
            server.close()
            await server.wait_closed()

        loop.run_until_complete(main())
    finally:
        loop._core = real_core


# --------------------------------------------------------------------- #
# 4. sock_* watches: a cancelled call must not remove a newer call's    #
# --------------------------------------------------------------------- #


def test_sock_recv_cancel_then_retry_gets_the_data(loop):
    """Cancel a pending sock_recv and issue another on the same socket in
    the same step (CPython's test_sock_client_racing): the second call
    receives the data. It hung: the cancelled call's cleanup removed the
    reader the second call had just installed."""

    async def main():
        a, b = socket.socketpair()
        a.setblocking(False)
        with a, b:
            first = asyncio.ensure_future(loop.sock_recv(a, 100))
            await asyncio.sleep(0)
            first.cancel()
            loop.call_later(0.05, b.send, b"data")
            # In this task, not a wait_for() task: the race needs the new
            # call to register before the cancelled one cleans up.
            async with asyncio.timeout(5):
                assert await loop.sock_recv(a, 100) == b"data"
            assert first.cancelled()

    loop.run_until_complete(main())


def test_sock_recv_into_and_recvfrom_cancel_then_retry(loop):
    async def main():
        a, b = socket.socketpair()
        a.setblocking(False)
        with a, b:
            buf = bytearray(16)
            first = asyncio.ensure_future(loop.sock_recv_into(a, buf))
            await asyncio.sleep(0)
            first.cancel()
            loop.call_later(0.05, b.send, b"into")
            async with asyncio.timeout(5):
                assert await loop.sock_recv_into(a, buf) == 4
            assert bytes(buf[:4]) == b"into"

        u1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        u2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        with u1, u2:
            u1.bind(("127.0.0.1", 0))
            u1.setblocking(False)
            first = asyncio.ensure_future(loop.sock_recvfrom(u1, 100))
            await asyncio.sleep(0)
            first.cancel()
            loop.call_later(0.05, u2.sendto, b"dgram", u1.getsockname())
            async with asyncio.timeout(5):
                data, _addr = await loop.sock_recvfrom(u1, 100)
            assert data == b"dgram"

    loop.run_until_complete(main())


def test_sock_sendall_cancel_then_retry_delivers(loop):
    """The sock_sendall form of the race (a blocked send cancelled, then a
    new one on the same socket)."""

    async def main():
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
        sock.setblocking(False)
        with listener, sock:
            await loop.sock_connect(sock, listener.getsockname())
            server, _ = listener.accept()
            server.setblocking(False)
            with server:
                size = 8192
                while size >= 4:
                    try:
                        while True:
                            sock.send(b" " * size)
                    except BlockingIOError:
                        pass
                    size //= 2
                first = asyncio.ensure_future(loop.sock_sendall(sock, b"hello"))
                await asyncio.sleep(0)
                first.cancel()

                async def recv_all():
                    got = b""
                    while True:
                        chunk = await loop.sock_recv(server, 8192)
                        if not chunk:
                            return got
                        got += chunk.strip()

                reader = asyncio.ensure_future(recv_all())
                async with asyncio.timeout(5):
                    await loop.sock_sendall(sock, b"world")
                sock.shutdown(socket.SHUT_WR)
                assert (await asyncio.wait_for(reader, 5)).endswith(b"world")

    loop.run_until_complete(main())


def test_sock_recv_cancelled_after_close_raises_cancelled_error(loop):
    """Cancelling a sock_recv whose socket was already closed raises
    CancelledError; the cleanup's removal of the dead watch used to raise
    OSError(EBADF) in its place."""

    async def main():
        a, b = socket.socketpair()
        a.setblocking(False)
        task = asyncio.ensure_future(loop.sock_recv(a, 100))
        await asyncio.sleep(0)
        a.close()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        b.close()

    loop.run_until_complete(main())


# --------------------------------------------------------------------- #
# 9. write() while close() waits for the buffer to drain                #
# --------------------------------------------------------------------- #


def test_write_after_close_is_sent_while_draining(loop):
    """close() with output still queued keeps accepting write()s until
    the queue drains, and sends them; cadeloop dropped them."""

    async def main():
        got = bytearray()
        eof = loop.create_future()
        conns = []

        class Sink(asyncio.Protocol):
            def connection_made(self, transport):
                transport.pause_reading()  # let the client's queue build up
                conns.append(transport)

            def data_received(self, data):
                got.extend(data)

            def eof_received(self):
                eof.set_result(None)

        server = await loop.create_server(Sink, "127.0.0.1", 0)
        transport, _ = await loop.create_connection(
            asyncio.Protocol, "127.0.0.1", server.sockets[0].getsockname()[1]
        )
        while not conns:
            await asyncio.sleep(0.01)
        sent = 0
        while transport.get_write_buffer_size() == 0:  # until the kernel is full
            transport.write(b"x" * (1 << 20))
            sent += 1 << 20
        transport.close()
        transport.write(b"TAIL")
        await asyncio.sleep(0.05)
        conns[0].resume_reading()
        await asyncio.wait_for(eof, 10)
        assert len(got) == sent + 4
        assert got.endswith(b"TAIL")
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_resume_writing_can_still_write_after_close(loop):
    """asyncio calls resume_writing before it decides the queue drained,
    so output written there goes out ahead of a pending close. sslproto's
    close_notify under backpressure is written exactly there."""

    async def main():
        got = bytearray()
        eof = loop.create_future()
        conns = []

        class Sink(asyncio.Protocol):
            def connection_made(self, transport):
                transport.pause_reading()
                conns.append(transport)

            def data_received(self, data):
                got.extend(data)

            def eof_received(self):
                eof.set_result(None)

        class Writer(asyncio.Protocol):
            def connection_made(self, transport):
                self.transport = transport
                self.paused = False

            def pause_writing(self):
                self.paused = True

            def resume_writing(self):
                self.transport.write(b"TRAILER")

        server = await loop.create_server(Sink, "127.0.0.1", 0)
        transport, proto = await loop.create_connection(
            Writer, "127.0.0.1", server.sockets[0].getsockname()[1]
        )
        while not conns:
            await asyncio.sleep(0.01)
        while not proto.paused:
            transport.write(b"x" * (1 << 20))
        transport.close()
        await asyncio.sleep(0.05)
        conns[0].resume_reading()
        await asyncio.wait_for(eof, 10)
        assert got.endswith(b"TRAILER")
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_tls_close_notify_survives_write_backpressure(loop):
    """The server closes the TLS session while the client still has
    output queued: the client must deliver all of it and then a
    close_notify (CPython's test_remote_shutdown_receives_trailing_data).
    cadeloop ended the session with a bare FIN instead."""
    import threading

    server_ctx, client_ctx = _tls_contexts()
    total = 32 * 128 * 1024
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    result = {}

    def serve():
        sock, _ = listener.accept()
        sock.settimeout(10)
        incoming, outgoing = ssl.MemoryBIO(), ssl.MemoryBIO()
        tls = server_ctx.wrap_bio(incoming, outgoing, server_side=True)
        try:
            while True:
                try:
                    tls.do_handshake()
                    sock.sendall(outgoing.read())
                    break
                except ssl.SSLWantReadError:
                    sock.sendall(outgoing.read())
                    incoming.write(sock.recv(16384))
            while True:
                try:
                    tls.read(4)
                    break
                except ssl.SSLWantReadError:
                    incoming.write(sock.recv(16384))
            tls.write(b"pong")
            sock.sendall(outgoing.read())
            time.sleep(0.2)
            try:
                tls.unwrap()
            except ssl.SSLWantReadError:
                pass
            sock.sendall(outgoing.read())
            received = 0
            while True:
                try:
                    received += len(tls.read(16384))
                except ssl.SSLWantReadError:
                    chunk = sock.recv(16384)
                    if not chunk:
                        result["server"] = ("EOF before close_notify", received)
                        return
                    incoming.write(chunk)
                except ssl.SSLZeroReturnError:
                    result["server"] = ("close_notify", received)
                    return
        except Exception as exc:  # reported through `result`
            result["server"] = repr(exc)
        finally:
            sock.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()

    async def main():
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", listener.getsockname()[1], ssl=client_ctx, server_hostname="localhost"
        )
        writer.write(b"ping")
        assert await reader.readexactly(4) == b"pong"
        for _ in range(32):
            writer.transport._test__append_write_backlog(b"x" * (128 * 1024))
        assert await asyncio.wait_for(reader.read(), 10) == b""
        writer.close()
        deadline = time.monotonic() + 10
        while "server" not in result and time.monotonic() < deadline:
            await asyncio.sleep(0.02)

    try:
        loop.run_until_complete(main())
    finally:
        thread.join(5)
        listener.close()
    assert result.get("server") == ("close_notify", total)


# --------------------------------------------------------------------- #
# 7. sock= keeps the caller's socket; 18. TransportSocket semantics     #
# --------------------------------------------------------------------- #


def test_create_server_sock_keeps_the_callers_socket(loop):
    """The caller's listening socket stays usable while the server runs,
    `server.sockets` wraps that very socket, and close() closes it. It was
    detached: getsockname() raised EBADF from the start."""

    async def main():
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        sock.listen()
        name = sock.getsockname()
        server = await asyncio.start_server(lambda r, w: w.close(), sock=sock)
        assert sock.getsockname() == name
        assert [s.fileno() for s in server.sockets] == [sock.fileno()]
        reader, writer = await asyncio.open_connection(*name)
        assert await reader.read() == b""
        writer.close()
        server.close()
        await server.wait_closed()
        assert sock.fileno() == -1

    loop.run_until_complete(main())


def test_create_connection_sock_keeps_the_callers_socket(loop):
    async def main():
        lost = loop.create_future()

        class P(asyncio.Protocol):
            def connection_lost(self, exc):
                lost.set_result(exc)

        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
        sock = socket.create_connection(server.sockets[0].getsockname())
        name = sock.getsockname()
        transport, _ = await loop.create_connection(P, sock=sock)
        assert sock.getsockname() == name
        assert transport.get_extra_info("socket").fileno() == sock.fileno()
        transport.close()
        await asyncio.wait_for(lost, 5)
        assert sock.fileno() == -1
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_connect_accepted_socket_keeps_the_callers_socket(loop):
    async def main():
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        client = socket.create_connection(listener.getsockname())
        accepted, _ = listener.accept()
        with listener, client:
            transport, _ = await loop.connect_accepted_socket(asyncio.Protocol, accepted)
            assert accepted.getpeername() == client.getsockname()
            assert transport.get_extra_info("socket").fileno() == accepted.fileno()
            transport.close()
            for _ in range(100):
                if accepted.fileno() == -1:
                    break
                await asyncio.sleep(0.01)
            assert accepted.fileno() == -1

    loop.run_until_complete(main())


def test_datagram_transport_socket_extra_and_ownership(loop):
    """get_extra_info('socket') on a datagram transport (multicast setup
    reaches for it); with sock= it is the caller's socket, which stays
    usable and is closed with the transport."""

    async def main():
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("127.0.0.1", 0))
        lost = loop.create_future()

        class P(asyncio.DatagramProtocol):
            def connection_lost(self, exc):
                lost.set_result(exc)

        transport, _ = await loop.create_datagram_endpoint(P, sock=sock)
        extra = transport.get_extra_info("socket")
        assert isinstance(extra, asyncio.trsock.TransportSocket)
        assert extra.fileno() == sock.fileno() != -1
        extra.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        transport.close()
        await asyncio.wait_for(lost, 5)
        assert sock.fileno() == -1

        transport, _ = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol, local_addr=("127.0.0.1", 0)
        )
        assert isinstance(transport.get_extra_info("socket"), asyncio.trsock.TransportSocket)
        transport.close()

    loop.run_until_complete(main())


def test_extra_socket_is_a_transport_socket(loop):
    """A TransportSocket refuses what would break the transport. A plain
    socket accepted setblocking(True), after which the next large write
    blocked inside writev with the GIL held and froze the process."""

    async def main():
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
        transport, _ = await loop.create_connection(
            asyncio.Protocol, "127.0.0.1", server.sockets[0].getsockname()[1]
        )
        sock = transport.get_extra_info("socket")
        assert isinstance(sock, asyncio.trsock.TransportSocket)
        assert transport.get_extra_info("socket") is sock
        with pytest.raises(ValueError, match="cannot be blocking"):
            sock.setblocking(True)
        with pytest.raises(ValueError):
            sock.settimeout(1.0)
        assert not hasattr(sock, "close")
        assert sock.family == socket.AF_INET
        assert sock.getsockname() == transport.get_extra_info("sockname")
        assert all(isinstance(s, asyncio.trsock.TransportSocket) for s in server.sockets)
        transport.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


# --------------------------------------------------------------------- #
# 11. protocol exceptions, error context, introspection after teardown   #
# --------------------------------------------------------------------- #


async def _pair(loop, client_factory, server_factory=asyncio.Protocol):
    server = await loop.create_server(server_factory, "127.0.0.1", 0)
    transport, protocol = await loop.create_connection(
        client_factory, "127.0.0.1", server.sockets[0].getsockname()[1]
    )
    return server, transport, protocol


def test_data_received_exception_reaches_connection_lost(loop):
    reported = []
    loop.set_exception_handler(lambda _loop, ctx: reported.append(ctx))
    boom = ZeroDivisionError("boom")

    async def main():
        lost = loop.create_future()

        class Boom(asyncio.Protocol):
            def data_received(self, data):
                raise boom

            def connection_lost(self, exc):
                lost.set_result(exc)

        class Greeter(asyncio.Protocol):
            def connection_made(self, transport):
                transport.write(b"hello")

        server, transport, protocol = await _pair(loop, Boom, Greeter)
        assert await asyncio.wait_for(lost, 5) is boom
        (ctx,) = reported
        assert ctx["message"] == "Fatal error: protocol.data_received() call failed."
        assert ctx["exception"] is boom
        assert ctx["transport"] is transport
        assert ctx["protocol"] is protocol
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_eof_received_exception_reaches_connection_lost(loop):
    reported = []
    loop.set_exception_handler(lambda _loop, ctx: reported.append(ctx))
    boom = ValueError("eof")

    async def main():
        lost = loop.create_future()

        class Boom(asyncio.Protocol):
            def eof_received(self):
                raise boom

            def connection_lost(self, exc):
                lost.set_result(exc)

        class Closer(asyncio.Protocol):
            def connection_made(self, transport):
                transport.write_eof()

        server, _transport, _ = await _pair(loop, Boom, Closer)
        assert await asyncio.wait_for(lost, 5) is boom
        assert [c["message"] for c in reported] == [
            "Fatal error: protocol.eof_received() call failed."
        ]
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_oserror_from_a_callback_is_not_reported_but_reaches_connection_lost(loop):
    reported = []
    loop.set_exception_handler(lambda _loop, ctx: reported.append(ctx))
    boom = ConnectionResetError("app-level")

    async def main():
        lost = loop.create_future()

        class Boom(asyncio.Protocol):
            def data_received(self, data):
                raise boom

            def connection_lost(self, exc):
                lost.set_result(exc)

        class Greeter(asyncio.Protocol):
            def connection_made(self, transport):
                transport.write(b"hello")

        server, _transport, _ = await _pair(loop, Boom, Greeter)
        assert await asyncio.wait_for(lost, 5) is boom
        assert reported == []
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_buffered_protocol_gets_no_size_hint_and_empty_buffers_are_fatal(loop):
    reported = []
    loop.set_exception_handler(lambda _loop, ctx: reported.append(ctx))

    async def main():
        hints = []
        lost = loop.create_future()

        class Buffered(asyncio.BufferedProtocol):
            def get_buffer(self, sizehint):
                hints.append(sizehint)
                return bytearray(0)

            def buffer_updated(self, nbytes):
                pass

            def connection_lost(self, exc):
                lost.set_result(exc)

        class Greeter(asyncio.Protocol):
            def connection_made(self, transport):
                transport.write(b"hello")

        server, _transport, _ = await _pair(loop, Buffered, Greeter)
        exc = await asyncio.wait_for(lost, 5)
        assert hints == [-1]
        assert isinstance(exc, RuntimeError)
        assert str(exc) == "get_buffer() returned an empty buffer"
        assert [c["message"] for c in reported] == ["Fatal error: protocol.get_buffer() call failed."]
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_pause_writing_failure_is_reported_with_the_transport(loop):
    reported = []
    loop.set_exception_handler(lambda _loop, ctx: reported.append(ctx))

    async def main():
        conns = []

        class Sink(asyncio.Protocol):
            def connection_made(self, transport):
                transport.pause_reading()
                conns.append(transport)

        class Boom(asyncio.Protocol):
            def pause_writing(self):
                raise RuntimeError("pause")

        server, transport, protocol = await _pair(loop, Boom, Sink)
        for _ in range(64):
            if reported:
                break
            transport.write(b"x" * (1 << 20))
        (ctx,) = reported
        assert ctx["message"] == "protocol.pause_writing() failed"
        assert ctx["transport"] is transport
        assert ctx["protocol"] is protocol
        assert not transport.is_closing()
        transport.abort()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_introspection_during_and_after_connection_lost(loop):
    """In connection_lost the protocol is still the transport's, and the
    peer/local names (and the socket) remain readable afterwards; they all
    read None once the entry was gone."""

    async def main():
        seen = {}
        lost = loop.create_future()

        class P(asyncio.Protocol):
            def connection_made(self, transport):
                self.transport = transport

            def connection_lost(self, exc):
                seen["protocol"] = self.transport.get_protocol()
                seen["peername"] = self.transport.get_extra_info("peername")
                lost.set_result(None)

        server, transport, protocol = await _pair(loop, P)
        peer = transport.get_extra_info("peername")
        local = transport.get_extra_info("sockname")
        assert peer == server.sockets[0].getsockname()
        transport.close()
        await asyncio.wait_for(lost, 5)
        assert seen == {"protocol": protocol, "peername": peer}
        assert transport.get_extra_info("peername") == peer
        assert transport.get_extra_info("sockname") == local
        assert transport.get_extra_info("missing", "dflt") == "dflt"
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_is_reading_is_false_once_closing(loop):
    async def main():
        conns = []

        class Sink(asyncio.Protocol):
            def connection_made(self, transport):
                transport.pause_reading()
                conns.append(transport)

        server, transport, _ = await _pair(loop, asyncio.Protocol, Sink)
        while not conns:
            await asyncio.sleep(0.01)
        while transport.get_write_buffer_size() == 0:
            transport.write(b"x" * (1 << 20))
        assert transport.is_reading()
        transport.close()
        assert not transport.is_reading()
        transport.resume_reading()  # ignored once closing
        assert not transport.is_reading()
        transport.abort()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


# --------------------------------------------------------------------- #
# 19. Server.wait_closed (Python 3.11 semantics)                        #
# --------------------------------------------------------------------- #


def test_wait_closed_waits_for_live_connections(loop):
    """wait_closed() called before close() returns only once every
    accepted connection is gone; it used to return at close()."""

    async def main():
        conns = []

        class Keep(asyncio.Protocol):
            def connection_made(self, transport):
                conns.append(transport)

        server = await loop.create_server(Keep, "127.0.0.1", 0)
        transport, _ = await loop.create_connection(
            asyncio.Protocol, "127.0.0.1", server.sockets[0].getsockname()[1]
        )
        while not conns:
            await asyncio.sleep(0.01)
        waiter = asyncio.ensure_future(server.wait_closed())
        await asyncio.sleep(0.05)
        assert not waiter.done()
        server.close()
        await asyncio.sleep(0.05)
        assert not waiter.done(), "returned with a connection still open"
        conns[0].close()
        await asyncio.wait_for(waiter, 5)
        transport.close()

    loop.run_until_complete(main())


def test_wait_closed_after_close_returns_at_once(loop):
    """3.11's quirk, kept for parity: after close(), wait_closed() does
    not wait for the remaining connections."""

    async def main():
        conns = []

        class Keep(asyncio.Protocol):
            def connection_made(self, transport):
                conns.append(transport)

        server = await loop.create_server(Keep, "127.0.0.1", 0)
        transport, _ = await loop.create_connection(
            asyncio.Protocol, "127.0.0.1", server.sockets[0].getsockname()[1]
        )
        while not conns:
            await asyncio.sleep(0.01)
        server.close()
        await asyncio.wait_for(server.wait_closed(), 1)
        conns[0].close()
        transport.close()

    loop.run_until_complete(main())


# --------------------------------------------------------------------- #
# 10. flow control                                                      #
# --------------------------------------------------------------------- #


class _FlowRecorder(asyncio.Protocol):
    def __init__(self):
        self.events = []

    def pause_writing(self):
        self.events.append("pause")

    def resume_writing(self):
        self.events.append("resume")


def test_large_write_to_an_idle_socket_does_not_pause(loop):
    """asyncio sends at once and pauses only for what the kernel would not
    take. cadeloop counted the whole write until its send was reaped, so
    any write over the high-water mark paused and then resumed."""

    async def main():
        server, transport, protocol = await _pair(loop, _FlowRecorder)
        transport.write(b"x" * (100 * 1024))
        await asyncio.sleep(0.05)
        assert protocol.events == []
        transport.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_write_buffer_limits_follow_asyncio(loop):
    async def main():
        server, transport, protocol = await _pair(loop, _FlowRecorder)
        with pytest.raises(ValueError, match=r"high \(-1\) must be >= low \(-1\) must be >= 0"):
            transport.set_write_buffer_limits(high=-1)
        with pytest.raises(ValueError, match=r"high \(1\) must be >= low \(2\) must be >= 0"):
            transport.set_write_buffer_limits(high=1, low=2)
        transport.set_write_buffer_limits(low=1000)
        assert transport.get_write_buffer_limits() == (1000, 4000)
        transport.set_write_buffer_limits(high=8000)
        assert transport.get_write_buffer_limits() == (2000, 8000)
        transport.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_start_tls_after_a_large_write(loop):
    """CPython's test_start_tls_client_reg_proto_1: a 100 KiB write and
    then start_tls. The eager pause handed SSLProtocol a resume_writing
    for the plain protocol's pause, and sslproto asserts on that."""
    import threading

    server_ctx, client_ctx = _tls_contexts()
    payload = b"1" * (100 * 1024)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    errors = []

    def recv_all(sock, n):
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                break
            buf += chunk
        return buf

    def serve():
        try:
            conn, _ = listener.accept()
            conn.settimeout(10)
            assert len(recv_all(conn, len(payload))) == len(payload)
            tls = server_ctx.wrap_socket(conn, server_side=True)
            tls.sendall(b"O")
            assert len(recv_all(tls, len(payload))) == len(payload)
            tls.close()
        except Exception as exc:  # reported below
            errors.append(exc)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    reported = _quiet(loop)

    async def main():
        on_data = loop.create_future()

        class Client(_FlowRecorder):
            def data_received(self, data):
                if not on_data.done():
                    on_data.set_result(data)

        transport, protocol = await loop.create_connection(Client, *listener.getsockname())
        transport.write(payload)
        assert protocol.events == []  # the kernel took it all: no pause
        tls = await asyncio.wait_for(
            loop.start_tls(transport, protocol, client_ctx, server_hostname="localhost"), 10
        )
        assert await asyncio.wait_for(on_data, 10) == b"O"
        tls.write(payload)
        await asyncio.sleep(0.1)
        tls.close()

    try:
        loop.run_until_complete(main())
    finally:
        thread.join(5)
        listener.close()
    assert errors == []
    assert not [c for c in reported if "AssertionError" in repr(c.get("exception"))]


def test_set_protocol_does_not_carry_a_pause_over(cade_loop):
    """pause_writing/resume_writing come in pairs per protocol: a protocol
    installed with set_protocol() while the old one was paused must not
    be resumed for a pause it never saw. (Kept stricter than asyncio,
    whose selector transport keeps the flag.)"""
    loop = cade_loop

    async def main():
        conns = []

        class Sink(asyncio.Protocol):
            def connection_made(self, transport):
                transport.pause_reading()
                conns.append(transport)

        server, transport, first = await _pair(loop, _FlowRecorder, Sink)
        while not conns:
            await asyncio.sleep(0.01)
        while "pause" not in first.events:
            transport.write(b"x" * (1 << 20))
        second = _FlowRecorder()
        transport.set_protocol(second)
        conns[0].resume_reading()
        for _ in range(200):
            if transport.get_write_buffer_size() == 0:
                break
            await asyncio.sleep(0.01)
        assert first.events == ["pause"]
        assert second.events == []
        transport.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


# --------------------------------------------------------------------- #
# 17. resolution                                                        #
# --------------------------------------------------------------------- #


def test_numeric_hosts_skip_the_resolver(loop):
    """An IP literal needs no getaddrinfo(); each one cost an executor
    round trip (three for this create_server/create_connection/
    create_datagram_endpoint trio, where asyncio and uvloop make none)."""
    calls = []
    real = loop.getaddrinfo

    async def counting(*args, **kwargs):
        calls.append(args)
        return await real(*args, **kwargs)

    loop.getaddrinfo = counting

    async def main():
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        transport, _ = await loop.create_connection(
            asyncio.Protocol, "127.0.0.1", port, local_addr=("127.0.0.1", 0)
        )
        udp, _ = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol, local_addr=("127.0.0.1", 0), remote_addr=("127.0.0.1", port)
        )
        sock = socket.socket()
        sock.setblocking(False)
        with sock:
            await loop.sock_connect(sock, ("127.0.0.1", port))
        udp.close()
        transport.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())
    assert calls == []


def test_default_executor_runs_the_lookups(loop):
    """set_default_executor() governs getaddrinfo/getnameinfo, as in
    asyncio; cadeloop resolved on a private pool that ignored it."""
    import concurrent.futures

    submitted = []

    class Recording(concurrent.futures.ThreadPoolExecutor):
        def submit(self, fn, *args, **kwargs):
            submitted.append(getattr(fn, "__name__", fn))
            return super().submit(fn, *args, **kwargs)

    executor = Recording(max_workers=1)
    loop.set_default_executor(executor)

    async def main():
        await loop.getaddrinfo("localhost", 80, type=socket.SOCK_STREAM)
        await loop.getnameinfo(("127.0.0.1", 80))

    try:
        loop.run_until_complete(main())
    finally:
        executor.shutdown(wait=True)
    assert submitted == ["getaddrinfo", "getnameinfo"]
