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
import signal
import socket
import ssl
import struct
import sys
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


def _bind_failure_text(addr):
    """The OS's own wording for "that address is taken", as the socket
    module reports it -- the text asyncio's bind error message embeds."""
    probe = socket.socket()
    try:
        probe.bind(addr)
    except OSError as e:
        return e.strerror
    finally:
        probe.close()
    raise AssertionError(f"{addr} was expected to be in use")


def _socket_error_text(code):
    """How the socket module words error `code`. Windows socket errors come
    from FormatMessage, with trailing whitespace and the final period
    stripped -- the C runtime's strerror() calls them "Unknown error"."""
    if sys.platform == "win32":
        import ctypes

        return ctypes.FormatError(code).rstrip(" \t\r\n.")
    return os.strerror(code)


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
        # Application-data record header + garbage, under the TLS layer --
        # through a duplicate of the socket: os.write() cannot take a socket
        # on Windows, where a socket handle is not a C runtime descriptor.
        with writer.get_extra_info("socket").dup() as raw:
            raw.send(b"\x17\x03\x03\x00\x20" + b"\x00" * 32)
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
            + _bind_failure_text(("127.0.0.1", port)).lower()
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
            f"error while attempting to bind on address {taken!r}: "
            + _bind_failure_text(taken).lower()
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
        assert exc.strerror == _socket_error_text(errno.ECONNRESET)
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
        # Port 0 gives each family's socket its own ephemeral port, and the
        # sockets come in no particular order (asyncio iterates a set of
        # address infos): dial the IPv4 one's port, not sockets[0]'s.
        port = next(s.getsockname()[1] for s in server.sockets if s.family == socket.AF_INET)
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

    if sys.platform == "win32" and not isinstance(loop, cadeloop.Loop):
        pytest.skip("stdlib's Windows selector loop raises WinError 10038 from select() on the closed socket")

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


# --------------------------------------------------------------------- #
# 20. signal handlers                                                   #
# --------------------------------------------------------------------- #

_posix_only = pytest.mark.skipif(not hasattr(signal, "SIGKILL"), reason="POSIX signals")


@_posix_only
def test_signal_handler_argument_checks(loop):
    """CPython's test_add_signal_handler error checks: SIGKILL is a
    RuntimeError that leaves nothing registered (so removing it answers
    False instead of raising), and remove validates like add."""

    def cb():
        pass

    with pytest.raises(TypeError):
        loop.add_signal_handler("boom", cb)
    with pytest.raises(TypeError):
        loop.remove_signal_handler("boom")
    for bad in (signal.NSIG + 1, 0, -1):
        with pytest.raises(ValueError):
            loop.add_signal_handler(bad, cb)
        with pytest.raises(ValueError):
            loop.remove_signal_handler(bad)
    with pytest.raises(RuntimeError, match=f"sig {int(signal.SIGKILL)} cannot be caught"):
        loop.add_signal_handler(signal.SIGKILL, cb)
    assert loop.remove_signal_handler(signal.SIGKILL) is False


@_posix_only
def test_signal_handler_from_another_thread_is_a_runtime_error(loop):
    import threading

    raised = []

    def worker():
        try:
            loop.add_signal_handler(signal.SIGUSR1, lambda: None)
        except Exception as exc:  # inspected below
            raised.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(5)
    assert len(raised) == 1 and type(raised[0]) is RuntimeError, raised
    assert loop.remove_signal_handler(signal.SIGUSR1) is False


@_posix_only
def test_signal_callback_runs_in_the_context_of_add_signal_handler(loop):
    import contextvars

    var = contextvars.ContextVar("var", default="unset")
    seen = []

    async def main():
        var.set("at add time")
        loop.add_signal_handler(signal.SIGUSR1, lambda: seen.append(var.get()))
        var.set("at signal time")
        os.kill(os.getpid(), signal.SIGUSR1)
        for _ in range(200):
            if seen:
                break
            await asyncio.sleep(0.01)

    try:
        loop.run_until_complete(main())
    finally:
        loop.remove_signal_handler(signal.SIGUSR1)
    assert seen == ["at add time"]


# --------------------------------------------------------------------- #
# 21. smaller API differences                                           #
# --------------------------------------------------------------------- #


def test_sendfile_dispatches_on_the_transport_mode(loop, tmp_path):
    """loop.sendfile() follows the transport's _sendfile_compatible mode:
    a datagram transport is a RuntimeError, not an AttributeError from the
    fallback path."""
    path = tmp_path / "data.bin"
    path.write_bytes(b"x" * 1000)

    async def main():
        udp, _ = await loop.create_datagram_endpoint(asyncio.DatagramProtocol, family=socket.AF_INET)
        with open(path, "rb") as f:
            with pytest.raises(RuntimeError, match="not supported"):
                await loop.sendfile(udp, f)
            assert f.tell() == 0
        udp.close()

    loop.run_until_complete(main())


@pytest.mark.skipif(
    not hasattr(os, "sendfile"),
    reason="the guard belongs to the native path: without os.sendfile (Windows) both loops use "
    "asyncio's fallback, which does not refuse writes",
)
def test_write_during_sendfile_raises(loop, tmp_path):
    """CPython's test_sendfile_prevents_bare_write: bytes written while a
    native sendfile runs would land in the middle of the file."""
    path = tmp_path / "data.bin"
    data = b"0123456789" * 100_000
    path.write_bytes(data)

    async def main():
        received = bytearray()
        done = loop.create_future()

        class Sink(asyncio.Protocol):
            def data_received(self, chunk):
                received.extend(chunk)
                if len(received) >= len(data) and not done.done():
                    done.set_result(None)

        server, transport, _ = await _pair(loop, asyncio.Protocol, Sink)
        started = loop.create_future()

        async def send():
            started.set_result(None)
            with open(path, "rb") as f:
                return await loop.sendfile(transport, f)

        task = asyncio.ensure_future(send())
        await started
        with pytest.raises(RuntimeError, match="sendfile is in progress"):
            transport.write(b"data")
        assert await asyncio.wait_for(task, 10) == len(data)
        await asyncio.wait_for(done, 10)
        assert bytes(received) == data
        transport.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_sock_methods_check_their_socket(loop):
    async def main():
        a, b = socket.socketpair()
        with a, b:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            wrapped = ctx.wrap_socket(socket.socket(), do_handshake_on_connect=False)
            with wrapped:
                with pytest.raises(TypeError, match="SSLSocket"):
                    await loop.sock_recv(wrapped, 1)
            loop.set_debug(True)
            try:
                a.setblocking(True)
                with pytest.raises(ValueError, match="must be non-blocking"):
                    await loop.sock_recv(a, 1)
                with pytest.raises(ValueError, match="must be non-blocking"):
                    await loop.sock_sendall(a, b"x")
            finally:
                loop.set_debug(False)

    loop.run_until_complete(main())


def test_add_reader_checks_its_descriptor(loop):
    """A non-file object is a ValueError, as in asyncio (it was an
    AttributeError/OverflowError from deeper down)."""
    with pytest.raises(ValueError, match="Invalid file object"):
        loop.add_reader(object(), lambda: None)
    with pytest.raises(ValueError, match="Invalid file object"):
        loop.add_writer(object(), lambda: None)
    with pytest.raises(ValueError, match="Invalid file object"):
        loop.remove_reader(object())
    with pytest.raises(ValueError, match="Invalid file object"):
        loop.remove_writer(object())


def test_add_reader_refuses_a_transport_socket(loop):
    """CPython's test_add_reader_or_writer_transport_fd."""

    async def main():
        rsock, wsock = socket.socketpair()
        with rsock, wsock:
            transport, _ = await loop.create_connection(asyncio.Protocol, sock=rsock)
            try:
                for call in (
                    lambda: loop.add_reader(rsock, lambda: None),
                    lambda: loop.add_reader(rsock.fileno(), lambda: None),
                    lambda: loop.remove_reader(rsock),
                    lambda: loop.add_writer(rsock.fileno(), lambda: None),
                    lambda: loop.remove_writer(rsock.fileno()),
                ):
                    with pytest.raises(RuntimeError, match="is used by transport"):
                        call()
            finally:
                transport.close()
                await asyncio.sleep(0.05)

    loop.run_until_complete(main())


def test_udp_sendto_details(loop):
    """3.11 sends no empty datagram; a host name is resolved as the
    socket module's sendto() would; a datagram transport is an
    asyncio.Transport."""

    async def main():
        got = asyncio.Queue()

        class Rx(asyncio.DatagramProtocol):
            def datagram_received(self, data, addr):
                got.put_nowait(data)

        errors = []

        class Tx(asyncio.DatagramProtocol):
            def error_received(self, exc):
                errors.append(exc)

        rx, _ = await loop.create_datagram_endpoint(Rx, local_addr=("127.0.0.1", 0))
        port = rx.get_extra_info("sockname")[1]
        tx, _ = await loop.create_datagram_endpoint(Tx, family=socket.AF_INET)
        assert isinstance(tx, asyncio.Transport)
        tx.sendto(b"", ("127.0.0.1", port))
        tx.sendto(b"first", ("localhost", port))
        assert await asyncio.wait_for(got.get(), 5) == b"first"
        with pytest.raises(TypeError):
            tx.sendto("text", ("127.0.0.1", port))
        tx.sendto(b"x", ("name.invalid", port))
        await asyncio.sleep(0.05)
        assert len(errors) == 1 and isinstance(errors[0], OSError), errors
        tx.close()
        rx.close()

    loop.run_until_complete(main())


def test_datagram_endpoint_argument_checks(loop):
    async def main():
        with pytest.raises(ValueError, match="unexpected address family"):
            await loop.create_datagram_endpoint(asyncio.DatagramProtocol)
        with pytest.raises(TypeError, match="2-tuple is expected"):
            await loop.create_datagram_endpoint(
                asyncio.DatagramProtocol, local_addr=("127.0.0.1", 0, 0, 0)
            )
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        with sock:
            with pytest.raises(ValueError, match="socket modifier keyword arguments"):
                await loop.create_datagram_endpoint(
                    asyncio.DatagramProtocol, sock=sock, local_addr=("127.0.0.1", 0)
                )

    loop.run_until_complete(main())


def test_write_takes_bytes_like_objects_only(loop):
    import array

    async def main():
        got = bytearray()

        class Sink(asyncio.Protocol):
            def data_received(self, data):
                got.extend(data)

        server, transport, _ = await _pair(loop, asyncio.Protocol, Sink)
        with pytest.raises(TypeError, match="bytes-like"):
            transport.write(array.array("b", [1, 2]))
        with pytest.raises(TypeError):
            transport.writelines([b"first", "second"])
        transport.write(memoryview(b"ok"))
        await asyncio.sleep(0.1)
        # writelines() rejected the whole list: "first" was never queued.
        assert bytes(got) == b"ok"
        transport.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_queue_matches_asyncio_queue_construction(cade_loop):
    assert cadeloop.Queue[int] is not None
    fast, ref = cadeloop.Queue(2.5), asyncio.Queue(2.5)
    for _ in range(3):
        fast.put_nowait(1)
        ref.put_nowait(1)
    assert fast.full() and ref.full()
    assert fast.maxsize == ref.maxsize == 2.5
    assert str(cadeloop.Queue()).startswith("<Queue maxsize=0")


# --------------------------------------------------------------------- #
# C10. connection_lost is scheduled, as asyncio's call_soon schedules it #
# --------------------------------------------------------------------- #


def test_connection_lost_runs_in_call_soon_order(loop):
    """abort() and an idle close() schedule connection_lost with
    call_soon in asyncio: it runs after what was scheduled before the
    call and before what is scheduled after it. cadeloop ran it in the
    next tick's network phase, ahead of both."""

    async def main():
        order = []

        class P(asyncio.Protocol):
            def __init__(self, name):
                self.name = name

            def connection_lost(self, exc):
                order.append(f"lost {self.name}")

        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        aborted, _ = await loop.create_connection(lambda: P("abort"), "127.0.0.1", port)
        closed, _ = await loop.create_connection(lambda: P("close"), "127.0.0.1", port)
        loop.call_soon(order.append, "before")
        aborted.abort()
        loop.call_soon(order.append, "between")
        closed.close()
        loop.call_soon(order.append, "after")
        await asyncio.sleep(0.05)
        assert order == ["before", "lost abort", "between", "lost close", "after"]
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_connection_lost_exception_is_reported_like_a_callback(loop):
    """A deferred connection_lost that raises is reported, and the
    socket is still closed."""
    reported = []
    loop.set_exception_handler(lambda _loop, ctx: reported.append(ctx))

    async def main():
        class P(asyncio.Protocol):
            def connection_lost(self, exc):
                raise ZeroDivisionError

        server, transport, _ = await _pair(loop, P)
        sock = transport.get_extra_info("socket")
        transport.abort()
        await asyncio.sleep(0.05)
        assert [type(c.get("exception")) for c in reported] == [ZeroDivisionError]
        assert sock.fileno() == -1
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_datagram_connection_lost_runs_in_call_soon_order(loop):
    async def main():
        order = []

        class P(asyncio.DatagramProtocol):
            def connection_lost(self, exc):
                order.append("lost")

        transport, _ = await loop.create_datagram_endpoint(P, local_addr=("127.0.0.1", 0))
        loop.call_soon(order.append, "before")
        transport.close()
        loop.call_soon(order.append, "after")
        await asyncio.sleep(0.05)
        assert order == ["before", "lost", "after"]

    loop.run_until_complete(main())


# --------------------------------------------------------------------- #
# Corked output at close()/abort(); write()'s order of checks           #
# --------------------------------------------------------------------- #


async def _peer_reads(loop, on_data):
    """Run `on_data(transport)` in a server's data_received; return what
    the peer reads until EOF."""

    class Server(asyncio.Protocol):
        def connection_made(self, transport):
            self.transport = transport

        def data_received(self, data):
            on_data(self.transport)

    server = await loop.create_server(Server, "127.0.0.1", 0)
    reader, writer = await asyncio.open_connection("127.0.0.1", server.sockets[0].getsockname()[1])
    writer.write(b"go")
    try:
        return await asyncio.wait_for(reader.read(), 5)
    finally:
        writer.close()
        server.close()
        await server.wait_closed()


def test_write_then_abort_delivers_the_write(loop):
    """asyncio's write() hands data to the socket at once, so a reply
    written just before abort() still reaches the peer; abort() discards
    only what the kernel would not take. cadeloop corked the write until
    the end of the tick, and abort() threw it away unsent."""

    def write_abort(transport):
        transport.write(b"reply")
        transport.abort()

    def writelines_abort(transport):
        transport.writelines([b"a", b"b"])
        transport.abort()

    async def main():
        assert await _peer_reads(loop, write_abort) == b"reply"
        assert await _peer_reads(loop, writelines_abort) == b"ab"

        got = bytearray()
        lost = loop.create_future()

        class Sink(asyncio.Protocol):
            def data_received(self, data):
                got.extend(data)

            def connection_lost(self, exc):
                lost.set_result(None)

        server, transport, _ = await _pair(loop, asyncio.Protocol, Sink)
        transport.write(b"from-a-task")
        transport.abort()
        await asyncio.wait_for(lost, 5)
        assert bytes(got) == b"from-a-task"
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_close_after_a_sent_write_drops_later_writes(loop):
    """With nothing left unsent, close() closes at once and a later write()
    is dropped, as in asyncio. cadeloop's corked first write made the
    close wait for its queue, and the late write went out with it."""

    def write_close_write(transport):
        transport.write(b"reply")
        transport.close()
        transport.write(b"late")

    async def main():
        assert await _peer_reads(loop, write_close_write) == b"reply"

    loop.run_until_complete(main())


def test_close_after_a_write_runs_connection_lost_in_call_soon_order(loop):
    async def main():
        order = []

        class P(asyncio.Protocol):
            def connection_lost(self, exc):
                order.append("lost")

        server, transport, _ = await _pair(loop, P)
        loop.call_soon(order.append, "before")
        transport.write(b"x")
        transport.close()
        loop.call_soon(order.append, "after")
        await asyncio.sleep(0.05)
        assert order == ["before", "lost", "after"]
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_write_after_write_eof_raises_in_asyncio_order(loop):
    """asyncio checks write_eof() ahead of the empty-write and
    dead-connection shortcuts: an empty write raises too, and so does any
    write once the transport is closed or gone. A write_eof() on a closing
    transport is ignored instead."""

    async def main():
        server, transport, _ = await _pair(loop, asyncio.Protocol)
        transport.write_eof()
        for write in (
            lambda: transport.write(b""),
            lambda: transport.write(b"x"),
            lambda: transport.writelines([]),
        ):
            with pytest.raises(RuntimeError, match=r"Cannot call write\(\) after write_eof\(\)"):
                write()
        transport.close()
        with pytest.raises(RuntimeError, match="after write_eof"):
            transport.write(b"x")
        await asyncio.sleep(0.05)  # connection_lost has run
        with pytest.raises(RuntimeError, match="after write_eof"):
            transport.write(b"x")

        other, _ = await loop.create_connection(
            asyncio.Protocol, "127.0.0.1", server.sockets[0].getsockname()[1]
        )
        other.close()
        other.write_eof()
        other.write(b"x")  # dropped, not refused
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_add_reader_refuses_the_extra_info_socket(loop):
    """_ensure_fd_no_transport for the descriptor get_extra_info('socket')
    reports. A cadeloop transport that owns its descriptor hands out a
    duplicate there, and add_reader() accepted that one."""

    async def main():
        server, transport, _ = await _pair(loop, asyncio.Protocol)
        fd = transport.get_extra_info("socket").fileno()
        with pytest.raises(RuntimeError, match="is used by transport"):
            loop.add_reader(fd, lambda: None)
        with pytest.raises(RuntimeError, match="is used by transport"):
            loop.remove_writer(fd)
        transport.close()
        await asyncio.sleep(0.05)
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_write_buffer_limits_take_any_real_number(loop):
    """asyncio keeps whatever number it is given, so a float limit such as
    1.5 * 2**20 works; cadeloop raised TypeError."""

    async def main():
        server, transport, _ = await _pair(loop, _FlowRecorder)
        transport.set_write_buffer_limits(high=1.5 * 2**20)
        low, high = transport.get_write_buffer_limits()
        assert (int(low), int(high)) == (393216, 1572864)
        with pytest.raises(ValueError, match=r"high \(1.5\) must be >= low \(1.7\) must be >= 0"):
            transport.set_write_buffer_limits(high=1.5, low=1.7)
        with pytest.raises(ValueError, match="must be >= 0"):
            transport.set_write_buffer_limits(high=float("nan"))
        transport.set_write_buffer_limits(high=float("inf"), low=0)
        assert transport.get_write_buffer_limits()[1] > 2**60
        transport.close()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_create_connection_with_only_a_port_dials_the_loopback(loop):
    """base_events resolves when either host or port is given (no host is
    getaddrinfo's loopback); cadeloop wanted both."""

    async def main():
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        transport, _ = await loop.create_connection(asyncio.Protocol, port=port)
        assert transport.get_extra_info("peername")[:2] == ("127.0.0.1", port)
        transport.close()
        with pytest.raises(ValueError, match="host and port was not specified"):
            await loop.create_connection(asyncio.Protocol)
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_sendfile_checks_for_a_closing_transport_first(loop, tmp_path):
    path = tmp_path / "text.txt"
    path.write_text("x")

    async def main():
        server, transport, _ = await _pair(loop, asyncio.Protocol)
        transport.close()
        with open(path) as f:  # text mode is an argument error too
            with pytest.raises(RuntimeError, match="Transport is closing"):
                await loop.sendfile(transport, f)
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


@pytest.mark.skipif(not hasattr(os, "sendfile"), reason="the native path needs os.sendfile")
def test_sendfile_falls_back_when_os_sendfile_cannot_read_the_file(cade_loop):
    """os.sendfile refuses a pipe before sending anything. As in asyncio
    that is SendfileNotAvailableError, and both entry points fall back to
    plain sends; cadeloop raised the OSError, then UnsupportedOperation
    from seeking a file that has no position. (asyncio itself returns 0
    for a pipe without sending: it sizes the transfer by fstat's
    st_size.)"""
    loop = cade_loop

    def pipe_file(data):
        rfd, wfd = os.pipe()
        os.write(wfd, data)
        os.close(wfd)
        return os.fdopen(rfd, "rb")

    async def main():
        a, b = socket.socketpair()
        a.setblocking(False)
        with a, b, pipe_file(b"pipe-data") as f:
            assert await loop.sock_sendfile(a, f) == 9
            assert b.recv(100) == b"pipe-data"
            with pytest.raises(asyncio.SendfileNotAvailableError):
                await loop.sock_sendfile(a, f, fallback=False)

        got = bytearray()
        lost = loop.create_future()

        class Sink(asyncio.Protocol):
            def data_received(self, data):
                got.extend(data)

            def connection_lost(self, exc):
                lost.set_result(None)

        server, transport, _ = await _pair(loop, asyncio.Protocol, Sink)
        with pipe_file(b"through-the-transport") as f:
            assert await loop.sendfile(transport, f) == 21
        transport.close()
        await asyncio.wait_for(lost, 5)
        assert bytes(got) == b"through-the-transport"
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


# ---- callback contexts ---------------------------------------------------- #
# cadeloop keeps no copy of an empty context in the queue: a callback
# scheduled from one runs in an empty context of its own at dispatch. These
# pin down that nothing about it is observable.

_CTX_VAR = __import__("contextvars").ContextVar("_CTX_VAR", default="unset")


def test_a_callbacks_context_changes_stay_its_own(loop):
    """Every callback runs in its own copy of the scheduler's context: a
    set() in one is invisible to the next, whether the scheduling context
    was empty or not, and a value set before scheduling is visible."""
    import contextvars

    out = []

    def setter():
        out.append(_CTX_VAR.get())
        _CTX_VAR.set("set-by-callback")

    loop.call_soon(setter)
    loop.call_soon(lambda: out.append(_CTX_VAR.get()))
    loop.call_later(0, setter)
    loop.call_later(0, lambda: out.append(_CTX_VAR.get()))
    ctx = contextvars.copy_context()
    ctx.run(_CTX_VAR.set, "scheduler")
    ctx.run(loop.call_soon, setter)
    ctx.run(loop.call_soon, lambda: out.append(_CTX_VAR.get()))
    loop.call_soon(loop.stop)
    loop.run_forever()
    assert out == ["unset", "unset", "scheduler", "scheduler", "unset", "unset"]


def test_call_soon_inside_a_callback_copies_that_callbacks_context(loop):
    out = []

    def parent():
        _CTX_VAR.set("parent")
        loop.call_soon(lambda: out.append(_CTX_VAR.get()))

    loop.call_soon(parent)
    loop.call_soon(lambda: out.append(_CTX_VAR.get()))
    loop.call_later(0.05, loop.stop)
    loop.run_forever()
    assert sorted(out) == ["parent", "unset"]


def test_a_reader_keeps_one_context_across_invocations(loop):
    """asyncio runs every callback of one watch in the context its handle
    copied once, so a value a reader sets is still there next time."""
    a, b = socket.socketpair()
    a.setblocking(False)
    seen = []

    def on_read():
        a.recv(16)
        seen.append(_CTX_VAR.get())
        _CTX_VAR.set(f"set-{len(seen)}")
        if len(seen) == 1:
            b.send(b"y")
        else:
            loop.remove_reader(a)
            loop.stop()

    try:
        loop.add_reader(a, on_read)
        b.send(b"x")
        loop.call_later(5, loop.stop)
        loop.run_forever()
    finally:
        a.close()
        b.close()
    assert seen == ["unset", "set-1"]


def test_call_soon_threadsafe_carries_the_producer_threads_context(loop):
    out = []

    def producer(value):
        if value is not None:
            _CTX_VAR.set(value)
        loop.call_soon_threadsafe(lambda: out.append(_CTX_VAR.get()))

    for value in (None, "thread"):
        t = __import__("threading").Thread(target=producer, args=(value,))
        t.start()
        t.join()
    loop.call_later(0.05, loop.stop)
    loop.run_forever()
    assert out == ["unset", "thread"]
