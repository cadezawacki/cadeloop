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
