"""The native TLS protocol for stream transports (crates/pyshim/src/tls.rs).

`Loop(native_tls=True)` puts cadeloop's Rust port of asyncio's sslproto
between an application's protocol and the transport, for
create_connection / create_server / start_tls. What it shares with
sslproto is tested where sslproto's behaviour is: test_asyncio_parity.py
runs its TLS cases on this loop as a third column, test_differential.py
runs generated scripts over it against asyncio, and CPython's own
test_sslproto and test_ssl run against it in CI. This module holds what
is specific to the port: the places it deliberately differs, what it is
for (memory, and the libraries people actually put on it), and the
objects it exposes.
"""

import asyncio
import gc
import os
import socket
import ssl
import threading
import time
import tracemalloc
import warnings
import weakref

import cadeloop
import pytest
from cadeloop import _core

trustme = pytest.importorskip("trustme")


@pytest.fixture(scope="module")
def contexts():
    ca = trustme.CA()
    cert = ca.issue_cert("localhost", "127.0.0.1")
    server_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    cert.configure_cert(server_ctx)
    client_ctx = ssl.create_default_context()
    ca.configure_trust(client_ctx)
    return server_ctx, client_ctx


@pytest.fixture()
def loop():
    lp = cadeloop.Loop(native_tls=True)
    asyncio.set_event_loop(lp)
    yield lp
    asyncio.set_event_loop(None)
    if not lp.is_closed():
        lp.close()


class _Echo(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.transport.write(data)


async def _echo_server(loop, server_ctx, factory=_Echo):
    server = await loop.create_server(factory, "127.0.0.1", 0, ssl=server_ctx)
    return server, server.sockets[0].getsockname()[1]


async def _connect(port, client_ctx):
    """A TLS stream to the local server: (reader, writer)."""
    return await asyncio.open_connection("127.0.0.1", port, ssl=client_ctx, server_hostname="localhost")


async def _dial(loop, factory, port, client_ctx):
    """A TLS connection to the local server: (transport, protocol)."""
    kw = {"ssl": client_ctx, "server_hostname": "localhost"}
    return await loop.create_connection(factory, "127.0.0.1", port, **kw)


# --------------------------------------------------------------------- #
# what the loop hands out                                               #
# --------------------------------------------------------------------- #


def test_native_tls_is_opt_in_and_the_environment_can_turn_it_on(monkeypatch):
    monkeypatch.delenv("CADELOOP_NATIVE_TLS", raising=False)  # CI runs this suite with it set, too
    lp = cadeloop.Loop()
    assert lp._native_tls is False
    lp.close()
    monkeypatch.setenv("CADELOOP_NATIVE_TLS", "1")
    lp = cadeloop.Loop()
    assert lp._native_tls is True
    lp.close()
    # An explicit argument wins over the environment.
    lp = cadeloop.Loop(native_tls=False)
    assert lp._native_tls is False
    lp.close()


def test_both_ends_get_the_native_transport_and_its_extra_info(loop, contexts):
    server_ctx, client_ctx = contexts

    async def main():
        made = loop.create_future()

        class Server(_Echo):
            def connection_made(self, transport):
                super().connection_made(transport)
                made.set_result(transport)

        server, port = await _echo_server(loop, server_ctx, Server)
        reader, writer = await _connect(port, client_ctx)
        for transport, context in ((writer.transport, client_ctx), (await made, server_ctx)):
            assert type(transport) is _core.SSLTransport
            assert transport.get_extra_info("sslcontext") is context
            assert isinstance(transport.get_extra_info("ssl_object"), ssl.SSLObject)
            assert transport.get_extra_info("cipher")[1].startswith("TLS")
            assert transport.get_extra_info("compression") is None
            # What the SSL layer does not know, the transport under it does.
            assert transport.get_extra_info("peername")[0] == "127.0.0.1"
            assert transport.get_extra_info("socket").family.name == "AF_INET"
            assert transport.get_extra_info("no-such-thing", "dflt") == "dflt"
            assert transport._start_tls_compatible is True
            assert not transport.can_write_eof()
            with pytest.raises(NotImplementedError):
                transport.write_eof()
            assert transport.get_write_buffer_limits() == (128 * 1024, 512 * 1024)
            assert transport.get_read_buffer_limits() == (64 * 1024, 256 * 1024)
            assert transport.is_reading() and not transport.is_closing()
        assert writer.transport.get_extra_info("peercert")["subject"]
        writer.write(b"ping")
        assert await reader.readexactly(4) == b"ping"
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


# --------------------------------------------------------------------- #
# the data path                                                         #
# --------------------------------------------------------------------- #


@pytest.mark.parametrize("size", [1, 100, 16383, 16384, 16385, 70_000, 1 << 20, 5 << 20])
def test_echo_of_every_size_is_intact(loop, contexts, size):
    server_ctx, client_ctx = contexts
    payload = os.urandom(size)

    async def main():
        server, port = await _echo_server(loop, server_ctx)
        reader, writer = await _connect(port, client_ctx)
        writer.write(payload)
        await writer.drain()
        assert await asyncio.wait_for(reader.readexactly(size), 20) == payload
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_many_records_in_one_segment_and_one_record_in_many(loop, contexts):
    """After a read that returned data, the port does not ask the SSL
    object again when there is nothing for it to find. Two ways that
    could go wrong: stopping while whole records are still waiting (many
    small writes arriving together), and mishandling a record that is
    still incomplete (one big write arriving in pieces)."""
    server_ctx, client_ctx = contexts

    async def main():
        got = bytearray()
        done = loop.create_future()
        total = 2000 * 7 + 3 * 16000

        class Sink(asyncio.Protocol):
            def data_received(self, data):
                got.extend(data)
                if len(got) >= total and not done.done():
                    done.set_result(None)

        server, port = await _echo_server(loop, server_ctx, Sink)
        # A raw socket under a hand-driven SSLObject, so the test decides
        # how the ciphertext is cut into segments.
        raw = socket.socket()
        raw.setblocking(False)
        await loop.sock_connect(raw, ("127.0.0.1", port))
        inbio, outbio = ssl.MemoryBIO(), ssl.MemoryBIO()
        obj = client_ctx.wrap_bio(inbio, outbio, server_hostname="localhost")
        while True:
            try:
                obj.do_handshake()
                break
            except ssl.SSLWantReadError:
                await loop.sock_sendall(raw, outbio.read())
                inbio.write(await loop.sock_recv(raw, 65536))
        await loop.sock_sendall(raw, outbio.read())
        # 2000 seven-byte records, sent as one piece.
        expected = bytearray()
        for i in range(2000):
            chunk = b"%07d" % i
            obj.write(chunk)
            expected += chunk
        await loop.sock_sendall(raw, outbio.read())
        # Three 16000-byte records, each sent 1000 bytes at a time.
        for i in range(3):
            chunk = bytes([65 + i]) * 16000
            obj.write(chunk)
            expected += chunk
            wire = outbio.read()
            for off in range(0, len(wire), 1000):
                await loop.sock_sendall(raw, wire[off : off + 1000])
                await asyncio.sleep(0)
        await asyncio.wait_for(done, 10)
        assert bytes(got) == bytes(expected)
        raw.close()
        server.close()
        await server.wait_closed()

    loop.set_exception_handler(lambda *_: None)  # the raw client leaves without a close_notify
    loop.run_until_complete(main())


@pytest.mark.parametrize("how", ["bytearray", "memoryview"])
def test_a_buffered_protocol_gets_its_data_whatever_get_buffer_returns(loop, contexts, how):
    """sslproto fills a BufferedProtocol's buffer through `buf[offset:]`.
    For a bytearray that slice is a copy, so every TLS record after the
    first of a read lands in a temporary, and the application is told its
    buffer holds data it never got: on CPython 3.11-3.13 the bytearray
    case of this test receives stale bytes. The port reads through a
    memoryview and does not have the bug."""
    server_ctx, client_ctx = contexts
    payload = os.urandom(1 << 20)

    async def main():
        done = loop.create_future()

        class Sink(asyncio.BufferedProtocol):
            def __init__(self):
                self.buf = bytearray(65536)
                self.got = bytearray()

            def get_buffer(self, sizehint):
                return self.buf if how == "bytearray" else memoryview(self.buf)

            def buffer_updated(self, nbytes):
                self.got += self.buf[:nbytes]
                if len(self.got) >= len(payload) and not done.done():
                    done.set_result(self.got)

        server, port = await _echo_server(loop, server_ctx, Sink)
        _, writer = await _connect(port, client_ctx)
        writer.write(payload)
        await writer.drain()
        assert bytes(await asyncio.wait_for(done, 20)) == payload
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_tls_inside_tls(loop, contexts):
    """start_tls() over a transport that is itself TLS -- what an HTTPS
    request through an HTTPS proxy is (aiohttp does exactly this)."""
    server_ctx, client_ctx = contexts

    async def main():
        class InnerEcho(asyncio.Protocol):
            """start_tls() does not call connection_made: the transport is
            given to the protocol by whoever upgraded the connection, and
            data can arrive before that."""

            def __init__(self):
                self.transport = None
                self.early = []

            def data_received(self, data):
                if self.transport is None:
                    self.early.append(data)
                else:
                    self.transport.write(data)

        class Upgrading(asyncio.Protocol):
            """Outer TLS server that starts an inner TLS session on "GO"."""

            def connection_made(self, transport):
                self.transport = transport

            def data_received(self, data):
                assert data == b"GO"
                self.transport.write(b"OK")
                loop.create_task(self.upgrade())

            async def upgrade(self):
                inner = InnerEcho()
                inner.transport = await loop.start_tls(self.transport, inner, server_ctx, server_side=True)
                for data in inner.early:
                    inner.transport.write(data)

        server, port = await _echo_server(loop, server_ctx, Upgrading)
        reader, writer = await _connect(port, client_ctx)
        writer.write(b"GO")
        assert await reader.readexactly(2) == b"OK"
        outer = writer.transport
        inner_reader = asyncio.StreamReader()
        inner_protocol = asyncio.StreamReaderProtocol(inner_reader)
        inner = await loop.start_tls(outer, inner_protocol, client_ctx, server_hostname="localhost")
        assert type(inner) is _core.SSLTransport and inner is not outer
        assert inner.get_extra_info("ssl_object") is not outer.get_extra_info("ssl_object")
        payload = os.urandom(200_000)
        inner.write(payload)
        assert await asyncio.wait_for(inner_reader.readexactly(len(payload)), 10) == payload
        inner.close()
        await asyncio.sleep(0.1)
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


# --------------------------------------------------------------------- #
# memory                                                                #
# --------------------------------------------------------------------- #


def _python_bytes_per_connection(native, contexts, connections=40):
    """What the interpreter allocates for one idle TLS connection (both
    ends of it), measured over `connections` of them."""
    server_ctx, client_ctx = contexts
    lp = cadeloop.Loop(native_tls=native)
    asyncio.set_event_loop(lp)

    async def main():
        server = await lp.create_server(_Echo, "127.0.0.1", 0, ssl=server_ctx)
        port = server.sockets[0].getsockname()[1]

        async def connect():
            reader, writer = await _connect(port, client_ctx)
            writer.write(b"x")
            await reader.readexactly(1)
            return writer

        warm = [await connect() for _ in range(4)]
        gc.collect()
        tracemalloc.start()
        before = tracemalloc.get_traced_memory()[0]
        writers = [await connect() for _ in range(connections)]
        gc.collect()
        used = tracemalloc.get_traced_memory()[0] - before
        tracemalloc.stop()
        for w in warm + writers:
            w.close()
        await asyncio.sleep(0.1)
        server.close()
        await server.wait_closed()
        return used / connections

    try:
        return lp.run_until_complete(main())
    finally:
        asyncio.set_event_loop(None)
        lp.close()


def test_a_tls_connection_no_longer_costs_a_quarter_megabyte_each_end(contexts):
    """sslproto keeps a 256 KiB receive buffer, zero-filled, for every
    connection. With a client and a server end in this process that is
    over half a megabyte a connection; the port keeps no buffer at all."""
    stdlib = _python_bytes_per_connection(False, contexts)
    native = _python_bytes_per_connection(True, contexts)
    assert stdlib > 2 * 256 * 1024, stdlib  # the reference really is that large
    assert native < 64 * 1024, native


# --------------------------------------------------------------------- #
# closing                                                               #
# --------------------------------------------------------------------- #


def test_close_from_another_thread_shuts_the_session_down_on_the_loop_thread(loop, contexts):
    """A StreamWriter collected on a worker thread calls close() there.
    The TLS shutdown -- and the protocol callbacks it leads to -- belong
    on the loop's thread (ADR-36)."""
    server_ctx, client_ctx = contexts

    async def main():
        loop_thread = threading.get_ident()
        lost_on = loop.create_future()
        server_saw_eof = loop.create_future()

        class Client(asyncio.Protocol):
            def connection_lost(self, exc):
                lost_on.set_result((threading.get_ident(), exc))

        class Server(_Echo):
            def eof_received(self):
                server_saw_eof.set_result(None)

        server, port = await _echo_server(loop, server_ctx, Server)
        transport, _ = await _dial(loop, Client, port, client_ctx)
        closer = threading.Thread(target=lambda: (time.sleep(0.05), transport.close()))
        closer.start()
        thread, exc = await asyncio.wait_for(lost_on, 5)
        closer.join()
        assert thread == loop_thread and exc is None
        await asyncio.wait_for(server_saw_eof, 5)  # a close_notify arrived: a clean TLS close
        assert transport.is_closing()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(main())


def test_abort_from_another_thread(loop, contexts):
    server_ctx, client_ctx = contexts

    async def main():
        loop_thread = threading.get_ident()
        lost_on = loop.create_future()

        class Client(asyncio.Protocol):
            def connection_lost(self, exc):
                lost_on.set_result((threading.get_ident(), exc))

        server, port = await _echo_server(loop, server_ctx)
        transport, _ = await _dial(loop, Client, port, client_ctx)
        aborter = threading.Thread(target=lambda: (time.sleep(0.05), transport.abort()))
        aborter.start()
        thread, exc = await asyncio.wait_for(lost_on, 5)
        aborter.join()
        assert thread == loop_thread and exc is None
        server.close()
        await server.wait_closed()

    loop.set_exception_handler(lambda *_: None)  # the server's end sees the reset
    loop.run_until_complete(main())


def test_an_unclosed_transport_warns_when_collected(loop, contexts):
    _, client_ctx = contexts

    # A protocol that was made and never connected: its transport exists
    # (sslproto makes it up front) and nothing ever closes it.
    ssl_protocol = _core.SSLProtocol(loop, asyncio.Protocol(), client_ctx, None, server_hostname="localhost")
    transport = ssl_protocol._app_transport
    assert not transport.is_closing()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        del ssl_protocol, transport
        gc.collect()
    assert any(
        issubclass(w.category, ResourceWarning) and "unclosed transport" in str(w.message) for w in caught
    ), [str(w.message) for w in caught]


def test_a_connection_and_its_protocols_are_collected(loop, contexts):
    """Protocol, TLS protocol and TLS transport refer to each other in a
    ring. Once the connection is gone, nothing may be left of it."""
    server_ctx, client_ctx = contexts

    class Client(asyncio.Protocol):
        def connection_made(self, transport):
            self.transport = transport  # the ring: protocol -> transport -> ... -> protocol

    async def main():
        server, port = await _echo_server(loop, server_ctx)
        transport, protocol = await _dial(loop, Client, port, client_ctx)
        refs = [weakref.ref(protocol), weakref.ref(transport), weakref.ref(transport._ssl_protocol)]
        transport.close()
        await asyncio.sleep(0.1)
        server.close()
        await server.wait_closed()
        return refs

    refs = loop.run_until_complete(main())
    loop.run_until_complete(asyncio.sleep(0))
    gc.collect()
    assert [r() for r in refs] == [None, None, None]


def test_a_protocol_that_never_connected_is_collected(loop, contexts):
    """Made, handed its transport, and dropped before any connection:
    the ring is there already and only the collector can break it."""
    _, client_ctx = contexts
    ssl_protocol = _core.SSLProtocol(loop, asyncio.Protocol(), client_ctx, None, server_hostname="localhost")
    transport = ssl_protocol._app_transport
    refs = [weakref.ref(ssl_protocol), weakref.ref(transport)]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ResourceWarning)  # it is unclosed, and says so
        del ssl_protocol, transport
        gc.collect()
    assert [r() for r in refs] == [None, None]


# --------------------------------------------------------------------- #
# the libraries people put on it                                        #
# --------------------------------------------------------------------- #


def test_aiohttp_client_and_server_over_tls(loop, contexts):
    aiohttp = pytest.importorskip("aiohttp")
    from aiohttp import web

    server_ctx, client_ctx = contexts

    async def main():
        async def hello(request):
            body = await request.read()
            answer = {"path": request.path, "echo": len(body), "tls": request.secure}
            return web.json_response(answer | {"via": type(request.transport).__name__})

        app = web.Application(client_max_size=8 << 20)
        app.router.add_route("*", "/{tail:.*}", hello)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=server_ctx)
        await site.start()
        port = runner.addresses[0][1]
        async with aiohttp.ClientSession() as session:
            # Keep-alive: several requests on one TLS connection, one of
            # them with a body large enough to need flow control.
            for i in range(5):
                async with session.get(f"https://localhost:{port}/item/{i}", ssl=client_ctx) as resp:
                    assert resp.status == 200
                    assert await resp.json() == {
                        "path": f"/item/{i}", "echo": 0, "tls": True, "via": "SSLTransport",
                    }
            upload = os.urandom(3 << 20)
            async with session.post(f"https://localhost:{port}/upload", data=upload, ssl=client_ctx) as r:
                assert (await r.json())["echo"] == len(upload)
            # A certificate the client does not trust is refused.
            with pytest.raises(aiohttp.ClientConnectorCertificateError):
                async with session.get(f"https://localhost:{port}/", ssl=ssl.create_default_context()):
                    pass
        await runner.cleanup()

    loop.set_exception_handler(lambda *_: None)  # the refused handshake, server side
    loop.run_until_complete(main())


def test_websockets_client_and_server_over_tls(loop, contexts):
    websockets = pytest.importorskip("websockets")
    server_ctx, client_ctx = contexts

    async def main():
        async def echo(connection):
            async for message in connection:
                await connection.send(message)

        async with websockets.serve(echo, "127.0.0.1", 0, ssl=server_ctx) as server:
            port = server.sockets[0].getsockname()[1]
            async with websockets.connect(f"wss://localhost:{port}", ssl=client_ctx) as ws:
                for message in ("hello", b"\x00\x01binary", "x" * 200_000, os.urandom(500_000)):
                    await ws.send(message)
                    assert await asyncio.wait_for(ws.recv(), 10) == message

    loop.run_until_complete(main())


def test_uvicorn_serving_https_on_the_native_transport(loop, contexts, tmp_path):
    uvicorn = pytest.importorskip("uvicorn")
    aiohttp = pytest.importorskip("aiohttp")
    _, client_ctx = contexts
    ca = trustme.CA()
    cert = ca.issue_cert("localhost", "127.0.0.1")
    certfile, keyfile = tmp_path / "cert.pem", tmp_path / "key.pem"
    cert.private_key_pem.write_to_path(str(keyfile))
    with open(certfile, "wb") as f:
        for blob in cert.cert_chain_pems:
            f.write(blob.bytes())
    trusting = ssl.create_default_context()
    ca.configure_trust(trusting)

    async def app(scope, receive, send):
        assert scope["type"] == "http" and scope["scheme"] == "https"
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-length", b"2")]})
        await send({"type": "http.response.body", "body": b"ok"})

    async def main():
        config = uvicorn.Config(
            app, host="127.0.0.1", port=0, log_level="error", loop="asyncio", lifespan="off",
            ssl_certfile=str(certfile), ssl_keyfile=str(keyfile),
        )
        server = uvicorn.Server(config)
        task = loop.create_task(server.serve())
        for _ in range(300):
            if server.started:
                break
            await asyncio.sleep(0.01)
        assert server.started
        port = server.servers[0].sockets[0].getsockname()[1]
        async with aiohttp.ClientSession() as session:
            for _ in range(3):
                async with session.get(f"https://localhost:{port}/", ssl=trusting) as resp:
                    assert resp.status == 200 and await resp.read() == b"ok"
        server.should_exit = True
        await asyncio.wait_for(task, 10)

    loop.run_until_complete(main())
