"""R-059 native TLS termination (M4): the engine drives OpenSSL via the
interpreter's own SSLContext.wrap_bio memory-BIO pair from Rust — no
asyncio.sslproto on the server side. The client side of each test uses
the stdlib ssl path, so the server is validated against an independent
implementation."""

import asyncio
import ssl
import struct

import pytest

trustme = pytest.importorskip("trustme")

import cadeloop  # noqa: E402
from test_websocket import client_frame, read_server_frame  # noqa: E402


@pytest.fixture(scope="module")
def certs():
    ca = trustme.CA()
    server_cert = ca.issue_cert("localhost", "127.0.0.1")
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_cert.configure_cert(server_ctx)
    client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ca.configure_trust(client_ctx)
    return server_ctx, client_ctx


@pytest.fixture()
def loop():
    lp = cadeloop.new_event_loop()
    asyncio.set_event_loop(lp)
    yield lp
    asyncio.set_event_loop(None)
    if not lp.is_closed():
        lp.close()


async def scope_echo_app(scope, receive, send):
    if scope["type"] != "http":
        return
    await receive()
    body = f"scheme={scope['scheme']} path={scope['path']}".encode()
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"text/plain")],
        }
    )
    await send({"type": "http.response.body", "body": body})


def listen_tls(loop, app, server_ctx, **kw):
    lid, bound, _fd = loop._core.http_listen(
        "127.0.0.1", 0, app, loop, tls=server_ctx, **kw
    )
    return lid, bound[1]


def test_https_request_and_keepalive(loop, certs):
    server_ctx, client_ctx = certs
    lid, port = listen_tls(loop, scope_echo_app, server_ctx)

    async def main():
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", port, ssl=client_ctx, server_hostname="localhost"
        )
        for i in range(2):  # keep-alive: two requests, one TLS session
            writer.write(f"GET /r{i} HTTP/1.1\r\nhost: localhost\r\n\r\n".encode())
            await writer.drain()
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
            assert b"200" in head.split(b"\r\n", 1)[0]
            clen = int(
                [l for l in head.split(b"\r\n") if l.lower().startswith(b"content-length")][0]
                .split(b":")[1]
            )
            body = await asyncio.wait_for(reader.readexactly(clen), 5)
            assert body == f"scheme=https path=/r{i}".encode()
        writer.close()

    loop.run_until_complete(main())
    loop._core.listener_close(lid)


def test_wss_over_native_tls(loop, certs):
    server_ctx, client_ctx = certs

    async def ws_app(scope, receive, send):
        assert scope["type"] == "websocket"
        assert scope["scheme"] == "wss"
        await receive()
        await send({"type": "websocket.accept"})
        msg = await receive()
        await send({"type": "websocket.send", "text": "tls:" + msg["text"]})
        await send({"type": "websocket.close"})

    lid, port = listen_tls(loop, ws_app, server_ctx)

    async def main():
        import base64
        import os

        reader, writer = await asyncio.open_connection(
            "127.0.0.1", port, ssl=client_ctx, server_hostname="localhost"
        )
        key = base64.b64encode(os.urandom(16)).decode()
        writer.write(
            f"GET /ws HTTP/1.1\r\nhost: localhost\r\nupgrade: websocket\r\n"
            f"connection: Upgrade\r\nsec-websocket-version: 13\r\n"
            f"sec-websocket-key: {key}\r\n\r\n".encode()
        )
        await writer.drain()
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
        assert b"101" in head.split(b"\r\n", 1)[0]
        writer.write(client_frame(0x1, b"hello"))
        await writer.drain()
        _fin, op, payload = await asyncio.wait_for(read_server_frame(reader), 5)
        assert (op, payload) == (0x1, b"tls:hello")
        _fin, op, _payload = await asyncio.wait_for(read_server_frame(reader), 5)
        assert op == 0x8
        writer.close()

    loop.run_until_complete(main())
    loop._core.listener_close(lid)


def test_plaintext_to_tls_port_is_dropped(loop, certs):
    server_ctx, _client_ctx = certs
    lid, port = listen_tls(loop, scope_echo_app, server_ctx)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")  # not a ClientHello
        await writer.drain()
        data = await asyncio.wait_for(reader.read(), 5)
        # Handshake failure: server may emit a TLS alert, never HTTP.
        assert b"HTTP/1.1" not in data
        writer.close()

    loop.run_until_complete(main())
    loop._core.listener_close(lid)


def test_serve_rejects_non_context():
    with pytest.raises(TypeError, match="SSLContext"):
        cadeloop.serve(scope_echo_app, ssl="not-a-context")


def test_large_https_response_is_not_truncated(loop, certs):
    """R-059: the staged plaintext is handed to SSLObject.write, whose
    return value says how much it actually consumed. Discarding that count
    (and treating a retryable SSLWantRead/WriteError as fatal) truncated or
    dropped responses with no error raised anywhere. Reported in the
    consolidated review on PR #1.

    A multi-megabyte body with a deliberately slow reader is the shape that
    puts back-pressure on the BIO."""
    server_ctx, client_ctx = certs
    payload = bytes((i * 7 + 11) % 251 for i in range(4096)) * 512  # 2 MiB, checkable

    async def big_app(scope, receive, send):
        if scope["type"] != "http":
            return
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        # Stream it in chunks so several SSL_write calls are involved.
        for off in range(0, len(payload), 64 * 1024):
            await send({
                "type": "http.response.body",
                "body": payload[off:off + 64 * 1024],
                "more_body": off + 64 * 1024 < len(payload),
            })

    lid, port = listen_tls(loop, big_app, server_ctx)

    async def main():
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", port, ssl=client_ctx, server_hostname="localhost"
        )
        writer.write(b"GET /big HTTP/1.1\r\nhost: localhost\r\n\r\n")
        await writer.drain()
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 15)
        assert b"200" in head.split(b"\r\n", 1)[0]
        assert b"transfer-encoding: chunked" in head.lower()
        body = b""
        while True:
            line = await asyncio.wait_for(reader.readuntil(b"\r\n"), 15)
            size = int(line.strip(), 16)
            chunk = await asyncio.wait_for(reader.readexactly(size + 2), 15)
            if size == 0:
                break
            body += chunk[:size]
            await asyncio.sleep(0)  # slow reader
        writer.close()
        return body

    body = loop.run_until_complete(main())
    assert len(body) == len(payload), f"truncated: {len(body)} of {len(payload)}"
    assert body == payload, "corrupted"
    loop._core.listener_close(lid)
