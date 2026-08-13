"""R-087 WebSockets (M4): RFC 6455 handshake + frames on the native
engine. The client side is hand-rolled here (masked frames, independent
accept-key check via hashlib), so the server implementation is tested
against the RFC, not against itself."""

import asyncio
import base64
import hashlib
import os
import struct

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


def listen(loop, app, **kw):
    lid, bound, _fd = loop._core.http_listen("127.0.0.1", 0, app, loop, **kw)
    return lid, bound[1]


# ---- client-side wire helpers ----------------------------------------- #

WS_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def expect_accept(key: bytes) -> str:
    return base64.b64encode(hashlib.sha1(key + WS_GUID).digest()).decode()


def client_frame(opcode: int, payload: bytes, fin: bool = True) -> bytes:
    mask = os.urandom(4)
    head = bytes([(0x80 if fin else 0) | opcode])
    n = len(payload)
    if n < 126:
        head += bytes([0x80 | n])
    elif n <= 0xFFFF:
        head += bytes([0x80 | 126]) + struct.pack(">H", n)
    else:
        head += bytes([0x80 | 127]) + struct.pack(">Q", n)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return head + mask + masked


async def read_server_frame(reader):
    """Server frames are unmasked."""
    h = await reader.readexactly(2)
    fin = bool(h[0] & 0x80)
    opcode = h[0] & 0x0F
    assert not h[1] & 0x80, "server frames must not be masked"
    n = h[1] & 0x7F
    if n == 126:
        n = struct.unpack(">H", await reader.readexactly(2))[0]
    elif n == 127:
        n = struct.unpack(">Q", await reader.readexactly(8))[0]
    payload = await reader.readexactly(n) if n else b""
    return fin, opcode, payload


async def handshake(port, path="/ws", extra=""):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    key = base64.b64encode(os.urandom(16))
    writer.write(
        f"GET {path} HTTP/1.1\r\nhost: x\r\nupgrade: websocket\r\n"
        f"connection: Upgrade\r\nsec-websocket-version: 13\r\n"
        f"sec-websocket-key: {key.decode()}\r\n{extra}\r\n".encode()
    )
    await writer.drain()
    head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
    return reader, writer, key, head


# ---- apps -------------------------------------------------------------- #


async def ws_echo_app(scope, receive, send):
    assert scope["type"] == "websocket"
    msg = await receive()
    assert msg["type"] == "websocket.connect"
    await send({"type": "websocket.accept"})
    while True:
        msg = await receive()
        if msg["type"] == "websocket.disconnect":
            return
        if msg.get("text") is not None:
            await send({"type": "websocket.send", "text": "echo:" + msg["text"]})
        else:
            await send({"type": "websocket.send", "bytes": b"echo:" + msg["bytes"]})


# ---- tests ------------------------------------------------------------- #


def test_handshake_echo_and_close(loop):
    lid, port = listen(loop, ws_echo_app)

    async def main():
        reader, writer, key, head = await handshake(port)
        status = head.split(b"\r\n", 1)[0]
        assert b"101" in status
        assert expect_accept(key).encode() in head  # independent SHA-1 check
        # text echo
        writer.write(client_frame(0x1, "héllo".encode()))
        await writer.drain()
        fin, op, payload = await asyncio.wait_for(read_server_frame(reader), 5)
        assert (fin, op) == (True, 0x1)
        assert payload.decode() == "echo:héllo"
        # binary echo
        writer.write(client_frame(0x2, b"\x00\x01\x02"))
        await writer.drain()
        _fin, op, payload = await asyncio.wait_for(read_server_frame(reader), 5)
        assert op == 0x2 and payload == b"echo:\x00\x01\x02"
        # close handshake: client close -> server echoes close, then EOF
        writer.write(client_frame(0x8, struct.pack(">H", 1000) + b"done"))
        await writer.drain()
        _fin, op, payload = await asyncio.wait_for(read_server_frame(reader), 5)
        assert op == 0x8 and struct.unpack(">H", payload[:2])[0] == 1000
        rest = await asyncio.wait_for(reader.read(), 5)
        assert rest == b""
        writer.close()

    loop.run_until_complete(main())
    loop._core.listener_close(lid)


def test_ping_gets_pong_and_fragmentation(loop):
    lid, port = listen(loop, ws_echo_app)

    async def main():
        reader, writer, _key, _head = await handshake(port)
        writer.write(client_frame(0x9, b"beat"))  # ping
        await writer.drain()
        _fin, op, payload = await asyncio.wait_for(read_server_frame(reader), 5)
        assert (op, payload) == (0xA, b"beat")
        # fragmented text: "frag" + "mented"
        writer.write(client_frame(0x1, b"frag", fin=False))
        writer.write(client_frame(0x0, b"mented"))
        await writer.drain()
        _fin, op, payload = await asyncio.wait_for(read_server_frame(reader), 5)
        assert (op, payload) == (0x1, b"echo:fragmented")
        writer.close()

    loop.run_until_complete(main())
    loop._core.listener_close(lid)


def test_app_reject_before_accept_is_403(loop):
    async def reject_app(scope, receive, send):
        msg = await receive()
        assert msg["type"] == "websocket.connect"
        await send({"type": "websocket.close", "code": 1008})

    lid, port = listen(loop, reject_app)

    async def main():
        reader, writer, _key, head = await handshake(port)
        assert b"403" in head.split(b"\r\n", 1)[0]
        writer.close()

    loop.run_until_complete(main())
    loop._core.listener_close(lid)


def test_ws_scope_contents_and_subprotocol(loop):
    seen = {}

    async def scope_app(scope, receive, send):
        seen.update(scope)
        await receive()
        await send({"type": "websocket.accept", "subprotocol": "chat"})
        await send({"type": "websocket.close"})

    lid, port = listen(loop, scope_app)

    async def main():
        reader, writer, _key, head = await handshake(
            port, path="/room?x=1", extra="sec-websocket-protocol: chat, superchat\r\n"
        )
        assert b"101" in head.split(b"\r\n", 1)[0]
        assert b"sec-websocket-protocol: chat" in head
        writer.close()

    loop.run_until_complete(main())
    assert seen["type"] == "websocket"
    assert seen["scheme"] == "ws"
    assert seen["path"] == "/room"
    assert seen["query_string"] == b"x=1"
    assert seen["subprotocols"] == ["chat", "superchat"]
    assert "method" not in seen
    loop._core.listener_close(lid)


def test_starlette_websocket_route(loop):
    starlette = pytest.importorskip("starlette")
    from starlette.applications import Starlette
    from starlette.routing import WebSocketRoute

    async def ws(websocket):
        await websocket.accept()
        text = await websocket.receive_text()
        await websocket.send_text(f"st:{text}")
        await websocket.close(code=1000)

    app = Starlette(routes=[WebSocketRoute("/ws", ws)])
    lid, port = listen(loop, app)

    async def main():
        reader, writer, _key, head = await handshake(port)
        assert b"101" in head.split(b"\r\n", 1)[0]
        writer.write(client_frame(0x1, b"ping"))
        await writer.drain()
        _fin, op, payload = await asyncio.wait_for(read_server_frame(reader), 5)
        assert (op, payload) == (0x1, b"st:ping")
        _fin, op, payload = await asyncio.wait_for(read_server_frame(reader), 5)
        assert op == 0x8  # server-initiated close
        writer.close()

    loop.run_until_complete(main())
    loop._core.listener_close(lid)
