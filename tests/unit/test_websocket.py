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


def test_upgrade_headers_reject_crlf_injection(loop):
    """The 101 head is a real HTTP response: a CRLF smuggled through
    websocket.accept's `headers` or `subprotocol` must not forge frames.
    Reported by Codex review on PR #1 (the http.response.start guard did
    not cover this second serialization path)."""

    async def app(scope, receive, send):
        assert (await receive())["type"] == "websocket.connect"
        await send({
            "type": "websocket.accept",
            "headers": [(b"x-echo", b"a\r\nx-injected: yes")],
        })

    lid, port = listen(loop, app)

    async def main():
        _reader, writer, _key, head = await handshake(port)
        writer.close()
        return head

    head = loop.run_until_complete(main())
    assert b"x-injected" not in head.lower(), "header injection reached the wire"
    assert b"101" not in head.split(b"\r\n", 1)[0]
    loop._core.listener_close(lid)


def test_upgrade_subprotocol_rejects_crlf_injection(loop):
    async def app(scope, receive, send):
        assert (await receive())["type"] == "websocket.connect"
        await send({"type": "websocket.accept", "subprotocol": "chat\r\nx-injected: yes"})

    lid, port = listen(loop, app)

    async def main():
        _reader, writer, _key, head = await handshake(port)
        writer.close()
        return head

    head = loop.run_until_complete(main())
    assert b"x-injected" not in head.lower()
    assert b"101" not in head.split(b"\r\n", 1)[0]
    loop._core.listener_close(lid)


def test_valid_upgrade_headers_still_pass(loop):
    """The guard must not reject a legitimate accept with extras."""

    async def app(scope, receive, send):
        assert (await receive())["type"] == "websocket.connect"
        await send({
            "type": "websocket.accept",
            "subprotocol": "chat",
            "headers": [(b"x-app", b"cadeloop")],
        })

    lid, port = listen(loop, app)

    async def main():
        _reader, writer, _key, head = await handshake(port, extra="sec-websocket-protocol: chat\r\n")
        writer.close()
        return head

    head = loop.run_until_complete(main())
    assert b"101" in head.split(b"\r\n", 1)[0]
    assert b"sec-websocket-protocol: chat" in head.lower()
    assert b"x-app: cadeloop" in head.lower()
    loop._core.listener_close(lid)


@pytest.mark.parametrize(
    "key",
    [
        "",                          # empty
        "short",                     # not 24 chars
        "AAAAAAAAAAAAAAAAAAAAAAAA",  # 24 chars but no "==" terminator
        "not-base64-at-all!!!!!==",  # 24 chars, illegal alphabet
        base64.b64encode(os.urandom(8)).decode(),  # 8-byte nonce, not 16
    ],
)
def test_invalid_sec_websocket_key_is_rejected(loop, key):
    """RFC 6455 §4.1: the key is a base64-encoded 16-byte nonce. Accepting
    anything else hands out a 101 to a non-WebSocket client. Reported by
    Codex review on PR #1."""
    lid, port = listen(loop, ws_echo_app)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(
            f"GET /ws HTTP/1.1\r\nhost: x\r\nupgrade: websocket\r\n"
            f"connection: Upgrade\r\nsec-websocket-version: 13\r\n"
            f"sec-websocket-key: {key}\r\n\r\n".encode()
        )
        await writer.drain()
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
        writer.close()
        return head

    head = loop.run_until_complete(main())
    assert b"101" not in head.split(b"\r\n", 1)[0], f"bad key {key!r} was upgraded"
    loop._core.listener_close(lid)


@pytest.mark.parametrize("code", [1004, 1005, 1006, 1015, 999, 5000, 0])
def test_reserved_close_codes_are_rejected(loop, code):
    """RFC 6455 §7.4: 1005/1006/1015 never appear on the wire and 1004 is
    reserved, so a peer must treat a close frame carrying one as a
    protocol error rather than the shutdown the app asked for. Reported by
    Codex review on PR #1."""

    async def app(scope, receive, send):
        assert (await receive())["type"] == "websocket.connect"
        await send({"type": "websocket.accept"})
        await send({"type": "websocket.close", "code": code})

    lid, port = listen(loop, app)

    async def main():
        reader, writer, _key, head = await handshake(port)
        assert b"101" in head.split(b"\r\n", 1)[0]
        frames = []
        try:
            while True:
                frames.append(await asyncio.wait_for(read_server_frame(reader), 5))
        except (asyncio.IncompleteReadError, asyncio.TimeoutError, OSError):
            pass
        writer.close()
        return frames

    frames = loop.run_until_complete(main())
    for _fin, op, payload in frames:
        if op == 0x8 and len(payload) >= 2:
            got = struct.unpack(">H", payload[:2])[0]
            assert got != code, f"reserved code {code} reached the wire"
    loop._core.listener_close(lid)


def test_valid_close_code_still_passes(loop):
    async def app(scope, receive, send):
        assert (await receive())["type"] == "websocket.connect"
        await send({"type": "websocket.accept"})
        await send({"type": "websocket.close", "code": 1001, "reason": "going away"})

    lid, port = listen(loop, app)

    async def main():
        reader, writer, _key, head = await handshake(port)
        _fin, op, payload = await asyncio.wait_for(read_server_frame(reader), 5)
        writer.close()
        return op, payload

    op, payload = loop.run_until_complete(main())
    assert op == 0x8
    assert struct.unpack(">H", payload[:2])[0] == 1001
    assert payload[2:] == b"going away"
    loop._core.listener_close(lid)


def test_pre_accept_flood_is_bounded(loop):
    """An app may do arbitrary work before websocket.accept (auth, a
    database round-trip) and a client is free to stream during that
    window. Buffering it without a cap lets one connection grow
    ws_trailing until the worker dies. Reported by Codex review on PR #1.

    Note the client cannot use handshake() here: the 101 is not sent until
    the app accepts, which is exactly the window under test."""
    release = asyncio.Event()

    async def app(scope, receive, send):
        assert (await receive())["type"] == "websocket.connect"
        await release.wait()  # never accepts while the client floods
        await send({"type": "websocket.accept"})

    lid, port = listen(loop, app)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        key = base64.b64encode(os.urandom(16))
        writer.write(
            f"GET /ws HTTP/1.1\r\nhost: x\r\nupgrade: websocket\r\n"
            f"connection: Upgrade\r\nsec-websocket-version: 13\r\n"
            f"sec-websocket-key: {key.decode()}\r\n\r\n".encode()
        )
        await writer.drain()
        blob = client_frame(0x2, b"z" * 60000)
        try:
            for _ in range(40):  # ~2.4 MiB, well past the 1 MiB cap
                writer.write(blob)
                await writer.drain()
                await asyncio.sleep(0)
        except OSError:
            pass  # the server dropping us mid-flood IS the cap working
            # (Windows reports it as ConnectionAbortedError, POSIX as
            # ConnectionResetError/BrokenPipeError)
        # The cap must have closed the connection rather than buffering on.
        try:
            rest = await asyncio.wait_for(reader.read(), 5)
        except (OSError, asyncio.IncompleteReadError):
            rest = b""  # server dropped us, which is the point
        release.set()
        await asyncio.sleep(0.05)
        writer.close()
        return rest

    rest = loop.run_until_complete(main())
    assert b"101 Switching Protocols" not in rest, "flood was accepted, not capped"
    loop._core.listener_close(lid)


@pytest.mark.parametrize(
    "header", [b"sec-websocket-accept", b"sec-websocket-protocol", b"upgrade", b"connection"]
)
def test_reserved_upgrade_headers_are_rejected(loop, header):
    """The server generates the handshake fields itself; an app adding its
    own produces a 101 with conflicting duplicates that a compliant client
    may reject. Reported by Codex review on PR #1."""

    async def app(scope, receive, send):
        assert (await receive())["type"] == "websocket.connect"
        await send({"type": "websocket.accept", "headers": [(header, b"x")]})

    lid, port = listen(loop, app)

    async def main():
        _reader, writer, _key, head = await handshake(port)
        writer.close()
        return head

    head = loop.run_until_complete(main())
    assert b"101" not in head.split(b"\r\n", 1)[0], head[:60]
    loop._core.listener_close(lid)


def test_sustained_traffic_does_not_stall_the_connection(loop):
    """The inbox budget must be decremented on BOTH delivery paths. An app
    already parked in receive() is woken through WsWake, not through
    HttpReceive.__call__, and that path did not subtract the message it
    delivered — so inbox_bytes only ever grew and reads paused for good
    after ~4 MiB, on a connection whose app had consumed everything.
    Reported by Codex review on PR #1; regression from my own inbox cap.

    The client waits for each echo before sending the next message. That
    is what forces the WsWake path: a flooding client keeps the inbox
    non-empty, so receive() takes the immediate path instead and the bug
    stays hidden."""

    async def app(scope, receive, send):
        assert (await receive())["type"] == "websocket.connect"
        await send({"type": "websocket.accept"})
        while True:
            msg = await receive()
            if msg["type"] == "websocket.disconnect":
                return
            await send({"type": "websocket.send", "bytes": b"ack"})

    lid, port = listen(loop, app)
    payload = b"m" * 60000
    n = 100  # 6 MiB total, well past the 4 MiB inbox budget

    async def main():
        reader, writer, _key, head = await handshake(port)
        assert b"101" in head.split(b"\r\n", 1)[0]
        acked = 0
        for _ in range(n):
            writer.write(client_frame(0x2, payload))
            await writer.drain()
            # Waiting for the ack guarantees the app is parked in
            # receive() with an empty inbox when the next message lands.
            _fin, op, _payload = await asyncio.wait_for(read_server_frame(reader), 5)
            assert op == 0x2
            acked += 1
        writer.close()
        return acked

    acked = loop.run_until_complete(main())
    assert acked == n, f"stalled after {acked} of {n} messages"
    loop._core.listener_close(lid)


def test_accept_rejects_a_subprotocol_the_client_did_not_offer(loop):
    """RFC 6455 4.1: the server may select one of the client's offers or
    none. Sending an unoffered one produced a handshake that looked clean
    on this side and made browsers fail the connection immediately -- a
    disconnect with nothing here to explain it."""

    async def app(scope, receive, send):
        assert (await receive())["type"] == "websocket.connect"
        await send({"type": "websocket.accept", "subprotocol": "superchat"})

    lid, port = listen(loop, app)

    async def main():
        _reader, writer, _key, head = await handshake(
            port, extra="sec-websocket-protocol: chat\r\n"
        )
        writer.close()
        return head

    head = loop.run_until_complete(main())
    assert b"101" not in head.split(b"\r\n", 1)[0], head[:80]
    assert b"superchat" not in head.lower(), head
    loop._core.listener_close(lid)


def test_accept_allows_selecting_none_of_the_offers(loop):
    """Declining every offer is explicitly permitted, and must not be
    confused with selecting an unoffered one."""

    async def app(scope, receive, send):
        assert (await receive())["type"] == "websocket.connect"
        await send({"type": "websocket.accept"})

    lid, port = listen(loop, app)

    async def main():
        _reader, writer, key, head = await handshake(
            port, extra="sec-websocket-protocol: chat, superchat\r\n"
        )
        writer.close()
        return key, head

    key, head = loop.run_until_complete(main())
    assert head.split(b"\r\n", 1)[0].endswith(b"101 Switching Protocols"), head[:80]
    assert expect_accept(key).encode() in head
    assert b"sec-websocket-protocol" not in head.lower(), head
    loop._core.listener_close(lid)


def test_accept_matches_an_offer_from_a_multi_value_header(loop):
    """The offer list the check reads must be the same one the app sees
    in its scope -- otherwise an app selecting straight out of its own
    scope gets rejected."""
    scopes = []

    async def app(scope, receive, send):
        scopes.append(scope["subprotocols"])
        assert (await receive())["type"] == "websocket.connect"
        await send({"type": "websocket.accept", "subprotocol": scope["subprotocols"][1]})

    lid, port = listen(loop, app)

    async def main():
        _reader, writer, _key, head = await handshake(
            port, extra="sec-websocket-protocol: chat,  superchat\r\n"
        )
        writer.close()
        return head

    head = loop.run_until_complete(main())
    assert scopes == [["chat", "superchat"]], scopes
    assert head.split(b"\r\n", 1)[0].endswith(b"101 Switching Protocols"), head[:80]
    assert b"sec-websocket-protocol: superchat" in head.lower(), head
    loop._core.listener_close(lid)
