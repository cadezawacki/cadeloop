"""Regression tests for the adversarial HTTP/WebSocket audit (H1-H11).

Each test fails on the pre-audit engine and passes after the fix. The
engine runs in a background thread driving ``run_forever`` with a native
listener; clients are raw blocking sockets on the main thread, so the
tests do not depend on an in-loop client being serviced before a
connection is torn down (that timing is exactly what H11 was about).
"""

import base64
import os
import socket
import struct
import threading
import time

import cadeloop
import pytest


class EngineServer:
    """A native HTTP/ASGI listener on its own loop/thread."""

    def __init__(self, app, sweep_interval=None, **listen_kw):
        self.app = app
        self.listen_kw = listen_kw
        self.sweep_interval = sweep_interval
        self.port = None
        self.loop = None
        self.lid = None
        self._ready = threading.Event()
        self._t = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        import asyncio

        lp = cadeloop.new_event_loop()
        asyncio.set_event_loop(lp)
        self.loop = lp
        lp.set_exception_handler(lambda l, c: None)  # keep noise out of pytest
        self.lid, bound, _ = lp._core.http_listen("127.0.0.1", 0, self.app, lp, **self.listen_kw)
        self.port = bound[1]
        if self.sweep_interval:
            def sweep():
                lp._core.http_sweep()
                lp.call_later(self.sweep_interval, sweep)
            lp.call_later(self.sweep_interval, sweep)
        self._ready.set()
        try:
            lp.run_forever()
        finally:
            try:
                lp._core.listener_close(self.lid)
            except Exception:
                pass
            lp.close()

    def run_on_loop(self, fn):
        """Call fn() on the loop thread, wait for it, return its result."""
        box = {}
        done = threading.Event()

        def wrapped():
            try:
                box["r"] = fn(self.loop)
            except Exception as e:  # noqa: BLE001
                box["e"] = e
            finally:
                done.set()

        self.loop.call_soon_threadsafe(wrapped)
        done.wait(5)
        if "e" in box:
            raise box["e"]
        return box.get("r")

    def conn_count(self):
        return self.run_on_loop(lambda lp: lp._core.http_connection_count())

    def __enter__(self):
        self._t.start()
        self._ready.wait(5)
        return self

    def __exit__(self, *a):
        if self.loop is not None:
            self.loop.call_soon_threadsafe(self.loop.stop)
        self._t.join(5)


def connect(port):
    s = socket.create_connection(("127.0.0.1", port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return s


def read_until_close(s, timeout=2.0):
    s.settimeout(timeout)
    out = b""
    while True:
        try:
            d = s.recv(65536)
        except socket.timeout:
            return out, "open"
        except ConnectionResetError:
            return out, "reset"
        if not d:
            return out, "eof"
        out += d


class Reader:
    """Buffered reader over a blocking socket: carries leftover bytes
    across pipelined responses/frames so none are dropped."""

    def __init__(self, s):
        self.s = s
        self.buf = b""

    def _fill(self, timeout):
        self.s.settimeout(timeout)
        d = self.s.recv(65536)
        if not d:
            raise ConnectionError("eof")
        self.buf += d

    def response(self, timeout=2.0):
        while b"\r\n\r\n" not in self.buf:
            self._fill(timeout)
        head, self.buf = self.buf.split(b"\r\n\r\n", 1)
        hdrs = {}
        for line in head.split(b"\r\n")[1:]:
            if b":" in line:
                k, v = line.split(b":", 1)
                hdrs[k.strip().lower()] = v.strip()
        if b"content-length" in hdrs:
            n = int(hdrs[b"content-length"])
            while len(self.buf) < n:
                self._fill(timeout)
            body, self.buf = self.buf[:n], self.buf[n:]
        else:
            body = b""
        return head, hdrs, body

    def ws_frame(self, timeout=2.0):
        def need(n):
            while len(self.buf) < n:
                self._fill(timeout)
        need(2)
        ln = self.buf[1] & 0x7F
        off = 2
        if ln == 126:
            need(4)
            ln = struct.unpack(">H", self.buf[2:4])[0]
            off = 4
        elif ln == 127:
            need(10)
            ln = struct.unpack(">Q", self.buf[2:10])[0]
            off = 10
        need(off + ln)
        op = self.buf[0] & 0x0F
        payload = self.buf[off:off + ln]
        self.buf = self.buf[off + ln:]
        return op, payload


def read_http_response(s, timeout=2.0):
    return Reader(s).response(timeout)


def ws_handshake(path="/ws"):
    key = base64.b64encode(os.urandom(16))
    return (b"GET " + path.encode() + b" HTTP/1.1\r\nHost: h\r\nUpgrade: websocket\r\n"
            b"Connection: Upgrade\r\nSec-WebSocket-Key: " + key + b"\r\n"
            b"Sec-WebSocket-Version: 13\r\n\r\n")


def ws_client_frame(opcode, payload, mask=b"\x11\x22\x33\x44", fin=True):
    n = len(payload)
    out = bytearray([(0x80 if fin else 0) | opcode])
    if n < 126:
        out.append(0x80 | n)
    elif n <= 0xFFFF:
        out.append(0x80 | 126)
        out += struct.pack(">H", n)
    else:
        out.append(0x80 | 127)
        out += struct.pack(">Q", n)
    out += mask
    out += bytes(b ^ mask[i & 3] for i, b in enumerate(payload))
    return bytes(out)


# --------------------------------------------------------------------- #
# H6: receive/send are bound to the request they were handed to          #
# --------------------------------------------------------------------- #

def test_h6_stale_send_does_not_corrupt_the_next_pipelined_response():
    """A send() captured by request 1 and called during request 2 must
    not splice bytes into request 2's response (it did: the chunked body
    of /two carried request 1's leaked bytes)."""
    import asyncio

    stash = {}
    log = []

    async def app(scope, receive, send):
        if scope["type"] != "http":
            return
        p = scope["path"]
        if p == "/one":
            stash["send"] = send
            await receive()
            await send({"type": "http.response.start", "status": 200,
                        "headers": [(b"content-length", b"3")]})
            await send({"type": "http.response.body", "body": b"one"})

            async def leaked():
                await asyncio.sleep(0.2)
                try:
                    await stash["send"]({"type": "http.response.body",
                                         "body": b"LEAK", "more_body": True})
                    log.append("leak-accepted")
                except Exception as e:  # noqa: BLE001
                    log.append(type(e).__name__)
            asyncio.get_running_loop().create_task(leaked())
            return
        # /two streams a chunked body with a gap the leak could land in.
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"TWO", "more_body": True})
        await asyncio.sleep(0.4)
        await send({"type": "http.response.body", "body": b"END", "more_body": False})

    with EngineServer(app) as srv:
        s = connect(srv.port)
        s.sendall(b"GET /one HTTP/1.1\r\nHost: h\r\n\r\nGET /two HTTP/1.1\r\nHost: h\r\n\r\n")
        data, _ = read_until_close(s, 2.0)
        s.close()
    # Request 2's response must be exactly its own chunks, no LEAK spliced in.
    body2 = data.split(b"\r\n\r\n", 2)[-1]
    assert b"LEAK" not in data, data
    assert b"TWO" in body2 and b"END" in body2, data
    assert "leak-accepted" not in log, log  # the stale send was refused


def test_h6_stale_receive_does_not_steal_the_next_requests_body():
    """A receive() captured by request 1 and called during request 2 must
    report disconnect, not consume request 2's body."""
    import asyncio

    stash = {}
    log = []

    async def app(scope, receive, send):
        if scope["type"] != "http":
            return
        if scope["path"] == "/one":
            stash["receive"] = receive
            await receive()

            async def thief():
                # Fire while request 2 is active with its body pending: a
                # tid-keyed cached receive reads req_body straight off the
                # CURRENT request (the theft); an epoch-bound one reports
                # disconnect.
                await asyncio.sleep(0.3)
                m = await stash["receive"]()
                log.append(("thief", m.get("type"), m.get("body")))
            asyncio.get_running_loop().create_task(thief())
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"one"})
            return
        # /two holds its body unread for a window the thief lands in, then
        # reads it -- the thief must not have stolen it.
        await asyncio.sleep(0.5)
        m = await receive()
        log.append(("two", m.get("body")))
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"two"})

    with EngineServer(app) as srv:
        s = connect(srv.port)
        s.sendall(b"GET /one HTTP/1.1\r\nHost: h\r\n\r\n")
        read_http_response(s)
        s.sendall(b"POST /two HTTP/1.1\r\nHost: h\r\nContent-Length: 6\r\n\r\nsecret")
        read_http_response(s)
        time.sleep(0.3)
        s.close()
    assert ("two", b"secret") in log, log
    assert ("thief", "http.disconnect", None) in log, log


# --------------------------------------------------------------------- #
# H2: slowloris -- the idle deadline is not reset by trickled bytes       #
# --------------------------------------------------------------------- #

@pytest.mark.parametrize("opener,trickle", [
    (b"", b"\r\n"),                                                    # pre-request CRLF
    (b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 1000000\r\n\r\n", b"a"),  # body drip
])
def test_h2_trickle_is_bounded_by_the_idle_deadline(opener, trickle):
    async def app(scope, receive, send):
        if scope["type"] != "http":
            return
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    with EngineServer(app, sweep_interval=0.1,
                      request_line_timeout=1.0, keepalive_idle=1.0) as srv:
        s = connect(srv.port)
        if opener:
            s.sendall(opener)
        t0 = time.monotonic()
        closed = False
        # Drip for well past both deadlines; a fixed timer must still fire.
        while time.monotonic() - t0 < 6.0:
            try:
                s.sendall(trickle)
            except OSError:
                closed = True
                break
            time.sleep(0.25)
            s.setblocking(False)
            try:
                if s.recv(1) == b"" or True:
                    # got the 408/close, or EOF
                    pass
            except BlockingIOError:
                s.setblocking(True)
                continue
            except OSError:
                closed = True
                break
            s.setblocking(True)
            closed = True
            break
        s.close()
        assert closed, "connection was never closed -- trickle reset the deadline"
        assert time.monotonic() - t0 < 5.5


# --------------------------------------------------------------------- #
# H4: a disconnected client whose app is parked is reclaimed              #
# --------------------------------------------------------------------- #

def test_h4_disconnected_parked_request_is_reclaimed():
    import asyncio

    async def app(scope, receive, send):
        if scope["type"] != "http":
            return
        # Parks on something OTHER than receive(): never observes the
        # disconnect on its own, so only the sweep can reclaim it.
        await asyncio.Event().wait()

    with EngineServer(app, sweep_interval=0.1,
                      request_line_timeout=1.0, keepalive_idle=0.5) as srv:
        socks = []
        for _ in range(10):
            s = connect(srv.port)
            s.sendall(b"GET /wait HTTP/1.1\r\nHost: h\r\n\r\n")
            socks.append(s)
        time.sleep(0.2)
        assert srv.conn_count() == 10
        for s in socks:
            s.close()  # client gone while the app is still parked
        # Within the idle deadline + a sweep or two, every connection is
        # reclaimed (fd freed, coroutine cancelled).
        deadline = time.monotonic() + 5.0
        while srv.conn_count() > 0 and time.monotonic() < deadline:
            time.sleep(0.1)
        assert srv.conn_count() == 0


# --------------------------------------------------------------------- #
# H5 / H3: post-handshake frames while the upgrade is queued             #
# --------------------------------------------------------------------- #

def test_h5_frame_after_queued_upgrade_is_delivered_intact():
    """A WS upgrade pipelined behind a slow request, with the first client
    frame arriving in a later recv: the frame must reach the app intact
    (the stale llhttp error_pos used to drop or corrupt it)."""
    import asyncio

    got = []

    async def app(scope, receive, send):
        if scope["type"] == "http":
            await asyncio.sleep(0.4)  # keeps the pipelined upgrade queued
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"slow"})
            return
        await receive()
        await send({"type": "websocket.accept"})
        while True:
            m = await receive()
            if m["type"] == "websocket.disconnect":
                return
            got.append(m.get("text"))
            await send({"type": "websocket.send", "text": m["text"]})

    payload = "hello-" + "x" * 300
    with EngineServer(app) as srv:
        s = connect(srv.port)
        s.sendall(b"GET /slow HTTP/1.1\r\nHost: h\r\n\r\n" + ws_handshake("/ws"))
        time.sleep(0.15)  # upgrade is queued behind /slow; parser paused
        s.sendall(ws_client_frame(0x1, payload.encode()))
        rd = Reader(s)
        rd.response()                      # /slow
        head, _, _ = rd.response()         # 101 (no body)
        assert head.startswith(b"HTTP/1.1 101"), head[:40]
        op, data = rd.ws_frame()           # echoed frame
        s.close()
    assert op == 0x1 and data.decode() == payload, (op, data[:40])
    assert got == [payload]


def test_h3_preupgrade_flood_is_bounded():
    """A client that floods more than WS_MAX_MESSAGE before its upgrade is
    even dispatched must have the connection dropped, not grow the heap."""
    import asyncio

    async def app(scope, receive, send):
        if scope["type"] == "http":
            await asyncio.sleep(3.0)  # keep the upgrade queued while we flood
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"slow"})
            return
        await receive()
        await send({"type": "websocket.accept"})

    with EngineServer(app) as srv:
        s = connect(srv.port)
        s.sendall(b"GET /slow HTTP/1.1\r\nHost: h\r\n\r\n" + ws_handshake("/ws"))
        time.sleep(0.1)
        # >1 MiB of frames before the upgrade is served. The bound tears
        # the connection down DURING the flood; the unbounded engine just
        # accumulates and only closes once /slow finishes (3 s later), so a
        # prompt drop is the regression signal (and is checked separately
        # from any memory measurement, which a unit test cannot do).
        t0 = time.monotonic()
        chunk = ws_client_frame(0x2, b"\x00" * 60000)
        dropped_at = None
        try:
            sent = 0
            while sent < 8 * 1024 * 1024 and time.monotonic() - t0 < 2.0:
                s.sendall(chunk)
                sent += len(chunk)
                time.sleep(0.003)
        except OSError:
            dropped_at = time.monotonic() - t0
        if dropped_at is None:
            _, st = read_until_close(s, 1.0)
            if st in ("eof", "reset"):
                dropped_at = time.monotonic() - t0
        s.close()
        assert dropped_at is not None, "flood before dispatch was not bounded"
        assert dropped_at < 2.5, f"dropped too late ({dropped_at:.1f}s): not bounded during the flood"


# --------------------------------------------------------------------- #
# H7 / H8 / H9: response-line + scope + header correctness                #
# --------------------------------------------------------------------- #

def test_h7_malformed_version_error_line_is_http11():
    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    with EngineServer(app) as srv:
        s = connect(srv.port)
        s.sendall(b"GET / HTTP/1.9\r\nHost: h\r\n\r\n")
        data, _ = read_until_close(s, 2.0)
        s.close()
    assert data.startswith(b"HTTP/1.1 400"), data[:40]
    assert not data.startswith(b"HTTP/1.9"), data[:40]


def test_h8_absolute_form_authority_strips_userinfo():
    seen = {}

    async def app(scope, receive, send):
        await receive()
        seen["headers"] = dict(scope["headers"])
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    with EngineServer(app) as srv:
        s = connect(srv.port)
        s.sendall(b"GET http://trusted@evil.example/x HTTP/1.1\r\nHost: ignored\r\n\r\n")
        read_http_response(s)
        s.close()
    assert seen["headers"][b"host"] == b"evil.example", seen["headers"]


def test_h9_non_digit_response_content_length_is_rejected():
    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200,
                    "headers": [(b"content-length", b"+2")]})
        await send({"type": "http.response.body", "body": b"ok"})

    with EngineServer(app) as srv:
        s = connect(srv.port)
        s.sendall(b"GET / HTTP/1.1\r\nHost: h\r\n\r\n")
        data, _ = read_until_close(s, 2.0)
        s.close()
    assert data.startswith(b"HTTP/1.1 500"), data[:40]
    assert b"content-length: +2" not in data.lower(), data[:120]


# --------------------------------------------------------------------- #
# H11: shutdown sends a 1012 close frame to a live WebSocket              #
# --------------------------------------------------------------------- #

def test_h11_shutdown_sends_1012_close_frame():
    """Deterministic: the client is a raw socket off the loop, so reading
    the close frame does not race the drain tearing the connection down."""
    got_disconnect = []

    async def app(scope, receive, send):
        assert scope["type"] == "websocket"
        await receive()
        await send({"type": "websocket.accept"})
        while True:
            m = await receive()
            if m["type"] == "websocket.disconnect":
                got_disconnect.append(m.get("code"))
                return

    with EngineServer(app) as srv:
        s = connect(srv.port)
        s.sendall(ws_handshake("/ws"))
        rd = Reader(s)
        head, _, _ = rd.response()
        assert head.startswith(b"HTTP/1.1 101"), head[:40]
        # Begin shutdown on the loop thread; run_forever keeps turning, so
        # the 1012 is flushed and the app receives its disconnect.
        busy = srv.run_on_loop(lambda lp: lp._core.http_begin_shutdown())
        assert busy == 1, busy
        op, payload = rd.ws_frame()
        s.close()
    assert op == 0x8, op  # close
    assert struct.unpack(">H", payload[:2])[0] == 1012, payload
    # And the app was actually told, rather than seeing a bare TCP close.
    deadline = time.monotonic() + 2.0
    while not got_disconnect and time.monotonic() < deadline:
        time.sleep(0.05)
    assert got_disconnect == [1012], got_disconnect
