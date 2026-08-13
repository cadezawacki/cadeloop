"""M2 native HTTP/1.1 + ASGI engine (R-080..R-086, R-123).

The native listener and the test client share the cadeloop loop: clients
are plain asyncio streams (which also exercises the M1 transport surface).
"""

import asyncio
import json
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


def listen(loop, app, **kw):
    lid, bound, _fd = loop._core.http_listen("127.0.0.1", 0, app, loop, **kw)
    return lid, bound[1]


async def echo_scope_app(scope, receive, send):
    """Reflects the scope + request body as JSON."""
    req = await receive()
    body = json.dumps(
        {
            "method": scope["method"],
            "path": scope["path"],
            "raw_path": scope["raw_path"].decode(),
            "query_string": scope["query_string"].decode(),
            "http_version": scope["http_version"],
            "scheme": scope["scheme"],
            "root_path": scope["root_path"],
            "asgi": scope["asgi"],
            "client_is_tuple": isinstance(scope["client"], tuple),
            "server_is_tuple": isinstance(scope["server"], tuple),
            "headers": [[k.decode(), v.decode()] for k, v in scope["headers"]],
            "body": req["body"].decode("latin-1"),
            "more_body": req.get("more_body", False),
        }
    ).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json")],
        }
    )
    await send({"type": "http.response.body", "body": body})


async def _request(port, raw, read_all=False, timeout=5.0):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(raw)
    await writer.drain()
    if read_all:
        data = await asyncio.wait_for(reader.read(), timeout)
    else:
        data = await asyncio.wait_for(_read_one_response(reader), timeout)
    writer.close()
    return data


async def _read_one_response(reader):
    head = await reader.readuntil(b"\r\n\r\n")
    headers = _parse_headers(head)
    if "content-length" in headers:
        body = await reader.readexactly(int(headers["content-length"]))
        return head + body
    if headers.get("transfer-encoding") == "chunked":
        body = b""
        while True:
            line = await reader.readuntil(b"\r\n")
            size = int(line.strip(), 16)
            chunk = await reader.readexactly(size + 2)
            body += line + chunk
            if size == 0:
                return head + body
    return head


def _parse_headers(head):
    lines = head.decode("latin-1").split("\r\n")[1:]
    return {
        k.strip().lower(): v.strip()
        for k, v in (l.split(":", 1) for l in lines if ":" in l)
    }


def _body(resp):
    return resp.split(b"\r\n\r\n", 1)[1]


# --------------------------------------------------------------------- #
# scope correctness (R-081)                                             #
# --------------------------------------------------------------------- #


def test_scope_fields(loop):
    lid, port = listen(loop, echo_scope_app)
    resp = loop.run_until_complete(
        _request(port, b"GET /a%20b/c?x=1&y=2 HTTP/1.1\r\nHost: h\r\nX-Custom: v\r\n\r\n")
    )
    assert resp.startswith(b"HTTP/1.1 200 OK\r\n")
    data = json.loads(_body(resp))
    assert data["method"] == "GET"
    assert data["path"] == "/a b/c"  # percent-decoded
    assert data["raw_path"] == "/a%20b/c"  # raw
    assert data["query_string"] == "x=1&y=2"
    assert data["http_version"] == "1.1"
    assert data["scheme"] == "http"
    assert data["asgi"]["version"] == "3.0"
    assert data["client_is_tuple"] and data["server_is_tuple"]
    assert ["host", "h"] in data["headers"]  # names lower-cased, bytes pairs
    assert ["x-custom", "v"] in data["headers"]
    headers = _parse_headers(resp.split(b"\r\n\r\n", 1)[0])
    assert headers["server"] == "cadeloop"
    assert "GMT" in headers["date"]
    loop._core.listener_close(lid)


def test_request_body_delivery(loop):
    lid, port = listen(loop, echo_scope_app)
    resp = loop.run_until_complete(
        _request(
            port,
            b"POST /p HTTP/1.1\r\nHost: h\r\nContent-Length: 11\r\n\r\nhello world",
        )
    )
    data = json.loads(_body(resp))
    assert data["method"] == "POST"
    assert data["body"] == "hello world"
    assert data["more_body"] is False
    loop._core.listener_close(lid)


def test_chunked_request_body_decoded(loop):
    lid, port = listen(loop, echo_scope_app)
    resp = loop.run_until_complete(
        _request(
            port,
            b"POST /c HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n"
            b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n",
        )
    )
    assert json.loads(_body(resp))["body"] == "hello world"
    loop._core.listener_close(lid)


# --------------------------------------------------------------------- #
# keep-alive & pipelining (R-085)                                       #
# --------------------------------------------------------------------- #


def test_keepalive_reuse(loop):
    lid, port = listen(loop, echo_scope_app)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        for i in range(5):
            writer.write(f"GET /r{i} HTTP/1.1\r\nHost: h\r\n\r\n".encode())
            await writer.drain()
            resp = await asyncio.wait_for(_read_one_response(reader), 5)
            assert json.loads(_body(resp))["path"] == f"/r{i}"
        writer.close()

    loop.run_until_complete(main())
    loop._core.listener_close(lid)


def test_pipelined_burst_in_order(loop):
    lid, port = listen(loop, echo_scope_app)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        burst = b"".join(
            f"GET /n{i} HTTP/1.1\r\nHost: h\r\n\r\n".encode() for i in range(20)
        )
        writer.write(burst)
        await writer.drain()
        for i in range(20):
            resp = await asyncio.wait_for(_read_one_response(reader), 5)
            assert json.loads(_body(resp))["path"] == f"/n{i}"  # strict order
        writer.close()

    loop.run_until_complete(main())
    loop._core.listener_close(lid)


def test_connection_close_honored(loop):
    async def app(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"connection", b"close")],
            }
        )
        await send({"type": "http.response.body", "body": b"bye"})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(
        _request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n", read_all=True)
    )
    assert _body(resp) == b"bye"  # read() hit EOF -> server closed
    loop._core.listener_close(lid)


# --------------------------------------------------------------------- #
# response framing (R-084)                                              #
# --------------------------------------------------------------------- #


def test_streaming_response_chunked(loop):
    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        for part in (b"one", b"two", b"three"):
            await send({"type": "http.response.body", "body": part, "more_body": True})
        await send({"type": "http.response.body", "body": b""})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    headers = _parse_headers(resp.split(b"\r\n\r\n", 1)[0])
    assert headers["transfer-encoding"] == "chunked"
    assert b"3\r\none\r\n" in resp and b"5\r\nthree\r\n" in resp
    assert resp.endswith(b"0\r\n\r\n")
    loop._core.listener_close(lid)


def test_streaming_with_content_length_not_chunked(loop):
    async def app(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-length", b"6")],
            }
        )
        await send({"type": "http.response.body", "body": b"abc", "more_body": True})
        await send({"type": "http.response.body", "body": b"def"})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    headers = _parse_headers(resp.split(b"\r\n\r\n", 1)[0])
    assert "transfer-encoding" not in headers
    assert _body(resp) == b"abcdef"
    loop._core.listener_close(lid)


def test_head_body_suppressed(loop):
    async def app(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-length", b"5")],
            }
        )
        await send({"type": "http.response.body", "body": b"hello"})

    lid, port = listen(loop, app)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"HEAD / HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
        await writer.drain()
        resp = await asyncio.wait_for(reader.read(), 5)
        writer.close()
        return resp

    resp = loop.run_until_complete(main())
    head, body = resp.split(b"\r\n\r\n", 1)
    assert b"content-length: 5" in head.lower()
    assert body == b""  # HEAD: headers only, no payload bytes
    loop._core.listener_close(lid)


def test_app_supplied_date_and_server_replaced(loop):
    async def app(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"server", b"impostor"), (b"date", b"yesterday")],
            }
        )
        await send({"type": "http.response.body", "body": b"x"})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    headers = _parse_headers(resp.split(b"\r\n\r\n", 1)[0])
    assert headers["server"] == "cadeloop"
    assert headers["date"] != "yesterday" and "GMT" in headers["date"]
    loop._core.listener_close(lid)


# --------------------------------------------------------------------- #
# error paths (R-086)                                                   #
# --------------------------------------------------------------------- #


def test_malformed_request_400(loop):
    lid, port = listen(loop, echo_scope_app)
    resp = loop.run_until_complete(
        _request(port, b"GARBAGE\r\n\r\n", read_all=True)
    )
    assert resp.startswith(b"HTTP/1.1 400 Bad Request")
    loop._core.listener_close(lid)


def test_uri_limit_414(loop):
    lid, port = listen(loop, echo_scope_app, max_url=32)
    resp = loop.run_until_complete(
        _request(port, b"GET /" + b"a" * 100 + b" HTTP/1.1\r\nHost: h\r\n\r\n", read_all=True)
    )
    assert resp.startswith(b"HTTP/1.1 414 ")
    loop._core.listener_close(lid)


def test_header_limit_431(loop):
    lid, port = listen(loop, echo_scope_app, max_headers=3)
    raw = b"GET / HTTP/1.1\r\n" + b"".join(
        f"H{i}: v\r\n".encode() for i in range(10)
    ) + b"\r\n"
    resp = loop.run_until_complete(_request(port, raw, read_all=True))
    assert resp.startswith(b"HTTP/1.1 431 ")
    loop._core.listener_close(lid)


def test_body_limit_413(loop):
    lid, port = listen(loop, echo_scope_app, max_body=8)
    resp = loop.run_until_complete(
        _request(
            port,
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 100\r\n\r\n" + b"z" * 100,
            read_all=True,
        )
    )
    assert resp.startswith(b"HTTP/1.1 413 ")
    loop._core.listener_close(lid)


def test_app_exception_500(loop):
    async def app(scope, receive, send):
        raise ValueError("boom")

    lid, port = listen(loop, app)
    seen = []
    loop.set_exception_handler(lambda lp, ctx: seen.append(ctx))
    resp = loop.run_until_complete(
        _request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n", read_all=True)
    )
    assert resp.startswith(b"HTTP/1.1 500 Internal Server Error")
    assert any("ValueError" in repr(ctx) for ctx in seen)
    loop._core.listener_close(lid)


def test_incomplete_response_is_500(loop):
    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        # returns without ever sending a body message

    lid, port = listen(loop, app)
    loop.set_exception_handler(lambda lp, ctx: None)
    resp = loop.run_until_complete(
        _request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n", read_all=True)
    )
    # Headers were never flushed (head is buffered until the first body
    # chunk), so the server can still answer 500 — but a hard close is also
    # conformant; accept either.
    assert resp == b"" or resp.startswith(b"HTTP/1.1 500")
    loop._core.listener_close(lid)


def test_bad_asgi_message_type_rejected(loop):
    errors = []

    async def app(scope, receive, send):
        try:
            await send({"type": "websocket.send"})
        except RuntimeError as e:
            errors.append(str(e))
            raise

    lid, port = listen(loop, app)
    loop.set_exception_handler(lambda lp, ctx: None)
    loop.run_until_complete(
        _request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n", read_all=True)
    )
    assert errors and "websocket.send" in errors[0]
    loop._core.listener_close(lid)


# --------------------------------------------------------------------- #
# suspension / eager driver (R-056)                                     #
# --------------------------------------------------------------------- #


def test_app_suspends_on_sleep_and_resumes(loop):
    async def app(scope, receive, send):
        await asyncio.sleep(0.02)  # forces AppTask suspension via a timer
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"slept"})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    assert _body(resp) == b"slept"
    loop._core.listener_close(lid)


def test_concurrent_connections_with_suspension(loop):
    async def app(scope, receive, send):
        await asyncio.sleep(0.02)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": scope["path"].encode()})

    lid, port = listen(loop, app)

    async def one(i):
        resp = await _request(port, f"GET /c{i} HTTP/1.1\r\nHost: h\r\n\r\n".encode())
        assert _body(resp) == f"/c{i}".encode()

    loop.run_until_complete(asyncio.gather(*(one(i) for i in range(16))))
    loop._core.listener_close(lid)


def test_non_eager_stdlib_task_path(loop):
    saw_task = []

    async def app(scope, receive, send):
        saw_task.append(asyncio.current_task() is not None)
        await asyncio.sleep(0.01)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"task"})

    lid, port = listen(loop, app, eager=False)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    assert _body(resp) == b"task"
    assert saw_task == [True]  # §16: real asyncio.Task identity
    loop._core.listener_close(lid)


def test_receive_resolves_disconnect(loop):
    events = []
    done = None

    async def app(scope, receive, send):
        first = await receive()
        events.append(first["type"])
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})
        second = await receive()  # must block until the client goes away
        events.append(second["type"])
        done.set()

    lid, port = listen(loop, app)

    async def main():
        nonlocal done
        done = asyncio.Event()
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET / HTTP/1.1\r\nHost: h\r\n\r\n")
        await writer.drain()
        await asyncio.wait_for(_read_one_response(reader), 5)
        assert events == ["http.request"]  # second receive() is parked
        writer.close()
        await asyncio.wait_for(done.wait(), 5)
        assert events == ["http.request", "http.disconnect"]

    loop.run_until_complete(main())
    loop._core.listener_close(lid)


# --------------------------------------------------------------------- #
# framework interop (R-123)                                             #
# --------------------------------------------------------------------- #


def test_starlette_routes_and_streaming(loop):
    starlette = pytest.importorskip("starlette.applications")
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse, StreamingResponse
    from starlette.routing import Route

    ran_background = []

    async def hello(request):
        return JSONResponse({"q": request.query_params.get("q"), "p": request.path_params.get("name")})

    async def stream(request):
        async def gen():
            for i in range(3):
                yield f"part{i}".encode()

        return StreamingResponse(gen(), media_type="text/plain")

    async def bg(request):
        from starlette.background import BackgroundTask

        async def work():
            ran_background.append(True)

        return JSONResponse({"ok": True}, background=BackgroundTask(work))

    app = Starlette(routes=[
        Route("/hello/{name}", hello),
        Route("/stream", stream),
        Route("/bg", bg),
    ])
    lid, port = listen(loop, app)

    resp = loop.run_until_complete(
        _request(port, b"GET /hello/world?q=1 HTTP/1.1\r\nHost: h\r\n\r\n")
    )
    assert json.loads(_body(resp)) == {"q": "1", "p": "world"}

    resp = loop.run_until_complete(_request(port, b"GET /stream HTTP/1.1\r\nHost: h\r\n\r\n"))
    assert b"part0" in resp and b"part2" in resp

    resp = loop.run_until_complete(_request(port, b"GET /bg HTTP/1.1\r\nHost: h\r\n\r\n"))
    assert json.loads(_body(resp)) == {"ok": True}
    loop.run_until_complete(asyncio.sleep(0.05))
    assert ran_background == [True]
    loop._core.listener_close(lid)


def test_fastapi_route(loop):
    fastapi = pytest.importorskip("fastapi")
    from fastapi import FastAPI

    app = FastAPI()

    @app.get("/items/{item_id}")
    async def read_item(item_id: int, q: str | None = None):
        return {"item_id": item_id, "q": q}

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(
        _request(port, b"GET /items/42?q=x HTTP/1.1\r\nHost: h\r\n\r\n")
    )
    assert json.loads(_body(resp)) == {"item_id": 42, "q": "x"}
    loop._core.listener_close(lid)


# --------------------------------------------------------------------- #
# serve() + lifespan + CLI (R-081, R-101)                               #
# --------------------------------------------------------------------- #


def test_serve_end_to_end_with_lifespan(tmp_path):
    """Full stack in a subprocess: CLI -> serve() -> lifespan -> requests
    -> SIGTERM -> clean shutdown."""
    import subprocess
    import time
    import urllib.request

    app_py = tmp_path / "smokeapp.py"
    app_py.write_text(
        "import sys\n"
        "events = []\n"
        "async def app(scope, receive, send):\n"
        "    if scope['type'] == 'lifespan':\n"
        "        while True:\n"
        "            msg = await receive()\n"
        "            if msg['type'] == 'lifespan.startup':\n"
        "                scope['state']['ready'] = 'yes'\n"
        "                await send({'type': 'lifespan.startup.complete'})\n"
        "            elif msg['type'] == 'lifespan.shutdown':\n"
        "                print('LIFESPAN_SHUTDOWN', flush=True)\n"
        "                await send({'type': 'lifespan.shutdown.complete'})\n"
        "                return\n"
        "    else:\n"
        "        await receive()\n"
        "        body = scope['state'].get('ready', 'no').encode()\n"
        "        await send({'type': 'http.response.start', 'status': 200, 'headers': []})\n"
        "        await send({'type': 'http.response.body', 'body': body})\n"
    )
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    import os
    env = dict(os.environ)
    root = str(tmp_path)
    pkg = os.path.join(os.path.dirname(__file__), "..", "..", "python")
    env["PYTHONPATH"] = os.pathsep.join([root, os.path.abspath(pkg), env.get("PYTHONPATH", "")])
    proc = subprocess.Popen(
        [sys.executable, "-m", "cadeloop", "smokeapp:app", "--port", str(port)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        deadline = time.time() + 10
        last_err = None
        while time.time() < deadline:
            try:
                resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=1)
                break
            except Exception as e:  # noqa: BLE001 — startup poll
                last_err = e
                time.sleep(0.1)
        else:
            proc.kill()
            raise AssertionError(f"server never came up: {last_err}; out={proc.stdout.read()}")
        assert resp.read() == b"yes"  # lifespan state reached the scope
        if sys.platform == "win32":
            # send_signal(SIGTERM) is TerminateProcess on Windows — no
            # graceful path exists to assert until the R-052
            # SetConsoleCtrlHandler work (M4). Serving + state above is
            # the Windows contract for this test.
            proc.kill()
            proc.communicate(timeout=10)
        else:
            proc.send_signal(_import_signal().SIGTERM)
            out, _ = proc.communicate(timeout=10)
            assert "LIFESPAN_SHUTDOWN" in out
            assert proc.returncode == 0
    finally:
        if proc.poll() is None:
            proc.kill()


def _import_signal():
    import signal

    return signal
