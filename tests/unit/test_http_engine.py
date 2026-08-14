"""M2 native HTTP/1.1 + ASGI engine (R-080..R-086, R-123).

The native listener and the test client share the cadeloop loop: clients
are plain asyncio streams (which also exercises the M1 transport surface).
"""

import asyncio
import json
import os
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


def test_starlette_base_http_middleware(loop):
    """BaseHTTPMiddleware wraps the app call in its own anyio task group
    + memory streams (a distinct code path from a plain route handler) —
    a common real-world compatibility trap per Starlette's own docs."""
    starlette = pytest.importorskip("starlette.applications")
    from starlette.applications import Starlette
    from starlette.middleware import Middleware
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.requests import Request
    from starlette.responses import PlainTextResponse
    from starlette.routing import Route

    class AddHeaderMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next):
            response = await call_next(request)
            response.headers["x-cadeloop"] = "yes"
            return response

    async def hello(request):
        return PlainTextResponse("hi")

    app = Starlette(
        routes=[Route("/hi", hello)],
        middleware=[Middleware(AddHeaderMiddleware)],
    )
    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, b"GET /hi HTTP/1.1\r\nHost: h\r\n\r\n"))
    assert resp.startswith(b"HTTP/1.1 200 OK\r\n"), resp[:200]
    assert b"x-cadeloop: yes" in resp.lower()
    assert _body(resp) == b"hi"
    loop._core.listener_close(lid)


def test_starlette_cors_and_gzip_middleware(loop):
    starlette = pytest.importorskip("starlette.applications")
    from starlette.applications import Starlette
    from starlette.middleware import Middleware
    from starlette.middleware.cors import CORSMiddleware
    from starlette.middleware.gzip import GZipMiddleware
    from starlette.responses import PlainTextResponse
    from starlette.routing import Route

    async def big(request):
        return PlainTextResponse("x" * 4000)  # over GZipMiddleware's min size

    app = Starlette(
        routes=[Route("/big", big)],
        middleware=[
            Middleware(CORSMiddleware, allow_origins=["https://example.com"]),
            Middleware(GZipMiddleware, minimum_size=500),
        ],
    )
    lid, port = listen(loop, app)

    # CORS preflight
    resp = loop.run_until_complete(
        _request(
            port,
            b"OPTIONS /big HTTP/1.1\r\nHost: h\r\n"
            b"Origin: https://example.com\r\n"
            b"Access-Control-Request-Method: GET\r\n\r\n",
        )
    )
    assert resp.startswith(b"HTTP/1.1 200 OK\r\n"), resp[:200]
    assert b"access-control-allow-origin: https://example.com" in resp.lower()

    # gzip'd response, decompressed content matches
    resp = loop.run_until_complete(
        _request(
            port,
            b"GET /big HTTP/1.1\r\nHost: h\r\nOrigin: https://example.com\r\nAccept-Encoding: gzip\r\n\r\n",
        )
    )
    assert b"content-encoding: gzip" in resp.lower()
    import gzip

    assert gzip.decompress(_body(resp)) == b"x" * 4000
    loop._core.listener_close(lid)


def test_starlette_file_response(loop, tmp_path):
    """FileResponse reads the file via anyio.to_thread (Starlette's own
    os.stat + threaded read path) — exercises the same eager-engine/
    anyio interop as the sync-route fix, from a different Starlette
    entry point."""
    starlette = pytest.importorskip("starlette.applications")
    from starlette.applications import Starlette
    from starlette.responses import FileResponse
    from starlette.routing import Route

    payload = os.urandom(200_000)
    f = tmp_path / "blob.bin"
    f.write_bytes(payload)

    async def serve_file(request):
        return FileResponse(str(f), media_type="application/octet-stream")

    app = Starlette(routes=[Route("/file", serve_file)])
    lid, port = listen(loop, app)

    resp = loop.run_until_complete(_request(port, b"GET /file HTTP/1.1\r\nHost: h\r\n\r\n"))
    assert resp.startswith(b"HTTP/1.1 200 OK\r\n"), resp[:200]
    assert _body(resp) == payload
    # The client seeing the last byte doesn't guarantee the server-side
    # AppTask has also finished (anyio's to_thread worker-stop
    # done-callback fires on a later tick) — give the loop one more
    # beat so the anyio worker thread gets stopped before the fixture
    # closes the loop, same pattern as the /bg background-task test
    # above.
    loop.run_until_complete(asyncio.sleep(0.05))
    loop._core.listener_close(lid)


def test_streaming_disconnect_no_spurious_error(loop):
    """A client that disconnects mid-StreamingResponse is normal ASGI
    traffic (SSE clients, tab closes, LB idle timeouts): Starlette's
    disconnect race makes the app coroutine return before sending the
    final chunk (resp != Done), which cadeloop's on_coro_finished used to
    report as 'ASGI application returned without completing the
    response' even though nothing went wrong — real uvicorn logs nothing
    for the identical scenario."""
    starlette = pytest.importorskip("starlette.applications")
    from starlette.applications import Starlette
    from starlette.responses import StreamingResponse
    from starlette.routing import Route

    async def stream(request):
        async def gen():
            yield b"first-chunk"
            for _ in range(50):
                await asyncio.sleep(0.05)
                yield b"more"  # never reached once the client disconnects

        return StreamingResponse(gen(), media_type="text/plain")

    app = Starlette(routes=[Route("/stream", stream)])
    lid, port = listen(loop, app)

    errors = []
    loop.set_exception_handler(lambda l, ctx: errors.append(ctx))

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /stream HTTP/1.1\r\nHost: h\r\n\r\n")
        await writer.drain()
        await reader.readuntil(b"\r\n\r\n")  # head
        await reader.read(64)  # first chunk
        writer.close()
        await writer.wait_closed()
        # Give the server time to notice the disconnect and unwind the
        # generator via Starlette's cancel-the-other-task race.
        await asyncio.sleep(0.5)

    loop.run_until_complete(main())
    assert errors == [], f"spurious error(s) logged for a normal client disconnect: {errors}"
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


def test_fastapi_sync_route(loop):
    """Plain `def` routes/dependencies run via anyio.to_thread.run_sync
    (Starlette's run_in_threadpool) under the eager engine. anyio's
    WorkerThread reads `current_task()._loop` and calls
    `current_task().add_done_callback(...)` on the AppTask driving the
    request — both previously missing, crashing every sync route with
    AttributeError -> HTTP 500."""
    fastapi = pytest.importorskip("fastapi")
    from fastapi import Depends, FastAPI

    app = FastAPI()

    def sync_dependency():
        return "dep-ok"

    @app.get("/sync/{item_id}")
    def read_item_sync(item_id: int, dep: str = Depends(sync_dependency)):
        return {"item_id": item_id, "dep": dep}

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(
        _request(port, b"GET /sync/7 HTTP/1.1\r\nHost: h\r\n\r\n")
    )
    assert resp.startswith(b"HTTP/1.1 200 OK\r\n"), resp[:200]
    assert json.loads(_body(resp)) == {"item_id": 7, "dep": "dep-ok"}
    loop._core.listener_close(lid)


def test_contextvar_isolation_across_requests(loop):
    """AppTask::step_inner previously drove coroutines with no
    PyContext_Enter/Exit boundary, so a contextvar set in one request
    stayed visible to every later (or concurrently interleaved) request
    on the same worker — silent state corruption for anything using
    Sentry/OTel/structlog/correlation-ID-style ContextVar patterns."""
    import contextvars

    cv = contextvars.ContextVar("cadeloop_test_cv", default=None)

    async def cv_app(scope, receive, send):
        await receive()
        seen_before = cv.get()
        cv.set(scope["raw_path"].decode())
        await asyncio.sleep(0)  # also exercise isolation across a suspend/resume
        seen_after = cv.get()
        body = json.dumps({"seen_before": seen_before, "seen_after": seen_after}).encode()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": body})

    lid, port = listen(loop, cv_app)

    async def main():
        first = await _request(port, b"GET /a HTTP/1.1\r\nHost: h\r\n\r\n")
        second = await _request(port, b"GET /b HTTP/1.1\r\nHost: h\r\n\r\n")
        return first, second

    first, second = loop.run_until_complete(main())
    first_data = json.loads(_body(first))
    second_data = json.loads(_body(second))
    assert first_data == {"seen_before": None, "seen_after": "/a"}
    assert second_data == {"seen_before": None, "seen_after": "/b"}, (
        "contextvar leaked from an earlier request: " + repr(second_data)
    )
    loop._core.listener_close(lid)


def test_contextvar_isolation_concurrent_requests(loop):
    """Same as above but genuinely interleaved: two requests in flight at
    once (each suspending mid-request), so a shared ambient context would
    let one request's ContextVar.set() bleed into the other's next step
    while both are still pending — not just across separate connections
    handled back-to-back."""
    import contextvars

    cv = contextvars.ContextVar("cadeloop_test_cv2", default=None)

    async def cv_app(scope, receive, send):
        await receive()
        tag = scope["raw_path"].decode()
        cv.set(tag)
        await asyncio.sleep(0.01)  # let the other in-flight request run
        seen = cv.get()
        body = json.dumps({"tag": tag, "seen": seen}).encode()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": body})

    lid, port = listen(loop, cv_app)

    async def main():
        return await asyncio.gather(
            _request(port, b"GET /x HTTP/1.1\r\nHost: h\r\n\r\n"),
            _request(port, b"GET /y HTTP/1.1\r\nHost: h\r\n\r\n"),
        )

    resp_x, resp_y = loop.run_until_complete(main())
    data_x = json.loads(_body(resp_x))
    data_y = json.loads(_body(resp_y))
    assert data_x == {"tag": "/x", "seen": "/x"}, data_x
    assert data_y == {"tag": "/y", "seen": "/y"}, data_y
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
    popen_kwargs = {}
    if sys.platform == "win32":
        # CTRL_BREAK_EVENT (below) targets a process GROUP; a fresh one
        # keeps the event from also reaching this test process.
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    proc = subprocess.Popen(
        [sys.executable, "-m", "cadeloop", "smokeapp:app", "--port", str(port)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        **popen_kwargs,
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
            # Popen.send_signal(SIGTERM) is TerminateProcess on Windows —
            # uncatchable by design, no graceful path exists there (see
            # add_signal_handler's docstring in loop.py). CTRL_BREAK_EVENT
            # IS catchable, via the R-052 SetConsoleCtrlHandler wiring;
            # this is the realistic stand-in for an external supervisor
            # that speaks that protocol instead of a blind kill.
            os.kill(proc.pid, _import_signal().CTRL_BREAK_EVENT)
            out, _ = proc.communicate(timeout=10)
            assert "LIFESPAN_SHUTDOWN" in out
            assert proc.returncode == 0
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


# --------------------------------------------------------------------- #
# R-080 connection timeouts + R-140 access log (M2 close-out)           #
# --------------------------------------------------------------------- #


def _arm_sweep(loop, interval=0.05):
    """Test-sized version of the facade's timeout sweep timer."""
    stop = {"flag": False}

    def sweep():
        if stop["flag"] or loop.is_closed():
            return
        loop._core.http_sweep()
        loop.call_later(interval, sweep)

    loop.call_later(interval, sweep)
    return lambda: stop.__setitem__("flag", True)


def test_keepalive_idle_timeout_closes(loop):
    lid, port = listen(loop, echo_scope_app, keepalive_idle=0.3, request_line_timeout=5.0)
    cancel = _arm_sweep(loop)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET / HTTP/1.1\r\nhost: x\r\n\r\n")
        await writer.drain()
        await _read_one_response(reader)
        # Keep-alive honored, then the idle window expires -> clean close
        # (EOF, no 408: the client sent nothing wrong).
        t0 = loop.time()
        rest = await asyncio.wait_for(reader.read(), 3.0)
        assert rest == b""
        assert loop.time() - t0 < 2.5
        writer.close()

    loop.run_until_complete(main())
    cancel()
    loop._core.listener_close(lid)


def test_request_head_timeout_408(loop):
    lid, port = listen(loop, echo_scope_app, request_line_timeout=0.3, keepalive_idle=10.0)
    cancel = _arm_sweep(loop)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET / HTT")  # head never completes
        await writer.drain()
        data = await asyncio.wait_for(reader.read(), 3.0)
        assert b"408" in data.split(b"\r\n", 1)[0]
        writer.close()
        # The listener stays healthy for the next connection.
        resp = await _request(port, b"GET /ok HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
        assert b"200" in resp.split(b"\r\n", 1)[0]

    loop.run_until_complete(main())
    cancel()
    loop._core.listener_close(lid)


def test_slowloris_drip_still_times_out(loop):
    # R-080: the head window anchors at head START; drip-fed bytes must
    # not extend it (the classic slowloris hold-open).
    lid, port = listen(loop, echo_scope_app, request_line_timeout=0.4, keepalive_idle=10.0)
    cancel = _arm_sweep(loop)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET / HTTP/1.1\r\n")
        await writer.drain()
        t0 = loop.time()

        async def read_resp():
            return await asyncio.wait_for(reader.read(), 5.0)

        read_task = loop.create_task(read_resp())
        for _ in range(25):  # drip a header byte every 100ms, forever-ish
            if read_task.done():
                break
            try:
                writer.write(b"a")
                await writer.drain()
            except (ConnectionResetError, BrokenPipeError, OSError):
                break
            await asyncio.sleep(0.1)
        data = await read_task
        elapsed = loop.time() - t0
        assert b"408" in data.split(b"\r\n", 1)[0]
        assert elapsed < 1.5, f"drip extended the head window to {elapsed:.2f}s"
        writer.close()

    loop.run_until_complete(main())
    cancel()
    loop._core.listener_close(lid)


def test_access_log_sink(loop):
    records = []
    lid, port = listen(loop, echo_scope_app)
    loop._core.set_access_log(
        lambda peer, method, target, status, dur: records.append(
            (peer, method, target, status, dur)
        )
    )

    async def main():
        resp = await _request(port, b"GET /hello?x=1 HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
        assert b"200" in resp.split(b"\r\n", 1)[0]

    loop.run_until_complete(main())
    loop._core.set_access_log(None)
    assert len(records) == 1
    peer, method, target, status, dur = records[0]
    assert method == "GET"
    assert target == b"/hello?x=1"
    assert status == 200
    assert peer is not None and peer[0] == "127.0.0.1"
    assert dur >= 0.0
    loop._core.listener_close(lid)


def test_http_listen_fd_adopts_existing_listener(loop):
    # The spawn worker model's adopt path (R-090): the engine takes over
    # an already-bound, already-listening socket. Platform-neutral half
    # of the WSADuplicateSocketW handoff.
    ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ls.bind(("127.0.0.1", 0))
    ls.listen(64)
    port = ls.getsockname()[1]
    ls.setblocking(False)
    fd = ls.detach()
    lid, bound, _fd = loop._core.http_listen_fd(fd, echo_scope_app, loop)
    assert bound[1] == port

    async def main():
        resp = await _request(
            port, b"GET /adopted HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n"
        )
        assert b"200" in resp.split(b"\r\n", 1)[0]
        assert b"/adopted" in resp

    loop.run_until_complete(main())
    loop._core.listener_close(lid)


def test_response_header_injection_is_rejected(loop):
    """R-086: an app reflecting unsanitized input into a header must not be
    able to forge the response frame (CRLF injection / response splitting).
    Reported by Codex review on PR #1."""

    async def app(scope, receive, send):
        if scope["type"] != "http":
            return
        await receive()
        await send({
            "type": "http.response.start",
            "status": 200,
            # The classic shape: attacker-controlled value smuggling a
            # header terminator plus a whole second response.
            "headers": [(b"x-echo", b"a\r\nx-injected: yes\r\n\r\nHTTP/1.1 200 OK")],
        })
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    assert b"x-injected" not in resp.lower(), "header injection reached the wire"
    assert resp.count(b"HTTP/1.1") == 1, "response was split"
    assert b"500" in resp.split(b"\r\n", 1)[0]
    loop._core.listener_close(lid)


def test_valid_response_headers_still_pass(loop):
    """The injection guard must not reject ordinary headers — including
    obs-text (>=0x80) bytes, which RFC 7230 permits in field values."""

    async def app(scope, receive, send):
        if scope["type"] != "http":
            return
        await receive()
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [
                (b"x-token-name_123", b"plain value"),
                (b"x-tabbed", b"has\tinternal tab"),
                (b"x-obs-text", "café".encode()),
            ],
        })
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    assert b" 200 " in resp.split(b"\r\n", 1)[0], resp[:120]
    assert b"x-obs-text" in resp.lower()
    loop._core.listener_close(lid)


@pytest.mark.parametrize("status", [204, 304])
def test_bodyless_status_suppresses_body_and_framing(loop, status):
    """RFC 7230 3.3.2: a 204/304 response carries no body and no framing
    headers. Emitting either desynchronises a keep-alive stream, because
    the client starts reading the NEXT response immediately. Reported by
    Codex review on PR #1."""

    async def app(scope, receive, send):
        await receive()
        if scope["path"] == "/empty":
            await send({"type": "http.response.start", "status": status, "headers": []})
            # An app that sends a body anyway must still not put it on the
            # wire — that is exactly what corrupts the stream.
            await send({"type": "http.response.body", "body": b"junk"})
        else:
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"second"})

    lid, port = listen(loop, app)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(
            b"GET /empty HTTP/1.1\r\nHost: h\r\n\r\n"
            b"GET /next HTTP/1.1\r\nHost: h\r\n\r\n"
        )
        await writer.drain()
        first = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
        second = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
        body = await asyncio.wait_for(reader.readexactly(6), 5)
        writer.close()
        return first, second, body

    first, second, body = loop.run_until_complete(main())
    assert str(status).encode() in first.split(b"\r\n", 1)[0]
    assert b"content-length" not in first.lower()
    assert b"transfer-encoding" not in first.lower()
    assert b"junk" not in first
    # The second response is intact, i.e. the stream never desynchronised.
    assert b"200" in second.split(b"\r\n", 1)[0]
    assert body == b"second"
    loop._core.listener_close(lid)


def test_absolute_form_request_target_is_stripped(loop):
    """RFC 7230 5.3.2: servers MUST accept absolute-form targets (proxies
    always send them). Leaving the scheme+authority in `path` misses every
    route. Reported by Codex review on PR #1."""
    lid, port = listen(loop, echo_scope_app)
    resp = loop.run_until_complete(
        _request(port, b"GET http://example.com/deep/path?x=1 HTTP/1.1\r\nHost: h\r\n\r\n")
    )
    payload = json.loads(resp.split(b"\r\n\r\n", 1)[1])
    assert payload["path"] == "/deep/path"
    assert payload["raw_path"] == "/deep/path"
    assert payload["query_string"] == "x=1"
    loop._core.listener_close(lid)


def test_absolute_form_with_empty_path_becomes_root(loop):
    lid, port = listen(loop, echo_scope_app)
    resp = loop.run_until_complete(
        _request(port, b"GET http://example.com HTTP/1.1\r\nHost: h\r\n\r\n")
    )
    payload = json.loads(resp.split(b"\r\n\r\n", 1)[1])
    assert payload["path"] == "/"
    loop._core.listener_close(lid)


def test_buffered_body_survives_client_half_close(loop):
    """A fully-received request's body must reach the app even if the peer
    half-closed before the app called receive(); reporting http.disconnect
    first silently drops data the client did send. Reported by Codex review
    on PR #1."""
    entered = asyncio.Event()
    may_receive = asyncio.Event()
    seen = {}

    async def app(scope, receive, send):
        entered.set()
        await may_receive.wait()
        msg = await receive()
        seen["type"] = msg["type"]
        seen["body"] = msg.get("body", b"")
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"POST / HTTP/1.1\r\nHost: h\r\ncontent-length: 5\r\n\r\nhello")
        await writer.drain()
        await asyncio.wait_for(entered.wait(), 5)
        # Half-close: the request is complete, but the peer is done sending.
        writer.write_eof()
        await asyncio.sleep(0.1)  # let the EOF land in the engine
        may_receive.set()
        data = await asyncio.wait_for(reader.read(), 5)
        writer.close()
        return data

    loop.run_until_complete(main())
    assert seen["type"] == "http.request"
    assert seen["body"] == b"hello"
    loop._core.listener_close(lid)
