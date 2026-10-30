"""M2 native HTTP/1.1 + ASGI engine (R-080..R-086, R-123).

The native listener and the test client share the cadeloop loop: clients
are plain asyncio streams (which also exercises the M1 transport surface).
"""

import asyncio
import json
import os
import socket
import sys

import cadeloop
import pytest


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


async def _request(port, raw, read_all=False, timeout=5.0, host="127.0.0.1"):
    reader, writer = await asyncio.open_connection(host, port)
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
            if size == 0:
                # The zero chunk introduces an optional trailer section
                # closed by a blank line -- readexactly(2) here read the
                # first two bytes of a trailer instead.
                body += line
                while True:
                    tline = await reader.readuntil(b"\r\n")
                    body += tline
                    if tline == b"\r\n":
                        break
                break
            chunk = await reader.readexactly(size + 2)
            body += line + chunk
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


def test_parse_error_waits_for_inflight_response(loop):
    """A malformed pipelined request must be answered in its pipeline
    position. Enqueueing the 400 the moment the parse fails let the
    client read it as the response to an earlier valid request that was
    still being served (or spliced into that response's unfinished
    body). Reported on PR #1."""
    release = asyncio.Event()

    async def app(scope, receive, send):
        await release.wait()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"first"})

    lid, port = listen(loop, app)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        burst = b"GET /ok HTTP/1.1\r\nHost: h\r\n\r\nGARBAGE\r\n\r\n"
        writer.write(burst)
        await writer.drain()
        # Release the app only once the server has consumed the malformed
        # bytes too, so slow delivery cannot mask the reordering.
        while loop._core.stats()["bytes_received"] < len(burst):
            await asyncio.sleep(0.01)
        release.set()
        data = await asyncio.wait_for(reader.read(), 5)
        writer.close()
        return data

    data = loop.run_until_complete(main())
    assert data.startswith(b"HTTP/1.1 200 OK\r\n"), data[:64]
    at = data.find(b"HTTP/1.1 400 ")
    assert at > 0, data
    first = data[:at]
    headers = _parse_headers(first.split(b"\r\n\r\n", 1)[0])
    assert headers["content-length"] == "5"
    assert first.endswith(b"first"), first
    loop._core.listener_close(lid)


def test_net_error_hook_raising_fatal_stops_the_loop(loop):
    """An exception handler that raises KeyboardInterrupt on an ASGI
    app failure must stop the loop (CPython re-raises the two fatal
    exceptions out of call_exception_handler), not be demoted to an
    unraisable warning while the 500 goes out as if nothing happened.
    Reported on PR #1."""

    async def app(scope, receive, send):
        raise ValueError("boom")

    def handler(lp, ctx):
        raise KeyboardInterrupt

    loop.set_exception_handler(handler)
    lid, port = listen(loop, app)
    try:
        with pytest.raises(KeyboardInterrupt):
            loop.run_until_complete(
                _request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n", read_all=True)
            )
    finally:
        # The interrupted request's futures are collected later, and a
        # never-retrieved exception is reported through the handler from
        # their __del__ -- where a raising handler can only produce an
        # unraisable-exception warning in whatever test runs next.
        loop.set_exception_handler(None)
    loop._core.listener_close(lid)


def test_cancelled_receive_waiters_are_pruned(loop):
    """A cancelled receive() future must not stay parked forever: the
    heartbeat pattern (wait_for(receive(), t) in a loop) would grow the
    waiter queue without bound on an idle connection. Parking a new
    waiter prunes the dead ones. Reported by Codex on PR #4 against the
    waiter-queue fix."""
    import gc
    import weakref

    results = {}

    async def app(scope, receive, send):
        await receive()  # body
        refs = []
        for _ in range(10):
            f = receive()  # parks a waiter
            refs.append(weakref.ref(f))
            f.cancel()
            del f
        f = receive()  # parking this one prunes the ten cancelled
        f.cancel()
        del f
        gc.collect()
        results["alive"] = sum(1 for r in refs if r() is not None)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)
    loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    assert results["alive"] == 0, f"{results['alive']} cancelled waiters retained"
    loop._core.listener_close(lid)


def test_concurrent_receive_waiters_all_resolve_on_disconnect(loop):
    """Two receive() calls awaiting concurrently must BOTH resolve when
    the client disconnects. Parking the second waiter used to displace
    the first future, whose awaiter then stayed pending forever.
    Reported on PR #1."""
    results = {}
    done = asyncio.Event()

    async def app(scope, receive, send):
        await receive()  # body
        await send({"type": "http.response.start", "status": 200, "headers": []})
        # Still streaming: both receive() calls park until the disconnect.
        await send({"type": "http.response.body", "body": b"ok", "more_body": True})
        r1, r2 = await asyncio.gather(receive(), receive())
        results["types"] = (r1["type"], r2["type"])
        done.set()

    lid, port = listen(loop, app)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET / HTTP/1.1\r\nHost: h\r\n\r\n")
        await writer.drain()
        await asyncio.wait_for(reader.readuntil(b"ok\r\n"), 5)  # the first chunk
        writer.close()  # disconnect must release BOTH waiters
        await asyncio.wait_for(done.wait(), 5)

    loop.run_until_complete(main())
    assert results["types"] == ("http.disconnect", "http.disconnect")
    loop._core.listener_close(lid)


def test_request_parsed_ahead_of_malformed_bytes_still_answered(loop):
    """The parser keeps requests that completed ahead of malformed bytes
    in the same buffer. Dropping them on the parse error made the client
    read the 400 as the answer to the valid request. Reported on PR #1."""
    lid, port = listen(loop, echo_scope_app)
    data = loop.run_until_complete(
        _request(port, b"GET /ok HTTP/1.1\r\nHost: h\r\n\r\nGARBAGE\r\n\r\n", read_all=True)
    )
    assert data.startswith(b"HTTP/1.1 200 OK\r\n"), data[:64]
    at = data.find(b"HTTP/1.1 400 ")
    assert at > 0, data
    assert json.loads(_body(data[:at]))["path"] == "/ok"
    loop._core.listener_close(lid)


def test_asterisk_form_target_preserved(loop):
    """OPTIONS * is a valid server-wide request (RFC 7230 5.3.4);
    normalizing the target to "/" made it indistinguishable from an
    OPTIONS on the root resource. Reported on PR #1."""
    lid, port = listen(loop, echo_scope_app)
    resp = loop.run_until_complete(
        _request(port, b"OPTIONS * HTTP/1.1\r\nHost: h\r\n\r\n")
    )
    data = json.loads(_body(resp))
    assert data["path"] == "*"
    assert data["raw_path"] == "*"
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


def test_error_response_matches_http10_request_version(loop):
    """Engine-generated errors carried an HTTP/1.1 status line even when
    the failing request was parsed as HTTP/1.0; strict 1.0 clients can
    reject the response. The version is used when the parser saw one
    (test_malformed_request_400 covers the 1.1 default for requests
    whose version never parsed). Reported on PR #1."""
    lid, port = listen(loop, echo_scope_app, max_body=8)
    resp = loop.run_until_complete(
        _request(
            port,
            b"POST / HTTP/1.0\r\nHost: h\r\nContent-Length: 100\r\n\r\n" + b"z" * 100,
            read_all=True,
        )
    )
    assert resp.startswith(b"HTTP/1.0 413 ")
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
        await send({"type": "http.response.body", "body": b"ok", "more_body": True})
        # The response is still streaming, so this must block until the
        # client goes away. (Once the response is complete, receive()
        # answers http.disconnect at once.)
        second = await receive()
        events.append(second["type"])
        done.set()

    lid, port = listen(loop, app)

    async def main():
        nonlocal done
        done = asyncio.Event()
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET / HTTP/1.1\r\nHost: h\r\n\r\n")
        await writer.drain()
        await asyncio.wait_for(reader.readuntil(b"ok\r\n"), 5)  # the first chunk
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
    pytest.importorskip("starlette.applications")
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
    pytest.importorskip("starlette.applications")
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
    pytest.importorskip("starlette.applications")
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
    pytest.importorskip("starlette.applications")
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
    pytest.importorskip("starlette.applications")
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
    pytest.importorskip("fastapi")
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
    pytest.importorskip("fastapi")
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


@pytest.mark.parametrize("status", [0, 1, 99, 1000, 65535])
def test_invalid_response_status_is_rejected(loop, status):
    """A status line is three digits by grammar; anything else is a
    malformed start-line. Reported by Codex review on PR #1."""

    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": status, "headers": []})
        await send({"type": "http.response.body", "body": b"x"})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    assert resp.startswith(b"HTTP/1.1 500"), resp[:40]
    loop._core.listener_close(lid)


def test_app_supplied_transfer_encoding_is_stripped(loop):
    """Response framing is the server's job (R-084). Echoing an app's
    `Transfer-Encoding: chunked` would pair it with our own content-length
    (or duplicate our chunked header) while the body bytes are not chunk-
    framed, leaving the client to parse payload as chunk headers. Reported
    by Codex review on PR #1."""

    async def app(scope, receive, send):
        await receive()
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"transfer-encoding", b"chunked")],
        })
        await send({"type": "http.response.body", "body": b"hello"})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    head = resp.split(b"\r\n\r\n", 1)[0].lower()
    assert head.count(b"transfer-encoding") == 0
    assert b"content-length: 5" in head
    assert _body(resp) == b"hello"
    loop._core.listener_close(lid)


@pytest.mark.parametrize(
    "chunks,label",
    [([b"short"], "underflow"), ([b"way too many bytes here"], "overflow")],
)
def test_declared_content_length_is_enforced(loop, chunks, label):
    """R-084: a body that disagrees with the declared content-length
    desynchronises a keep-alive stream — the client reads into the next
    response or treats the surplus as one. Reported by Codex review on
    PR #1."""

    async def app(scope, receive, send):
        await receive()
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-length", b"10")],
        })
        for c in chunks:
            await send({"type": "http.response.body", "body": c})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n", read_all=True))
    # The mismatch is refused rather than put on the wire: either a 500
    # (caught before the head was committed) or a truncated/closed stream.
    assert b"way too many bytes here" not in resp, f"{label}: surplus reached the wire"
    loop._core.listener_close(lid)


def test_conflicting_content_length_headers_are_rejected(loop):
    async def app(scope, receive, send):
        await receive()
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-length", b"5"), (b"content-length", b"9")],
        })
        await send({"type": "http.response.body", "body": b"hello"})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    assert resp.startswith(b"HTTP/1.1 500")
    loop._core.listener_close(lid)


def test_matching_content_length_still_passes(loop):
    async def app(scope, receive, send):
        await receive()
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-length", b"5")],
        })
        await send({"type": "http.response.body", "body": b"hel", "more_body": True})
        await send({"type": "http.response.body", "body": b"lo"})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    assert resp.startswith(b"HTTP/1.1 200")
    assert _body(resp) == b"hello"
    loop._core.listener_close(lid)


def test_pipelined_burst_behind_a_slow_request_is_bounded(loop):
    """R-085: while one request is being served, every subsequently parsed
    pipelined request was retained in full and the receive path kept
    reposting reads, so a client could grow conn.pending until the process
    died. Reading is now paused at the budget and resumed as the queue
    drains — the backlog belongs in the peer's send buffer, not our heap.
    Reported by Codex review on PR #1 (F01).

    The burst must still be served correctly and in order once released.
    """
    release = asyncio.Event()
    served = []

    async def app(scope, receive, send):
        await receive()
        if scope["path"] == "/slow":
            await release.wait()
        served.append(scope["path"])
        body = scope["path"].encode()
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-length", str(len(body)).encode())],
        })
        await send({"type": "http.response.body", "body": body})

    lid, port = listen(loop, app)
    n = 300  # far past the 64-deep budget

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        burst = b"GET /slow HTTP/1.1\r\nHost: h\r\n\r\n" + b"".join(
            f"GET /n{i} HTTP/1.1\r\nHost: h\r\n\r\n".encode() for i in range(n)
        )
        writer.write(burst)
        # drain() may not complete while the server is paused — that IS
        # the backpressure, so do not await it before releasing.
        await asyncio.sleep(0.2)
        release.set()
        got = []
        for _ in range(n + 1):
            resp = await asyncio.wait_for(_read_one_response(reader), 20)
            got.append(_body(resp).decode())
        writer.close()
        return got

    got = loop.run_until_complete(main())
    assert got[0] == "/slow"
    assert got[1:] == [f"/n{i}" for i in range(n)], "pipelined order broken"
    # The bound actually engaged — otherwise this test would pass just as
    # happily against an unbounded queue.
    assert loop._core.stats()["pipeline_pauses"] > 0, "reading was never paused"
    loop._core.listener_close(lid)


def test_a_pipelining_client_does_not_starve_other_connections(loop):
    """R-085 fairness: one 64 KiB read carries ~2400 minimal pipelined
    requests, and the pump served all of them back to back in one pass --
    every other connection's requests waited behind the whole burst. It
    now yields after a quantum, so a request arriving on another
    connection is served within a few quanta, while the burst itself is
    still answered completely and in order.

    /b is sent once the server is serving the burst, from a thread of its
    own: how much of the burst the kernel buffers depends on how much the
    server reads ahead of its pipeline budget, and a client that sent /b
    only after the whole burst had been written measured that instead."""
    import threading
    import time

    order = []

    async def app(scope, receive, send):
        order.append(scope["path"])
        await send(
            {"type": "http.response.start", "status": 200, "headers": [(b"content-length", b"2")]}
        )
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)
    n = 4000

    def read_responses(sock, count):
        buf = b""
        while buf.count(b"HTTP/1.1 200 ") < count:
            chunk = sock.recv(1 << 16)
            assert chunk, "connection closed before every response arrived"
            buf += chunk
        return buf

    served_before_b = []

    def client():
        a = socket.create_connection(("127.0.0.1", port))
        b = socket.create_connection(("127.0.0.1", port))
        try:
            for s in (a, b):
                s.settimeout(20)
            # Both connections live on the server before the burst starts.
            b.sendall(b"GET /warm HTTP/1.1\r\nHost: h\r\n\r\n")
            read_responses(b, 1)
            burst = threading.Thread(target=a.sendall, args=(b"GET /a HTTP/1.1\r\nHost: h\r\n\r\n" * n,))
            burst.start()
            deadline = time.monotonic() + 10
            while order.count("/a") < 1 and time.monotonic() < deadline:
                time.sleep(0.0005)
            served_before_b.append(order.count("/a"))
            b.sendall(b"GET /b HTTP/1.1\r\nHost: h\r\n\r\n")
            read_responses(b, 1)
            read_responses(a, n)
            burst.join()
        finally:
            a.close()
            b.close()

    loop.run_until_complete(loop.run_in_executor(None, client))
    loop._core.listener_close(lid)
    assert order.count("/a") == n, "the burst was not answered in full"
    behind = order.index("/b") - 1 - served_before_b[0]
    # Served back to back, /b waited for the whole first read of the burst
    # (~700 requests on a fresh loopback connection, up to ~2400 per
    # 64 KiB); with the quantum it is a few quanta at most.
    assert behind < 256, f"/b was served only after {behind} more of the other client's requests"

def test_oversized_body_is_rejected_by_default(loop):
    """R-086: the engine buffers a whole request body before dispatching,
    so an unlimited default let any unauthenticated client turn one
    request into unbounded resident memory. The default is now finite and
    over it the client gets a 413, not a silent close."""
    from cadeloop.config import Config

    assert Config().max_body == 16 * 1024 * 1024, "default must stay finite"

    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    # Small explicit cap so the test does not have to push 16 MiB.
    lid, port = listen(loop, app, max_body=8)
    resp = loop.run_until_complete(
        _request(
            port,
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 32\r\n\r\n" + b"x" * 32,
            read_all=True,
        )
    )
    assert b"413" in resp.split(b"\r\n", 1)[0], resp[:60]
    loop._core.listener_close(lid)


def test_unlimited_body_is_still_available_explicitly(loop):
    """`max_body=None` remains an explicit opt-in for large uploads."""

    async def app(scope, receive, send):
        msg = await receive()
        n = str(len(msg["body"])).encode()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": n})

    lid, port = listen(loop, app, max_body=None)
    body = b"y" * 200_000
    resp = loop.run_until_complete(
        _request(
            port,
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: "
            + str(len(body)).encode()
            + b"\r\n\r\n"
            + body,
        )
    )
    assert _body(resp) == b"200000"
    loop._core.listener_close(lid)


def test_asgi_send_applies_write_backpressure(loop):
    """R-084: `send()` returned an already-completed awaitable no matter
    how deep the write queue was, so a streaming app whose only suspension
    point is `await send(...)` could enqueue its whole stream against a
    slow client — the configured watermarks never reached the ASGI
    producer. Reported by Codex review on PR #1 (twice).

    A client that stops reading must eventually make `send()` block."""
    import threading

    chunk = b"q" * 65536
    sends_completed = []
    stop_app = threading.Event()

    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        for i in range(400):
            if stop_app.is_set():
                break
            await send({"type": "http.response.body", "body": chunk, "more_body": True})
            sends_completed.append(i)
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    lid, port = listen(loop, app)

    async def main():
        # Connect, ask, then never read: the queue must back up.
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /stream HTTP/1.1\r\nHost: h\r\n\r\n")
        await writer.drain()
        await asyncio.sleep(0.6)  # let the app run as far as it can
        n = len(sends_completed)
        stop_app.set()
        writer.close()
        return n

    n = loop.run_until_complete(main())
    loop._core.listener_close(lid)
    # Without backpressure the app runs all 400 sends straight through,
    # buffering ~26 MB. With it, it parks once the queue passes the
    # high-water mark and only the socket's own capacity gets through.
    assert n < 400, f"app completed all {n} sends — send() never blocked"


def test_informational_status_is_rejected(loop):
    """1xx is interim: a client keeps waiting for a final response after
    one. This path allows a single http.response.start and treats its body
    as the complete response, so emitting 1xx would leave the client
    reading the next keep-alive response as this one's. Reported by Codex
    review on PR #1."""

    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 103, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    assert resp.startswith(b"HTTP/1.1 500")
    loop._core.listener_close(lid)


def test_app_content_length_stripped_from_204(loop):
    """HTTP forbids Content-Length on a 204. Suppressing only the
    GENERATED framing was not enough — an app-supplied header was still
    copied through, and a client honouring it reads the next keep-alive
    response's bytes as this one's body. Reported by Codex review on PR #1."""

    async def app(scope, receive, send):
        await receive()
        if scope["path"] == "/empty":
            await send({
                "type": "http.response.start",
                "status": 204,
                "headers": [(b"content-length", b"12")],
            })
            await send({"type": "http.response.body", "body": b""})
        else:
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"second"})

    lid, port = listen(loop, app)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(
            b"GET /empty HTTP/1.1\r\nHost: h\r\n\r\nGET /next HTTP/1.1\r\nHost: h\r\n\r\n"
        )
        await writer.drain()
        first = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
        second = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
        body = await asyncio.wait_for(reader.readexactly(6), 5)
        writer.close()
        return first, second, body

    first, second, body = loop.run_until_complete(main())
    assert b"204" in first.split(b"\r\n", 1)[0]
    assert b"content-length" not in first.lower(), first
    assert b"200" in second.split(b"\r\n", 1)[0]
    assert body == b"second"
    loop._core.listener_close(lid)


def test_server_sockets_is_empty_after_close(loop):
    """asyncio reports an empty tuple after close. Rebuilding the view
    would duplicate descriptors the native listener already closed —
    raising EBADF, or handing back a duplicate of whatever unrelated
    socket has since reused the number. Reported by Codex review on PR #1
    (twice)."""

    async def main():
        server = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
        server.close()
        await server.wait_closed()
        return server.sockets

    assert loop.run_until_complete(main()) == ()


def test_304_keeps_application_content_length(loop):
    """RFC 7232 4.1 explicitly PERMITS Content-Length on a 304, reporting
    the size the representation would have had. My first pass at stripping
    it from bodyless statuses discarded that valid cache metadata; only
    204 (and 1xx) actually forbid the header. Reported by Codex review on
    PR #1."""

    async def app(scope, receive, send):
        await receive()
        await send({
            "type": "http.response.start",
            "status": 304,
            "headers": [(b"content-length", b"1234"), (b"etag", b'"abc"')],
        })
        await send({"type": "http.response.body", "body": b""})

    lid, port = listen(loop, app)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET / HTTP/1.1\r\nHost: h\r\n\r\n")
        await writer.drain()
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
        writer.close()
        return head

    head = loop.run_until_complete(main())
    assert b"304" in head.split(b"\r\n", 1)[0]
    assert b"content-length: 1234" in head.lower(), head
    assert b'etag: "abc"' in head.lower()
    loop._core.listener_close(lid)


def test_http10_request_gets_an_http10_status_line(loop):
    """The body path already used HTTP/1.0-compatible close-delimited
    framing for a 1.0 request, but the status line said 1.1 regardless,
    and a strict 1.0-only client may reject the higher version. Reported
    by Codex review on PR #1."""

    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(
        _request(port, b"GET / HTTP/1.0\r\nHost: h\r\n\r\n", read_all=True)
    )
    assert resp.startswith(b"HTTP/1.0 200"), resp[:40]
    # and 1.1 still answers 1.1
    resp11 = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    assert resp11.startswith(b"HTTP/1.1 200")
    loop._core.listener_close(lid)


@pytest.mark.parametrize(
    "raw,label",
    [
        (b"GET / HTTP/1.1\r\n\r\n", "no Host"),
        (b"GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n", "two Hosts"),
    ],
)
def test_http11_requires_exactly_one_host(loop, raw, label):
    """RFC 7230 5.4. llhttp validates syntax only, so without this a
    request with no Host — or two disagreeing ones — reaches the app and
    the authority it routes on can differ from what an intermediary saw.
    Reported by Codex review on PR #1 (twice)."""
    seen = []

    async def app(scope, receive, send):
        seen.append(scope["path"])
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, raw, read_all=True))
    assert b"400" in resp.split(b"\r\n", 1)[0], f"{label}: {resp[:60]!r}"
    assert not seen, f"{label}: request reached the application"
    loop._core.listener_close(lid)


def test_http10_without_host_is_still_accepted(loop):
    """The requirement is HTTP/1.1-only; 1.0 has no Host requirement."""

    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.0\r\n\r\n", read_all=True))
    assert resp.startswith(b"HTTP/1.0 200"), resp[:40]
    loop._core.listener_close(lid)


# --------------------------------------------------------------------- #
# graceful shutdown drain (R-092)                                       #
# --------------------------------------------------------------------- #


def test_shutdown_drains_the_in_flight_response(loop):
    """SIGTERM stops run_forever; going straight from there to
    loop.close() cancelled the in-flight write, so a client that had
    already been promised a content-length got a truncated body. The
    configured grace was honoured between workers but never inside one."""
    from cadeloop.server import _drain_connections

    body = b"x" * 5000
    reached = []

    async def slow_app(scope, receive, send):
        await receive()
        reached.append(1)
        loop.call_soon(loop.stop)  # the "SIGTERM" lands mid-request
        await asyncio.sleep(0.15)
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-length", str(len(body)).encode())],
            }
        )
        await send({"type": "http.response.body", "body": body})

    lid, port = listen(loop, slow_app)
    got = []

    async def client():
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(b"GET / HTTP/1.1\r\nHost: h\r\n\r\n")
        await w.drain()
        got.append(await r.read())
        w.close()

    task = loop.create_task(client())
    loop.run_forever()
    assert reached, "the request never reached the app"
    loop._core.listener_close(lid)
    _drain_connections(loop, 5.0)
    # The drain returns as soon as the connection count reaches zero, and
    # that can be the same tick that resolves the client's read -- leaving
    # its continuation queued but not yet stepped. Sampling done() at that
    # exact instant is a race (it lost on windows-2025, with the read
    # future already finished). Give the task a bounded chance to run
    # instead; a client that genuinely never finishes still fails here,
    # and the untruncated-body assertion below is what carries the point.
    loop.run_until_complete(asyncio.wait_for(task, 5.0))
    assert got[0].endswith(body), f"truncated response: ...{got[0][-40:]!r}"
    assert loop._core.http_connection_count() == 0


def test_shutdown_closes_idle_keepalive_connections_at_once(loop):
    """An idle keep-alive client must not hold the drain open for the
    whole grace period -- only in-flight work should."""
    from cadeloop.server import _drain_connections

    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)

    async def client():
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(b"GET / HTTP/1.1\r\nHost: h\r\n\r\n")
        await w.drain()
        await r.readuntil(b"ok")  # response read; connection now idle
        return r, w

    r, w = loop.run_until_complete(client())
    assert loop._core.http_connection_count() == 1
    loop._core.listener_close(lid)
    t0 = loop.time()
    _drain_connections(loop, 30.0)
    assert loop._core.http_connection_count() == 0
    assert loop.time() - t0 < 5.0, "idle connection waited out the grace period"
    w.close()


def test_shutdown_sends_a_websocket_close_frame(loop):
    """A WebSocket never finishes on its own, so shutdown has to tell it.
    Without a close frame the peer just saw the TCP connection vanish."""
    import base64

    from cadeloop.server import _drain_connections

    async def ws_app(scope, receive, send):
        assert scope["type"] == "websocket"
        await receive()
        await send({"type": "websocket.accept"})
        while True:
            msg = await receive()
            if msg["type"] == "websocket.disconnect":
                return

    lid, port = listen(loop, ws_app)
    key = base64.b64encode(b"0123456789abcdef")

    async def client():
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(
            b"GET /ws HTTP/1.1\r\nHost: h\r\nUpgrade: websocket\r\n"
            b"Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n"
            b"Sec-WebSocket-Key: " + key + b"\r\n\r\n"
        )
        await w.drain()
        head = await r.readuntil(b"\r\n\r\n")
        assert head.startswith(b"HTTP/1.1 101"), head[:40]
        return r, w

    r, w = loop.run_until_complete(client())
    assert loop._core.http_connection_count() == 1
    loop._core.listener_close(lid)

    frame = []

    async def read_close():
        frame.append(await r.read(4))

    loop.create_task(read_close())
    _drain_connections(loop, 5.0)
    # 0x88 = FIN | opcode 8 (close); payload is the 2-byte status code
    # plus the reason, unmasked from a server.
    assert frame and frame[0][0] == 0x88, frame
    assert int.from_bytes(frame[0][2:4], "big") == 1012, frame
    assert loop._core.http_connection_count() == 0
    w.close()


# --------------------------------------------------------------------- #
# immediate-flush latency mode (R-060 / R-035)                          #
# --------------------------------------------------------------------- #


def _stream_three_chunks_sends(immediate):
    """Send a 3-chunk streaming response; report sends posted.

    `HttpSend.__call__` resolves without suspending, so all three chunks
    are produced inside one tick -- exactly the window corking coalesces
    and immediate flush does not. The test client shares this loop, so
    its own request write is counted too; only the difference between the
    two modes is meaningful, which is what the caller asserts on."""

    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        for i in range(3):
            await send(
                {"type": "http.response.body", "body": b"c%d" % i, "more_body": i < 2}
            )

    lp = cadeloop.new_event_loop()
    asyncio.set_event_loop(lp)
    try:
        if immediate:
            lp._core.set_immediate_flush(True)
        lid, port = listen(lp, app)
        before = lp._core.stats()["sends_posted"]
        # Keep-alive stays open, so read exactly one (chunked) response
        # rather than to EOF.
        resp = lp.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
        assert b"c0" in resp and b"c2" in resp, resp[:160]
        lp._core.listener_close(lid)
        return lp._core.stats()["sends_posted"] - before
    finally:
        asyncio.set_event_loop(None)
        lp.close()


def test_immediate_flush_trades_syscalls_for_tail_latency():
    """The mode's effect is a send count, not a wall-clock number.
    Asserting on timing here would measure the load generator, not the
    server -- on a 4-core box the run-to-run p99 spread swamped the
    difference in both directions, so no latency claim is made from it."""
    corked = _stream_three_chunks_sends(immediate=False)
    immediate = _stream_three_chunks_sends(immediate=True)
    assert immediate > corked, (
        f"immediate flush posted {immediate} sends, corked {corked} -- "
        "the corked path is supposed to coalesce the chunks and this one is not"
    )


def test_latency_mode_spin_selects_immediate_flush():
    assert cadeloop.Config().immediate_flush is False
    assert cadeloop.Config(latency_mode="throughput").immediate_flush is False
    assert cadeloop.Config(latency_mode="spin").immediate_flush is True
    # An explicit setting always wins over the preset.
    assert cadeloop.Config(latency_mode="spin", immediate_flush=False).immediate_flush is False
    assert cadeloop.Config(latency_mode="balanced", immediate_flush=True).immediate_flush is True


def test_only_latency_mode_spin_spins_before_every_park():
    """R-060: the other modes spin only while spins find work; "spin"
    has the core to itself and spins unconditionally."""
    assert cadeloop.Config().adaptive_spin is True
    assert cadeloop.Config(latency_mode="throughput").adaptive_spin is True
    assert cadeloop.Config(latency_mode="spin").adaptive_spin is False
    assert cadeloop.Config(latency_mode="spin", adaptive_spin=True).adaptive_spin is True
    assert cadeloop.Config(adaptive_spin=False).adaptive_spin is False


def test_adaptive_spin_reaches_the_loop(monkeypatch):
    import cadeloop.__main__ as cli
    from cadeloop import server as srv

    seen = {}
    monkeypatch.setattr(cli, "serve", lambda app, host, port, **kw: seen.update(kw))
    monkeypatch.setattr(cli, "load_app", lambda spec: None)
    assert cli.main(["m:a", "--no-adaptive-spin"]) == 0 and seen["adaptive_spin"] is False

    class Built(Exception):
        pass

    def loop(**kwargs):
        seen.update(kwargs)
        raise Built

    monkeypatch.setattr(srv, "Loop", loop)
    for mode, adaptive in (("balanced", True), ("spin", False)):
        with pytest.raises(Built):
            srv._serve_single(None, "127.0.0.1", 0, cadeloop.Config(latency_mode=mode))
        assert seen["adaptive_spin"] is adaptive, mode


# --------------------------------------------------------------------- #
# Expect: 100-continue (RFC 7231 5.1.1) and 205 framing (6.3.6)         #
# --------------------------------------------------------------------- #


def test_expect_100_continue_gets_an_interim_response(loop):
    """A client that sends `Expect: 100-continue` holds the body back
    until the interim response arrives. Ignoring the expectation made
    every such upload wait out the client's own continue timeout -- and
    the strict clients that never give up wait forever."""

    async def app(scope, receive, send):
        msg = await receive()
        body = msg["body"]
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"got:" + body})

    lid, port = listen(loop, app)

    async def client():
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\n"
            b"Expect: 100-continue\r\n\r\n"
        )
        await w.drain()
        # The interim response must arrive BEFORE any body is sent.
        interim = await asyncio.wait_for(r.readuntil(b"\r\n\r\n"), 5.0)
        w.write(b"hello")
        await w.drain()
        rest = await asyncio.wait_for(_read_one_response(r), 5.0)
        w.close()
        return interim, rest

    interim, rest = loop.run_until_complete(client())
    assert interim == b"HTTP/1.1 100 Continue\r\n\r\n", interim
    assert b"got:hello" in rest, rest
    loop._core.listener_close(lid)


def test_expect_100_continue_is_case_insensitive_and_listed(loop):
    """The expectation is a comma-separated list and the token is
    case-insensitive (RFC 7231 5.1.1)."""

    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)

    async def client():
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 1\r\n"
            b"Expect: 100-Continue\r\n\r\n"
        )
        await w.drain()
        interim = await asyncio.wait_for(r.readuntil(b"\r\n\r\n"), 5.0)
        w.write(b"x")
        await w.drain()
        await asyncio.wait_for(_read_one_response(r), 5.0)
        w.close()
        return interim

    assert loop.run_until_complete(client()).startswith(b"HTTP/1.1 100"), "case-sensitive match"
    loop._core.listener_close(lid)


def test_oversized_declared_body_is_refused_before_it_is_sent(loop):
    """The other half of what the expectation is for: a Content-Length
    already over the cap is known at headers-complete, so the upload is
    refused before a byte of it is buffered."""

    async def app(scope, receive, send):  # pragma: no cover - never reached
        raise AssertionError("the app must not see an over-cap request")

    lid, port = listen(loop, app, max_body=16)

    async def client():
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 4096\r\n"
            b"Expect: 100-continue\r\n\r\n"
        )
        await w.drain()
        data = await asyncio.wait_for(r.read(), 5.0)
        w.close()
        return data

    resp = loop.run_until_complete(client())
    assert resp.startswith(b"HTTP/1.1 413"), resp[:60]
    assert b"100 Continue" not in resp, "sent Continue for a request already refused"
    loop._core.listener_close(lid)


def test_205_frames_its_empty_payload(loop):
    """205 carries no content, but unlike 204/304 it is not self-framing
    (RFC 7230 3.3.3 rule 1 does not list it), so RFC 7231 6.3.6 requires
    an explicit zero length. Without one the client reads the next
    keep-alive response as this one's body."""

    async def app(scope, receive, send):
        await receive()
        # An application that sends a body on a 205 is wrong; the bytes
        # must not reach the wire either way.
        await send({"type": "http.response.start", "status": 205, "headers": []})
        await send({"type": "http.response.body", "body": b"should not be sent"})

    lid, port = listen(loop, app)

    async def client():
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(b"GET /a HTTP/1.1\r\nHost: h\r\n\r\n")
        await w.drain()
        first = await asyncio.wait_for(_read_one_response(r), 5.0)
        # Same connection: a mis-framed 205 desynchronises this.
        w.write(b"GET /b HTTP/1.1\r\nHost: h\r\n\r\n")
        await w.drain()
        second = await asyncio.wait_for(_read_one_response(r), 5.0)
        w.close()
        return first, second

    first, second = loop.run_until_complete(client())
    assert first.startswith(b"HTTP/1.1 205"), first[:40]
    assert b"content-length: 0" in first.lower(), first
    assert b"should not be sent" not in first, first
    assert second.startswith(b"HTTP/1.1 205"), f"keep-alive desynchronised: {second[:60]!r}"
    loop._core.listener_close(lid)


def test_204_stays_self_framing(loop):
    """The 205 change must not give 204 a content-length: rule 1 makes it
    self-framing and a framing header there is what desynchronises."""

    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(
        _request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n")
    )
    assert resp.startswith(b"HTTP/1.1 204"), resp[:40]
    assert b"content-length" not in resp.lower(), resp
    loop._core.listener_close(lid)


# --------------------------------------------------------------------- #
# ASGI http.response.trailers extension                                 #
# --------------------------------------------------------------------- #


def test_scope_declares_the_trailers_extension(loop):
    """An application checks scope["extensions"] before setting
    trailers=True, so the engine has to declare it or nothing ever uses
    the feature."""
    seen = {}

    async def app(scope, receive, send):
        seen.update(scope.get("extensions") or {})
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)
    loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    assert "http.response.trailers" in seen, seen
    loop._core.listener_close(lid)


def test_response_trailers_ride_the_chunked_terminator(loop):
    """Trailers live in the chunked terminator and nowhere else, so a
    promised-trailers response must stream even when the body arrives in
    one message -- and the terminating 0-chunk has to be withheld until
    the trailers are there to go out with it."""

    async def app(scope, receive, send):
        await receive()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"trailer", b"x-checksum")],
                "trailers": True,
            }
        )
        await send({"type": "http.response.body", "body": b"payload"})
        await send(
            {"type": "http.response.trailers", "headers": [(b"x-checksum", b"abc123")]}
        )

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    assert b"transfer-encoding: chunked" in resp.lower(), resp
    assert b"payload" in resp, resp
    assert b"x-checksum: abc123" in resp, resp
    # The terminator introduces the trailer section, and the blank line
    # closes it -- in that order.
    body = resp.split(b"\r\n\r\n", 1)[1]
    assert body.endswith(b"0\r\nx-checksum: abc123\r\n\r\n"), body
    loop._core.listener_close(lid)


def test_trailers_can_arrive_in_several_messages(loop):
    async def app(scope, receive, send):
        await receive()
        await send(
            {"type": "http.response.start", "status": 200, "headers": [], "trailers": True}
        )
        await send({"type": "http.response.body", "body": b"x", "more_body": True})
        await send({"type": "http.response.body", "body": b"y"})
        await send(
            {
                "type": "http.response.trailers",
                "headers": [(b"x-a", b"1")],
                "more_trailers": True,
            }
        )
        await send({"type": "http.response.trailers", "headers": [(b"x-b", b"2")]})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    body = resp.split(b"\r\n\r\n", 1)[1]
    assert body.endswith(b"0\r\nx-a: 1\r\nx-b: 2\r\n\r\n"), body
    loop._core.listener_close(lid)


def test_a_response_without_trailers_is_unchanged(loop):
    """The default path must not grow a withheld terminator."""

    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"x", "more_body": True})
        await send({"type": "http.response.body", "body": b"y"})

    lid, port = listen(loop, app)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    body = resp.split(b"\r\n\r\n", 1)[1]
    assert body.endswith(b"0\r\n\r\n"), body
    loop._core.listener_close(lid)


def test_trailers_reject_fields_that_change_framing(loop):
    """RFC 7230 4.1.2: a recipient merging trailers into the header set
    must not have its framing or routing decided after the fact."""
    errors = []

    async def app(scope, receive, send):
        await receive()
        await send(
            {"type": "http.response.start", "status": 200, "headers": [], "trailers": True}
        )
        await send({"type": "http.response.body", "body": b"x"})
        try:
            await send(
                {
                    "type": "http.response.trailers",
                    "headers": [(b"content-length", b"999")],
                }
            )
        except Exception as exc:
            errors.append(str(exc))
            await send({"type": "http.response.trailers", "headers": []})

    lid, port = listen(loop, app)
    loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    assert errors and "not allowed in trailers" in errors[0], errors
    loop._core.listener_close(lid)


# --------------------------------------------------------------------- #
# scope correctness: ASGI address shape, absolute-form authority         #
# --------------------------------------------------------------------- #


def test_asgi_client_and_server_are_two_item(loop):
    """Pins the ASGI contract on the ordinary path. Note this does NOT
    discriminate the IPv6 truncation on its own -- an IPv4 sockaddr is
    already two-item -- which is what the IPv6 test below is for."""
    seen = {}

    async def app(scope, receive, send):
        seen["client"] = scope["client"]
        seen["server"] = scope["server"]
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)
    loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    loop._core.listener_close(lid)
    host, prt = seen["client"]  # exactly the unpacking apps do
    assert isinstance(prt, int)
    assert len(seen["client"]) == 2, seen["client"]
    assert len(seen["server"]) == 2, seen["server"]


def test_absolute_form_authority_overrides_a_conflicting_host(loop):
    """RFC 7230 5.4: a recipient of an absolute-form target uses THAT
    authority and ignores Host. Otherwise an intermediary routes on the
    request-target while this server's host routing, trusted-host checks
    or cache key read an attacker-controlled header."""
    seen = {}

    async def app(scope, receive, send):
        seen["headers"] = dict(scope["headers"])
        seen["path"] = scope["path"]
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)
    loop.run_until_complete(
        _request(
            port,
            b"GET http://target.example/x HTTP/1.1\r\nHost: evil.example\r\n\r\n",
        )
    )
    loop._core.listener_close(lid)
    assert seen["path"] == "/x", seen["path"]
    assert seen["headers"][b"host"] == b"target.example", seen["headers"]


def test_absolute_form_supplies_host_when_absent(loop):
    seen = {}

    async def app(scope, receive, send):
        seen["headers"] = dict(scope["headers"])
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)
    # HTTP/1.0 has no mandatory Host, so the authority is the only source.
    loop.run_until_complete(
        _request(port, b"GET http://target.example/y HTTP/1.0\r\n\r\n", read_all=True)
    )
    loop._core.listener_close(lid)
    assert seen["headers"].get(b"host") == b"target.example", seen["headers"]


def test_ordinary_host_header_is_untouched(loop):
    """The rewrite must apply ONLY to absolute-form targets."""
    seen = {}

    async def app(scope, receive, send):
        seen["headers"] = dict(scope["headers"])
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)
    loop.run_until_complete(_request(port, b"GET /z HTTP/1.1\r\nHost: normal.example\r\n\r\n"))
    loop._core.listener_close(lid)
    assert seen["headers"][b"host"] == b"normal.example", seen["headers"]


def test_asgi_client_is_two_item_for_ipv6(loop):
    """The regression this guards: the transport keeps IPv6 addresses in
    their full (host, port, flowinfo, scope_id) form -- correct, since
    that is what the socket APIs take and return -- but ASGI defines
    client/server as two-item [host, port], so passing the socket form
    straight through breaks the near-universal
    `host, port = scope["client"]` on IPv6 requests and nowhere else."""
    seen = {}

    async def app(scope, receive, send):
        seen["client"] = scope["client"]
        seen["server"] = scope["server"]
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    try:
        lid, bound, _fd = loop._core.http_listen("::1", 0, app, loop)
    except OSError as exc:
        pytest.skip(f"IPv6 unavailable: {exc}")
    try:
        loop.run_until_complete(
            _request(bound[1], b"GET / HTTP/1.1\r\nHost: h\r\n\r\n", host="::1")
        )
    finally:
        loop._core.listener_close(lid)
    assert len(seen["client"]) == 2, f"IPv6 client leaked its socket form: {seen['client']!r}"
    assert len(seen["server"]) == 2, f"IPv6 server leaked its socket form: {seen['server']!r}"
    host, prt = seen["client"]
    assert isinstance(prt, int)


def test_shutdown_drains_a_response_whose_bytes_are_still_queued():
    """The application has finished and the engine holds the bytes, but
    they have not all reached the peer. The drain classified that
    connection idle and tore it down immediately, truncating exactly the
    response `grace` exists to protect -- the same gap the idle sweep had,
    in the one place that kept its own copy of the predicate.

    Reaching that state needs two things the obvious test does not do, and
    without either it passes against the unfixed build:

    * a tiny client receive buffer, or default loopback buffers absorb the
      whole body before shutdown and nothing is left queued;
    * a write high-water above the body, or `send()` suspends and the app
      is still RUNNING -- which makes every predicate say busy, for the
      wrong reason.

    Measured against the unfixed build: 2,807,709 of 4,194,304 bytes.
    """
    from cadeloop.server import _drain_connections

    body = b"y" * (4 * 1024 * 1024)

    async def big_app(scope, receive, send):
        await receive()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-length", str(len(body)).encode())],
            }
        )
        await send({"type": "http.response.body", "body": body})

    lp = cadeloop.Loop(high_water=64 * 1024 * 1024, low_water=16 * 1024 * 1024)
    asyncio.set_event_loop(lp)
    got = []
    try:
        lid, bound, _fd = lp._core.http_listen("127.0.0.1", 0, big_app, lp)
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        client_sock.connect(("127.0.0.1", bound[1]))
        client_sock.setblocking(False)

        async def slow_client():
            r, w = await asyncio.open_connection(sock=client_sock)
            w.write(b"GET / HTTP/1.1\r\nHost: h\r\n\r\n")
            await w.drain()
            head = await r.readuntil(b"\r\n\r\n")
            lp.call_soon(lp.stop)
            return r, w, head

        r, w, head = lp.run_until_complete(slow_client())
        assert head.startswith(b"HTTP/1.1 200"), head[:40]
        lp._core.listener_close(lid)

        async def finish():
            got.append(await r.readexactly(len(body)))

        task = lp.create_task(finish())
        _drain_connections(lp, 30.0)
        # The drain may return before the CLIENT has consumed its own
        # receive buffer, so finish reading here. What matters is that the
        # server did not tear the connection down mid-body: without the
        # fix this raises IncompleteReadError.
        lp.run_until_complete(asyncio.wait_for(task, 30.0))
        w.close()
    finally:
        asyncio.set_event_loop(None)
        if not lp.is_closed():
            lp.close()
    assert len(got[0]) == len(body), f"{len(got[0])} of {len(body)} bytes"


def test_awaiting_a_foreign_loops_future_fails_the_request(loop):
    """An eager ASGI app that awaits a Future belonging to another event
    loop used to have `_wake` registered on it with no check. If that loop
    is not running the request hangs for good; if it runs on another
    thread, `_wake` fires there and StateCell rejects the cross-thread
    access, wedging the request behind a logged exception. asyncio.Task
    raises RuntimeError into the coroutine instead, which reaches the
    app-failure path and ends the request. Reported by Codex on PR #1."""
    other = asyncio.new_event_loop()
    try:
        foreign = other.create_future()

        async def app(scope, receive, send):
            if scope["type"] == "lifespan":  # pragma: no cover
                return
            await foreign  # never resolves, and not ours anyway

        lid, port = listen(loop, app)

        async def main():
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET / HTTP/1.1\r\nHost: h\r\n\r\n")
            await writer.drain()
            data = await asyncio.wait_for(reader.read(), 5.0)
            writer.close()
            return data

        resp = loop.run_until_complete(main())
        loop._core.listener_close(lid)
    finally:
        other.close()

    # The point is that the request TERMINATES rather than hanging until
    # the read times out; a 500 is the app-failure path doing its job.
    assert resp.startswith(b"HTTP/1.1 500"), resp[:80]


def test_a_non_eager_spawn_failure_is_a_500_not_a_dead_worker(loop):
    """With eager_tasks=False the request is handed to loop.create_task().
    That can fail on the application's account -- an ASGI callable that
    returns a non-coroutine, or an installed task factory that rejects it
    -- and the error propagated straight out of pump_requests, through
    the native tick, stopping the whole worker's event loop. The eager
    path turns the same per-request mistake into a 500 and keeps serving.
    Reported by Codex on PR #1."""

    def app(scope, receive, send):  # not a coroutine function
        return 42  # create_task() cannot take this

    lid, port = listen(loop, app, eager=False)

    async def main():
        first = None
        for _ in range(2):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET / HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
            await writer.drain()
            data = await asyncio.wait_for(reader.read(), 5.0)
            writer.close()
            if first is None:
                first = data
        return first, data

    first, second = loop.run_until_complete(main())
    loop._core.listener_close(lid)
    assert first.startswith(b"HTTP/1.1 500"), first[:80]
    # The point: the loop survived the first failure and served again.
    assert second.startswith(b"HTTP/1.1 500"), second[:80]


def test_request_trailers_do_not_reach_the_asgi_headers(loop):
    """llhttp drives the same header callbacks for the TRAILER section of
    a chunked request, and on_headers_complete -- with it the one-Host
    check -- has already run by then. So a trailer was committed as an
    ordinary header: `Host: attacker` reached scope["headers"] past a
    legitimate `Host: legit`, in the very field routing is decided from.
    ASGI has no request-trailer mechanism, so they are dropped. Reported
    by Codex on PR #1."""
    seen = {}

    async def app(scope, receive, send):
        while True:
            if not (await receive()).get("more_body"):
                break
        seen["headers"] = [(k.decode(), v.decode()) for k, v in scope["headers"]]
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)

    async def main():
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(
            b"POST / HTTP/1.1\r\nHost: legit\r\nTransfer-Encoding: chunked\r\n"
            b"Trailer: Host\r\n\r\n"
            b"4\r\nbody\r\n0\r\n"
            b"Host: attacker\r\nX-Injected: yes\r\n\r\n"
        )
        await w.drain()
        head = await asyncio.wait_for(r.readuntil(b"\r\n\r\n"), 5.0)
        w.close()
        return head

    head = loop.run_until_complete(main())
    loop._core.listener_close(lid)
    assert head.startswith(b"HTTP/1.1 200"), head[:60]
    names = [k for k, _ in seen["headers"]]
    assert names.count("host") == 1, seen["headers"]
    assert ("host", "attacker") not in seen["headers"], seen["headers"]
    assert "x-injected" not in names, seen["headers"]


def test_a_connection_that_sends_nothing_hits_the_head_timeout(loop):
    """`in_head()` is false until the first byte arrives, so a client that
    connected and said nothing was classified as keep-alive IDLE rather
    than as owing a request head. With keepalive_idle=0 it never expired
    at all, and with the defaults it sat for 75s instead of the 5s the
    slow-request limit exists to enforce -- a no-byte flood could hold
    sockets and per-connection state indefinitely. Reported by Codex."""

    async def app(scope, receive, send):  # pragma: no cover - never reached
        await receive()

    # keepalive_idle=0 disables the idle window entirely: only the head
    # deadline can close this connection, which is the point.
    lid, port = listen(loop, app, request_line_timeout=0.2, keepalive_idle=0.0)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        # Not one byte is sent.
        for _ in range(60):
            loop._core.http_sweep()
            await asyncio.sleep(0.05)
            if loop._core.http_connection_count() == 0:
                break
        # The head deadline answers 408 and then closes, so a clean EOF
        # after whatever it sent is the signal -- not an empty first read.
        rest = await asyncio.wait_for(reader.read(), 5.0)
        writer.close()
        return rest

    rest = loop.run_until_complete(main())
    loop._core.listener_close(lid)
    assert loop._core.http_connection_count() == 0, (
        "a connection that never sent a byte was never timed out"
    )
    assert rest.startswith(b"HTTP/1.1 408"), rest[:60]


def test_a_trailer_does_not_leak_into_the_next_pipelined_request(loop):
    """The trailer section's LAST field/value pair stays in the parser's
    accumulator -- nothing calls commit_header() after it, because the
    message ended instead of another header starting. The next PIPELINED
    request then committed it as its own first header: `Authorization:
    Bearer stolen` from request one arrived as request two's opening
    field, on a keep-alive connection that a proxy may well be pooling.
    Round 10 stopped trailers reaching their OWN request; this is the same
    injection crossing a request boundary. Reported by Codex on PR #1."""
    seen = []

    async def app(scope, receive, send):
        while True:
            if not (await receive()).get("more_body"):
                break
        seen.append([(k.decode(), v.decode()) for k, v in scope["headers"]])
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-length", b"2")],
            }
        )
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)

    async def main():
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(
            b"POST /one HTTP/1.1\r\nHost: legit\r\nTransfer-Encoding: chunked\r\n\r\n"
            b"4\r\nbody\r\n0\r\n"
            b"Authorization: Bearer stolen\r\n\r\n"
            b"GET /two HTTP/1.1\r\nHost: legit\r\nX-Real: yes\r\n\r\n"
        )
        await w.drain()
        for _ in range(40):
            await asyncio.sleep(0.02)
            if len(seen) >= 2:
                break
        w.close()

    loop.run_until_complete(main())
    loop._core.listener_close(lid)
    assert len(seen) == 2, seen
    names = [k for k, _ in seen[1]]
    assert "authorization" not in names, seen[1]
    assert names == ["host", "x-real"], seen[1]


def test_a_slow_body_upload_is_not_killed_by_the_head_deadline(loop):
    """The head-phase flag was first set when a whole REQUEST had been
    parsed -- which for a request with a body means after the upload
    finishes. So the connection stayed in the head phase for the entire
    upload, and since body activity deliberately does not re-anchor that
    phase (the slowloris rule), every upload longer than
    request_line_timeout was 408ed while it was actively sending.
    Reported by Codex on PR #1; a regression from the fix one round
    earlier."""
    got = []

    async def app(scope, receive, send):
        body = b""
        while True:
            m = await receive()
            body += m.get("body", b"")
            if not m.get("more_body"):
                break
        got.append(len(body))
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-length", b"2")],
            }
        )
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app, request_line_timeout=0.3, keepalive_idle=0.0)

    async def main():
        r, w = await asyncio.open_connection("127.0.0.1", port)
        chunks = 8
        size = 100
        w.write(
            b"POST / HTTP/1.1\r\nHost: h\r\n"
            b"Content-Length: " + str(chunks * size).encode() + b"\r\n\r\n"
        )
        await w.drain()
        # Each chunk is well inside the head window, but the upload as a
        # whole runs several times past it.
        for _ in range(chunks):
            loop._core.http_sweep()
            await asyncio.sleep(0.1)
            w.write(b"u" * size)
            await w.drain()
        head = await asyncio.wait_for(r.readuntil(b"\r\n\r\n"), 5.0)
        w.close()
        return head

    head = loop.run_until_complete(main())
    loop._core.listener_close(lid)
    assert head.startswith(b"HTTP/1.1 200"), head[:60]
    assert got == [800], got


def test_scope_metadata_dicts_are_not_shared_between_requests(loop):
    """`scope["asgi"]` and `scope["extensions"]` were one process-wide
    dict each, handed to every request. They are NESTED inside the scope,
    so the shallow copy an application customarily makes does not protect
    them: one middleware writing there changed what every later request --
    and every other loop in the process -- was told about the spec version
    or which extensions exist. Reported by Codex on PR #1."""
    seen = []

    async def app(scope, receive, send):
        await receive()
        seen.append((dict(scope["asgi"]), dict(scope["extensions"])))
        # What a middleware might plausibly do, and used to do globally.
        scope["asgi"]["spec_version"] = "tampered"
        scope["extensions"].clear()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-length", b"2")],
            }
        )
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)

    async def one():
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(b"GET / HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
        await w.drain()
        await asyncio.wait_for(r.read(), 5.0)
        w.close()

    loop.run_until_complete(one())
    loop.run_until_complete(one())
    loop._core.listener_close(lid)

    assert len(seen) == 2, seen
    assert seen[0] == seen[1], f"request 2 inherited request 1's mutations: {seen}"
    assert seen[1][0]["spec_version"] == "2.3", seen[1][0]
    assert "http.response.trailers" in seen[1][1], seen[1][1]


# --------------------------------------------------------------------- #
# receive() and the request lifecycle                                   #
# --------------------------------------------------------------------- #


def test_receive_after_the_response_answers_http_disconnect(loop):
    """ASGI: receive() returns http.disconnect "if receive is called after
    a response has been sent". It parked until the app coroutine returned
    instead, so an app that awaited it after responding deadlocked against
    itself -- and the connection's next keep-alive request, served only
    once the app returns, waited with it. A listener parked before the
    response completed is released at completion, too."""
    seen = []

    async def app(scope, receive, send):
        await receive()
        listener = receive()  # parked beside the response
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-length", b"2")],
            }
        )
        await send({"type": "http.response.body", "body": b"ok"})
        try:
            after = await asyncio.wait_for(receive(), 2)
            parked = await asyncio.wait_for(listener, 2)
            seen.append((after["type"], parked["type"]))
        except asyncio.TimeoutError:
            seen.append("timed out")

    lid, port = listen(loop, app)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        for _ in range(2):
            writer.write(b"GET / HTTP/1.1\r\nHost: h\r\n\r\n")
            await writer.drain()
            await asyncio.wait_for(_read_one_response(reader), 1)
        writer.close()

    loop.run_until_complete(main())
    loop._core.listener_close(lid)
    assert seen == [("http.disconnect", "http.disconnect")] * 2, seen


@pytest.mark.parametrize("variant", ["response-close", "client-half-close"])
def test_requests_pipelined_behind_a_close_are_not_run(loop, variant):
    """Once the connection closes after a response -- the response said
    `connection: close`, or the client half-closed -- no request pipelined
    behind it is processed (RFC 7230 6.6). The pump dispatched them anyway:
    the application ran, side effects included, for responses the closing
    connection then discarded."""
    calls = []
    release = asyncio.Event()

    async def app(scope, receive, send):
        calls.append(scope["path"])
        await receive()
        headers = [(b"content-length", b"2")]
        if scope["path"] == "/first":
            await release.wait()  # the rest is queued behind it by now
            if variant == "response-close":
                headers.append((b"connection", b"close"))
        await send({"type": "http.response.start", "status": 200, "headers": headers})
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(
            b"GET /first HTTP/1.1\r\nHost: h\r\n\r\n"
            b"POST /charge HTTP/1.1\r\nHost: h\r\ncontent-length: 0\r\n\r\n"
        )
        if variant == "client-half-close":
            writer.write_eof()
        await writer.drain()
        await asyncio.sleep(0.1)  # all of it (the EOF included) has landed
        release.set()
        data = await asyncio.wait_for(reader.read(), 5)
        writer.close()
        return data

    data = loop.run_until_complete(main())
    loop._core.listener_close(lid)
    assert calls == ["/first"], calls
    assert data.count(b"HTTP/1.1 200") == 1, data


def test_a_parked_parse_error_still_sees_the_client_leave(loop):
    """A malformed request pipelined behind one still being served parks
    its 400 -- and the engine stopped reading. A long-poll handler waiting
    in receive() for its client to leave was never told when it did, and
    the sweep, seeing a request still active, held the connection for
    good."""
    parked = asyncio.Event()
    seen = []

    async def app(scope, receive, send):
        await receive()
        waiter = receive()
        parked.set()
        seen.append((await waiter)["type"])

    lid, port = listen(loop, app)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /poll HTTP/1.1\r\nHost: h\r\n\r\n")
        await writer.drain()
        await asyncio.wait_for(parked.wait(), 5)
        writer.write(b"\x01\x02 not http\r\n\r\n")
        await writer.drain()
        writer.close()  # ...and the client leaves
        for _ in range(200):
            if seen:
                break
            await asyncio.sleep(0.01)

    loop.run_until_complete(main())
    loop._core.listener_close(lid)
    assert seen == ["http.disconnect"], seen


def test_a_receive_cancelled_before_it_runs_loses_no_body(loop):
    """receive() takes the request body when called, not when awaited, so
    a task wrapping it that was cancelled before its first step --
    wait_for(receive(), 0) on 3.11, an aborted task group -- lost the
    body: the next receive() parked for a disconnect instead."""
    got = []

    async def app(scope, receive, send):
        t = asyncio.ensure_future(receive())
        t.cancel()  # before it ever ran
        try:
            await t
        except asyncio.CancelledError:
            pass
        got.append(await asyncio.wait_for(receive(), 2))
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    lid, port = listen(loop, app)
    loop.run_until_complete(
        _request(port, b"POST / HTTP/1.1\r\nHost: h\r\ncontent-length: 5\r\n\r\nhello")
    )
    loop._core.listener_close(lid)
    assert got == [{"type": "http.request", "body": b"hello", "more_body": False}], got


def test_a_cancel_requested_while_the_handler_runs_cancels_its_next_await(loop):
    """A cancel() that reaches the eager driver while the handler is
    running can only be latched -- nothing is awaited yet. asyncio's Task
    applies it to the next future awaited; the driver waited for that
    future to finish by itself instead (for ever, for one that never
    does)."""
    seen = []

    async def app(scope, receive, send):
        await receive()
        lp = asyncio.get_running_loop()
        never = asyncio.Event()
        lp.call_later(2, never.set)  # the latch only fired when this did
        t0 = lp.time()
        asyncio.current_task().cancel()
        try:
            await never.wait()
            seen.append("not cancelled")
        except asyncio.CancelledError:
            seen.append(lp.time() - t0)
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    lid, port = listen(loop, app)
    loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    loop._core.listener_close(lid)
    assert len(seen) == 1 and seen[0] != "not cancelled" and seen[0] < 1.0, seen


@pytest.mark.parametrize("eager", [True, False])
def test_a_cancel_scope_knows_its_own_cancellation_at_a_checkpoint(loop, eager):
    """anyio tells its own cancellations from anyone else's by the message
    Task.cancel(msg) was given. The eager driver latched a cancel that
    found nothing awaited -- a checkpoint, sleep(0), an uncontended Lock --
    without its message and delivered a bare CancelledError, which the
    cancel scope did not recognise as its own: it escaped the `with`
    block, and a FastAPI route under move_on_after() answered 500."""
    anyio = pytest.importorskip("anyio")
    out = []

    async def app(scope, receive, send):
        await receive()
        with anyio.CancelScope() as cs:
            cs.cancel()
            await anyio.sleep(0)
        out.append(cs.cancelled_caught)
        with anyio.move_on_after(0.02) as cs2:
            while True:
                await anyio.lowlevel.checkpoint()
        out.append(cs2.cancelled_caught)
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    lid, port = listen(loop, app, eager=eager)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    loop._core.listener_close(lid)
    assert resp.startswith(b"HTTP/1.1 204"), resp
    assert out == [True, True], out


@pytest.mark.parametrize("eager", [True, False])
def test_nested_timeouts_sharing_a_deadline_fire_the_outer_one(loop, eager):
    """cancelling() and uncancel() pinned at 0 told every asyncio.timeout
    that no other cancellation was pending: two nested scopes expiring
    together both cancelled the driver, the inner one absorbed both, and
    the code after it ran on past the outer deadline."""
    out = []

    async def app(scope, receive, send):
        await receive()
        when = asyncio.get_running_loop().time() + 0.05
        try:
            async with asyncio.timeout_at(when):
                try:
                    async with asyncio.timeout_at(when):
                        await asyncio.sleep(1)
                except TimeoutError:
                    out.append("inner")
                await asyncio.sleep(0.5)
                out.append("ran past the outer deadline")
        except TimeoutError:
            out.append("outer")
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    lid, port = listen(loop, app, eager=eager)
    resp = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    loop._core.listener_close(lid)
    assert resp.startswith(b"HTTP/1.1 204"), resp
    assert out == ["outer"], out


@pytest.mark.parametrize("eager", [True, False])
def test_what_the_apps_synchronous_call_sets_stays_with_its_request(loop, eager):
    """The application was called in the loop's own context; only the
    coroutine it returned ran in a copy. A middleware whose plain
    `__call__` sets a contextvar -- the user, from a header -- before
    returning the inner app's coroutine left it set for every request
    after it, on any connection."""
    import contextvars

    user = contextvars.ContextVar("cadeloop_test_user", default=None)

    async def inner(scope, receive, send):
        await receive()
        body = json.dumps({"user": user.get()}).encode()
        headers = [(b"content-length", str(len(body)).encode())]
        await send({"type": "http.response.start", "status": 200, "headers": headers})
        await send({"type": "http.response.body", "body": body})

    class Auth:
        def __call__(self, scope, receive, send):
            for k, v in scope["headers"]:
                if k == b"x-user":
                    user.set(v.decode())
            return inner(scope, receive, send)

    lid, port = listen(loop, Auth(), eager=eager)
    first = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\nX-User: alice\r\n\r\n"))
    second = loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    loop._core.listener_close(lid)
    assert json.loads(_body(first)) == {"user": "alice"}
    assert json.loads(_body(second)) == {"user": None}, _body(second)
    assert user.get() is None, "set in the context the loop runs in"


@pytest.mark.parametrize("eager", [True, False])
def test_a_handler_raising_systemexit_still_answers_its_request(loop, eager):
    """SystemExit (or KeyboardInterrupt) from a handler stops the loop, as
    from any asyncio Task -- but it left before its request was answered,
    with the driver never marked finished: the client got nothing until
    the shutdown's deadline closed the connection, and the shutdown
    reported a task still running. The request is now answered (500)
    first. On the non-eager path the Task already raised it out of the
    loop; its done callback no longer raises it a second time, which cut
    short the shutdown the first one started."""

    async def app(scope, receive, send):
        await receive()
        await asyncio.sleep(0.01)
        sys.exit(3)

    lid, port = listen(loop, app, eager=eager)
    client = loop.create_task(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    with pytest.raises(SystemExit):
        loop.run_until_complete(client)
    # The loop runs on, as serve()'s shutdown drain does.
    resp = loop.run_until_complete(client)
    loop._core.listener_close(lid)
    assert resp.startswith(b"HTTP/1.1 500"), resp
    assert all(d.done() for d in loop._core.http_request_drivers())


def test_contextvar_isolation_pipelined_requests(loop):
    """A request pipelined behind one that suspended started with every
    contextvar that one had set: the eager driver pumped it from inside
    the finished request's own wakeup, and copied its context from there."""
    import contextvars

    cv = contextvars.ContextVar("cadeloop_test_cv3", default=None)

    async def cv_app(scope, receive, send):
        await receive()
        seen_before = cv.get()
        cv.set(scope["path"])
        await asyncio.sleep(0)
        body = json.dumps({"seen_before": seen_before}).encode()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": body})

    lid, port = listen(loop, cv_app)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /a HTTP/1.1\r\nHost: h\r\n\r\nGET /b HTTP/1.1\r\nHost: h\r\n\r\n")
        await writer.drain()
        out = [await asyncio.wait_for(_read_one_response(reader), 5) for _ in range(2)]
        writer.close()
        return out

    first, second = loop.run_until_complete(main())
    loop._core.listener_close(lid)
    assert json.loads(_body(first)) == {"seen_before": None}
    assert json.loads(_body(second)) == {"seen_before": None}, _body(second)


def test_a_cancelled_non_eager_request_is_not_an_application_error(loop):
    """With eager_tasks off, a request task that was cancelled -- one still
    in flight when a graceful shutdown's deadline passes, say -- was
    reported as "Exception in ASGI application: RuntimeError: ASGI
    application task cancelled". A cancellation is how a request is
    abandoned, as the eager driver already treats it."""
    reports = []
    loop.set_exception_handler(lambda _loop, ctx: reports.append(ctx.get("message")))
    tasks = []

    async def app(scope, receive, send):
        await receive()
        tasks.append(asyncio.current_task())
        await asyncio.sleep(3600)

    lid, port = listen(loop, app, eager=False)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET / HTTP/1.1\r\nHost: h\r\n\r\n")
        await writer.drain()
        for _ in range(500):
            if tasks:
                break
            await asyncio.sleep(0.01)
        tasks[0].cancel()
        data = await asyncio.wait_for(reader.read(), 5)
        writer.close()
        return data

    data = loop.run_until_complete(main())
    loop._core.listener_close(lid)
    assert data.startswith(b"HTTP/1.1 500"), data[:40]
    assert not [r for r in reports if "Exception in ASGI application" in str(r)], reports


def test_close_cancels_what_is_parked_on_a_connection(loop):
    """loop.close() tore native connections down and dropped what was
    parked on them pending: receive()/send() waiters were never resolved,
    and an eager handler's coroutine was never finalized -- it and the
    future it awaited kept each other alive in a cycle the collector
    cannot see. Both are cancelled first now."""
    import gc
    import weakref

    refs, ran, waiters, holders = {}, [], [], []
    parked = asyncio.Event()

    async def app(scope, receive, send):
        await receive()

        async def listener():
            waiter = receive()  # parks: the body was already delivered
            waiters.append(waiter)
            await waiter

        holders.append(asyncio.ensure_future(listener()))
        await asyncio.sleep(0)  # the listener parks
        refs["coro"] = weakref.ref(asyncio.current_task().get_coro())
        parked.set()
        try:
            await asyncio.sleep(3600)
        finally:
            ran.append("finally")

    lid, port = listen(loop, app)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET / HTTP/1.1\r\nHost: h\r\n\r\n")
        await writer.drain()
        await asyncio.wait_for(parked.wait(), 5)
        return writer

    writer = loop.run_until_complete(main())
    loop.set_exception_handler(lambda _loop, _ctx: None)
    loop._core.listener_close(lid)
    loop.close()
    gc.collect()
    writer.close()
    assert waiters and waiters[0].cancelled(), waiters
    assert ran == ["finally"] and refs["coro"]() is None, (ran, refs["coro"]())


# --------------------------------------------------------------------- #
# a request's turn on its connection ends with its response             #
# --------------------------------------------------------------------- #


def _ok_then(work):
    """An app that answers "ok" and then does ``work(path)`` inside the same
    ASGI call -- where Starlette runs a BackgroundTask."""

    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-length", b"2")]})
        await send({"type": "http.response.body", "body": b"ok"})
        await work(scope["path"])

    return app


@pytest.mark.parametrize("eager", [True, False])
def test_background_work_does_not_hold_up_the_next_request(loop, eager):
    """A request ended when its coroutine returned, not when its response
    completed, so work done after the response -- Starlette runs a
    BackgroundTask inside the app call, after the last body message --
    held the connection: the next request on it, pipelined or not, waited
    for that work (1.5 s behind a 1.5 s task; uvicorn answers it at once).
    The connection is now released when the response completes, and the
    coroutine runs on by itself."""
    finished = []

    async def work(path):
        if path == "/slow":
            await asyncio.sleep(1.0)
            finished.append(path)

    lid, port = listen(loop, _ok_then(work), eager=eager)

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /slow HTTP/1.1\r\nHost: h\r\n\r\nGET /a HTTP/1.1\r\nHost: h\r\n\r\n")
        await writer.drain()
        t0 = loop.time()
        for _ in range(2):
            await asyncio.wait_for(_read_one_response(reader), 5)
        pipelined = loop.time() - t0
        t0 = loop.time()
        writer.write(b"GET /b HTTP/1.1\r\nHost: h\r\n\r\n")
        await writer.drain()
        await asyncio.wait_for(_read_one_response(reader), 5)
        sequential = loop.time() - t0
        writer.close()
        await asyncio.sleep(1.2)  # the background work still completes
        return pipelined, sequential

    pipelined, sequential = loop.run_until_complete(main())
    loop._core.listener_close(lid)
    assert pipelined < 0.5 and sequential < 0.5, (pipelined, sequential)
    assert finished == ["/slow"], finished


@pytest.mark.parametrize("eager", [True, False])
def test_a_close_delimited_response_ends_before_background_work(loop, eager):
    """An HTTP/1.0 response without a length ends when the connection
    closes, and the close waited for the coroutine to return: the client
    sat through the background work before it saw the end of its response
    (1.52 s against uvicorn's 0.02 s)."""

    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"hello ", "more_body": True})
        await send({"type": "http.response.body", "body": b"world"})
        await asyncio.sleep(1.0)

    lid, port = listen(loop, app, eager=eager)

    async def main():
        t0 = loop.time()
        data = await _request(port, b"GET / HTTP/1.0\r\n\r\n", read_all=True)
        return data, loop.time() - t0

    data, took = loop.run_until_complete(main())
    loop._core.listener_close(lid)
    assert data.endswith(b"hello world"), data
    assert took < 0.5, took


@pytest.mark.parametrize("eager", [True, False])
def test_background_work_survives_the_dead_peer_reclaim(loop, eager):
    """The sweep reclaims a request whose client has gone (H4) -- and it
    counted one still doing background work after a complete response as
    such: a client that read its response and closed had the work
    cancelled once the idle deadline passed."""
    outcome = []

    async def work(path):
        try:
            await asyncio.sleep(0.6)
            outcome.append("done")
        except asyncio.CancelledError:
            outcome.append("cancelled")
            raise

    lid, port = listen(loop, _ok_then(work), eager=eager, keepalive_idle=0.1)

    def sweep():
        loop._core.http_sweep()
        sweeper[0] = loop.call_later(0.03, sweep)

    sweeper = [loop.call_soon(sweep)]

    async def main():
        await _request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n")  # reads, then closes
        await asyncio.sleep(1.0)

    try:
        loop.run_until_complete(main())
    finally:
        sweeper[0].cancel()
    loop._core.listener_close(lid)
    assert outcome == ["done"], outcome


@pytest.mark.parametrize("eager", [True, False])
def test_a_request_torn_off_by_a_reset_can_still_be_cancelled(loop, eager):
    """A connection reset tore the connection down and dropped the eager
    driver with it: nothing could reach the handler any more, so a
    graceful shutdown never cancelled it -- its `finally` never ran -- and,
    AppTask being opaque to the cycle collector, the coroutine was never
    reclaimed either."""
    import struct

    from cadeloop.server import _cancel_requests

    started = asyncio.Event()
    ran = []

    async def app(scope, receive, send):
        await receive()
        started.set()
        try:
            await asyncio.sleep(3600)
        finally:
            ran.append("finally")

    lid, port = listen(loop, app, eager=eager)
    s = socket.create_connection(("127.0.0.1", port))
    try:
        s.sendall(b"GET / HTTP/1.1\r\nHost: h\r\n\r\n")
        loop.run_until_complete(asyncio.wait_for(started.wait(), 5))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    finally:
        s.close()  # RST

    async def gone():
        while loop._core.http_connection_count():
            await asyncio.sleep(0.01)

    loop.run_until_complete(asyncio.wait_for(gone(), 5))
    loop._core.listener_close(lid)
    assert len(loop._core.http_request_drivers()) == 1
    _cancel_requests(loop, 2.0)
    assert ran == ["finally"], ran
    assert loop._core.http_request_drivers() == []


def test_the_drain_waits_for_background_work(loop):
    """Released at its response, background work no longer keeps a
    connection open, so the drain counts it by itself: a graceful
    shutdown lets it finish within the grace period, as uvicorn waits for
    every request task."""
    from cadeloop.server import _drain_connections

    ran = []

    async def work(path):
        await asyncio.sleep(0.4)
        ran.append("background")

    lid, port = listen(loop, _ok_then(work))
    loop.run_until_complete(_request(port, b"GET / HTTP/1.1\r\nHost: h\r\n\r\n"))
    loop._core.listener_close(lid)
    _drain_connections(loop, 5.0)
    assert ran == ["background"], ran


# --------------------------------------------------------------------- #
# 100 Continue in pipeline order; HTTP/1.0 keep-alive announced          #
# --------------------------------------------------------------------- #


async def _stream_then_upload_app(scope, receive, send):
    if scope["path"] == "/stream":
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"a" * 10, "more_body": True})
        await asyncio.sleep(0.2)
        await send({"type": "http.response.body", "body": b"b" * 10, "more_body": True})
        await send({"type": "http.response.body", "body": b""})
        return
    msg = await receive()
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"got:" + msg["body"]})


def test_a_pipelined_expect_waits_for_the_responses_ahead_of_it(loop):
    """The 100 Continue for a pipelined `Expect` head was written as soon
    as the head was parsed -- into the middle of the earlier request's
    streamed response, breaking its chunked framing. It now follows every
    response ahead of it, and still arrives before the body is sent."""
    lid, port = listen(loop, _stream_then_upload_app)

    async def client():
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(
            b"GET /stream HTTP/1.1\r\nHost: h\r\n\r\n"
            b"POST /u HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n"
        )
        await w.drain()
        first = await asyncio.wait_for(_read_one_response(r), 5.0)
        interim = await asyncio.wait_for(r.readuntil(b"\r\n\r\n"), 5.0)
        w.write(b"hello")
        await w.drain()
        second = await asyncio.wait_for(_read_one_response(r), 5.0)
        w.close()
        return first, interim, second

    first, interim, second = loop.run_until_complete(client())
    assert b"100 Continue" not in first, first
    assert first.endswith(b"0\r\n\r\n") and b"a" * 10 in first and b"b" * 10 in first, first
    assert interim == b"HTTP/1.1 100 Continue\r\n\r\n", interim
    assert second.startswith(b"HTTP/1.1 200") and second.endswith(b"got:hello"), second
    loop._core.listener_close(lid)


def test_an_expect_whose_body_already_arrived_gets_no_interim_response(loop):
    """A client that sent the body with the head needs no 100 (RFC 7231
    5.1.1 lets the server omit it). Pipelined behind a streaming response
    it was written into that response; sent after this request's own
    response it would answer the next request instead."""
    lid, port = listen(loop, _stream_then_upload_app)

    async def client():
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(
            b"GET /stream HTTP/1.1\r\nHost: h\r\n\r\n"
            b"POST /u HTTP/1.1\r\nHost: h\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\nhello"
            b"POST /u HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\n\r\nhi"
        )
        await w.drain()
        out = [await asyncio.wait_for(_read_one_response(r), 5.0) for _ in range(3)]
        w.close()
        return out

    first, second, third = loop.run_until_complete(client())
    assert b"100 Continue" not in first, first
    assert second.startswith(b"HTTP/1.1 200") and second.endswith(b"got:hello"), second
    assert third.startswith(b"HTTP/1.1 200") and third.endswith(b"got:hi"), third
    loop._core.listener_close(lid)


def test_http10_keep_alive_is_announced(loop):
    """An HTTP/1.0 connection persists only if the response says so. One
    that asked for keep-alive was kept open without being told, so a 1.0
    client reading to EOF waited out the idle timeout."""

    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    lid, port = listen(loop, app)

    async def client():
        r, w = await asyncio.open_connection("127.0.0.1", port)
        req = b"GET / HTTP/1.0\r\nHost: h\r\nConnection: keep-alive\r\n\r\n"
        w.write(req)
        first = await asyncio.wait_for(_read_one_response(r), 5.0)
        w.write(req)
        second = await asyncio.wait_for(_read_one_response(r), 5.0)
        w.close()
        # Without the request, no promise: the server closes after one.
        plain_req = b"GET / HTTP/1.0\r\nHost: h\r\n\r\n"
        plain = await asyncio.wait_for(_request(port, plain_req, read_all=True), 5.0)
        return first, second, plain

    first, second, plain = loop.run_until_complete(client())
    for resp in (first, second):
        assert resp.startswith(b"HTTP/1.0 200"), resp
        head = resp.split(b"\r\n\r\n")[0] + b"\r\n\r\n"
        assert _parse_headers(head).get("connection") == "keep-alive", resp
    assert b"keep-alive" not in plain.lower(), plain
    loop._core.listener_close(lid)


def test_a_close_delimited_http10_response_is_not_announced_persistent(loop):
    """A streamed response to a 1.0 client has no length to frame it, so it
    ends at close -- and must not claim the connection will persist."""

    async def app(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"one", "more_body": True})
        await send({"type": "http.response.body", "body": b"two"})

    lid, port = listen(loop, app)
    raw = b"GET / HTTP/1.0\r\nHost: h\r\nConnection: keep-alive\r\n\r\n"
    resp = loop.run_until_complete(_request(port, raw, read_all=True))
    assert resp.startswith(b"HTTP/1.0 200") and resp.endswith(b"onetwo"), resp
    assert b"keep-alive" not in resp.lower(), resp
    loop._core.listener_close(lid)


def test_idle_connections_hold_small_receive_buffers(loop):
    """A connection keeps a receive posted, buffer and all, for as long as
    it is idle. At a fixed 64 KiB that was 64 KiB of committed memory per
    idle keep-alive connection; reads now start at 4 KiB and grow only
    when they fill the buffer -- so a large upload still arrives whole."""

    async def app(scope, receive, send):
        n, more = 0, True
        while more:
            msg = await receive()
            n += len(msg.get("body", b""))
            more = msg.get("more_body", False)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": str(n).encode()})

    lid, port = listen(loop, app)

    async def client():
        conns = []
        for _ in range(32):
            r, w = await asyncio.open_connection("127.0.0.1", port)
            w.write(b"GET / HTTP/1.1\r\nHost: h\r\n\r\n")
            await asyncio.wait_for(_read_one_response(r), 5.0)
            conns.append((r, w))
        await asyncio.sleep(0.05)
        idle = loop.stats()["buffer_bytes_in_use"]
        r, w = conns[0]
        size = 4 * 1024 * 1024
        w.write(b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: %d\r\n\r\n" % size + b"z" * size)
        resp = await asyncio.wait_for(_read_one_response(r), 10.0)
        for _, w in conns:
            w.close()
        return idle, resp, size

    idle, resp, size = loop.run_until_complete(client())
    # 32 server-side connections and the 32 client streams, all on this
    # loop: 64 receives posted. At 64 KiB apiece that was 4 MiB.
    assert idle <= 64 * 16384, f"{idle / 64 / 1024:.1f} KiB per idle receive"
    assert _body(resp) == str(size).encode(), resp[-80:]
    loop._core.listener_close(lid)


def test_an_early_answer_to_an_upload_survives_the_close(loop):
    """Closing with the peer's upload still arriving made the close a
    reset, and Windows discards what a connection received but had not yet
    read when the RST lands: a client answered 413 at headers-complete
    while still sending got ECONNRESET instead of the 413. The response is
    now followed by a FIN, and the peer's input drained, before the close."""

    async def app(scope, receive, send):  # pragma: no cover - never reached
        raise AssertionError("the app must not see an over-cap request")

    lid, port = listen(loop, app, max_body=1024)
    size = 8 * 1024 * 1024

    def uploader():
        s = socket.create_connection(("127.0.0.1", port))
        try:
            s.sendall(b"POST / HTTP/1.1\r\nHost: h\r\nContent-Length: %d\r\n\r\n" % size)
            chunk = b"z" * 65536
            for _ in range(size // len(chunk)):
                s.sendall(chunk)  # a naive client: it never reads while sending
            s.settimeout(5)
            data = b""
            while chunk := s.recv(65536):
                data += chunk
            return data
        finally:
            s.close()

    data = loop.run_until_complete(loop.run_in_executor(None, uploader))
    assert data.startswith(b"HTTP/1.1 413"), data[:80]
    loop._core.listener_close(lid)
