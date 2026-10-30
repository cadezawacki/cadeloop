"""One ASGI app for every Windows HTTP/WebSocket workload.

Every contender serves this same module, so response bytes, framing and
application work are identical across servers. Paths:

  /          13-byte plaintext (the classic "Hello, World!")
  /json      a small JSON document serialized per request
  /large     64 KiB body, one message
  /stream    1 MiB body as 64 x 16 KiB chunks (more_body)
  /upload    reads the whole request body, answers its length
  /sleep     awaits asyncio.sleep(0.001) first (a short timer per request)
  /ws        WebSocket echo (text and bytes)
"""

import asyncio
import json

PLAIN = b"Hello, World!"
H_PLAIN = [(b"content-type", b"text/plain"), (b"content-length", b"13")]
LARGE = b"x" * 65536
H_LARGE = [(b"content-type", b"application/octet-stream"), (b"content-length", b"65536")]
CHUNK = b"y" * 16384
H_STREAM = [(b"content-type", b"application/octet-stream")]
H_JSON = [(b"content-type", b"application/json")]
DOC = {"message": "Hello, World!", "items": [{"id": i, "name": f"item-{i}", "ok": i % 2 == 0} for i in range(10)]}
JSON_LEN = len(json.dumps(DOC).encode())


async def _lifespan(receive, send):
    while True:
        msg = await receive()
        if msg["type"] == "lifespan.startup":
            await send({"type": "lifespan.startup.complete"})
        elif msg["type"] == "lifespan.shutdown":
            await send({"type": "lifespan.shutdown.complete"})
            return


async def _websocket(receive, send):
    msg = await receive()
    if msg["type"] != "websocket.connect":
        return
    await send({"type": "websocket.accept"})
    while True:
        msg = await receive()
        if msg["type"] == "websocket.receive":
            if msg.get("bytes") is not None:
                await send({"type": "websocket.send", "bytes": msg["bytes"]})
            else:
                await send({"type": "websocket.send", "text": msg["text"]})
        else:
            return


async def app(scope, receive, send):
    kind = scope["type"]
    if kind == "http":
        path = scope["path"]
        if path == "/":
            await send({"type": "http.response.start", "status": 200, "headers": H_PLAIN})
            await send({"type": "http.response.body", "body": PLAIN})
        elif path == "/json":
            body = json.dumps(DOC).encode()
            await send({"type": "http.response.start", "status": 200,
                        "headers": H_JSON + [(b"content-length", str(len(body)).encode())]})
            await send({"type": "http.response.body", "body": body})
        elif path == "/large":
            await send({"type": "http.response.start", "status": 200, "headers": H_LARGE})
            await send({"type": "http.response.body", "body": LARGE})
        elif path == "/stream":
            await send({"type": "http.response.start", "status": 200, "headers": H_STREAM})
            for _ in range(63):
                await send({"type": "http.response.body", "body": CHUNK, "more_body": True})
            await send({"type": "http.response.body", "body": CHUNK})
        elif path == "/upload":
            n = 0
            more = True
            while more:
                msg = await receive()
                n += len(msg.get("body", b""))
                more = msg.get("more_body", False)
            body = str(n).encode()
            await send({"type": "http.response.start", "status": 200,
                        "headers": [(b"content-length", str(len(body)).encode())]})
            await send({"type": "http.response.body", "body": body})
        elif path == "/sleep":
            await asyncio.sleep(0.001)
            await send({"type": "http.response.start", "status": 200, "headers": H_PLAIN})
            await send({"type": "http.response.body", "body": PLAIN})
        else:
            await send({"type": "http.response.start", "status": 404, "headers": [(b"content-length", b"0")]})
            await send({"type": "http.response.body", "body": b""})
    elif kind == "websocket":
        await _websocket(receive, send)
    elif kind == "lifespan":
        await _lifespan(receive, send)
