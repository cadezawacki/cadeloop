"""Plaintext 'Hello, World!' ASGI app (R-132 plaintext benchmark shape)."""

BODY = b"Hello, World!"
HEADERS = [(b"content-type", b"text/plain"), (b"content-length", b"13")]


async def app(scope, receive, send):
    if scope["type"] != "http":
        return
    await send({"type": "http.response.start", "status": 200, "headers": HEADERS})
    await send({"type": "http.response.body", "body": BODY})
