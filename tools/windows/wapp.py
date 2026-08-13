"""Spec-addressable ASGI app for the multi-worker validation step.

The response carries the worker's PID so the step's output shows real
request distribution across the spawned pool (R-090).
"""

import os


async def app(scope, receive, send):
    if scope["type"] != "http":
        return
    await receive()
    body = f"ok pid={os.getpid()}".encode()
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"text/plain")],
        }
    )
    await send({"type": "http.response.body", "body": body})
