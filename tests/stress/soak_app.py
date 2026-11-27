"""The server side of soak_features.py: one app that uses, together, the
server features a long-running service would have switched on.

Run by soak_features.py as `cadeloop.serve("soak_app:app", ...)`; the
settings come from the environment (SOAK_DIR holds the certificate, the
file to send, and the access log).
"""

from __future__ import annotations

import asyncio
import json
import os
import ssl
import sys
import threading

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from soak_timers import rss_bytes  # noqa: E402

DIR = os.environ.get("SOAK_DIR", ".")
PORT = int(os.environ.get("SOAK_PORT", "0"))

app = FastAPI()
TAPPED = {"batches": 0, "records": 0, "statuses": {}}
_client = None


class Item(BaseModel):
    id: int
    name: str
    ok: bool


class Doc(BaseModel):
    message: str
    items: list[Item]


ITEMS = [{"id": i, "name": f"item-{i}", "ok": i % 2 == 0} for i in range(10)]
CACHE: dict[int, dict] = {}


def tap(batch):
    """access_log_tap: called on the loop thread with a batch of records."""
    TAPPED["batches"] += 1
    TAPPED["records"] += len(batch)
    for _client, _method, _target, status, _duration_ms in batch:
        TAPPED["statuses"][status] = TAPPED["statuses"].get(status, 0) + 1


@app.get("/json", response_model=Doc)
async def model():
    return {"message": "Hello, World!", "items": ITEMS}


@app.get("/sync", response_model=Doc)
def sync():
    return {"message": "Hello, World!", "items": ITEMS}


@app.get("/alloc")
async def alloc(n: int = 2000):
    # What a handler that builds a result set leaves for the collector.
    rows = [{"id": i, "tags": [i, i + 1], "ok": True} for i in range(n)]
    return {"rows": len(rows)}


@app.get("/cache")
async def cache(fill: int = 0, churn: int = 400):
    # State built after startup and kept: what gc_mode="refreeze" is for. A
    # full collection walks all of it until one runs long and the survivors
    # are frozen; from then on the entries replaced here are freed by
    # reference counting, frozen or not.
    if fill and not CACHE:
        CACHE.update((i, {"id": i, "tags": [i, i + 1]}) for i in range(fill))
    if CACHE:
        start = TAPPED["records"] * 7919
        for i in range(churn):
            key = (start + i) % len(CACHE)
            CACHE[key] = {"id": key, "tags": [key, key + 1]}
    return {"entries": len(CACHE)}


@app.get("/small")
async def small():
    return FileResponse(os.path.join(DIR, "small.bin"), media_type="application/octet-stream")


@app.get("/file")
async def file():
    return FileResponse(os.path.join(DIR, "large.bin"), media_type="application/octet-stream")


@app.get("/upstream")
async def upstream():
    """An outgoing HTTPS request by host name, from inside a handler: the
    resolver threads, and a TLS client connection on the server's loop."""
    global _client
    import aiohttp

    if _client is None:
        ctx = ssl.create_default_context(cafile=os.path.join(DIR, "ca.pem"))
        # No DNS cache in the client, and connections that are not kept:
        # every call resolves "localhost" and makes a TLS connection.
        connector = aiohttp.TCPConnector(ssl=ctx, use_dns_cache=False, force_close=True)
        _client = aiohttp.ClientSession(connector=connector)
    async with _client.get(f"https://localhost:{PORT}/json") as resp:
        return {"status": resp.status, "bytes": len(await resp.read())}


@app.websocket("/ws")
async def ws(sock: WebSocket):
    await sock.accept()
    try:
        while True:
            message = await sock.receive()
            if message["type"] != "websocket.receive":
                return
            if message.get("text") is not None:
                await sock.send_text(message["text"])
            else:
                await sock.send_bytes(message["bytes"])
    except WebSocketDisconnect:
        return


@app.get("/_soak")
async def soak():
    """What the driver samples: this process as it sees itself."""
    loop = asyncio.get_running_loop()
    stats = loop.stats()
    try:
        fds = len(os.listdir("/proc/self/fd"))
    except OSError:
        fds = None
    watch = getattr(loop, "_gc_watch", None)
    return json.loads(json.dumps({
        "rss": rss_bytes(),
        "fds": fds,
        "threads": threading.active_count(),
        "tapped": TAPPED,
        "gc": watch.snapshot() if watch is not None else None,
        "loop": {k: stats.get(k) for k in (
            "connections", "access_log_records", "access_log_dropped", "dns_lookups", "dns_coalesced",
            "dns_cache_hits", "buffers_in_use", "buffer_bytes_held", "stale_buffer_ids", "unreaped_ops")},
    }))
