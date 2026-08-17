#!/usr/bin/env python3
"""Windows backend behavioral validation (M1 IOCP + M3 RIO gates).

Runs the same battery against each requested backend and prints a
PASS/FAIL line per check plus a JSON results file. Standalone on purpose
(no pytest): every check builds a fresh loop, so one wedged backend
cannot poison the rest, and a hang is bounded by the per-check watchdog.

Usage:
    python tools/windows/rio_smoke.py [iocp] [rio] [--out results.json]

Checks (each on a fresh loop of the backend under test):
  construct        loop builds; stats()["backend"] matches
  echo_small       200 x 1 KiB request/response round trips, one conn
  echo_large       10 MiB each direction (partial sends, >64K staging)
  many_conns       120 concurrent echo conns on rio_cq_size=1024 —
                   forces repeated CQ doubling (R-041) on RIO
  http_native      native engine: keep-alive, pipelined burst, chunked
                   streaming, HEAD, 400 path
  abrupt_close     clients vanish mid-traffic; loop stays healthy
  mixed_outbound   create_connection (plain socket -> IOCP fallback on
                   RIO) talks to a native-accept (RQ) server socket
  soak_echo        3s sustained echo churn; loop closes clean
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import sys
import threading
import time
import traceback

# Prefer an INSTALLED cadeloop (wheel validation on Rust-less machines);
# fall back to the repo tree for the in-repo validation flow.
try:
    import cadeloop
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
    import cadeloop  # noqa: E402

CHECK_TIMEOUT = 90.0

# Final stats() snapshot of the last loop a check closed. Carries the RIO
# diagnosis counters (rio_notifies / rio_watchdog_reaps) and the LIVE
# backend name (a mid-run RIONotify failure downgrades "rio" to
# "rio-polling") into the per-check result rows.
LAST_STATS: dict = {}
_STAT_KEYS = ("backend", "polls", "completions", "rio_notifies", "rio_watchdog_reaps")


def _loop(backend: str, **kw) -> "cadeloop.Loop":
    from cadeloop.loop import Loop

    lp = Loop(backend=backend, **kw)
    orig_close = lp.close

    def close_with_snapshot():
        try:
            st = lp.stats()
            for k in _STAT_KEYS:
                if k in st:
                    LAST_STATS[k] = st[k]
        except Exception:
            pass
        orig_close()

    lp.close = close_with_snapshot
    return lp


async def _echo_server(loop):
    async def handler(reader, writer):
        while True:
            data = await reader.read(1 << 16)
            if not data:
                break
            writer.write(data)
            await writer.drain()
        writer.close()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


# ------------------------------------------------------------------ #
# checks                                                             #
# ------------------------------------------------------------------ #


def check_construct(backend):
    lp = _loop(backend)
    try:
        name = lp.stats()["backend"]
        if backend != "auto":
            # Degraded-mode names are suffixed (e.g. "rio-polling" when the
            # CQ runs without IOCP notification) — still the right backend.
            assert name == backend or name.startswith(backend + "-"), (
                f"stats backend {name!r} != {backend!r}"
            )
        return {"backend_name": name}
    finally:
        lp.close()


def check_echo_small(backend):
    lp = _loop(backend)
    asyncio.set_event_loop(lp)
    try:
        async def main():
            server, port = await _echo_server(lp)
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            payload = os.urandom(1024)
            t0 = time.perf_counter()
            for _ in range(200):
                writer.write(payload)
                await writer.drain()
                got = await reader.readexactly(len(payload))
                assert got == payload
            dt = time.perf_counter() - t0
            writer.close()
            server.close()
            await server.wait_closed()
            return {"msgs_per_sec": round(200 / dt)}

        return lp.run_until_complete(asyncio.wait_for(main(), CHECK_TIMEOUT))
    finally:
        asyncio.set_event_loop(None)
        lp.close()


def check_echo_large(backend):
    lp = _loop(backend)
    asyncio.set_event_loop(lp)
    try:
        async def main():
            server, port = await _echo_server(lp)
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            chunk = os.urandom(256 * 1024)
            total = 40  # 10 MiB
            received = bytearray()

            async def drain_reader():
                while len(received) < total * len(chunk):
                    received.extend(await reader.read(1 << 20))

            consumer = lp.create_task(drain_reader())
            for _ in range(total):
                writer.write(chunk)
                await writer.drain()
            await consumer
            assert bytes(received) == chunk * total, "10 MiB corrupted in flight"
            writer.close()
            server.close()
            await server.wait_closed()
            return {"mib": 10, "intact": True}

        return lp.run_until_complete(asyncio.wait_for(main(), CHECK_TIMEOUT))
    finally:
        asyncio.set_event_loop(None)
        lp.close()


def check_many_conns(backend):
    # Small CQ on purpose: 120 conns x (rq_recv+rq_send)=64 slots needs
    # 7680 >> 1024, forcing RIOResizeCompletionQueue several times.
    lp = _loop(backend, rio_cq_size=1024)
    asyncio.set_event_loop(lp)
    try:
        async def main():
            server, port = await _echo_server(lp)

            async def one(i):
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                msg = bytes([i % 256]) * 512
                for _ in range(5):
                    writer.write(msg)
                    await writer.drain()
                    assert await reader.readexactly(len(msg)) == msg
                writer.close()

            await asyncio.gather(*(one(i) for i in range(120)))
            server.close()
            await server.wait_closed()
            return {"conns": 120, "cq_started_at": 1024}

        return lp.run_until_complete(asyncio.wait_for(main(), CHECK_TIMEOUT))
    finally:
        asyncio.set_event_loop(None)
        lp.close()


def check_http_native(backend):
    lp = _loop(backend)
    asyncio.set_event_loop(lp)
    try:
        async def app(scope, receive, send):
            if scope["type"] != "http":
                return
            await receive()
            if scope["path"] == "/stream":
                await send({"type": "http.response.start", "status": 200, "headers": []})
                for part in (b"one", b"two", b"three"):
                    await send({"type": "http.response.body", "body": part, "more_body": True})
                await send({"type": "http.response.body", "body": b""})
                return
            body = scope["path"].encode()
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            })
            await send({"type": "http.response.body", "body": body})

        lid, bound, _fd = lp._core.http_listen("127.0.0.1", 0, app, lp)
        port = bound[1]

        async def request_raw(raw, read_all=True):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(raw)
            await writer.drain()
            data = await asyncio.wait_for(reader.read(1 << 20), 10)
            writer.close()
            return data

        async def main():
            # keep-alive reuse
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            for i in range(20):
                writer.write(f"GET /ka{i} HTTP/1.1\r\nHost: h\r\n\r\n".encode())
                await writer.drain()
                head = await reader.readuntil(b"\r\n\r\n")
                assert b"200 OK" in head
                clen = int([l for l in head.split(b"\r\n") if b"content-length" in l.lower()][0].split(b":")[1])
                body = await reader.readexactly(clen)
                assert body == f"/ka{i}".encode()
            # pipelined burst, strict order
            burst = b"".join(f"GET /p{i} HTTP/1.1\r\nHost: h\r\n\r\n".encode() for i in range(10))
            writer.write(burst)
            await writer.drain()
            blob = b""
            deadline = time.monotonic() + 10
            while blob.count(b"200 OK") < 10 and time.monotonic() < deadline:
                blob += await reader.read(1 << 16)
            for i in range(10):
                assert f"/p{i}".encode() in blob
            order = [blob.index(f"/p{i}".encode()) for i in range(10)]
            assert order == sorted(order), "pipelined responses out of order"
            writer.close()
            # chunked stream
            resp = await request_raw(b"GET /stream HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
            assert b"transfer-encoding: chunked" in resp.lower() and b"three" in resp
            # HEAD suppression
            resp = await request_raw(b"HEAD /h HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
            head, _, body = resp.partition(b"\r\n\r\n")
            assert body == b"", "HEAD must carry no body"
            # malformed -> in-cell 400
            resp = await request_raw(b"NOT A REQUEST\r\n\r\n")
            assert resp.startswith(b"HTTP/1.1 400"), resp[:60]
            return {"keepalive": 20, "pipelined": 10, "chunked": True, "head": True, "b400": True}

        result = lp.run_until_complete(asyncio.wait_for(main(), CHECK_TIMEOUT))
        lp._core.listener_close(lid)
        return result
    finally:
        asyncio.set_event_loop(None)
        lp.close()


def check_abrupt_close(backend):
    lp = _loop(backend)
    asyncio.set_event_loop(lp)
    lp.set_exception_handler(lambda l, ctx: None)  # resets are the point here
    try:
        async def main():
            server, port = await _echo_server(lp)

            def storm():
                for _ in range(50):
                    try:
                        s = socket.create_connection(("127.0.0.1", port), timeout=2)
                        s.sendall(b"x" * 4096)
                        s.close()  # vanish with data possibly in flight
                    except OSError:
                        pass

            t = threading.Thread(target=storm)
            t.start()
            while t.is_alive():
                await asyncio.sleep(0.05)
            t.join()
            # Loop must still serve after the storm.
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"alive?")
            await writer.drain()
            assert await reader.readexactly(6) == b"alive?"
            writer.close()
            server.close()
            await server.wait_closed()
            return {"storm_conns": 50, "healthy_after": True}

        return lp.run_until_complete(asyncio.wait_for(main(), CHECK_TIMEOUT))
    finally:
        asyncio.set_event_loop(None)
        lp.close()


def check_mixed_outbound(backend):
    # Outbound create_connection sockets are plain (no REGISTERED_IO
    # flag): on RIO they must fall back to inner-IOCP ops while the
    # accept side runs an RQ — this proves the mixed mode end to end.
    lp = _loop(backend)
    asyncio.set_event_loop(lp)
    try:
        async def main():
            server, port = await _echo_server(lp)
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            payload = os.urandom(8192)
            for _ in range(50):
                writer.write(payload)
                await writer.drain()
                assert await reader.readexactly(len(payload)) == payload
            writer.close()
            server.close()
            await server.wait_closed()
            return {"roundtrips": 50}

        return lp.run_until_complete(asyncio.wait_for(main(), CHECK_TIMEOUT))
    finally:
        asyncio.set_event_loop(None)
        lp.close()


def check_soak_echo(backend):
    lp = _loop(backend)
    asyncio.set_event_loop(lp)
    try:
        async def main():
            server, port = await _echo_server(lp)
            stop_at = time.monotonic() + 3.0
            msgs = 0

            async def churn(i):
                nonlocal msgs
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                data = os.urandom(2048)
                while time.monotonic() < stop_at:
                    writer.write(data)
                    await writer.drain()
                    assert await reader.readexactly(len(data)) == data
                    msgs += 1
                writer.close()

            await asyncio.gather(*(churn(i) for i in range(8)))
            server.close()
            await server.wait_closed()
            return {"seconds": 3, "msgs": msgs}

        return lp.run_until_complete(asyncio.wait_for(main(), CHECK_TIMEOUT + 10))
    finally:
        asyncio.set_event_loop(None)
        lp.close()


CHECKS = [
    ("construct", check_construct),
    ("echo_small", check_echo_small),
    ("echo_large", check_echo_large),
    ("many_conns", check_many_conns),
    ("http_native", check_http_native),
    ("abrupt_close", check_abrupt_close),
    ("mixed_outbound", check_mixed_outbound),
    ("soak_echo", check_soak_echo),
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("backends", nargs="*", default=None)
    parser.add_argument("--out", default="rio-smoke-results.json")
    args = parser.parse_args()
    backends = args.backends or (["iocp", "rio"] if sys.platform == "win32" else ["auto"])

    results = {"platform": sys.platform, "python": sys.version, "backends": {}}
    failures = 0
    for backend in backends:
        print(f"=== backend={backend} ===", flush=True)
        results["backends"][backend] = {}
        for name, fn in CHECKS:
            t0 = time.perf_counter()
            LAST_STATS.clear()
            try:
                detail = fn(backend)
                dt = time.perf_counter() - t0
                stats = dict(LAST_STATS)
                print(f"  PASS  {name:16s} ({dt:.2f}s)  {detail}  stats={stats}", flush=True)
                results["backends"][backend][name] = {
                    "ok": True,
                    "secs": round(dt, 3),
                    **(detail or {}),
                    "stats": stats,
                }
            except BaseException as e:  # noqa: BLE001 — report and continue
                dt = time.perf_counter() - t0
                failures += 1
                tb = traceback.format_exc()
                stats = dict(LAST_STATS)
                print(f"  FAIL  {name:16s} ({dt:.2f}s)  {type(e).__name__}: {e}  stats={stats}", flush=True)
                print("        " + tb.replace("\n", "\n        "), flush=True)
                results["backends"][backend][name] = {
                    "ok": False,
                    "secs": round(dt, 3),
                    "error": f"{type(e).__name__}: {e}",
                    "traceback": tb,
                    "stats": stats,
                }
    with open(args.out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nwrote {args.out}; {failures} failure(s)", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
