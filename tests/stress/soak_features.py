#!/usr/bin/env python3
"""Server soak: this release's features together, under sustained load.

One cadeloop server (soak_app.py) with HTTPS, the native access log and
its Python tap, file sending, WebSocket compression, `gc_mode="refreeze"`,
native TLS for its own outgoing connections and the resolver threads all
switched on. This process drives it -- on a cadeloop loop with native TLS,
so the client side of that code soaks too -- with a mix of requests,
WebSocket conversations, handlers that call out over HTTPS by host name,
and connections dropped without warning.

    python tests/stress/soak_features.py --seconds 600

It fails if, between the end of warm-up and the end of the run, either
process's memory grew by more than --max-growth (and --slack-bytes), if
the server ends with more descriptors or threads than it had when it was
warm, if any request failed, if the access log or the tap lost a record,
or if a connection is still counted after every client has gone.

Linux and Windows; descriptor counts are checked where /proc has them.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import socket
import ssl
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from soak_timers import rss_bytes  # noqa: E402


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def prepare(directory: str) -> None:
    import trustme

    ca = trustme.CA()
    cert = ca.issue_cert("localhost", "127.0.0.1")
    ca.cert_pem.write_to_path(os.path.join(directory, "ca.pem"))
    cert.private_key_and_cert_chain_pem.write_to_path(os.path.join(directory, "server.pem"))
    with open(os.path.join(directory, "small.bin"), "wb") as f:
        f.write(os.urandom(10 * 1024))  # under the inline limit: read on the loop thread
    with open(os.path.join(directory, "large.bin"), "wb") as f:
        f.write(os.urandom(700 * 1024))  # over it: read in pieces on the file pool


SERVER = r"""
import os, ssl, sys
import cadeloop
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ctx.load_cert_chain(os.path.join(os.environ["SOAK_DIR"], "server.pem"))
cadeloop.serve(
    "soak_app:app", "127.0.0.1", int(os.environ["SOAK_PORT"]), ssl=ctx,
    access_log_file=os.path.join(os.environ["SOAK_DIR"], "access.log"),
    access_log_tap="soak_app:tap",
    ws_compression=True, native_tls=True, gc_mode="refreeze", grace=5.0,
)
"""


async def drive(port: int, directory: str, seconds: float, args) -> dict:
    import aiohttp
    import websockets

    ctx = ssl.create_default_context(cafile=os.path.join(directory, "ca.pem"))
    base = f"https://localhost:{port}"
    counts = {"requests": 0, "ws_messages": 0, "aborted": 0, "errors": []}
    samples = []
    stop = asyncio.Event()
    rng = random.Random(20261127)
    mix = {"/json": 6, "/sync": 2, "/alloc": 2, "/cache": 2, "/small": 3, "/file": 2, "/upstream": 2}
    paths = [path for path, weight in mix.items() for _ in range(weight)]
    sizes = {"/small": 10 * 1024, "/file": 700 * 1024}

    def failed(what, exc):
        if len(counts["errors"]) < 20:
            counts["errors"].append(f"{what}: {exc!r}")

    async def http_worker(i):
        while not stop.is_set():
            # A session per few hundred requests: connections are made and
            # torn down throughout, not only at the start.
            connector = aiohttp.TCPConnector(ssl=ctx, limit=2)
            async with aiohttp.ClientSession(connector=connector) as session:
                for _ in range(rng.randint(50, 400)):
                    if stop.is_set():
                        break
                    path = rng.choice(paths)
                    try:
                        async with session.get(base + path) as resp:
                            body = await resp.read()
                            if resp.status != 200 or (path in sizes and len(body) != sizes[path]):
                                failed(path, (resp.status, len(body)))
                    except Exception as exc:  # noqa: BLE001 -- every failure is a finding
                        failed(path, exc)
                    counts["requests"] += 1

    async def ws_worker(i):
        payload = json.dumps({"n": list(range(600)), "text": "soak " * 200})  # ~4 KiB, compresses
        while not stop.is_set():
            try:
                async with websockets.connect(f"wss://localhost:{port}/ws", ssl=ctx) as conn:
                    for _ in range(rng.randint(20, 300)):
                        if stop.is_set():
                            break
                        await conn.send(payload)
                        if await conn.recv() != payload:
                            failed("ws", "echo differs")
                        counts["ws_messages"] += 1
            except Exception as exc:  # noqa: BLE001
                failed("ws", exc)

    async def rude_client():
        """Connections that vanish mid-request: half a TLS handshake, half
        a request, a reset in the middle of a large response."""
        while not stop.is_set():
            await asyncio.sleep(rng.uniform(0.02, 0.1))
            try:
                kind = rng.randrange(3)
                if kind == 0:  # TCP only: the TLS handshake never starts
                    _reader, writer = await asyncio.open_connection("127.0.0.1", port)
                    writer.transport.abort()
                elif kind == 1:  # handshake, then half a request, then gone
                    _reader, writer = await asyncio.open_connection("localhost", port, ssl=ctx)
                    writer.write(b"GET /json HTTP/1.1\r\nHost: localhost\r\n")
                    await asyncio.sleep(0.01)
                    writer.transport.abort()
                else:  # ask for the large file and leave after the first bytes
                    reader, writer = await asyncio.open_connection("localhost", port, ssl=ctx)
                    writer.write(b"GET /file HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    await reader.read(4096)
                    writer.transport.abort()
                counts["aborted"] += 1
            except (OSError, ssl.SSLError) as exc:
                failed("rude", exc)

    async def sampler():
        connector = aiohttp.TCPConnector(ssl=ctx, limit=1)
        async with aiohttp.ClientSession(connector=connector) as session:
            started = time.monotonic()
            while not stop.is_set():
                try:
                    async with session.get(base + "/_soak") as resp:
                        sample = await resp.json()
                    sample["t"] = time.monotonic() - started
                    sample["client_rss"] = rss_bytes()
                    sample["requests"] = counts["requests"]
                    samples.append(sample)
                except Exception as exc:  # noqa: BLE001
                    failed("/_soak", exc)
                try:
                    await asyncio.wait_for(stop.wait(), args.sample_every)
                except asyncio.TimeoutError:
                    pass

    # State the server builds once it is up, and keeps: a full collection
    # walks all of it until gc_mode="refreeze" takes it out of the way.
    connector = aiohttp.TCPConnector(ssl=ctx, limit=1)
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(base + f"/cache?fill={args.cache_entries}&churn=0") as resp:
            await resp.read()
    workers = [asyncio.ensure_future(http_worker(i)) for i in range(args.http_workers)]
    workers += [asyncio.ensure_future(ws_worker(i)) for i in range(args.ws_workers)]
    workers.append(asyncio.ensure_future(rude_client()))
    sampling = asyncio.ensure_future(sampler())
    await asyncio.sleep(seconds)
    stop.set()
    await asyncio.gather(*workers, sampling)
    # Everything this process opened is closed. The server's own view,
    # once it has had a moment to notice, over one fresh connection.
    await asyncio.sleep(1.0)
    connector = aiohttp.TCPConnector(ssl=ctx, limit=1)
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(base + "/_soak") as resp:
            final = await resp.json()
    final["client_rss"] = rss_bytes()
    return {"counts": counts, "samples": samples, "final": final}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=float, default=120)
    parser.add_argument("--warmup-share", type=float, default=0.25,
                        help="share of the run after which the baseline is taken")
    parser.add_argument("--max-growth", type=float, default=0.05)
    parser.add_argument("--slack-bytes", type=int, default=8 * 1024 * 1024,
                        help="growth below this many bytes is never a failure (allocator noise)")
    parser.add_argument("--http-workers", type=int, default=24)
    parser.add_argument("--ws-workers", type=int, default=6)
    parser.add_argument("--sample-every", type=float, default=2.0)
    parser.add_argument("--cache-entries", type=int, default=200_000,
                        help="entries in the cache the server builds after startup (0: none)")
    parser.add_argument("--out", help="write every sample here as JSON")
    args = parser.parse_args()

    import cadeloop
    from cadeloop.loop import Loop

    port = free_port()
    with tempfile.TemporaryDirectory(prefix="cadeloop-soak-") as directory:
        prepare(directory)
        env = dict(os.environ, SOAK_DIR=directory, SOAK_PORT=str(port))
        env["PYTHONPATH"] = os.pathsep.join(
            p for p in (HERE, os.path.dirname(os.path.dirname(os.path.abspath(cadeloop.__file__))),
                        env.get("PYTHONPATH")) if p)
        server = subprocess.Popen([sys.executable, "-c", SERVER], env=env, cwd=HERE)
        try:
            deadline = time.monotonic() + 30
            while True:
                try:
                    socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
                    break
                except OSError:
                    if server.poll() is not None or time.monotonic() > deadline:
                        print("the server did not start")
                        return 1
                    time.sleep(0.1)
            loop = Loop(native_tls=True)
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(drive(port, directory, args.seconds, args))
            finally:
                asyncio.set_event_loop(None)
                loop.close()
        finally:
            server.terminate()
            try:
                code = server.wait(30)
            except subprocess.TimeoutExpired:
                server.kill()
                code = "killed after 30 s"
        with open(os.path.join(directory, "access.log"), "rb") as f:
            logged = sum(1 for _ in f)

    counts, samples, final = result["counts"], result["samples"], result["final"]
    if args.out:
        with open(args.out, "w") as f:
            json.dump(result, f)
    warm = [s for s in samples if s["t"] >= args.seconds * args.warmup_share]
    problems = []
    if len(warm) < 3:
        problems.append("too few samples after warm-up to judge anything")
        warm = samples or [final]
    first, last = warm[0], warm[-1]

    def grew(name, before, after):
        allowed = max(args.max_growth * before, args.slack_bytes)
        note = f"{name}: {before / 2**20:.1f} -> {after / 2**20:.1f} MiB ({(after - before) / 2**20:+.1f})"
        if after - before > allowed:
            problems.append(f"{note}, more than the {allowed / 2**20:.1f} MiB allowed")
        return note

    lines = [
        f"{counts['requests']} requests, {counts['ws_messages']} WebSocket messages, "
        f"{counts['aborted']} connections dropped mid-flight, in {args.seconds:.0f} s",
        grew("server memory after warm-up", first["rss"], last["rss"]),
        grew("client memory after warm-up", first["client_rss"], last["client_rss"]),
    ]
    if first.get("fds") is not None:
        # At rest again: one client connection (the one asking) and whatever
        # the server holds for itself.
        lines.append(f"server descriptors: {first['fds']} when warm and busy, {final['fds']} at rest")
        if final["fds"] > first["fds"]:
            problems.append(f"the server holds more descriptors at rest ({final['fds']}) than under load "
                            f"({first['fds']})")
    lines.append(f"server threads: {first['threads']} when warm, {final['threads']} at the end")
    if final["threads"] > first["threads"] + 2:
        problems.append(f"server threads grew from {first['threads']} to {final['threads']}")
    stats = final["loop"]
    lines.append(f"access log: {logged} lines, {final['tapped']['records']} records through the tap in "
                 f"{final['tapped']['batches']} batches, {stats['access_log_dropped']} dropped")
    lines.append(f"name lookups: {stats['dns_lookups']} made, {stats['dns_coalesced']} shared, "
                 f"{stats['dns_cache_hits']} from the cache")
    if final.get("gc"):
        gc_stats = final["gc"]
        lines.append(f"collector: {gc_stats['collections']} collections, longest "
                     f"{gc_stats['pause_ms_longest']} ms, {gc_stats['refreezes']} refreezes, "
                     f"{gc_stats['frozen_objects']} objects frozen")
        if args.cache_entries >= 100_000 and not gc_stats["refreezes"]:
            problems.append("a cache of that size was built after startup and nothing was refrozen")
    if counts["errors"]:
        problems.append(f"{len(counts['errors'])}+ failed requests, the first: {counts['errors'][:5]}")
    if stats["access_log_dropped"]:
        problems.append(f"the access log dropped {stats['access_log_dropped']} records")
    # Every finished request is one line and one tapped record. The line
    # count is read after shutdown, the tap count a moment before it, so
    # the log may hold the last few requests more.
    tapped = final["tapped"]["records"]
    if not tapped <= logged <= tapped + 50:
        problems.append(f"access log lines ({logged}) and tapped records ({tapped}) disagree")
    if logged < counts["requests"]:
        problems.append(f"{counts['requests']} requests were answered but only {logged} were logged")
    if stats["connections"] > 2:
        problems.append(f"{stats['connections']} connections still counted with every client gone")
    for key in ("stale_buffer_ids", "unreaped_ops"):
        if stats.get(key):
            problems.append(f"loop.stats()[{key!r}] is {stats[key]}; it must be 0")
    if code not in (0, -15):
        problems.append(f"the server exited with {code!r}")
    print("\n".join(lines))
    if problems:
        print("\nFAILED:\n  " + "\n  ".join(problems))
        return 1
    print("\nok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
