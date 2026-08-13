#!/usr/bin/env python3
"""RIO stall bisector: localize WHICH machinery stalls, in ~30 seconds.

Built for the machine-2 finding: ``Loop(backend="rio")`` constructs in
full-notify mode, timers fire, yet every rio_smoke data-path check hits
the 90s watchdog. Each step here runs on a FRESH loop with a 5s timeout
and pairs the loop side with a PLAIN blocking-socket peer on a thread
(no event loop), so exactly one piece of loop machinery is under test
per step:

  timers            pure scheduling — the baseline that already works
  wakeup_threadsafe cross-thread PostQueuedCompletionStatus wakeup
  outbound_connect  ConnectEx handshake (inner IOCP on rio)
  outbound_echo     data on a PLAIN outbound socket — on rio these
                    recv/send ops take the inner-IOCP fallback, NOT RQs
  accept            AcceptEx completion (inner IOCP on rio)
  server_recv       first RIOReceive completion on an accepted
                    (WSA_FLAG_REGISTERED_IO) socket — the RQ recv path
  server_send       RIOSend completion on an accepted socket

Every step reports the loop's final stats: ``rio_notifies`` counts
RIONotify completion packets received, ``rio_watchdog_reaps`` counts
completions the poll-top drain found while a notification was armed
(nonzero = the CQ works but notification delivery does not; the 50ms
watchdog park cap is what kept I/O moving).

Reading the outcome:
  * all PASS, rio_watchdog_reaps > 0   RIO completions reach the CQ but
    RIONotify delivery is broken on this machine; the watchdog carries
    the load (worst-case +50ms latency on an idle wakeup).
  * server_recv FAIL, outbound_echo PASS, both counters 0   RIOReceive
    completions never enter the CQ: the registered-I/O data path is
    broken while plain IOCP works — RIO is unusable on this machine.
  * outbound_echo FAIL too   the inner IOCP port itself misbehaves
    under the hybrid — a cadeloop bug, report the JSON.
  * wakeup_threadsafe FAIL   parked GetQueuedCompletionStatusEx never
    wakes at all.

Usage:
    python tools/windows/rio_bisect.py [rio] [iocp] [--out rio-bisect.json]
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
    import cadeloop  # noqa: F401
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "python"))
    import cadeloop  # noqa: E402,F401

STEP_TIMEOUT = 5.0  # per-step async watchdog
SOCK_TIMEOUT = 3.0  # plain-socket timeouts resolve BEFORE the watchdog
STAT_KEYS = ("backend", "polls", "completions", "rio_notifies", "rio_watchdog_reaps")


def make_loop(backend: str):
    from cadeloop.loop import Loop

    return Loop(backend=backend)


class ThreadEcho:
    """Blocking-socket echo server on daemon threads — no event loop."""

    def __init__(self):
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(8)
        self.sock.settimeout(STEP_TIMEOUT + 2)
        self.port = self.sock.getsockname()[1]
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        try:
            while True:
                conn, _ = self.sock.accept()
                conn.settimeout(SOCK_TIMEOUT)
                threading.Thread(target=self._echo, args=(conn,), daemon=True).start()
        except OSError:
            pass

    @staticmethod
    def _echo(conn):
        try:
            while True:
                data = conn.recv(65536)
                if not data:
                    break
                conn.sendall(data)
        except OSError:
            pass
        finally:
            conn.close()

    def close(self):
        self.sock.close()


# ------------------------------------------------------------------ #
# steps — each receives a fresh loop and returns a detail dict        #
# ------------------------------------------------------------------ #


def step_timers(lp):
    async def main():
        t0 = time.perf_counter()
        await asyncio.sleep(0.05)
        return {"slept_s": round(time.perf_counter() - t0, 3)}

    return lp.run_until_complete(asyncio.wait_for(main(), STEP_TIMEOUT))


def step_wakeup_threadsafe(lp):
    async def main():
        fut = lp.create_future()
        threading.Timer(0.2, lambda: lp.call_soon_threadsafe(fut.set_result, "woke")).start()
        return {"result": await fut}

    return lp.run_until_complete(asyncio.wait_for(main(), STEP_TIMEOUT))


def step_outbound_connect(lp):
    srv = ThreadEcho()
    try:
        async def main():
            _reader, writer = await asyncio.open_connection("127.0.0.1", srv.port)
            writer.close()
            return {"connected": True}

        return lp.run_until_complete(asyncio.wait_for(main(), STEP_TIMEOUT))
    finally:
        srv.close()


def step_outbound_echo(lp):
    srv = ThreadEcho()
    try:
        async def main():
            reader, writer = await asyncio.open_connection("127.0.0.1", srv.port)
            payload = b"ping" * 256  # 1 KiB
            writer.write(payload)
            await writer.drain()
            got = await reader.readexactly(len(payload))
            assert got == payload
            writer.close()
            return {"echoed_bytes": len(got)}

        return lp.run_until_complete(asyncio.wait_for(main(), STEP_TIMEOUT))
    finally:
        srv.close()


def step_accept(lp):
    async def main():
        ev = asyncio.Event()

        async def handler(_reader, writer):
            ev.set()
            writer.close()

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        def client():
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=SOCK_TIMEOUT):
                    time.sleep(0.2)
            except OSError:
                pass

        threading.Thread(target=client, daemon=True).start()
        await ev.wait()
        server.close()
        await server.wait_closed()
        return {"accepted": True}

    return lp.run_until_complete(asyncio.wait_for(main(), STEP_TIMEOUT))


def step_server_recv(lp):
    async def main():
        fut = lp.create_future()

        async def handler(reader, writer):
            data = await reader.readexactly(1024)
            if not fut.done():
                fut.set_result(len(data))
            writer.close()

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        def client():
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=SOCK_TIMEOUT) as s:
                    s.sendall(b"x" * 1024)
                    time.sleep(STEP_TIMEOUT)  # hold open until the loop reads
            except OSError:
                pass

        threading.Thread(target=client, daemon=True).start()
        n = await fut
        server.close()
        await server.wait_closed()
        return {"received_bytes": n}

    return lp.run_until_complete(asyncio.wait_for(main(), STEP_TIMEOUT))


def step_server_send(lp):
    async def main():
        done = threading.Event()
        got: list = []

        async def handler(_reader, writer):
            writer.write(b"y" * 1024)
            await writer.drain()
            while not done.is_set():  # keep the conn open while the client reads
                await asyncio.sleep(0.05)
            writer.close()

        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        def client():
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=SOCK_TIMEOUT) as s:
                    buf = b""
                    while len(buf) < 1024:
                        chunk = s.recv(4096)
                        if not chunk:
                            break
                        buf += chunk
                    got.append(len(buf))
            except OSError as e:
                got.append(f"client-error: {e}")
            finally:
                done.set()

        threading.Thread(target=client, daemon=True).start()
        while not done.is_set():
            await asyncio.sleep(0.05)
        server.close()
        await server.wait_closed()
        assert got and got[0] == 1024, f"client saw {got!r}"
        return {"client_read_bytes": got[0]}

    return lp.run_until_complete(asyncio.wait_for(main(), STEP_TIMEOUT))


STEPS = [
    ("timers", step_timers),
    ("wakeup_threadsafe", step_wakeup_threadsafe),
    ("outbound_connect", step_outbound_connect),
    ("outbound_echo", step_outbound_echo),
    ("accept", step_accept),
    ("server_recv", step_server_recv),
    ("server_send", step_server_send),
]


def run_step(backend, fn):
    lp = make_loop(backend)
    asyncio.set_event_loop(lp)
    stats: dict = {}
    try:
        detail = fn(lp)
        ok, err, tb = True, None, None
    except BaseException as e:  # noqa: BLE001 — report and continue
        detail, ok = None, False
        err, tb = f"{type(e).__name__}: {e}", traceback.format_exc()
    finally:
        try:
            st = lp.stats()
            stats = {k: st[k] for k in STAT_KEYS if k in st}
        except Exception:
            pass
        asyncio.set_event_loop(None)
        lp.close()
    return ok, detail, err, tb, stats


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("backends", nargs="*", default=None)
    parser.add_argument("--out", default="rio-bisect.json")
    args = parser.parse_args()
    backends = args.backends or (["rio"] if sys.platform == "win32" else ["auto"])

    results = {"platform": sys.platform, "python": sys.version, "backends": {}}
    failures = 0
    for backend in backends:
        print(f"=== backend={backend} ===", flush=True)
        results["backends"][backend] = {}
        for name, fn in STEPS:
            t0 = time.perf_counter()
            ok, detail, err, tb, stats = run_step(backend, fn)
            dt = time.perf_counter() - t0
            if ok:
                print(f"  PASS  {name:18s} ({dt:.2f}s)  {detail}  stats={stats}", flush=True)
                results["backends"][backend][name] = {
                    "ok": True, "secs": round(dt, 3), **(detail or {}), "stats": stats,
                }
            else:
                failures += 1
                print(f"  FAIL  {name:18s} ({dt:.2f}s)  {err}  stats={stats}", flush=True)
                results["backends"][backend][name] = {
                    "ok": False, "secs": round(dt, 3), "error": err,
                    "traceback": tb, "stats": stats,
                }
    with open(args.out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nwrote {args.out}; {failures} failure(s)", flush=True)
    print(__doc__[__doc__.index("Reading the outcome:"):].rstrip(), flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
