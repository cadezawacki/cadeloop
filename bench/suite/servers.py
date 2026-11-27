"""Server launcher for the benchmark suite (run by run_suite.py).

  servers.py http --impl cadeloop|uvicorn|hypercorn|granian
                  [--loop asyncio|uvloop|winloop|cadeloop]
                  [--http h11|httptools] [--ws wsproto|websockets]
                  [--app app|fapp] [--access-log stdout|PATH] [--pylog]
                  --port P [--workers N] [--tls CERT KEY]
  servers.py echo --loop asyncio|uvloop|winloop|rloop|rsloop|cadeloop --port P
                  [--tls CERT KEY] [--native-tls]

`--app fapp` serves the FastAPI app instead of the bare ASGI one.
`--access-log` turns each server's own access log on: `stdout` is where
uvicorn, hypercorn and granian write theirs (the runner sends a server's
stdout to a file); cadeloop's native log takes a path. `--pylog` is
cadeloop's other access log, through Python's `logging`, to stdout.

Readiness is NOT announced here: the runner waits until a real request
(or echo round trip) succeeds.
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def settle_allocator():
    """Put the C allocator in the state a long-running program is in.

    glibc serves an allocation of 128 KiB or more with its own mmap(), and
    gives it back with munmap(), until the program frees one such block --
    from then on blocks up to that size come from the heap (the "dynamic
    mmap threshold"). Python's ssl module allocates a 256 KiB buffer for
    every read that asyncio's and uvloop's TLS make, so until that first
    large free a TLS server pays an mmap, an mremap and a munmap per read,
    and the page faults that go with them: uvloop's 1 KiB TLS echo ran at
    28 K messages/s in that state and 85 K out of it, on the same code.

    Which state a server starts in is an accident of what it imported: a
    module with a compiled file over 128 KiB flips it. Real applications
    read files and bodies larger than that as a matter of course, so the
    measured state is the settled one, and every contender is put there
    the same way rather than by luck. BENCH_COLD_ALLOCATOR=1 leaves the
    allocator as the process found it (to reproduce the difference).
    """
    if os.environ.get("BENCH_COLD_ALLOCATOR"):
        return
    block = bytearray(8 << 20)
    del block


def make_loop(kind, **kwargs):
    if kind == "asyncio":
        # The stdlib default: proactor (IOCP) on Windows, selector (epoll)
        # on Linux.
        return asyncio.new_event_loop()
    if kind == "cadeloop" and kwargs:
        from cadeloop.loop import Loop

        return Loop(**kwargs)
    return __import__(kind).new_event_loop()


class Echo(asyncio.Protocol):
    def connection_made(self, transport):
        self.t = transport

    def data_received(self, data):
        self.t.write(data)


async def echo_main(port, ssl=None):
    loop = asyncio.get_running_loop()
    await loop.create_server(Echo, "127.0.0.1", port, ssl=ssl)
    await asyncio.Event().wait()


def ssl_ctx(cert, key):
    import ssl

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert, key)
    return ctx


def main():
    p = argparse.ArgumentParser()
    p.add_argument("kind", choices=["http", "echo"])
    p.add_argument("--impl", default="cadeloop")
    p.add_argument("--loop", default="asyncio")
    p.add_argument("--http", default="h11", help="uvicorn's HTTP parser")
    p.add_argument("--ws", default="wsproto", help="uvicorn's WebSocket implementation")
    p.add_argument("--app", default="app", choices=["app", "fapp"], help="the bare ASGI app, or the FastAPI one")
    p.add_argument("--access-log", default=None, help="'stdout', or (cadeloop's native log) a file path")
    p.add_argument("--pylog", action="store_true", help="cadeloop: the access log through Python's logging")
    p.add_argument("--native-tls", action="store_true", help="echo on cadeloop: Loop(native_tls=True)")
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--tls", nargs=2, metavar=("CERT", "KEY"))
    a = p.parse_args()
    settle_allocator()
    target = f"{a.app}:app"
    if a.kind == "echo":
        # The explicit False matters: CADELOOP_NATIVE_TLS in the
        # environment must not decide which TLS path a contender measures.
        kwargs = {"native_tls": a.native_tls} if a.loop == "cadeloop" else {}
        lp = make_loop(a.loop, **kwargs)
        asyncio.set_event_loop(lp)
        lp.run_until_complete(echo_main(a.port, ssl_ctx(*a.tls) if a.tls else None))
        return
    if a.impl == "cadeloop":
        import cadeloop

        kw = {"workers": a.workers}
        if a.tls:
            kw["ssl"] = ssl_ctx(*a.tls)
        if a.pylog:
            import logging

            logging.basicConfig(level=logging.INFO, stream=sys.stdout)
            kw["access_log"] = True
        elif a.access_log:
            kw["access_log_file"] = a.access_log
        # The spawn worker model re-imports the app by name in each child.
        app = target if a.workers > 1 else __import__(a.app).app
        cadeloop.serve(app, "127.0.0.1", a.port, **kw)
    elif a.impl == "uvicorn":
        import uvicorn

        kw = {}
        if a.tls:
            kw = {"ssl_certfile": a.tls[0], "ssl_keyfile": a.tls[1]}
        # With the access log on, uvicorn's default logging setup: one
        # line per request on stdout.
        logging_kw = (
            {"log_level": "info", "access_log": True} if a.access_log else {"log_level": "critical", "access_log": False}
        )
        cfg = uvicorn.Config(target, host="127.0.0.1", port=a.port, lifespan="off",
                             http=a.http, ws=a.ws, loop="none", **logging_kw, **kw)
        lp = make_loop(a.loop)
        lp.run_until_complete(uvicorn.Server(cfg).serve())
    elif a.impl == "hypercorn":
        from hypercorn.asyncio import serve
        from hypercorn.config import Config

        cfg = Config()
        cfg.bind = [f"127.0.0.1:{a.port}"]
        cfg.loglevel = "CRITICAL"
        if a.access_log:
            cfg.accesslog = "-"
        if a.tls:
            cfg.certfile, cfg.keyfile = a.tls
        lp = make_loop(a.loop)
        lp.run_until_complete(serve(__import__(a.app).app, cfg))
    elif a.impl == "granian":
        # granian runs its own Rust HTTP stack and drives the app on
        # uvloop; one worker, as every other contender.
        from granian import Granian
        from granian.constants import Interfaces, Loops

        kw = {}
        if a.tls:
            from pathlib import Path

            kw = {"ssl_cert": Path(a.tls[0]), "ssl_key": Path(a.tls[1])}
        logging_kw = {"log_enabled": True, "log_access": True} if a.access_log else {"log_enabled": False}
        Granian(target, address="127.0.0.1", port=a.port, interface=Interfaces.ASGI, workers=a.workers,
                loop=Loops.uvloop, **logging_kw, **kw).serve()


if __name__ == "__main__":
    main()
