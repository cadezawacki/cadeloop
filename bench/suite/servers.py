"""Server launcher for the benchmark suite (run by run_suite.py).

  servers.py http --impl cadeloop|uvicorn|hypercorn|granian
                  [--loop asyncio|uvloop|winloop|cadeloop]
                  [--http h11|httptools] [--ws wsproto|websockets]
                  --port P [--workers N] [--tls CERT KEY]
  servers.py echo --loop asyncio|uvloop|winloop|rloop|rsloop|cadeloop --port P

Readiness is NOT announced here: the runner waits until a real request
(or echo round trip) succeeds.
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def make_loop(kind):
    if kind == "asyncio":
        # The stdlib default: proactor (IOCP) on Windows, selector (epoll)
        # on Linux.
        return asyncio.new_event_loop()
    return __import__(kind).new_event_loop()


class Echo(asyncio.Protocol):
    def connection_made(self, transport):
        self.t = transport

    def data_received(self, data):
        self.t.write(data)


async def echo_main(port):
    loop = asyncio.get_running_loop()
    await loop.create_server(Echo, "127.0.0.1", port)
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
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--tls", nargs=2, metavar=("CERT", "KEY"))
    a = p.parse_args()
    if a.kind == "echo":
        lp = make_loop(a.loop)
        lp.run_until_complete(echo_main(a.port))
        return
    if a.impl == "cadeloop":
        import cadeloop

        kw = {"workers": a.workers}
        if a.tls:
            kw["ssl"] = ssl_ctx(*a.tls)
        # The spawn worker model re-imports the app by name in each child.
        target = "app:app" if a.workers > 1 else __import__("app").app
        cadeloop.serve(target, "127.0.0.1", a.port, **kw)
    elif a.impl == "uvicorn":
        import uvicorn

        kw = {}
        if a.tls:
            kw = {"ssl_certfile": a.tls[0], "ssl_keyfile": a.tls[1]}
        cfg = uvicorn.Config("app:app", host="127.0.0.1", port=a.port, log_level="critical", lifespan="off",
                             http=a.http, ws=a.ws, loop="none", access_log=False, **kw)
        lp = make_loop(a.loop)
        lp.run_until_complete(uvicorn.Server(cfg).serve())
    elif a.impl == "hypercorn":
        from hypercorn.asyncio import serve
        from hypercorn.config import Config

        from app import app

        cfg = Config()
        cfg.bind = [f"127.0.0.1:{a.port}"]
        cfg.loglevel = "CRITICAL"
        if a.tls:
            cfg.certfile, cfg.keyfile = a.tls
        lp = make_loop(a.loop)
        lp.run_until_complete(serve(app, cfg))
    elif a.impl == "granian":
        # granian runs its own Rust HTTP stack and drives the app on
        # uvloop; one worker, as every other contender.
        from granian import Granian
        from granian.constants import Interfaces, Loops

        kw = {}
        if a.tls:
            from pathlib import Path

            kw = {"ssl_cert": Path(a.tls[0]), "ssl_key": Path(a.tls[1])}
        Granian("app:app", address="127.0.0.1", port=a.port, interface=Interfaces.ASGI, workers=a.workers,
                loop=Loops.uvloop, log_enabled=False, **kw).serve()


if __name__ == "__main__":
    main()
