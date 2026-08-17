#!/usr/bin/env python3
"""HTTP benchmark server launcher.

Contenders (R-132 matrix, Linux-runnable subset):
  uvicorn+<loop>   — uvicorn (h11) on {asyncio, uvloop, cadeloop, rloop, rsloop}
  hypercorn        — hypercorn on stdlib asyncio
  socketify        — socketify.py (C/uWebSockets; the reference ceiling)
  cadeloop-native  — cadeloop.serve(): the M2 native llhttp/ASGI engine
"""

import argparse
import sys

from app import app  # noqa: F401 (import from CWD=bench/http)


def install_loop(kind: str):
    if kind == "asyncio":
        return
    import asyncio
    if kind.startswith("aiofastnet"):
        # aiofastnet is a transport layer over a base loop (spec paragraph 17):
        # "aiofastnet" = asyncio base; "aiofastnet-<loop>" = stacked on that loop.
        base = kind.split("-", 1)[1] if "-" in kind else "asyncio"
        install_loop(base)
        import aiofastnet

        aiofastnet.install_policy()
        return
    module = __import__(kind)  # cadeloop | uvloop | rloop | rsloop
    factory = module.new_event_loop

    class Policy(asyncio.DefaultEventLoopPolicy):
        def new_event_loop(self):
            return factory()

    asyncio.set_event_loop_policy(Policy())


def make_loop(kind: str):
    """Return an actual loop instance of `kind`.

    Setting a policy is NOT enough for uvicorn. As of 0.52 its
    `--loop asyncio` factory returns `asyncio.SelectorEventLoop` (or
    `ProactorEventLoop`) by class, ignoring whatever policy is installed
    -- so `install_loop("cadeloop")` + `uvicorn.run(loop="asyncio")`
    silently benchmarks the *stdlib* loop under a cadeloop-shaped label,
    and every alternative loop reports the same number for the obvious
    wrong reason. Hand uvicorn `loop="none"` and drive `Server.serve()`
    from a loop we constructed here instead.
    """
    if kind == "asyncio":
        import asyncio

        return asyncio.new_event_loop()
    return __import__(kind).new_event_loop()  # cadeloop | uvloop | rloop | rsloop


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--server",
        required=True,
        choices=[
            "uvicorn",
            "uvicorn-httptools",
            "hypercorn",
            "granian",
            "socketify",
            "cadeloop-native",
            "cadeloop-native-rio",
            "cadeloop-native-w2",
            "cadeloop-native-w4",
        ],
    )
    parser.add_argument("--loop", default="asyncio")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    if args.server.startswith("cadeloop-native"):
        import cadeloop

        workers = 1
        backend = "auto"
        if args.server.endswith("-rio"):
            backend = "rio"
        elif "-w" in args.server:
            workers = int(args.server.rsplit("-w", 1)[1])
        print(f"READY {args.port}", flush=True)
        cadeloop.serve(app, "127.0.0.1", args.port, workers=workers, backend=backend)
    elif args.server in ("uvicorn", "uvicorn-httptools"):
        import uvicorn

        config = uvicorn.Config(
            "app:app",
            host="127.0.0.1",
            port=args.port,
            log_level="critical",
            lifespan="off",
            # h11 is uvicorn's pure-Python parser and the apples-to-apples
            # comparison for a loop swap: it puts the parsing on the loop
            # under test. httptools is its C parser -- the configuration a
            # tuned uvicorn deployment actually runs, and the honest
            # opponent for a native engine.
            http="h11" if args.server == "uvicorn" else "httptools",
            # "none": build no loop of its own. See make_loop() -- any
            # other value and uvicorn picks the loop by class and ignores
            # the one we are trying to measure.
            loop="none",
            access_log=False,
        )
        server = uvicorn.Server(config)
        loop = make_loop(args.loop)
        print(f"READY {args.port}", flush=True)
        loop.run_until_complete(server.serve())
    elif args.server == "granian":
        # Rust HTTP core (hyper) driving an ASGI app -- the closest
        # architectural comparison to cadeloop's native engine, and the
        # one contender that is not parsing HTTP in Python.
        from granian import Granian
        from granian.constants import Interfaces

        print(f"READY {args.port}", flush=True)
        Granian(
            target="app:app",
            address="127.0.0.1",
            port=args.port,
            interface=Interfaces.ASGI,
            workers=1,
            log_enabled=False,
            log_access=False,
        ).serve()
    elif args.server == "hypercorn":
        import asyncio

        from hypercorn.asyncio import serve
        from hypercorn.config import Config

        install_loop(args.loop)
        cfg = Config()
        cfg.bind = [f"127.0.0.1:{args.port}"]
        cfg.loglevel = "CRITICAL"
        print(f"READY {args.port}", flush=True)
        asyncio.run(serve(app, cfg))
    elif args.server == "socketify":
        from socketify import ASGI

        print(f"READY {args.port}", flush=True)
        ASGI(app).listen(args.port, lambda config: None).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
