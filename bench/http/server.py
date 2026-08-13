#!/usr/bin/env python3
"""HTTP benchmark server launcher.

Contenders (R-132 matrix, Linux-runnable subset):
  uvicorn+<loop>   — uvicorn (h11) on {asyncio, uvloop, cadeloop, rloop, rsloop}
  hypercorn        — hypercorn on stdlib asyncio
  socketify        — socketify.py (C/uWebSockets; the reference ceiling)
"""

import argparse
import sys

from app import app  # noqa: F401 (import from CWD=bench/http)


def install_loop(kind: str):
    if kind == "asyncio":
        return
    import asyncio

    module = __import__(kind)  # cadeloop | uvloop | rloop | rsloop
    factory = module.new_event_loop

    class Policy(asyncio.DefaultEventLoopPolicy):
        def new_event_loop(self):
            return factory()

    asyncio.set_event_loop_policy(Policy())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", required=True, choices=["uvicorn", "hypercorn", "socketify"])
    parser.add_argument("--loop", default="asyncio")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    if args.server == "uvicorn":
        import uvicorn

        install_loop(args.loop)
        print(f"READY {args.port}", flush=True)
        uvicorn.run(
            "app:app",
            host="127.0.0.1",
            port=args.port,
            log_level="critical",
            lifespan="off",
            http="h11",  # HTTP/1.1
            loop="asyncio",  # use the installed policy's loop
            access_log=False,
        )
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
