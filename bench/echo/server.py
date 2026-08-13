#!/usr/bin/env python3
"""Raw-protocol TCP echo server for the echo benchmark (R-003 shape).

Identical minimal Protocol on every contender loop — differences measured
are transport/loop machinery, not app code.
"""

import argparse
import asyncio
import sys


class Echo(asyncio.Protocol):
    __slots__ = ("t",)

    def connection_made(self, transport):
        self.t = transport

    def data_received(self, data):
        self.t.write(data)

    def connection_lost(self, exc):
        pass


def install(kind: str):
    if kind == "asyncio":
        return
    if kind.startswith("aiofastnet"):
        # aiofastnet is a transport layer over a base loop (spec paragraph 17):
        # "aiofastnet" = asyncio base; "aiofastnet-<loop>" = stacked on that loop.
        base = kind.split("-", 1)[1] if "-" in kind else "asyncio"
        install(base)
        import aiofastnet

        aiofastnet.install_policy()
        return
    module = __import__(kind)  # cadeloop | uvloop | rloop | rsloop
    factory = module.new_event_loop

    class Policy(asyncio.DefaultEventLoopPolicy):
        def new_event_loop(self):
            return factory()

    asyncio.set_event_loop_policy(Policy())


async def main(port: int) -> None:
    loop = asyncio.get_running_loop()
    server = await loop.create_server(Echo, "127.0.0.1", port)
    bound = server.sockets[0].getsockname()[1]
    print(f"READY {bound}", flush=True)
    await asyncio.sleep(3600)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", required=True)
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    install(args.loop)
    try:
        asyncio.run(main(args.port))
    except KeyboardInterrupt:
        sys.exit(0)
