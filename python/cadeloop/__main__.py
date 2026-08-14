"""``python -m cadeloop module:app [--workers N] [--port P] ...`` (R-101).

Every ``Config`` field is exposed as a ``--flag`` mapping 1:1 to cfg.
"""

from __future__ import annotations

import argparse
import dataclasses
import sys
import typing

from .config import Config
from .server import load_app, serve


def _add_config_args(parser: argparse.ArgumentParser) -> list[str]:
    hints = typing.get_type_hints(Config)
    handled = []
    skip = {"workers", "backend", "latency_mode", "access_log"}  # explicit below
    for f in dataclasses.fields(Config):
        if f.name in skip:
            continue
        flag = "--" + f.name.replace("_", "-")
        hint = hints[f.name]
        optional = typing.get_origin(hint) in (typing.Union, __import__("types").UnionType)
        if optional:
            hint = next(a for a in typing.get_args(hint) if a is not type(None))
        if hint is bool:
            group = parser.add_mutually_exclusive_group()
            group.add_argument(flag, dest=f.name, action="store_true", default=None)
            group.add_argument(
                "--no-" + f.name.replace("_", "-"),
                dest=f.name,
                action="store_false",
                default=None,
            )
        else:
            parser.add_argument(flag, dest=f.name, type=hint, default=None)
        handled.append(f.name)
    return handled


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m cadeloop",
        description="cadeloop ASGI server (native HTTP engine: milestone M2)",
    )
    parser.add_argument("app", help="ASGI application as 'module:attribute'")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", "-p", type=int, default=8000)
    parser.add_argument("--workers", "-w", type=int, default=1)
    backends = ("auto", "iocp", "rio") if sys.platform == "win32" else ("auto", "epoll")
    parser.add_argument("--backend", choices=backends, default="auto")
    parser.add_argument(
        "--latency-mode", choices=("throughput", "balanced", "spin"), default="balanced"
    )
    parser.add_argument("--access-log", action="store_true")
    cfg_fields = _add_config_args(parser)
    ns = parser.parse_args(argv)

    cfg = {
        name: getattr(ns, name)
        for name in cfg_fields
        if getattr(ns, name) is not None
    }
    # Validate the spec now so a typo fails before we bind anything, but
    # hand serve() the STRING: the fork-free (Windows) worker model needs
    # it to re-import the app in each child, and a resolved callable makes
    # serve() reject --workers > 1 as though a bare callable were passed.
    load_app(ns.app)
    serve(
        ns.app,
        ns.host,
        ns.port,
        workers=ns.workers,
        backend=ns.backend,
        latency_mode=ns.latency_mode,
        access_log=ns.access_log,
        **cfg,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
