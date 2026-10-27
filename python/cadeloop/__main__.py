"""``python -m cadeloop module:app [--workers N] [--port P] ...`` (R-101).

Every ``Config`` field is exposed as a ``--flag`` mapping 1:1 to cfg.
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import sys
import typing

from .config import _GC_MODES, Config
from .server import _check_app_spec, _worker_count, load_app, serve

# Distinguishes "flag absent" from "--flag none" for the int|None options.
_UNSET = object()


def _optional_int(text: str):
    """An int-or-None CLI value.

    `max_body`, `spin_us` and `stats_endpoint` all use None to mean
    something a number cannot say -- no body cap, derive the spin window
    from latency_mode, no stats listener. With a plain `type=int` those
    states were unreachable from the command line: `--max-body none`
    failed argparse outright, so a CLI user could not turn the 16 MiB cap
    off at all.
    """
    if text.strip().lower() in ("none", "null", ""):
        return None
    try:
        return int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected an integer or 'none', got {text!r}") from None


def _port(text: str) -> int:
    try:
        port = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a port number, got {text!r}") from None
    if not 0 <= port <= 65535:
        raise argparse.ArgumentTypeError(f"port must be in 0..65535, got {port}")
    return port


# String-valued fields with a closed set of values: argparse rejects a typo
# with the list of choices instead of a Config traceback.
_CHOICES = {"gc_mode": _GC_MODES}


def _add_config_args(parser: argparse.ArgumentParser) -> list[str]:
    hints = typing.get_type_hints(Config)
    handled = []
    skip = {"workers", "backend", "latency_mode"}  # explicit below
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
        elif optional and hint is int:
            # Sentinel, not None: None is a meaningful *value* for these,
            # so "unset" has to be distinguishable from "set to none".
            parser.add_argument(flag, dest=f.name, type=_optional_int, default=_UNSET)
        else:
            parser.add_argument(flag, dest=f.name, type=hint, default=None, choices=_CHOICES.get(f.name))
        handled.append(f.name)
    return handled


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m cadeloop",
        description="cadeloop ASGI server (native HTTP engine: milestone M2)",
    )
    parser.add_argument("app", help="ASGI application as 'module:attribute'")
    parser.add_argument(
        "--app-dir",
        default=".",
        help="directory to import the application from (default: the current one)",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", "-p", type=_port, default=8000)
    parser.add_argument("--workers", "-w", type=int, default=1)
    backends = ("auto", "iocp", "rio") if sys.platform == "win32" else ("auto", "epoll")
    parser.add_argument("--backend", choices=backends, default="auto")
    parser.add_argument(
        "--latency-mode", choices=("throughput", "balanced", "spin"), default="balanced"
    )
    cfg_fields = _add_config_args(parser)
    ns = parser.parse_args(argv)

    cfg = {
        name: getattr(ns, name)
        for name in cfg_fields
        if getattr(ns, name) is not _UNSET and getattr(ns, name) is not None
    }
    # ... except an explicit `none`, which is a value and must be passed.
    cfg.update(
        {
            name: None
            for name in cfg_fields
            if getattr(ns, name, _UNSET) is None
            and parser.get_default(name) is _UNSET
        }
    )
    # The installed `cadeloop` script runs with ITS OWN directory on
    # sys.path, not the current one (only `python -m` adds that), so the
    # README's `cadeloop myapp:app` could not import myapp from the project
    # directory at all. uvicorn's --app-dir works the same way.
    app_dir = os.path.abspath(ns.app_dir)
    if app_dir not in sys.path:
        sys.path.insert(0, app_dir)
    # Validate the spec now so a typo fails before we bind anything, but
    # hand serve() the STRING: the fork-free (Windows) worker model needs
    # it to re-import the app in each child, and a resolved callable makes
    # serve() reject --workers > 1 as though a bare callable were passed.
    # That model's supervisor never serves, so there it only locates the
    # module: importing the app would cost a full extra copy of it.
    if not hasattr(os, "fork") and _worker_count(ns.workers) > 1:
        _check_app_spec(ns.app)
    else:
        load_app(ns.app)
    serve(
        ns.app,
        ns.host,
        ns.port,
        workers=ns.workers,
        backend=ns.backend,
        latency_mode=ns.latency_mode,
        **cfg,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
