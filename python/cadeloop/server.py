"""``cadeloop.serve`` (R-101) — milestone-gated entry point.

The native HTTP/1.1 + ASGI 3.0 engine (R-080..R-088) ships in milestone M2
on top of the M1 IOCP transports. This module already owns the public
signature and config validation so the CLI and integration points are stable
from M0.
"""

from __future__ import annotations

import importlib

from .config import Config

__all__ = ["serve", "load_app"]


def load_app(spec: str):
    """Resolve a ``module:attribute`` ASGI application spec."""
    module_name, sep, attr = spec.partition(":")
    if not sep or not module_name or not attr:
        raise ValueError(
            f"invalid application spec {spec!r} — expected 'module:attribute'"
        )
    module = importlib.import_module(module_name)
    try:
        return getattr(module, attr)
    except AttributeError:
        raise AttributeError(
            f"module {module_name!r} has no attribute {attr!r}"
        ) from None


def serve(
    app,
    host: str = "127.0.0.1",
    port: int = 8000,
    *,
    workers: int = 1,
    backend: str = "auto",
    ssl=None,
    latency_mode: str = "balanced",
    access_log: bool = False,
    **cfg,
):
    """Serve an ASGI 3.0 application (R-101).

    Not functional before milestone M2: this validates configuration and
    then raises ``NotImplementedError``.
    """
    config = Config(
        workers=workers,
        backend=backend,
        latency_mode=latency_mode,
        access_log=access_log,
        **cfg,
    )
    del ssl  # accepted (stable signature); native TLS engine lands in M4
    if not callable(app):
        raise TypeError(f"ASGI app must be callable, got {app!r}")
    raise NotImplementedError(
        "cadeloop.serve(): the native HTTP/ASGI engine arrives in milestone "
        f"M2 (validated config: workers={config.workers}, "
        f"backend={config.backend!r}). See docs/roadmap.md."
    )
