"""``cadeloop.serve`` (R-101): the native HTTP/1.1 + ASGI 3.0 server (M2).

The hot path lives in Rust (``CoreLoop.http_listen``): llhttp parsing,
scope construction, eager coroutine stepping, and response serialization
all happen without touching this module. Python owns only the cold path —
config validation, app loading, the ASGI *lifespan* protocol (R-081), GC
tuning (R-075), and signal wiring.
"""

from __future__ import annotations

import asyncio
import gc
import importlib
import logging
import signal as _signal

from .config import Config
from .loop import Loop

__all__ = ["serve", "load_app"]

logger = logging.getLogger("cadeloop")


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


class _Lifespan:
    """ASGI lifespan driver (R-081), uvicorn-style ``auto``: if the app
    errors before startup completes, lifespan is disabled and serving
    proceeds (frameworks without lifespan support keep working)."""

    def __init__(self, app, loop: Loop):
        self.app = app
        self.loop = loop
        self.enabled = True
        self.state: dict = {}
        self._queue: asyncio.Queue | None = None
        self._startup: asyncio.Future | None = None
        self._shutdown: asyncio.Future | None = None
        self._task = None

    async def _receive(self):
        assert self._queue is not None
        return await self._queue.get()

    async def _send(self, message):
        kind = message.get("type")
        if kind == "lifespan.startup.complete":
            if not self._startup.done():
                self._startup.set_result(None)
        elif kind == "lifespan.startup.failed":
            if not self._startup.done():
                self._startup.set_exception(
                    RuntimeError(message.get("message") or "lifespan startup failed")
                )
        elif kind == "lifespan.shutdown.complete":
            if not self._shutdown.done():
                self._shutdown.set_result(None)
        elif kind == "lifespan.shutdown.failed":
            if not self._shutdown.done():
                self._shutdown.set_result(None)
            logger.error("lifespan shutdown failed: %s", message.get("message", ""))
        else:
            raise RuntimeError(f"unexpected lifespan message type: {kind!r}")

    def startup(self) -> None:
        loop = self.loop
        self._queue = asyncio.Queue()
        self._startup = loop.create_future()
        self._shutdown = loop.create_future()
        scope = {
            "type": "lifespan",
            "asgi": {"version": "3.0", "spec_version": "2.0"},
            "state": self.state,
        }

        async def _run():
            try:
                await self.app(scope, self._receive, self._send)
            except BaseException as exc:  # noqa: BLE001 — app may not do lifespan
                if not self._startup.done():
                    # Errored before startup completed: app has no lifespan
                    # support -> disable, keep serving ('auto' behavior).
                    self.enabled = False
                    self._startup.set_result(None)
                    logger.debug("lifespan disabled: %r", exc)
                else:
                    logger.exception("lifespan task crashed")
            if not self._startup.done():
                # Returned without startup.complete: no lifespan support.
                self.enabled = False
                self._startup.set_result(None)
            if not self._shutdown.done():
                self._shutdown.set_result(None)

        self._task = loop.create_task(_run())
        self._queue.put_nowait({"type": "lifespan.startup"})
        loop.run_until_complete(self._startup)  # raises on startup.failed

    def shutdown(self) -> None:
        if not self.enabled or self._task is None or self._task.done():
            return
        self._queue.put_nowait({"type": "lifespan.shutdown"})
        try:
            self.loop.run_until_complete(
                asyncio.wait_for(asyncio.shield(self._shutdown), timeout=10.0)
            )
        except (TimeoutError, asyncio.TimeoutError, RuntimeError):
            logger.warning("lifespan shutdown timed out")


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
    """Serve an ASGI 3.0 application on the native HTTP engine (R-101).

    Blocks until stopped (SIGINT/SIGTERM or ``loop.stop()`` from a
    handler). ``app`` may be a callable or a ``"module:attribute"`` spec.
    """
    config = Config(
        workers=workers,
        backend=backend,
        latency_mode=latency_mode,
        access_log=access_log,
        **cfg,
    )
    del ssl  # accepted (stable signature); native TLS engine lands in M4
    if isinstance(app, str):
        app = load_app(app)
    if not callable(app):
        raise TypeError(f"ASGI app must be callable, got {app!r}")
    if config.workers not in (0, 1):
        # §8/R-090 multi-process supervisor arrives in M3.
        logger.warning(
            "workers=%d requested; multi-process serving arrives in M3 — "
            "running a single worker",
            config.workers,
        )

    loop = Loop(
        backend=config.backend,
        spin_us=config.spin_us,
        high_water=config.write_high_water,
        low_water=config.write_low_water,
        accept_pool=config.accept_pool,
    )
    asyncio.set_event_loop(loop)
    lifespan = _Lifespan(app, loop)
    lid = None
    installed_signals = []
    try:
        lifespan.startup()
        lid, bound, _fd = loop._core.http_listen(
            host,
            port,
            app,
            loop,
            state=lifespan.state,
            accept_pool=config.accept_pool,
            eager=config.eager_tasks,
            max_header_bytes=config.max_header_bytes,
            max_headers=config.max_headers,
            max_url=config.max_url,
            max_body=config.max_body,
        )
        # R-075: freeze the post-startup heap out of the cyclic collector.
        if config.gc_mode == "freeze":
            gc.collect()
            gc.freeze()
        elif config.gc_mode == "disable":
            gc.collect()
            gc.disable()
        for sig in (_signal.SIGINT, _signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, loop.stop)
                installed_signals.append(sig)
            except (NotImplementedError, RuntimeError, ValueError):
                pass
        shown = bound if bound else (host, port)
        logger.info("cadeloop serving on http://%s:%s", shown[0], shown[1])
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass
    finally:
        if config.gc_mode == "disable":
            gc.enable()
        for sig in installed_signals:
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, RuntimeError, ValueError):
                pass
        if lid is not None:
            loop._core.listener_close(lid)
        lifespan.shutdown()
        loop.close()
        asyncio.set_event_loop(None)
