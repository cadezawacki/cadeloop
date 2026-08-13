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
import os
import signal as _signal
import time

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
    ``workers`` > 1 forks a supervised worker pool (§8, R-090..R-093);
    ``workers=0`` means one worker per CPU.
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

    n = config.workers if config.workers > 0 else (os.cpu_count() or 1)
    if n > 1:
        if not hasattr(os, "fork"):
            # Windows worker model (WSADuplicateSocketW handle passing) is
            # the M3-Windows item; until then run a single worker there.
            logger.warning("workers=%d requires fork; running a single worker", n)
        else:
            return _serve_multi(app, host, port, config, n)
    return _serve_single(app, host, port, config)


def _serve_single(app, host, port, config: Config, *, reuse_port: bool = False, worker_id=None):
    """One worker: loop + lifespan + native listener (the M2 path)."""
    loop = Loop(
        backend=config.backend,
        spin_us=config.spin_us,
        high_water=config.write_high_water,
        low_water=config.write_low_water,
        accept_pool=config.accept_pool,
        rio_cq_size=config.rio_cq_size,
        rio_rq_recv=config.rio_rq_recv,
        rio_rq_send=config.rio_rq_send,
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
            reuse_port=reuse_port,
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
        who = f"worker {worker_id} " if worker_id is not None else ""
        logger.info("cadeloop %sserving on http://%s:%s", who, shown[0], shown[1])
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


# --------------------------------------------------------------------- #
# multi-process worker model (§8, R-090..R-093)                          #
# --------------------------------------------------------------------- #

# Give up when a worker keeps dying immediately (R-092 supervision):
_CRASH_STREAK_LIMIT = 5
_CRASH_FAST_SECS = 1.0


def _spawn_worker(app, host, port, config: Config, idx: int, ncpu: int) -> int:
    pid = os.fork()
    if pid != 0:
        return pid
    # ---- child ----
    status = 1
    try:
        # A worker owns its own signal handling (installed by
        # _serve_single via the loop); drop the supervisor's handlers.
        _signal.signal(_signal.SIGTERM, _signal.SIG_DFL)
        _signal.signal(_signal.SIGINT, _signal.SIG_IGN)  # supervisor forwards TERM
        if config.pin and hasattr(os, "sched_setaffinity"):
            # R-091: pin each worker to one CPU (accept balancing is the
            # kernel's job via SO_REUSEPORT).
            try:
                os.sched_setaffinity(0, {idx % ncpu})
            except OSError:
                pass
        _serve_single(app, host, port, config, reuse_port=True, worker_id=idx)
        status = 0
    except BaseException:  # noqa: BLE001 — worker death is the supervisor's signal
        logger.exception("worker %d crashed", idx)
    finally:
        os._exit(status)
    return 0  # unreachable


def _serve_multi(app, host, port, config: Config, n: int):
    """Supervisor: fork N workers each binding with SO_REUSEPORT (the
    kernel load-balances accepts), restart crashed ones (R-092), forward
    SIGTERM/SIGINT, and drain within ``config.grace`` seconds."""
    if port == 0:
        raise ValueError(
            "workers > 1 requires an explicit port (each worker binds it "
            "with SO_REUSEPORT; port 0 would scatter workers across ports)"
        )
    ncpu = os.cpu_count() or 1
    logger.info("cadeloop supervisor: %d workers on http://%s:%s", n, host, port)
    children: dict[int, tuple[int, float]] = {}  # pid -> (idx, spawn_time)
    for idx in range(n):
        pid = _spawn_worker(app, host, port, config, idx, ncpu)
        children[pid] = (idx, time.monotonic())

    stopping = False
    exit_code = 0

    def _forward(signum, _frame):
        nonlocal stopping
        stopping = True
        for pid in list(children):
            try:
                os.kill(pid, _signal.SIGTERM)
            except ProcessLookupError:
                pass

    old_term = _signal.signal(_signal.SIGTERM, _forward)
    old_int = _signal.signal(_signal.SIGINT, _forward)
    crash_streak = 0
    try:
        while children:
            try:
                pid, status = os.waitpid(-1, 0)
            except ChildProcessError:
                break
            except InterruptedError:
                continue
            if pid not in children:
                continue
            idx, spawned = children.pop(pid)
            if stopping:
                continue
            clean = os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
            if clean:
                # A worker exiting cleanly on its own means stop was
                # requested inside it — treat as a shutdown signal.
                _forward(None, None)
                continue
            fast = time.monotonic() - spawned < _CRASH_FAST_SECS
            crash_streak = crash_streak + 1 if fast else 1
            if crash_streak >= _CRASH_STREAK_LIMIT:
                logger.error(
                    "worker %d died %d times in under %.0fs — giving up",
                    idx,
                    crash_streak,
                    _CRASH_FAST_SECS,
                )
                exit_code = 1
                _forward(None, None)
                continue
            logger.warning("worker %d died (status %d) — restarting", idx, status)
            npid = _spawn_worker(app, host, port, config, idx, ncpu)
            children[npid] = (idx, time.monotonic())
        # Drain: give survivors `grace` seconds, then SIGKILL (R-092).
        deadline = time.monotonic() + config.grace
        while children and time.monotonic() < deadline:
            try:
                pid, _status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                children.clear()
                break
            if pid == 0:
                time.sleep(0.05)
                continue
            children.pop(pid, None)
        for pid in children:
            logger.warning("worker pid %d exceeded grace=%ss — SIGKILL", pid, config.grace)
            try:
                os.kill(pid, _signal.SIGKILL)
                os.waitpid(pid, 0)
            except (ProcessLookupError, ChildProcessError):
                pass
    finally:
        _signal.signal(_signal.SIGTERM, old_term)
        _signal.signal(_signal.SIGINT, old_int)
    if exit_code:
        raise SystemExit(exit_code)
