"""``cadeloop.serve`` (R-101): the native HTTP/1.1 + ASGI 3.0 server (M2).

The hot path lives in Rust (``CoreLoop.http_listen``): llhttp parsing,
scope construction, eager coroutine stepping, and response serialization
all happen without touching this module. Python owns only the cold path —
config validation, app loading, the ASGI *lifespan* protocol (R-081), GC
tuning (R-075), and signal wiring.
"""

from __future__ import annotations

import asyncio
import dataclasses
import gc
import importlib
import json
import logging
import os
import queue
import signal as _signal
import socket as _socket
import subprocess
import sys
import threading
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
        # Set when the lifespan task ENDS after startup completed without
        # a shutdown having been asked for -- by raising, or by simply
        # returning. Either way the worker is serving and the
        # application's lifespan is gone.
        self.crashed: BaseException | None = None
        self._shutdown_requested = False

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
                    # Startup already succeeded, so the worker is serving
                    # -- but the application's lifespan task is gone, and
                    # whatever it was holding open (pools, clients,
                    # background tasks) may have failed or been torn down
                    # with it. Logging and carrying on left a worker that
                    # looks healthy and is not. Stop serving instead.
                    logger.exception("lifespan task crashed")
                    self.crashed = exc
                    self.loop.stop()
            if not self._startup.done():
                # Returned without startup.complete: no lifespan support.
                self.enabled = False
                self._startup.set_result(None)
            elif self.enabled and self.crashed is None and not self._shutdown_requested:
                # Returned NORMALLY after startup.complete, with nobody
                # having asked for shutdown. Nothing raised, so the branch
                # above never ran -- but the lifespan context has exited
                # and taken its pools, clients and background tasks with
                # it, and the worker was about to serve as though it had
                # not. Exactly the "looks healthy and is not" state the
                # crash path exists to prevent, reached without an
                # exception, so it gets the same outcome.
                self.crashed = RuntimeError(
                    "lifespan task returned after startup.complete without a shutdown "
                    "request; its context and resources are already gone"
                )
                logger.error("%s", self.crashed)
                self.loop.stop()
            if not self._shutdown.done():
                self._shutdown.set_result(None)

        self._task = loop.create_task(_run())
        self._queue.put_nowait({"type": "lifespan.startup"})
        loop.run_until_complete(self._startup)  # raises on startup.failed
        if self.crashed is not None:
            # Crashed in the same breath as startup.complete: the future
            # was already resolved, so the wait above saw success.
            raise RuntimeError("lifespan task crashed during startup") from self.crashed

    def shutdown(self) -> None:
        self._shutdown_requested = True
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
    if ssl is not None:
        import ssl as _ssl_mod

        if not isinstance(ssl, _ssl_mod.SSLContext):
            raise TypeError(f"ssl must be an ssl.SSLContext, got {type(ssl).__name__}")
        # The engine speaks HTTP/1.1 only. A context built for a
        # general-purpose server usually advertises h2 as well, and a
        # client that prefers it would negotiate h2 successfully and then
        # have every request rejected as malformed -- a working TLS
        # handshake followed by a server that looks broken. SSLContext
        # exposes no way to read the list back, so the only way to know
        # what it advertises is to set it. (The engine also refuses an
        # unsupported ALPN selection at handshake time, which covers the
        # contexts that reach it without passing through here.)
        ssl.set_alpn_protocols(["http/1.1"])
    app_spec = app if isinstance(app, str) else None
    if isinstance(app, str):
        app = load_app(app)
    if not callable(app):
        raise TypeError(f"ASGI app must be callable, got {app!r}")

    n = config.workers if config.workers > 0 else (os.cpu_count() or 1)
    if n > 1:
        if hasattr(os, "fork"):
            return _serve_multi(app, host, port, config, n, ssl_ctx=ssl)
        if ssl is not None:
            raise ValueError(
                "workers > 1 with ssl requires the fork worker model "
                "(an SSLContext cannot cross a spawn boundary); run one "
                "worker or terminate TLS upstream"
            )
        if app_spec is not None:
            # Fork-free model (R-090): the supervisor owns the listener and
            # the accept loop and hands each ACCEPTED connection to a
            # worker. The listener itself never crosses the boundary — see
            # ADR-25 for why sharing it cannot work on Windows.
            return _serve_multi_spawn(app_spec, host, port, config, n)
        # Spawned workers re-import the app, so a bare callable cannot
        # cross the process boundary (uvicorn has the same rule). Matches
        # the ssl+workers>1 precedent above: fail loudly rather than
        # silently running 1 worker when workers=N was explicitly asked
        # for — a warning buried in logs reads as "scaling isn't working"
        # instead of the actual, easy-to-fix configuration mistake.
        raise ValueError(
            f"workers={n} on this platform requires an app import string "
            '("module:attribute"), not a bare callable — spawned workers '
            "re-import the app, so it cannot cross the process boundary. "
            "Pass app as a string, or workers=1 to run a single process "
            "with the callable as given."
        )
    return _serve_single(app, host, port, config, ssl_ctx=ssl)



def _bind_candidates(host, port):
    """Bind addresses the native listener will accept, in preference order.

    ``http_listen`` parses its host with Rust's ``IpAddr``, so a name --
    ``localhost``, or any DNS host -- was rejected outright with
    ``ValueError: invalid IP address``. Only the spawn supervisor resolved
    anything, so ``serve(app, host="localhost")`` and the equivalent CLI
    invocation failed at startup on every other worker model.

    Returns ``(ip, flowinfo, scope_id)`` triples: getaddrinfo is also what
    carries the scope of a link-local address (``fe80::1%eth0``), which
    the listener needs as separate sockaddr fields.
    """
    if host is None or host == "":
        # asyncio spells "every interface" as None; the native listener
        # binds one socket, so name the family explicitly.
        host = "0.0.0.0"
    infos = _socket.getaddrinfo(host, port, type=_socket.SOCK_STREAM, flags=_socket.AI_PASSIVE)
    out = []
    for family, _t, _p, _canon, sockaddr in infos:
        if family == _socket.AF_INET:
            cand = (sockaddr[0], 0, 0)
        elif family == _socket.AF_INET6:
            cand = (sockaddr[0], sockaddr[2] if len(sockaddr) > 2 else 0,
                    sockaddr[3] if len(sockaddr) > 3 else 0)
        else:
            continue
        if cand not in out:
            out.append(cand)
    if not out:
        raise OSError(f"getaddrinfo returned no bindable address for {host!r}:{port}")
    return out


def _serve_single(
    app,
    host,
    port,
    config: Config,
    *,
    reuse_port: bool = False,
    worker_id=None,
    control_channel=None,
    ssl_ctx=None,
    ready_fd=None,
):
    """One worker: loop + lifespan + native listener (the M2 path).

    ``control_channel``: set for a spawn-model worker (R-090). Such a
    worker never listens — the supervisor owns the accept loop and hands
    over already-accepted connections, which this process adopts into its
    own completion port. b"STOP" (or EOF, a dead supervisor) drains it.
    See ADR-25 for why the listener cannot simply be shared instead.
    """
    loop = Loop(
        backend=config.backend,
        spin_us=config.spin_us,
        high_water=config.write_high_water,
        low_water=config.write_low_water,
        accept_pool=config.accept_pool,
        rio_cq_size=config.rio_cq_size,
        rio_rq_recv=config.rio_rq_recv,
        rio_rq_send=config.rio_rq_send,
        dns_cache=config.dns_cache,
        dns_cache_ttl=config.dns_cache_ttl,
        tfo=config.tfo,
        loopback_fast_path=config.loopback_fast_path,
    )
    asyncio.set_event_loop(loop)
    lifespan = _Lifespan(app, loop)
    lid = None
    installed_signals = []
    served = False
    access_log = None
    stats_lid = None
    # What the collector looked like before we touched it, so the finally
    # can put it back exactly -- and only what WE changed.
    gc_was_enabled = gc.isenabled()
    gc_froze = False
    prior_handlers: dict = {}
    try:
        lifespan.startup()
        if control_channel is not None:
            # Spawn-model worker: nothing to bind. Connections arrive over
            # the channel already accepted (R-090 / ADR-25).
            bound = None
            threading.Thread(
                target=_channel_pump,
                args=(control_channel, loop, _make_adopter(loop, app, config, lifespan, ssl_ctx)),
                daemon=True,
            ).start()
        else:
            # Try every resolved address in turn: `localhost` resolves to
            # ::1 first on most systems, and binding that fails outright
            # on a host with IPv6 disabled even though 127.0.0.1 -- the
            # next candidate -- would have worked.
            candidates = _bind_candidates(host, port)
            last_error = None
            for ip, flowinfo, scope_id in candidates:
                try:
                    lid, bound, _fd = loop._core.http_listen(
                        ip,
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
                        request_line_timeout=config.request_line_timeout,
                        keepalive_idle=config.keepalive_idle,
                        tls=ssl_ctx,
                        flowinfo=flowinfo,
                        scope_id=scope_id,
                    )
                    break
                except OSError as exc:
                    last_error = exc
            else:
                raise last_error
        if config.immediate_flush:
            loop._core.set_immediate_flush(True)
        if config.access_log:
            access_log = _AccessLog(logging.getLogger("cadeloop.access"))
            loop._core.set_access_log(access_log.sink)
        if config.stats_endpoint is not None and worker_id in (None, 0):
            stats_lid, _ = _start_stats_endpoint(loop, config.stats_endpoint, worker_id)
        _arm_timeout_sweep(loop, config)
        # R-075: freeze the post-startup heap out of the cyclic collector.
        if config.gc_mode == "freeze":
            gc.collect()
            # gc.unfreeze() is all-or-nothing, so it cannot separate our
            # startup heap from a permanent generation the caller built
            # themselves. Freezing anyway and skipping the unfreeze --
            # which is what this did when the restore was first added --
            # just moves the leak: our objects join theirs and stay there.
            # With no way to undo it, the honest choice is not to do it:
            # skip the optimisation and leave the caller's policy alone.
            if gc.get_freeze_count() == 0:
                gc.freeze()
                gc_froze = True
            else:
                logger.info(
                    "gc_mode='freeze' skipped: %d objects were already frozen by the "
                    "caller, and gc.unfreeze() could not later separate ours from theirs",
                    gc.get_freeze_count(),
                )
        elif config.gc_mode == "disable":
            gc.collect()
            gc.disable()
        stop_signals = [_signal.SIGINT, _signal.SIGTERM]
        if hasattr(_signal, "SIGBREAK"):
            stop_signals.append(_signal.SIGBREAK)  # CTRL+BREAK (R-052)
        for sig in stop_signals:
            try:
                # An embedding process may already have its own handler.
                # remove_signal_handler() in the cleanup resets the
                # disposition to DEFAULT rather than putting theirs back,
                # and serve() returns while the process carries on -- so
                # every later signal bypassed the application's handler.
                # Remember what was there and restore it below.
                prior_handlers[sig] = _signal.getsignal(sig)
                loop.add_signal_handler(sig, loop.stop)
                installed_signals.append(sig)
            except (NotImplementedError, RuntimeError, ValueError):
                prior_handlers.pop(sig, None)
        shown = bound if bound else (host, port)
        who = f"worker {worker_id} " if worker_id is not None else ""
        logger.info("cadeloop %sserving on http://%s:%s", who, shown[0], shown[1])
        served = True
        if ready_fd is not None:
            # Tells the supervisor this worker reached the serving state,
            # so a death from here on is a crash rather than a failure to
            # start. Written once, here and nowhere earlier: everything
            # above (bind, lifespan startup, listener setup) is precisely
            # what "not ready" has to cover.
            try:
                os.write(ready_fd, b"R")
            except OSError:
                pass
            finally:
                os.close(ready_fd)
                ready_fd = None
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        if lifespan.crashed is not None:
            # Raised inside the try, so the drain and teardown below still
            # run -- in-flight requests finish, then the caller (or the
            # supervisor, which restarts on a non-zero exit) learns the
            # worker is not serviceable.
            raise RuntimeError(
                "lifespan task crashed; worker stopped serving"
            ) from lifespan.crashed
    finally:
        # serve() is an ordinary callable: it returns, and the process
        # carries on. Leaving the collector as we reconfigured it made
        # every object alive at startup permanently uncollectable for the
        # rest of that process -- including the caller's, and including
        # cycles created before serve() and dropped after it. `disable`
        # was already restored here; `freeze`, the DEFAULT mode, was not.
        if gc_froze:
            gc.unfreeze()
        if gc_was_enabled and not gc.isenabled():
            gc.enable()
        elif not gc_was_enabled and gc.isenabled():
            gc.disable()
        for sig in installed_signals:
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, RuntimeError, ValueError):
                pass
            # remove_signal_handler() leaves SIG_DFL; put back whatever the
            # caller had, so an embedder's handler survives serve().
            prior = prior_handlers.get(sig)
            if prior is not None:
                try:
                    _signal.signal(sig, prior)
                except (OSError, ValueError, TypeError):
                    pass
        if lid is not None:
            loop._core.listener_close(lid)
        if stats_lid is not None:
            loop._core.listener_close(stats_lid)
        if served:
            _drain_connections(loop, config.grace)
        if access_log is not None:
            access_log.close()
        lifespan.shutdown()
        loop.close()
        asyncio.set_event_loop(None)


def _drain_connections(loop, grace):
    """R-092: let in-flight requests finish before the loop is closed.

    ``loop.close()`` cancels every pending operation, so going straight
    there from ``run_forever()`` truncated whatever was mid-response --
    ``grace`` was honoured between *workers* but never inside one. The
    listener is already closed when this runs, so the connection set only
    shrinks: the native side ends keep-alive on every connection (each
    then closes as its response completes), closes the ones that are idle
    right now, and sends a 1012 close frame to live WebSockets, which is
    what keeps them from waiting out the whole deadline.
    """
    if grace <= 0 or loop.is_closed():
        return
    busy = loop._core.http_begin_shutdown()
    if not busy:
        return
    logger.info("cadeloop: draining %d connection(s), grace=%ss", busy, grace)
    deadline = loop.time() + grace

    def poll():
        if loop._core.http_connection_count() == 0 or loop.time() >= deadline:
            loop.stop()
        else:
            loop.call_later(0.02, poll)

    loop.call_soon(poll)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        # A second interrupt during the drain means "now", not "later".
        return
    left = loop._core.http_connection_count()
    if left:
        logger.warning(
            "cadeloop: %d connection(s) still open after grace=%ss - closing", left, grace
        )


def _start_stats_endpoint(loop, port: int, worker_id):
    """R-141: serve `loop.stats()` as JSON, bound to loopback only.

    Documented in docs/ops.md since M2 but never implemented -- setting
    the option configured nothing and the endpoint was silently absent.

    Bound on one worker only. Every worker binding the same port would
    hand each scrape whichever process the kernel happened to pick,
    making a counter series meaningless; the payload carries `worker` so
    the reader knows whose numbers these are.
    """

    async def stats_app(scope, receive, send):
        if scope["type"] != "http":
            return
        await receive()
        payload = dict(loop._core.stats())
        payload["worker"] = 0 if worker_id is None else worker_id
        body = json.dumps(payload).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"cache-control", b"no-store"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    lid, bound, _fd = loop._core.http_listen("127.0.0.1", port, stats_app, loop)
    logger.info("cadeloop stats endpoint on http://127.0.0.1:%s", bound[1])
    return lid, bound[1]


class _AccessLog:
    """R-140 access log, emitted off the loop thread.

    The engine calls the sink inline, on the loop thread, once per
    completed request. A logging handler is free to block -- a file
    handler on a slow disk, a stream into a full pipe, anything
    network-backed -- and one such call stalls every connection the
    worker is holding and adds its own latency to the request it is
    logging. Records go to a bounded queue instead and a daemon thread
    does the emitting.

    The queue is bounded on purpose: a writer that cannot keep up should
    cost bounded memory, not unbounded memory and not the request path.
    Records past the bound are dropped and counted, and the count is
    reported once the writer catches up -- silently losing them would
    make the log quietly wrong, which is worse than a gap you can see.
    """

    __slots__ = ("_logger", "_queue", "_thread", "_dropped", "_lock")

    def __init__(self, logger, maxsize: int = 10_000):
        self._logger = logger
        self._queue: queue.Queue = queue.Queue(maxsize)
        self._dropped = 0
        self._lock = threading.Lock()
        self._thread = threading.Thread(
            target=self._run, name="cadeloop-access-log", daemon=True
        )
        self._thread.start()

    def sink(self, peer, method, target, status, dur_ms):
        """Called from the engine, on the loop thread. Must not block."""
        try:
            self._queue.put_nowait((peer, method, target, status, dur_ms))
        except queue.Full:
            with self._lock:
                self._dropped += 1

    def _run(self):
        while True:
            item = self._queue.get()
            if item is None:
                return
            with self._lock:
                dropped, self._dropped = self._dropped, 0
            if dropped:
                self._logger.warning(
                    "access log fell behind: %d record(s) dropped", dropped
                )
            peer, method, target, status, dur_ms = item
            client = f"{peer[0]}:{peer[1]}" if peer else "-"
            try:
                self._logger.info(
                    '%s "%s %s" %d %.2fms',
                    client,
                    method,
                    target.decode("latin-1"),
                    status,
                    dur_ms,
                )
            except Exception:  # a handler failing must not kill the writer
                logger.exception("access log handler raised")

    def close(self, timeout: float = 2.0):
        """Flush what is queued, then stop the writer."""
        # Wait for the writer to make room rather than deleting a record
        # to fit the sentinel: a full queue at shutdown is exactly when a
        # slow handler has built a backlog, so discarding the oldest entry
        # -- silently, without even counting it as dropped -- punched a
        # hole in the access log precisely where it mattered most.
        try:
            self._queue.put(None, timeout=timeout)
        except queue.Full:
            # The writer is not draining at all. Displacing one record is
            # now the only way to stop it; count it like any other drop so
            # the gap is reported rather than hidden.
            try:
                self._queue.get_nowait()
                with self._lock:
                    self._dropped += 1
                self._queue.put_nowait(None)
            except (queue.Empty, queue.Full):
                pass
        self._thread.join(timeout)
        # The writer reports drops as it consumes records; anything
        # counted after it read the sentinel would otherwise go unsaid.
        with self._lock:
            dropped, self._dropped = self._dropped, 0
        if dropped:
            self._logger.warning(
                "access log fell behind: %d record(s) dropped", dropped
            )


def _arm_timeout_sweep(loop, config: Config):
    """R-080: arm the coarse repeating timer driving the head/idle
    timeout sweep. Interval adapts so short (test-sized) timeouts still
    fire promptly; 0/negative timeouts disable their window natively."""
    windows = [t for t in (config.request_line_timeout, config.keepalive_idle) if t and t > 0]
    if not windows:
        return
    interval = max(0.05, min(1.0, min(windows) / 4))

    def sweep():
        loop._core.http_sweep()
        loop.call_later(interval, sweep)

    loop.call_later(interval, sweep)


def _socket_from_frame(body: bytes, fd):
    """Materialise a handed-over connection (worker side).

    Neither branch has ever been associated with a completion port: the
    supervisor accepts with a plain blocking ``accept()`` on a listener it
    deliberately never registers with an IOCP, which is precisely what
    makes the receiving worker's association legal (ADR-25).
    """
    if fd is not None:
        return _socket.socket(fileno=fd)  # POSIX: arrived via SCM_RIGHTS
    return _socket.fromshare(body)  # win32: WSADuplicateSocketW blob


def _make_adopter(loop, app, config: Config, lifespan, ssl_ctx):
    """Build the loop-thread callback that adopts one handed-over socket
    into the native HTTP engine."""

    def adopt(sock):
        fd = sock.detach()  # the engine owns the handle from here
        adopted = False
        try:
            loop._core.http_adopt(
                fd,
                app,
                loop,
                state=lifespan.state,
                eager=config.eager_tasks,
                max_header_bytes=config.max_header_bytes,
                max_headers=config.max_headers,
                max_url=config.max_url,
                max_body=config.max_body,
                request_line_timeout=config.request_line_timeout,
                keepalive_idle=config.keepalive_idle,
                tls=ssl_ctx,
            )
            adopted = True
        except Exception:  # noqa: BLE001 — one bad handoff must not kill the worker
            logger.exception("worker %s could not adopt a connection", os.getpid())
        finally:
            if not adopted:
                # detach() gave up Python's ownership and the engine never
                # took it, so nothing would ever close this handle.
                # Re-wrapping is the portable close (closesocket on
                # Windows, close(2) elsewhere).
                try:
                    _socket.socket(fileno=fd).close()
                except OSError:
                    pass

    return adopt


# Handoffs this worker will hold un-adopted before it starts refusing
# them. Reaching it means the loop thread has not run a tick in the time
# it took the supervisor to send this many connections.
_MAX_PENDING_ADOPTIONS = 1024


def _channel_pump(chan, loop, adopt):
    """Worker side of the spawn channel: adopt handed-over connections
    until STOP or EOF (a dead supervisor), then stop the loop.

    This thread is independent of the event loop, so nothing here is
    paced by the loop making progress. When the loop thread is wedged in
    synchronous application code, every frame still became an open socket
    plus a callback on the loop's unbounded cross-thread queue, and the
    supervisor -- which sees only a live process -- kept sending. The
    worker grew both without limit until it ran out of handles or memory.

    A connection a wedged worker is holding is not being served anyway, so
    beyond the cap it is closed rather than queued: the client learns at
    once and can be retried elsewhere, the worker stays bounded, and the
    stall is logged instead of silently absorbed.
    """
    slots = threading.Semaphore(_MAX_PENDING_ADOPTIONS)
    refused = 0

    def adopt_and_release(sock):
        try:
            adopt(sock)
        finally:
            slots.release()

    try:
        while True:
            cmd, body, fd = chan.recv_frame()
            if cmd is None or cmd == b"STOP":
                break
            if cmd != b"CONN":
                continue
            try:
                sock = _socket_from_frame(body, fd)
            except OSError:
                logger.exception("worker could not materialise a handed-over connection")
                continue
            if not slots.acquire(blocking=False):
                sock.close()
                refused += 1
                if refused == 1 or refused % 100 == 0:
                    logger.warning(
                        "worker refused %d handoff(s): %d adoptions still queued, "
                        "the event loop is not draining them",
                        refused,
                        _MAX_PENDING_ADOPTIONS,
                    )
                continue
            try:
                loop.call_soon_threadsafe(adopt_and_release, sock)
            except RuntimeError:
                slots.release()
                sock.close()
                break  # loop already closing
    except OSError:
        pass
    try:
        loop.call_soon_threadsafe(loop.stop)
    except RuntimeError:
        pass  # loop already closed


# --------------------------------------------------------------------- #
# multi-process worker model (§8, R-090..R-093)                          #
# --------------------------------------------------------------------- #

# Give up when a worker keeps dying immediately (R-092 supervision):
_CRASH_STREAK_LIMIT = 5
_CRASH_FAST_SECS = 1.0
# How long a worker must stay up to count as stable when we cannot tell
# whether it ever served. Well past any plausible startup: the whole point
# is that a deterministic startup failure -- a database connection timing
# out after five seconds -- must not read as a healthy run.
_STABLE_SECS = 30.0


def _crash_streak_next(streak: int, *, became_ready, uptime: float) -> int:
    """The restart-limit rule, in ONE place for both supervisors.

    It lived twice, and the two copies were not the same: the fork
    supervisor was taught to use readiness while the spawn supervisor kept
    asking only "did this die within a second of starting". A worker that
    fails slowly -- which is the ordinary shape of a startup failure --
    therefore reset the streak on every death there and was restarted
    forever, with no worker ever serving.

    `became_ready` is True/False where a supervisor has a readiness
    signal, and None where it does not; without one, staying up for
    `_STABLE_SECS` is the stand-in. Never serving always advances the
    streak, however long the failure took.
    """
    if became_ready is False:
        return streak + 1
    if uptime >= _STABLE_SECS:
        return 1
    return streak + 1


def _spawn_worker(app, host, port, config: Config, idx: int, ncpu: int, ssl_ctx=None):
    """Fork one worker. Returns (pid, ready_fd).

    `ready_fd` is the read end of a pipe the child writes one byte to once
    it is actually serving. The supervisor needs that distinction: its
    crash-loop guard used to ask only "did this die within
    _CRASH_FAST_SECS of being forked", so an application that fails
    SLOWLY -- a database connection that times out after five seconds is
    the ordinary case -- reset the streak on every death and was restarted
    forever. "Died without ever serving" is the signal that matters, and
    it does not depend on how long the failure took.
    """
    ready_r, ready_w = os.pipe()
    pid = os.fork()
    if pid != 0:
        os.close(ready_w)
        os.set_blocking(ready_r, False)
        return pid, ready_r
    # ---- child ----
    os.close(ready_r)
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
        _serve_single(
            app,
            host,
            port,
            config,
            reuse_port=True,
            worker_id=idx,
            ssl_ctx=ssl_ctx,
            ready_fd=ready_w,
        )
        status = 0
    except BaseException:  # noqa: BLE001 — worker death is the supervisor's signal
        logger.exception("worker %d crashed", idx)
    finally:
        os._exit(status)
    return 0  # unreachable


def _close_ready_fd(entry) -> None:
    """Release a supervised worker's readiness pipe.

    Every path that forgets a child has to come through here, or the
    supervisor leaks one descriptor per worker per restart -- which a
    crash-restart cycle turns into a slow exhaustion of the one process
    that is supposed to survive it.
    """
    if not isinstance(entry, tuple) or len(entry) < 3:
        return
    try:
        os.close(entry[2])
    except OSError:
        pass


def _worker_became_ready(ready_fd) -> bool:
    """Did this worker ever reach the serving state?

    Non-blocking: the byte is either already in the pipe or the worker
    died before writing it. Closes the descriptor either way.
    """
    try:
        return bool(os.read(ready_fd, 1))
    except (BlockingIOError, OSError):
        return False
    finally:
        try:
            os.close(ready_fd)
        except OSError:
            pass


def _kill_children(children) -> None:
    """SIGKILL and reap every supervised worker, then forget them.

    Used on any supervisor-side failure: a half-started fleet with no
    supervisor left to signal it would otherwise keep the port bound and
    the application's resources held for the life of the machine.
    """
    for pid, entry in list(children.items()):
        try:
            os.kill(pid, _signal.SIGKILL)
            os.waitpid(pid, 0)
        except (ProcessLookupError, ChildProcessError):
            pass
        _close_ready_fd(entry)
    children.clear()


def _serve_multi(app, host, port, config: Config, n: int, ssl_ctx=None):
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
    # pid -> (idx, spawn_time, ready_fd)
    children: dict[int, tuple[int, float, int]] = {}
    try:
        for idx in range(n):
            pid, ready_fd = _spawn_worker(app, host, port, config, idx, ncpu, ssl_ctx=ssl_ctx)
            children[pid] = (idx, time.monotonic(), ready_fd)
    except BaseException:
        # A fork failing partway (a process limit, say) raised before the
        # signal handlers were installed and before the cleanup block
        # below, so every worker already started kept serving while the
        # caller saw startup fail -- holding the port and the application's
        # resources with no supervisor left to stop them.
        _kill_children(children)
        raise

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
            if stopping:
                # The bounded drain below (config.grace, then SIGKILL) is
                # what enforces the shutdown deadline, so leave the reap
                # loop the moment shutdown begins.
                break
            try:
                # WNOHANG, not a blocking wait. A blocking waitpid() is
                # restarted by CPython after a signal handler that does not
                # raise -- and _forward does not raise -- so the `stopping`
                # check above was unreachable until some child happened to
                # exit. A worker wedged in synchronous application code
                # never exits, so shutdown hung indefinitely instead of
                # killing it after `grace`. That is the bug the comment
                # here previously claimed to have fixed.
                pid, status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                break
            except InterruptedError:
                continue
            if pid == 0:
                # Nothing exited. Sleep briefly rather than spinning; the
                # signal handler runs between iterations either way.
                time.sleep(0.05)
                continue
            if pid not in children:
                continue
            idx, spawned, ready_fd = children.pop(pid)
            # Consumes and closes ready_fd, so every reaped child is
            # accounted for on this path.
            became_ready = _worker_became_ready(ready_fd)
            if stopping:
                continue
            clean = os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
            if clean:
                # A worker exiting cleanly on its own means stop was
                # requested inside it — treat as a shutdown signal.
                _forward(None, None)
                continue
            crash_streak = _crash_streak_next(
                crash_streak,
                became_ready=became_ready,
                uptime=time.monotonic() - spawned,
            )
            if crash_streak >= _CRASH_STREAK_LIMIT:
                logger.error(
                    "worker %d died %d times %s — giving up",
                    idx,
                    crash_streak,
                    "without ever serving" if not became_ready else "without staying up",
                )
                exit_code = 1
                _forward(None, None)
                continue
            logger.warning("worker %d died (status %d) — restarting", idx, status)
            try:
                npid, nready = _spawn_worker(app, host, port, config, idx, ncpu, ssl_ctx=ssl_ctx)
            except BaseException:
                # A replacement fork can fail for the same reasons the
                # initial ones can, and this one raised straight through
                # the `finally` below -- which only restores signal
                # dispositions. Every surviving worker was left running,
                # listening and unsupervised while the caller was told the
                # server had failed. Same cleanup as the startup path.
                _kill_children(children)
                raise
            children[npid] = (idx, time.monotonic(), nready)
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
            _close_ready_fd(children.pop(pid, None))
        for pid in children:
            logger.warning("worker pid %d exceeded grace=%ss — SIGKILL", pid, config.grace)
            try:
                os.kill(pid, _signal.SIGKILL)
                os.waitpid(pid, 0)
            except (ProcessLookupError, ChildProcessError):
                pass
    finally:
        for entry in children.values():
            _close_ready_fd(entry)
        _signal.signal(_signal.SIGTERM, old_term)
        _signal.signal(_signal.SIGINT, old_int)
    if exit_code:
        raise SystemExit(exit_code)


# --------------------------------------------------------------------- #
# spawn worker model — fork-free platforms (Windows), R-090..R-093       #
# --------------------------------------------------------------------- #


class _SpawnWorker:
    """Supervisor's handle on one spawned worker: the process plus our end
    of its control/handoff channel."""

    __slots__ = ("proc", "sock", "stdin", "idx", "spawned")

    def __init__(self, proc, sock, stdin, idx, spawned):
        self.proc = proc
        self.sock = sock
        self.stdin = stdin
        self.idx = idx
        self.spawned = spawned

    def alive(self) -> bool:
        return self.proc.poll() is None

    def close(self):
        for closer in (self.sock, self.stdin):
            try:
                if closer is not None:
                    closer.close()
            except OSError:
                pass


def _send_frame(worker: _SpawnWorker, payload: bytes, fd=None) -> None:
    """Write one length-prefixed frame to a worker.

    POSIX rides an AF_UNIX SOCK_SEQPACKET pair so a descriptor can be
    attached to the frame it belongs to (SCM_RIGHTS needs a message
    boundary); Windows rides the child's stdin pipe and carries the
    connection inline as ``socket.share()`` bytes instead. The length
    prefix is redundant on SEQPACKET but kept so both sides parse one
    format.
    """
    frame = len(payload).to_bytes(4, "big") + payload
    if worker.sock is not None:
        if fd is None:
            worker.sock.sendall(frame)
        else:
            _socket.send_fds(worker.sock, [frame], [fd])
    else:
        worker.stdin.write(frame)
        worker.stdin.flush()


def _handoff(worker: _SpawnWorker, conn) -> None:
    """Give one ACCEPTED connection to a worker (R-090).

    The socket has never been associated with a completion port — the
    supervisor's listener is deliberately never registered with one — so
    the receiving worker may legally associate it with its own (ADR-25).
    """
    if sys.platform == "win32":
        _send_frame(worker, b"CONN " + conn.share(worker.proc.pid))
    else:
        _send_frame(worker, b"CONN", fd=conn.fileno())


def _spawn_shared_worker(spec, config: Config, idx: int, ncpu: int) -> _SpawnWorker:
    """Spawn one worker and hand it its startup header. Unlike the model
    this replaced, no listener crosses the boundary — only connections do."""
    win = sys.platform == "win32"
    if win:
        proc = subprocess.Popen(
            [sys.executable, "-m", "cadeloop._winworker"],
            stdin=subprocess.PIPE,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
        worker = _SpawnWorker(proc, None, proc.stdin, idx, time.monotonic())
    else:
        sup, child = _socket.socketpair(_socket.AF_UNIX, _socket.SOCK_SEQPACKET)
        try:
            proc = subprocess.Popen(
                [sys.executable, "-m", "cadeloop._winworker"],
                stdin=child.fileno(),
            )
        except BaseException:
            sup.close()
            raise
        finally:
            child.close()
        worker = _SpawnWorker(proc, sup, None, idx, time.monotonic())
    header = {
        "spec": spec,
        "config": dataclasses.asdict(config) | {"workers": 1},
        "worker_id": idx,
        "pin": (idx % ncpu) if config.pin else None,
    }
    try:
        _send_frame(worker, b"HELLO " + json.dumps(header).encode())
    except (OSError, ValueError):
        proc.kill()
        worker.close()
        raise
    return worker


def _serve_multi_spawn(spec: str, host, port, config: Config, n: int):
    """Fork-free supervisor (R-090..R-093): the supervisor owns the
    listener AND the accept loop, and hands each accepted connection to a
    worker round-robin.

    It does NOT share the listener itself. On Windows a file object binds
    to exactly one completion port for life, so a duplicated listener can
    only ever be driven by whichever worker associated it first — the
    others' completions get delivered to that worker's port carrying
    pointers into their own address space, which is a crash, not a race
    (ADR-25). Accepting centrally costs one handoff per connection and
    keeps every socket associated exactly once, by the process that
    drives it.

    The listener is deliberately never registered with a completion port:
    a plain blocking ``accept()`` keeps the sockets it yields unassociated,
    which is what makes them legal to hand over.
    """
    if port == 0:
        raise ValueError(
            "workers > 1 requires an explicit port (the supervisor binds "
            "one listener; port 0 would be unknowable to callers)"
        )
    # Resolve first: hardcoding AF_INET made every IPv6 host ("::1", an
    # IPv6-only deployment) fail at bind, even though the single-worker
    # and fork paths accept them.
    infos = _socket.getaddrinfo(
        host, port, type=_socket.SOCK_STREAM, flags=_socket.AI_PASSIVE
    )
    if not infos:
        raise OSError(f"getaddrinfo returned nothing for {host!r}:{port}")
    family, socktype, proto, _canon, sockaddr = infos[0]
    lsock = _socket.socket(family, socktype, proto)
    try:
        if family == _socket.AF_INET6 and hasattr(_socket, "IPV6_V6ONLY"):
            # Keep a wildcard IPv6 listener off the IPv4 port, matching
            # create_server's per-family listeners.
            lsock.setsockopt(_socket.IPPROTO_IPV6, _socket.IPV6_V6ONLY, 1)
        lsock.bind(sockaddr)
        lsock.listen(1024)
    except OSError:
        lsock.close()
        raise
    ncpu = os.cpu_count() or 1
    logger.info("cadeloop supervisor: %d workers on http://%s:%s (accept+handoff)", n, host, port)

    # `workers` is read by the accept thread and mutated by the
    # supervision loop, so every access goes through this lock. Without it
    # the accept thread could evaluate len(workers), lose the race to a
    # removal, and index out of range — killing the only thread that
    # distributes connections.
    workers_lock = threading.Lock()
    workers = []
    stopping = False
    exit_code = 0

    def _live_workers():
        with workers_lock:
            return list(workers)

    try:
        for idx in range(n):
            w = _spawn_shared_worker(spec, config, idx, ncpu)
            with workers_lock:
                workers.append(w)
    except BaseException:
        # A failure partway through startup used to leave the children
        # already spawned running and the listener bound, because the
        # cleanup below had not been entered yet.
        for w in _live_workers():
            try:
                _send_frame(w, b"STOP")
            except (OSError, ValueError):
                pass
        for w in _live_workers():
            try:
                w.proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                w.proc.kill()
            w.close()
        try:
            lsock.close()
        except OSError:
            pass
        raise

    def _stop_all():
        nonlocal stopping
        stopping = True
        try:
            lsock.close()  # unblocks the accept loop
        except OSError:
            pass
        for w in _live_workers():
            try:
                _send_frame(w, b"STOP")
            except (OSError, ValueError):
                pass

    def _accept_loop():
        """Distribute connections round-robin over the live workers."""
        turn = 0
        while not stopping:
            try:
                conn, _addr = lsock.accept()
            except OSError:
                return  # listener closed: shutting down
            try:
                # One snapshot per connection: the supervision loop may
                # add or remove workers at any moment.
                live = _live_workers()
                for _ in range(len(live)):
                    w = live[turn % len(live)] if live else None
                    turn += 1
                    if w is None or not w.alive():
                        continue
                    try:
                        _handoff(w, conn)
                        break
                    except (OSError, ValueError):
                        continue  # worker died mid-handoff; try the next
                if not live:
                    logger.warning("no live worker to accept a connection; dropping it")
            finally:
                conn.close()  # our handle; the worker holds its own now

    accepter = threading.Thread(target=_accept_loop, name="cadeloop-accept", daemon=True)
    accepter.start()

    crash_streak = 0
    try:
        while _live_workers():
            if stopping:
                # Same reason as the fork supervisor: once shutdown starts,
                # the bounded drain in `finally` owns the deadline. Spinning
                # here waiting for a wedged worker to exit meant grace was
                # only ever enforced after it already had.
                break
            time.sleep(0.2)
            for w in [w for w in _live_workers() if not w.alive()]:
                with workers_lock:
                    if w in workers:
                        workers.remove(w)
                w.close()
                if stopping:
                    continue
                if w.proc.returncode == 0:
                    # Clean self-exit means stop was requested inside the
                    # worker — treat as a shutdown signal (fork parity).
                    _stop_all()
                    continue
                # became_ready=None: this model's control channel runs
                # supervisor -> worker only on Windows, so there is no
                # readiness frame to read. `_STABLE_SECS` stands in --
                # which is the point of sharing the rule rather than
                # keeping a second copy that quietly says something else.
                crash_streak = _crash_streak_next(
                    crash_streak,
                    became_ready=None,
                    uptime=time.monotonic() - w.spawned,
                )
                if crash_streak >= _CRASH_STREAK_LIMIT:
                    logger.error(
                        "worker %d died %d times without staying up — giving up",
                        w.idx,
                        crash_streak,
                    )
                    exit_code = 1
                    _stop_all()
                    continue
                logger.warning("worker %d died (status %s) — restarting", w.idx, w.proc.returncode)
                replacement = _spawn_shared_worker(spec, config, w.idx, ncpu)
                with workers_lock:
                    workers.append(replacement)
    except KeyboardInterrupt:
        _stop_all()
    finally:
        if not stopping:
            _stop_all()
        deadline = time.monotonic() + config.grace
        for w in _live_workers():
            budget = max(0.0, deadline - time.monotonic())
            try:
                w.proc.wait(timeout=budget)
            except subprocess.TimeoutExpired:
                logger.warning("worker %d exceeded grace=%ss — killing", w.idx, config.grace)
                w.proc.kill()
            w.close()
        try:
            lsock.close()
        except OSError:
            pass
    if exit_code:
        raise SystemExit(exit_code)
