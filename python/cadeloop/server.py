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
    if ssl is not None:
        import ssl as _ssl_mod

        if not isinstance(ssl, _ssl_mod.SSLContext):
            raise TypeError(f"ssl must be an ssl.SSLContext, got {type(ssl).__name__}")
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
):
    """One worker: loop + lifespan + native listener (the M2 path).

    ``control_channel``: set for a spawn-model worker (R-090). Such a
    worker never listens — the supervisor owns the accept loop and hands
    over already-accepted connections, which this process adopts into its
    own completion port. b"STOP" (or EOF, a dead supervisor) drains it.
    See ADR-25 for why the listener cannot simply be shared instead.
    """
    # TEMPORARY (ADR-24): bisecting a Windows-only STATUS_ACCESS_VIOLATION
    # in the spawned worker model. worker_id is only set from
    # _winworker.main(), so this stays silent for every other caller.
    _trace = (
        (lambda stage: print(f"cadeloop._serve_single: {stage}", file=sys.stderr, flush=True))
        if worker_id is not None
        else (lambda stage: None)
    )
    _trace("start")
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
    )
    _trace("loop constructed")
    asyncio.set_event_loop(loop)
    lifespan = _Lifespan(app, loop)
    lid = None
    installed_signals = []
    try:
        _trace("about to run lifespan.startup")
        lifespan.startup()
        _trace("lifespan.startup returned")
        if control_channel is not None:
            # Spawn-model worker: nothing to bind. Connections arrive over
            # the channel already accepted (R-090 / ADR-25).
            bound = None
            _trace("starting channel pump")
            threading.Thread(
                target=_channel_pump,
                args=(control_channel, loop, _make_adopter(loop, app, config, lifespan, ssl_ctx)),
                daemon=True,
            ).start()
            _trace("channel pump started")
        else:
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
                request_line_timeout=config.request_line_timeout,
                keepalive_idle=config.keepalive_idle,
                tls=ssl_ctx,
            )
        if config.access_log:
            loop._core.set_access_log(_access_sink(logging.getLogger("cadeloop.access")))
        _arm_timeout_sweep(loop, config)
        # R-075: freeze the post-startup heap out of the cyclic collector.
        if config.gc_mode == "freeze":
            gc.collect()
            gc.freeze()
        elif config.gc_mode == "disable":
            gc.collect()
            gc.disable()
        _trace("about to install signal handlers")
        stop_signals = [_signal.SIGINT, _signal.SIGTERM]
        if hasattr(_signal, "SIGBREAK"):
            stop_signals.append(_signal.SIGBREAK)  # CTRL+BREAK (R-052)
        for sig in stop_signals:
            try:
                loop.add_signal_handler(sig, loop.stop)
                installed_signals.append(sig)
            except (NotImplementedError, RuntimeError, ValueError):
                pass
        shown = bound if bound else (host, port)
        who = f"worker {worker_id} " if worker_id is not None else ""
        logger.info("cadeloop %sserving on http://%s:%s", who, shown[0], shown[1])
        _trace("about to call run_forever")
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


def _access_sink(access_logger):
    """R-140 access-log sink: called from the engine per completed request
    with (peername, method, target_bytes, status, duration_ms)."""

    def sink(peer, method, target, status, dur_ms):
        client = f"{peer[0]}:{peer[1]}" if peer else "-"
        access_logger.info(
            '%s "%s %s" %d %.2fms',
            client,
            method,
            target.decode("latin-1"),
            status,
            dur_ms,
        )

    return sink


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
        except Exception:  # noqa: BLE001 — one bad handoff must not kill the worker
            logger.exception("worker %s could not adopt a connection", os.getpid())

    return adopt


def _channel_pump(chan, loop, adopt):
    """Worker side of the spawn channel: adopt handed-over connections
    until STOP or EOF (a dead supervisor), then stop the loop."""
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
            try:
                loop.call_soon_threadsafe(adopt, sock)
            except RuntimeError:
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


def _spawn_worker(app, host, port, config: Config, idx: int, ncpu: int, ssl_ctx=None) -> int:
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
        _serve_single(app, host, port, config, reuse_port=True, worker_id=idx, ssl_ctx=ssl_ctx)
        status = 0
    except BaseException:  # noqa: BLE001 — worker death is the supervisor's signal
        logger.exception("worker %d crashed", idx)
    finally:
        os._exit(status)
    return 0  # unreachable


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
    children: dict[int, tuple[int, float]] = {}  # pid -> (idx, spawn_time)
    for idx in range(n):
        pid = _spawn_worker(app, host, port, config, idx, ncpu, ssl_ctx=ssl_ctx)
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
            npid = _spawn_worker(app, host, port, config, idx, ncpu, ssl_ctx=ssl_ctx)
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
    lsock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    try:
        lsock.bind((host, port))
        lsock.listen(1024)
    except OSError:
        lsock.close()
        raise
    ncpu = os.cpu_count() or 1
    logger.info("cadeloop supervisor: %d workers on http://%s:%s (accept+handoff)", n, host, port)
    workers = [_spawn_shared_worker(spec, config, idx, ncpu) for idx in range(n)]

    stopping = False
    exit_code = 0

    def _stop_all():
        nonlocal stopping
        stopping = True
        try:
            lsock.close()  # unblocks the accept loop
        except OSError:
            pass
        for w in workers:
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
                for _ in range(len(workers)):
                    w = workers[turn % len(workers)] if workers else None
                    turn += 1
                    if w is None or not w.alive():
                        continue
                    try:
                        _handoff(w, conn)
                        break
                    except (OSError, ValueError):
                        continue  # worker died mid-handoff; try the next
                else:
                    # Every worker is down. Dropping beats hanging the peer;
                    # the supervision loop below is already restarting them.
                    logger.warning("no live worker to accept a connection; dropping it")
            finally:
                conn.close()  # our handle; the worker holds its own now

    accepter = threading.Thread(target=_accept_loop, name="cadeloop-accept", daemon=True)
    accepter.start()

    crash_streak = 0
    try:
        while workers:
            time.sleep(0.2)
            for w in [w for w in workers if not w.alive()]:
                workers.remove(w)
                w.close()
                if stopping:
                    continue
                if w.proc.returncode == 0:
                    # Clean self-exit means stop was requested inside the
                    # worker — treat as a shutdown signal (fork parity).
                    _stop_all()
                    continue
                fast = time.monotonic() - w.spawned < _CRASH_FAST_SECS
                crash_streak = crash_streak + 1 if fast else 1
                if crash_streak >= _CRASH_STREAK_LIMIT:
                    logger.error(
                        "worker %d died %d times in under %.0fs — giving up",
                        w.idx,
                        crash_streak,
                        _CRASH_FAST_SECS,
                    )
                    exit_code = 1
                    _stop_all()
                    continue
                logger.warning("worker %d died (status %s) — restarting", w.idx, w.proc.returncode)
                workers.append(_spawn_shared_worker(spec, config, w.idx, ncpu))
    except KeyboardInterrupt:
        _stop_all()
    finally:
        if not stopping:
            _stop_all()
        deadline = time.monotonic() + config.grace
        for w in list(workers):
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
