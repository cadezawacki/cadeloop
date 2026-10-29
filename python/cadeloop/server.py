"""``cadeloop.serve`` (R-101): the native HTTP/1.1 + ASGI 3.0 server (M2).

The hot path lives in Rust (``CoreLoop.http_listen``): llhttp parsing,
scope construction, eager coroutine stepping, and response serialization
all happen without touching this module. Python owns only the cold path —
config validation, app loading, the ASGI *lifespan* protocol (R-081), GC
tuning (R-075), and signal wiring.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import dataclasses
import errno
import gc
import importlib
import importlib.util
import json
import logging
import os
import queue
import re
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

_WIN = sys.platform == "win32"

# What "stop" means to a worker: Ctrl-C, a service manager, CTRL+BREAK (R-052)...
_STOP_SIGNALS = tuple(
    s for s in (_signal.SIGINT, _signal.SIGTERM, getattr(_signal, "SIGBREAK", None)) if s is not None
)
# ...and the interactive subset: repeating one of these is a person asking to hurry.
_INTERRUPT_SIGNALS = frozenset(
    s for s in (_signal.SIGINT, getattr(_signal, "SIGBREAK", None)) if s is not None
)

# An address this host cannot use at all (::1 with IPv6 disabled, an
# address that is not local), as opposed to one it refuses to give us.
# POSIX values plus their Winsock twins (WSAEPROTONOSUPPORT/EAFNOSUPPORT/
# EADDRNOTAVAIL); the native listener reports either.
_UNUSABLE_ADDRESS_ERRNOS = frozenset(
    {errno.EAFNOSUPPORT, errno.EADDRNOTAVAIL, errno.EPROTONOSUPPORT, 10043, 10047, 10049}
)
_ADDRINUSE_ERRNOS = frozenset({errno.EADDRINUSE, 10048})
_OS_ERROR_RE = re.compile(r"\(os error (\d+)\)")


def _os_errno(exc: OSError):
    """The errno of an OSError -- including the native listener's, which
    PyO3 builds from a Rust io::Error as OSError(message): the code then
    survives only as the message's "(os error N)" suffix."""
    if exc.errno is not None:
        return exc.errno
    m = _OS_ERROR_RE.search(str(exc))
    return int(m.group(1)) if m else None


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


def _check_app_spec(spec: str) -> None:
    """Validate a spec for the spawn model WITHOUT importing the app.

    The spawn supervisor never serves: importing the application there only
    to check its name cost a full extra copy of its import-time state --
    models, pools, threads -- next to the N workers that actually use it.
    Locating the module still catches the typo before anything binds.
    """
    module_name, sep, attr = spec.partition(":")
    if not sep or not module_name or not attr:
        raise ValueError(
            f"invalid application spec {spec!r} — expected 'module:attribute'"
        )
    if module_name == "__main__":
        # A spawned worker is `python -m cadeloop._winworker`: its __main__
        # is not the caller's script, so this spec could never resolve there.
        raise ValueError(
            f"{spec!r}: spawned workers cannot import the __main__ module -- move "
            "the application into an importable module"
        )
    if importlib.util.find_spec(module_name) is None:
        raise ModuleNotFoundError(f"No module named {module_name!r}", name=module_name)


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
        # Did the app take the lifespan.startup message? An exception after
        # that is its startup failing, not a framework without lifespan.
        self._startup_delivered = False
        self._startup_failed = False
        self._shutdown_failure_reported = False
        # What a crash while serving does; _serve_single routes it through
        # its shutdown controller.
        self.on_crash = loop.stop

    async def _receive(self):
        assert self._queue is not None
        message = await self._queue.get()
        if message.get("type") == "lifespan.startup":
            self._startup_delivered = True
        return message

    async def _send(self, message):
        kind = message.get("type")
        if kind == "lifespan.startup.complete":
            if not self._startup.done():
                self._startup.set_result(None)
        elif kind == "lifespan.startup.failed":
            if not self._startup.done():
                self._startup_failed = True
                self._startup.set_exception(
                    RuntimeError(message.get("message") or "lifespan startup failed")
                )
        elif kind == "lifespan.shutdown.complete":
            if not self._shutdown.done():
                self._shutdown.set_result(None)
        elif kind == "lifespan.shutdown.failed":
            self._shutdown_failure_reported = True
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
            except (KeyboardInterrupt, SystemExit):
                # Process-control exceptions are not "no lifespan
                # support": a Ctrl-C landing in synchronous startup code
                # (or the app's deliberate exit) must stop the worker,
                # not silently disable lifespan and serve on. The task
                # machinery re-raises these out of the loop tick.
                raise
            except BaseException as exc:  # noqa: BLE001 — app may not do lifespan
                if self._shutdown_requested:
                    # The application's shutdown hook raised. The worker is
                    # already stopping, so this is not a crash of a serving
                    # worker: report it once (Starlette has already sent
                    # shutdown.failed carrying the traceback) and let
                    # shutdown() finish -- the crash path below stopped the
                    # loop under it, which then read as "timed out".
                    if not self._shutdown_failure_reported:
                        logger.error("lifespan shutdown raised", exc_info=exc)
                elif self._startup_failed:
                    # startup.failed carries the error; startup() raises it.
                    logger.debug("lifespan task ended after startup.failed: %r", exc)
                elif not self._startup.done():
                    # Errored before startup completed: app has no lifespan
                    # support -> disable, keep serving ('auto' behavior).
                    self.enabled = False
                    self._startup.set_result(None)
                    if self._startup_delivered:
                        # ...except this one took lifespan.startup first: it
                        # does speak lifespan, and its startup broke.
                        # uvicorn's 'auto' serves on here as well, but at
                        # DEBUG it was invisible that whatever startup was
                        # meant to build -- pools, clients -- is missing.
                        logger.warning(
                            "lifespan startup raised after receiving lifespan.startup; "
                            "serving WITHOUT the application's lifespan",
                            exc_info=exc,
                        )
                    else:
                        logger.info("ASGI 'lifespan' protocol appears unsupported (%r)", exc)
                else:
                    # Startup already succeeded, so the worker is serving
                    # -- but the application's lifespan task is gone, and
                    # whatever it was holding open (pools, clients,
                    # background tasks) may have failed or been torn down
                    # with it. Logging and carrying on left a worker that
                    # looks healthy and is not. Stop serving instead.
                    logger.exception("lifespan task crashed")
                    self.crashed = exc
                    self.on_crash()
            if not self._startup.done():
                # Returned without startup.complete: no lifespan support.
                self.enabled = False
                self._startup.set_result(None)
            elif (
                self.enabled
                and self.crashed is None
                and not self._shutdown_requested
                and not self._startup_failed
            ):
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
                self.on_crash()
            if not self._shutdown.done():
                self._shutdown.set_result(None)

        self._task = loop.create_task(_run())
        self._queue.put_nowait({"type": "lifespan.startup"})
        loop.run_until_complete(self._startup)  # raises on startup.failed
        if self.crashed is not None:
            # Crashed in the same breath as startup.complete: the future
            # was already resolved, so the wait above saw success.
            raise RuntimeError("lifespan task crashed during startup") from self.crashed

    def shutdown(self, timeout: float = 10.0) -> None:
        self._shutdown_requested = True
        if not self.enabled or self._startup_failed or self._task is None or self._task.done():
            # Never started, or already over -- including an app that
            # reported startup.failed: uvicorn does not send it
            # lifespan.shutdown either, and nothing was started to stop.
            return
        self._queue.put_nowait({"type": "lifespan.shutdown"})
        try:
            self.loop.run_until_complete(
                asyncio.wait_for(asyncio.shield(self._shutdown), timeout=timeout)
            )
        except (TimeoutError, asyncio.TimeoutError):
            logger.warning("lifespan shutdown did not complete within %ss", timeout)
        except RuntimeError:
            # The loop was stopped under the wait: a forced exit.
            logger.warning("lifespan shutdown interrupted")


class _Shutdown:
    """Where every request to stop a worker goes, and what it means now.

    A stop comes from a signal, from the supervisor (a STOP frame, or EOF
    on the control channel or the lifeline: the supervisor is gone), from
    a lifespan task that died while serving, or from the application
    calling ``loop.stop()`` itself. Only the first request starts the
    shutdown; later ones never cut it short. The stop handlers used to be
    put back -- SIG_DFL, in a pool worker -- before the drain, so the
    second SIGTERM a group-wide stop delivers (Ctrl-C reaching the
    terminal's whole process group; systemd's default
    KillMode=control-group: once from outside, once forwarded by the
    supervisor) killed workers mid-drain. On a busy 4-worker pool
    lifespan.shutdown then ran in 0-1 of 4 workers and every in-flight
    request failed, while the supervisor exited 0.

    Only a person repeating an interrupt escalates, and only in a
    single-process server (uvicorn's "CTRL+C to force quit"): during
    startup it abandons the startup, during the drain it stops waiting for
    in-flight requests, during lifespan shutdown it stops waiting for the
    hook. A repeated SIGTERM never escalates -- an orchestrator escalates
    with SIGKILL on its own clock -- and nothing does inside a pool
    worker, whose deadline the supervisor owns.
    """

    __slots__ = ("loop", "interactive", "phase", "reason", "hurry", "abandoned")

    def __init__(self, loop, *, interactive: bool):
        self.loop = loop
        self.interactive = interactive
        # "starting" -> "serving" -> "draining" -> "closing" -> "closed"
        self.phase = "starting"
        self.reason = None
        self.hurry = False  # stop waiting for in-flight requests
        self.abandoned = False  # the lifespan startup was given up on

    def request(self, reason: str) -> None:
        if self.reason is not None:
            return
        self.reason = reason
        if self.phase == "serving":
            self.loop.stop()

    def on_signal(self, signum: int) -> None:
        if self.reason is None:
            self.request("signal")
        elif self.interactive and signum in _INTERRUPT_SIGNALS:
            if self.phase == "starting":
                self.abandoned = True
                self.loop.stop()
            elif self.phase in ("serving", "draining"):
                self.hurry = True
            elif self.phase == "closing":
                self.loop.stop()


def _allowed_cpus() -> list[int]:
    """The CPUs this process may run on: its affinity mask (taskset, a
    container's cpuset), not the machine's CPU count."""
    if hasattr(os, "sched_getaffinity"):
        try:
            cpus = sorted(os.sched_getaffinity(0))
        except OSError:
            cpus = []
        if cpus:
            return cpus
    return list(range(os.cpu_count() or 1))


def _worker_count(workers: int) -> int:
    """``workers=0``: one per CPU the process may use. os.cpu_count() counts
    the machine, so `taskset -c 2,3 ... -w 0` started four workers on a
    four-CPU box, two of them on CPUs the operator had excluded."""
    return workers if workers > 0 else len(_allowed_cpus())


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
    ``workers=0`` means one per CPU the process may run on.
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
    n = _worker_count(config.workers)
    spawn = n > 1 and not hasattr(os, "fork")
    app_spec = app if isinstance(app, str) else None
    if app_spec is not None and spawn:
        _check_app_spec(app_spec)  # the workers import it; the supervisor never serves
    else:
        if app_spec is not None:
            app = load_app(app_spec)
        if not callable(app):
            raise TypeError(f"ASGI app must be callable, got {app!r}")

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
    _serve_single(app, host, port, config, ssl_ctx=ssl)


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


def _listen_all(loop, host, port, app, lifespan, config: Config, *, reuse_port, ssl_ctx):
    """Listen on every address the host resolves to, as create_server does.

    Returns ``(listener ids, first bound address)``. Stopping at the first
    candidate that bound left a name resolving to several addresses with
    ONE listener: ``serve(host="localhost")`` on a dual-stack host (::1
    resolves first) refused every client of 127.0.0.1. An address the host
    cannot use at all is skipped -- ::1 with IPv6 disabled -- but any other
    failure (the port taken, no permission) is the caller's error, not a
    reason to quietly serve on fewer addresses. With port 0, the later
    addresses take the port the first one got, so one server is one port.
    """
    lids = []
    first = None
    skipped = None
    try:
        for ip, flowinfo, scope_id in _bind_candidates(host, port):
            want = first[1] if (port == 0 and first is not None) else port
            try:
                lid, bound, _fd = loop._core.http_listen(
                    ip,
                    want,
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
            except OSError as exc:
                code = _os_errno(exc)
                if code in _UNUSABLE_ADDRESS_ERRNOS or (want != port and code in _ADDRINUSE_ERRNOS):
                    skipped = exc
                    continue
                raise
            lids.append(lid)
            if first is None:
                first = bound
    except BaseException:
        for lid in lids:
            loop._core.listener_close(lid)
        raise
    if not lids:
        raise skipped
    return lids, first


def _current_event_loop():
    """The loop set_event_loop() last installed in this thread (or None),
    read without get_event_loop()'s side effects: it creates a loop, or
    warns, when there is none."""
    local = getattr(asyncio.get_event_loop_policy(), "_local", None)
    return getattr(local, "_loop", None)


def _install_stop_handlers(loop, shutdown, prior: dict) -> list:
    """Route SIGINT/SIGTERM (and SIGBREAK) to ``shutdown``; returns what was
    installed, with the handler each one replaced recorded in ``prior``."""
    if threading.current_thread() is not threading.main_thread():
        return []  # CPython delivers signals to the main thread only
    installed = []
    for sig in _STOP_SIGNALS:
        try:
            current = _signal.getsignal(sig)
        except (OSError, ValueError):
            continue
        if sig in _INTERRUPT_SIGNALS and current is _signal.SIG_IGN:
            # An inherited SIG_IGN is a decision made upstream: a
            # background job, nohup, or a pool worker whose supervisor
            # handles Ctrl-C and forwards a TERM. CPython leaves SIGINT
            # alone at startup in that case; overriding it here made every
            # Ctrl-C reach a pool worker twice, from the terminal and from
            # its supervisor.
            continue
        try:
            loop.add_signal_handler(sig, shutdown.on_signal, sig)
        except (NotImplementedError, RuntimeError, ValueError):
            continue
        prior[sig] = current
        installed.append(sig)
    return installed


def _restore_stop_handlers(loop, installed, prior: dict) -> None:
    """Put the embedder's handlers back -- after the drain and the lifespan
    shutdown, never before (see _Shutdown) -- and without a window on the
    way: remove_signal_handler() resets to SIG_DFL before the old handler
    is reinstalled, and a signal in between used to kill the process."""
    if not installed:
        return
    mask = getattr(_signal, "pthread_sigmask", None)
    old_mask = mask(_signal.SIG_BLOCK, installed) if mask else None
    try:
        for sig in installed:
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, RuntimeError, ValueError):
                pass
            previous = prior.get(sig)
            if previous is not None:
                try:
                    _signal.signal(sig, previous)
                except (OSError, ValueError, TypeError):
                    pass
    finally:
        if mask:
            # A signal that arrived meanwhile reaches the restored handler now.
            mask(_signal.SIG_SETMASK, old_mask)


def _watch_lifeline(loop, fd, shutdown) -> None:
    """Stop when the supervisor dies: it holds the only write end of the
    lifeline pipe, so its death -- SIGKILL, the OOM killer -- is EOF here.
    Fork workers had no way to notice, and kept serving the port for good."""

    def readable():
        try:
            data = os.read(fd, 64)
        except BlockingIOError:
            return
        except OSError:
            data = b""
        if not data:
            loop.remove_reader(fd)
            shutdown.request("supervisor")

    loop.add_reader(fd, readable)


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
    lifeline_fd=None,
    unblock_signals=None,
    gc_inherited: bool = False,
):
    """One worker: loop + lifespan + native listener (the M2 path).

    ``control_channel``: set for a spawn-model worker (R-090). Such a
    worker never listens — the supervisor owns the accept loop and hands
    over already-accepted connections, which this process adopts into its
    own completion port. b"STOP" (or EOF, a dead supervisor) drains it.
    See ADR-25 for why the listener cannot simply be shared instead.

    Fork-model workers also pass ``lifeline_fd`` (EOF: the supervisor is
    gone), ``unblock_signals`` (the stop signals the fork kept blocked until
    this worker's own handlers are in place) and ``gc_inherited`` (the
    supervisor froze the inherited heap before forking).

    Returns why it stopped: ``"app"`` (the application stopped the loop
    itself), ``"signal"``, ``"supervisor"`` or ``"lifespan"``.
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
    # Restored on the way out: serve() is an ordinary callable, and setting
    # None left an embedder's own current loop gone after it returned.
    prev_loop = _current_event_loop()
    asyncio.set_event_loop(loop)
    lifespan = _Lifespan(app, loop)
    shutdown = _Shutdown(loop, interactive=worker_id is None)
    lifespan.on_crash = lambda: shutdown.request("lifespan")
    lids = []
    stats_lid = None
    access_log = None
    hook = None
    installed_signals = []
    prior_handlers: dict = {}
    served = False
    # What the collector looked like before we touched it, so the finally
    # can put it back exactly -- and only what WE changed.
    gc_was_enabled = gc.isenabled()
    gc_froze = False
    try:
        # Stop signals are handled from the start, so one that lands during
        # lifespan startup lets the startup finish and then shuts down
        # cleanly (uvicorn's order) instead of killing the worker with its
        # resources half built.
        installed_signals = _install_stop_handlers(loop, shutdown, prior_handlers)
        if unblock_signals:
            # Held since before the fork (see _spawn_worker): one that
            # arrived in between is delivered now, to the handler above.
            _signal.pthread_sigmask(_signal.SIG_UNBLOCK, unblock_signals)
        if lifeline_fd is not None:
            _watch_lifeline(loop, lifeline_fd, shutdown)
        try:
            lifespan.startup()
        except RuntimeError:
            if not shutdown.abandoned:
                raise
            logger.warning("lifespan startup interrupted")
        if shutdown.reason is None:
            if control_channel is not None:
                # Spawn-model worker: nothing to bind. Connections arrive over
                # the channel already accepted (R-090 / ADR-25).
                bound = None
                threading.Thread(
                    target=_channel_pump,
                    args=(
                        control_channel,
                        loop,
                        _make_adopter(loop, app, config, lifespan, ssl_ctx),
                        shutdown,
                    ),
                    daemon=True,
                ).start()
            else:
                lids, bound = _listen_all(
                    loop, host, port, app, lifespan, config, reuse_port=reuse_port, ssl_ctx=ssl_ctx
                )
            if config.immediate_flush:
                loop._core.set_immediate_flush(True)
            hook = _RequestHook(loop)
            if config.access_log:
                access_log = _AccessLog(logging.getLogger("cadeloop.access"))
                hook.attach_access_log(access_log)
            if config.stats_endpoint is not None and worker_id in (None, 0):
                stats_lid, _ = _start_stats_endpoint(loop, config.stats_endpoint, worker_id)
            _arm_timeout_sweep(loop, config)
            gc_froze = _gc_after_startup(config, inherited=gc_inherited)
            if gc_froze and config.warmup > 0:
                hook.count_requests(config.warmup, _freeze_warm_heap)
            hook.start()
            shown = bound if bound else (host, port)
            who = f"worker {worker_id} " if worker_id is not None else ""
            logger.info("cadeloop %sserving on http://%s:%s", who, shown[0], shown[1])
            served = True
            shutdown.phase = "serving"
            if ready_fd is not None:
                # Tells the supervisor this worker reached the serving
                # state, so a death from here on is a crash rather than a
                # failure to start. Written here and nowhere earlier:
                # everything above (bind, lifespan startup, listener setup)
                # is precisely what "not ready" has to cover. The pipe
                # stays open: _spawn_worker reports how the worker ended.
                try:
                    os.write(ready_fd, b"R")
                except OSError:
                    pass
            if shutdown.reason is None:  # asked to stop while setting up
                try:
                    loop.run_forever()
                except KeyboardInterrupt:
                    # Only reachable when no SIGINT handler could be
                    # installed (a thread other than the main one).
                    shutdown.request("signal")
            if shutdown.reason is None:
                shutdown.reason = "app"  # the application stopped the loop
        if lifespan.crashed is not None:
            # Raised inside the try, so the drain and teardown below still
            # run -- in-flight requests finish, then the caller (or the
            # supervisor, which restarts on a non-zero exit) learns the
            # worker is not serviceable.
            raise RuntimeError(
                "lifespan task crashed; worker stopped serving"
            ) from lifespan.crashed
        return shutdown.reason
    finally:
        shutdown.phase = "draining"
        started = time.monotonic()
        try:
            for lid in lids:
                loop._core.listener_close(lid)
            if stats_lid is not None:
                loop._core.listener_close(stats_lid)
            if served:
                # Not the whole grace budget: the supervisor SIGKILLs
                # `grace` after shutdown begins, and lifespan.shutdown()
                # below needs a slice of it -- a drain that spends all of
                # the budget means the app's cleanup hook never runs.
                _drain_connections(loop, _drain_budget(config.grace), shutdown)
            if hook is not None:
                hook.stop()
            if access_log is not None:
                access_log.close()
            shutdown.phase = "closing"
            if not shutdown.abandoned:
                lifespan.shutdown()
            _finalize_loop(loop, config.grace - (time.monotonic() - started))
        finally:
            if lifeline_fd is not None:
                try:
                    loop.remove_reader(lifeline_fd)
                except (OSError, ValueError, RuntimeError):
                    pass
            _restore_stop_handlers(loop, installed_signals, prior_handlers)
            # serve() is an ordinary callable: it returns, and the process
            # carries on. Leaving the collector as we reconfigured it made
            # every object alive at startup permanently uncollectable for
            # the rest of that process -- including the caller's, and
            # including cycles created before serve() and dropped after it.
            if gc_froze:
                gc.unfreeze()
            if gc_was_enabled and not gc.isenabled():
                gc.enable()
            elif not gc_was_enabled and gc.isenabled():
                gc.disable()
            loop.close()
            asyncio.set_event_loop(prev_loop)
            shutdown.phase = "closed"


def _gc_after_startup(config: Config, *, inherited: bool) -> bool:
    """R-075: tune the collector once startup is done. True when this froze
    the heap, which the caller must unfreeze on the way out.

    ``inherited``: a pool worker whose supervisor froze the heap before
    forking (see _freeze_for_fork). Those objects are the supervisor's, so
    they are no reason to skip freezing the worker's own -- and with them
    already in the permanent generation, the collection here only walks
    what the worker allocated itself.
    """
    if config.gc_mode == "freeze":
        gc.collect()
        # gc.unfreeze() is all-or-nothing, so it cannot separate our
        # startup heap from a permanent generation the caller built
        # themselves. Freezing anyway and skipping the unfreeze -- which
        # is what this did when the restore was first added -- just moves
        # the leak: our objects join theirs and stay there. With no way to
        # undo it, the honest choice is not to do it: skip the optimisation
        # and leave the caller's policy alone.
        if inherited or gc.get_freeze_count() == 0:
            gc.freeze()
            return True
        logger.info(
            "gc_mode='freeze' skipped: %d objects were already frozen by the "
            "caller, and gc.unfreeze() could not later separate ours from theirs",
            gc.get_freeze_count(),
        )
    elif config.gc_mode == "disable":
        gc.collect()
        gc.disable()
    return False


def _freeze_warm_heap() -> None:
    """R-075 ``warmup``: freeze again once the first requests have built
    what applications build lazily -- route and template caches, compiled
    statements, validators. Frozen only at startup, those stayed in the
    oldest generation and every full collection walked them again."""
    gc.collect()
    gc.freeze()


def _drain_budget(grace):
    """Portion of the grace budget the connection drain may spend. The
    rest is reserved for the application's lifespan.shutdown hook, which
    runs after the drain but before the supervisor's SIGKILL deadline: a
    quarter of the budget, capped at 5 seconds -- enough for typical
    pool/client teardown without starving the drain."""
    return max(0.0, grace - min(5.0, grace * 0.25))


def _drain_connections(loop, grace, shutdown=None):
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
    if grace <= 0 or loop.is_closed() or (shutdown is not None and shutdown.hurry):
        return
    busy = loop._core.http_begin_shutdown()
    if not busy:
        return
    logger.info("cadeloop: draining %d connection(s), grace=%ss", busy, grace)
    deadline = loop.time() + grace

    def poll():
        if (
            loop._core.http_connection_count() == 0
            or loop.time() >= deadline
            or (shutdown is not None and shutdown.hurry)
        ):
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


def _finalize_loop(loop, budget: float) -> None:
    """What asyncio.run() does before closing its loop, bounded by what is
    left of the grace budget: cancel the tasks the application left
    running and let their cleanup run, finalize async generators, join the
    default executor. Going straight to close() meant a background task's
    ``finally`` (a metrics flusher, a consumer committing its offsets) and
    an async generator's cleanup never ran at all.
    """
    if loop.is_closed():
        return
    budget = max(budget, 0.25)  # even at grace=0, one pass to run cleanup that does not await
    try:
        tasks = [t for t in asyncio.all_tasks(loop) if not t.done()]
        # The eager engine runs a request without an asyncio Task (R-056),
        # so all_tasks() never lists it: a handler still awaiting here was
        # never cancelled, its cleanup never ran, and close() then dropped
        # it mid-await. Its driver is cancelled with the tasks.
        drivers = loop._core.http_request_drivers()
        if tasks or drivers:
            for task in tasks:
                task.cancel()
            for driver in drivers:
                driver.cancel()
            finished = [_when_finished(loop, driver) for driver in drivers]
            loop.run_until_complete(asyncio.wait(tasks + finished, timeout=budget))
            for task in tasks:
                if task.done() and not task.cancelled() and task.exception() is not None:
                    loop.call_exception_handler(
                        {
                            "message": "unhandled exception during shutdown",
                            "exception": task.exception(),
                            "task": task,
                        }
                    )
            pending = sum(not t.done() for t in tasks) + sum(not d.done() for d in drivers)
            if pending:
                logger.warning("cadeloop: %d task(s) still running after cancellation", pending)
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.run_until_complete(asyncio.wait_for(loop.shutdown_default_executor(), budget))
    except (RuntimeError, TimeoutError, asyncio.TimeoutError) as exc:
        # A forced exit stopped the loop under us, or the executor would
        # not finish in time: close anyway.
        logger.debug("cadeloop: loop finalization cut short: %r", exc)


def _when_finished(loop, driver):
    """A future resolved when an eager request driver finishes: the driver
    is not an asyncio Future, so asyncio.wait() cannot take it as is."""
    fut = loop.create_future()

    def finished(_driver):
        if not fut.done():
            fut.set_result(None)

    driver.add_done_callback(finished)
    return fut


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

    # max_body=0: the engine buffers a whole request body before the app
    # reaches receive(), and this listener inherited http_listen()'s
    # max_body=None ("no cap") rather than the server's own limit -- so
    # anything able to reach loopback could stream an arbitrarily large
    # POST and exhaust the serving worker. A read-only endpoint needs no
    # body at all.
    lid, bound, _fd = loop._core.http_listen("127.0.0.1", port, stats_app, loop, max_body=0)
    logger.info("cadeloop stats endpoint on http://127.0.0.1:%s", bound[1])
    return lid, bound[1]


class _RequestHook:
    """The engine's one per-request callback (``set_access_log``), shared by
    the access log and the R-075 warmup counter.

    It is installed only while someone needs it: with no sink the engine
    records nothing per request, and a sink that is called only to discard
    its record -- an access log whose logger is not enabled for INFO, the
    README's own default -- cost 18% of plain-text throughput. Whether the
    logger is enabled is re-read once a second, so a level changed at
    runtime takes effect without a restart.
    """

    _REFRESH_SECS = 1.0

    def __init__(self, loop):
        self._loop = loop
        self._access_log = None
        self._log_sink = None  # the access log's sink while its logger is enabled
        self._warmup_left = 0
        self._on_warm = None
        self._installed = None
        self._timer = None
        # Bound once: `self._count is self._count` is False for a fresh
        # bound method, and the install check compares identities.
        self._count_cb = self._count
        self._both_cb = self._both

    def attach_access_log(self, access_log) -> None:
        self._access_log = access_log

    def count_requests(self, n: int, on_warm) -> None:
        self._warmup_left = n
        self._on_warm = on_warm

    def start(self) -> None:
        self._refresh()

    def stop(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        self._install(None)

    def _refresh(self) -> None:
        log = self._access_log
        self._log_sink = log.sink if log is not None and log.enabled() else None
        self._update()
        if log is not None and not self._loop.is_closed():
            self._timer = self._loop.call_later(self._REFRESH_SECS, self._refresh)

    def _update(self) -> None:
        if self._warmup_left > 0:
            want = self._both_cb if self._log_sink is not None else self._count_cb
        else:
            want = self._log_sink
        self._install(want)

    def _install(self, sink) -> None:
        if sink is not self._installed and not self._loop.is_closed():
            self._loop._core.set_access_log(sink)
            self._installed = sink

    def _count(self, *_record) -> None:
        self._warmup_left -= 1
        if self._warmup_left == 0:
            # Outside the engine's callback: uninstalling a sink from
            # inside its own call is not something to rely on.
            self._loop.call_soon(self._update)
            self._loop.call_soon(self._on_warm)

    def _both(self, *record) -> None:
        self._log_sink(*record)
        self._count()


class _AccessLog:
    """R-140 access log, emitted off the loop thread.

    The engine calls the sink inline, on the loop thread, once per
    completed request. A logging handler is free to block -- a file
    handler on a slow disk, a stream into a full pipe, anything
    network-backed -- and one such call stalls every connection the
    worker is holding and adds its own latency to the request it is
    logging. Records go to a bounded buffer instead and a daemon thread
    does the emitting.

    The per-request cost is two C calls: no lock, no Condition.notify.
    The queue.Queue this replaced woke the writer thread once per record
    and the two then traded the GIL record by record (35k req/s logging to
    a file vs 62k here, single worker); the writer now wakes on a timer
    and emits whatever has accumulated in one batch.

    The buffer is bounded on purpose: a writer that cannot keep up should
    cost bounded memory, not unbounded memory and not the request path.
    Records past the bound are dropped and counted, and the count is
    reported once the writer catches up -- silently losing them would
    make the log quietly wrong, which is worse than a gap you can see.
    """

    __slots__ = (
        "_logger", "_records", "_maxsize", "_dropped", "_reported", "_wake", "_closing", "_thread",
    )

    _POLL_SECS = 0.05

    def __init__(self, logger, maxsize: int = 10_000):
        self._logger = logger
        self._records = collections.deque()
        self._maxsize = maxsize
        # Counted on the loop thread only and read by the writer, which
        # keeps its own tally of what it has reported: no shared counter
        # needs a lock.
        self._dropped = 0
        self._reported = 0
        self._wake = threading.Event()
        self._closing = False
        self._thread = threading.Thread(
            target=self._run, name="cadeloop-access-log", daemon=True
        )
        self._thread.start()

    def enabled(self) -> bool:
        return self._logger.isEnabledFor(logging.INFO)

    def sink(self, peer, method, target, status, dur_ms):
        """Called from the engine, on the loop thread. Must not block."""
        records = self._records
        if len(records) >= self._maxsize:
            self._dropped += 1
        else:
            records.append((peer, method, target, status, dur_ms))

    def _run(self):
        records = self._records
        while True:
            if not records:
                if self._closing:
                    break
                self._wake.wait(self._POLL_SECS)
                continue
            self._report_drops()
            # Bounded by maxsize: what was there when the batch started,
            # plus at most what the loop adds meanwhile.
            for _ in range(len(records)):
                try:
                    self._emit(records.popleft())
                except IndexError:
                    break
        self._report_drops()

    def _report_drops(self):
        dropped = self._dropped - self._reported
        if dropped:
            self._reported += dropped
            self._logger.warning("access log fell behind: %d record(s) dropped", dropped)

    def _emit(self, record):
        peer, method, target, status, dur_ms = record
        if peer:
            host = peer[0]
            client = f"[{host}]:{peer[1]}" if ":" in str(host) else f"{host}:{peer[1]}"
        else:
            client = "-"
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
        self._closing = True
        self._wake.set()
        self._thread.join(timeout)
        if self._thread.is_alive():
            # The writer is stuck in a handler: whatever it has not taken
            # is lost, and counted as such rather than silently.
            self._dropped += len(self._records)
            self._records.clear()
        # Anything counted after the writer's last report would otherwise
        # go unsaid.
        self._report_drops()


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


def _channel_pump(chan, loop, adopt, shutdown):
    """Worker side of the spawn channel: adopt handed-over connections
    until STOP or EOF (a dead supervisor), then ask the worker to stop.

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

    The stop goes through the worker's shutdown controller, not straight
    to loop.stop(): EOF arriving while the worker is already draining (it
    got its own signal, then the supervisor died) used to stop the drain
    loop early and cut every in-flight request.
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
        loop.call_soon_threadsafe(shutdown.request, "supervisor")
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
# The stop signals a fork keeps blocked from just before fork() until the
# child has its own handlers (and the parent has recorded the child).
_FORK_BLOCKED_SIGNALS = frozenset({_signal.SIGINT, _signal.SIGTERM})
# A spawn-model worker that stopped because something signalled it alone
# exits with this (EX_TEMPFAIL), not 0: a clean exit stops the whole pool.
_EXIT_RESTART = 75


def _crash_streak_next(streak: int, *, became_ready, uptime: float) -> int:
    """The restart-limit rule, in ONE place for both supervisors, applied
    per worker slot.

    It lived twice, and the two copies were not the same: the fork
    supervisor was taught to use readiness while the spawn supervisor kept
    asking only "did this die within a second of starting". A worker that
    fails slowly -- which is the ordinary shape of a startup failure --
    therefore reset the streak on every death there and was restarted
    forever, with no worker ever serving.

    `became_ready` is True/False where a supervisor has a readiness
    signal, and None where it does not; without one, staying up for
    `_STABLE_SECS` is the stand-in. Never serving always advances the
    streak, however long the failure took. A worker that did serve only
    counts when it died at once: one that served for seconds and then
    crashed is a crash to restart, not a crash LOOP -- with a single
    counter for the whole pool and a 30 s bar even for workers that had
    served, five one-off crashes on five healthy workers (a request that
    segfaults a C extension, retried by its client) shut the server down.
    """
    if became_ready is False:
        return streak + 1
    stable = _CRASH_FAST_SECS if became_ready else _STABLE_SECS
    if uptime >= stable:
        return 1
    return streak + 1


def _describe_status(status) -> str:
    if status is None:
        return "status unknown: reaped by another waiter"
    code = os.waitstatus_to_exitcode(status)
    if code < 0:
        try:
            return f"killed by {_signal.Signals(-code).name}"
        except ValueError:
            return f"killed by signal {-code}"
    return f"exit code {code}"


@contextlib.contextmanager
def _signals_blocked(sigs):
    if not hasattr(_signal, "pthread_sigmask"):
        yield
        return
    old = _signal.pthread_sigmask(_signal.SIG_BLOCK, sigs)
    try:
        yield
    finally:
        _signal.pthread_sigmask(_signal.SIG_SETMASK, old)


def _exit_worker(status: int):
    """End a forked worker -- os._exit, since returning would unwind into
    the supervisor's own frames -- after doing what multiprocessing does
    for its fork children: flush the standard streams. Plus
    logging.shutdown(), so buffered handlers flush too. Without it a
    worker's print() output to a pipe (systemd, docker, supervisord:
    block-buffered unless PYTHONUNBUFFERED) was discarded -- none of the
    lines a 2-worker pool printed reached its log.
    """
    try:
        logging.shutdown()
    except BaseException:  # noqa: BLE001 — nothing may stop the exit
        pass
    for stream in (sys.stdout, sys.stderr):
        try:
            if stream is not None:
                stream.flush()
        except BaseException:  # noqa: BLE001
            pass
    os._exit(status)


def _pin_loop_thread(cpu: int, cpus, idx=None) -> None:
    """R-091: pin the calling thread -- the worker's loop thread -- to
    ``cpu``, but not the threads it starts later.

    sched_setaffinity(0) pins the calling thread, and every thread it
    starts afterwards inherits the mask: the default executor, anyio's
    threadpool behind sync endpoints, the access-log writer. Pinned with
    the loop, GIL-releasing work in them (hashing, compression, image or
    numpy work) could never run beside it: a 2-worker pool did 13.9 req/s
    of executor work pinned against 22.2 unpinned (-37%). The loop thread
    keeps the cache locality pinning is for; a new thread widens its own
    mask back to ``cpus`` before it runs anything (threading's profile
    hook runs in each new thread ahead of run(), then hands back whatever
    profiler was set).
    """
    if not hasattr(os, "sched_setaffinity"):
        return
    try:
        os.sched_setaffinity(0, {cpu})
    except OSError as exc:
        logger.warning("worker %s could not be pinned to CPU %d: %s", idx, cpu, exc)
        return
    widen = set(cpus)
    previous = threading.getprofile()

    def _unpin_new_thread(_frame, _event, _arg):
        sys.setprofile(previous)
        try:
            os.sched_setaffinity(0, widen)
        except OSError:
            pass

    threading.setprofile(_unpin_new_thread)


def _spawn_worker(app, host, port, config: Config, idx: int, cpus, ssl_ctx=None, inherited=(),
                  *, lifeline=None, gc_inherited=False):
    """Fork one worker. Returns (pid, ready_fd).

    `ready_fd` is the read end of a pipe the child writes b"R" to once it
    is actually serving. The supervisor needs that distinction: its
    crash-loop guard used to ask only "did this die within
    _CRASH_FAST_SECS of being forked", so an application that fails
    SLOWLY -- a database connection that times out after five seconds is
    the ordinary case -- reset the streak on every death and was restarted
    forever. "Died without ever serving" is the signal that matters, and
    it does not depend on how long the failure took. The child adds b"S"
    when it stopped because the APPLICATION stopped its own loop: the one
    clean exit that asks for the whole pool to stop, as opposed to a worker
    that was signalled on its own (recycling one bloated worker with a
    SIGTERM is standard practice, and used to stop the entire server).

    Call with the stop signals blocked (``_signals_blocked``): the child
    resets the supervisor's handlers and installs its own before they are
    unblocked, so a signal can never run the supervisor's _forward inside
    a worker -- which would SIGTERM every sibling -- and one that arrives
    early is held for the worker's own handler instead of killing it.
    """
    ready_r, ready_w = os.pipe()
    # What the supervisor has buffered but not yet written -- an import-time
    # print() of the application, with stdout a pipe -- would otherwise be
    # copied into the child and written again by _exit_worker's flush: once
    # per worker, and once more per replacement. multiprocessing flushes
    # before forking for the same reason.
    for stream in (sys.stdout, sys.stderr):
        try:
            if stream is not None:
                stream.flush()
        except (OSError, ValueError):
            pass
    try:
        pid = os.fork()
    except BaseException:
        os.close(ready_r)
        os.close(ready_w)
        raise
    if pid != 0:
        os.close(ready_w)
        os.set_blocking(ready_r, False)
        return pid, ready_r
    # ---- child: nothing here may unwind into the supervisor's frames ----
    status = 1
    try:
        os.close(ready_r)
        # fork (no exec) ignores close-on-exec, so this child also inherited
        # a read end for every worker already running, and the lifeline's
        # write end: holding that one would keep its own lifeline open.
        for fd in inherited:
            try:
                os.close(fd)
            except OSError:
                pass
        # A worker owns its own signal handling (installed by
        # _serve_single via the loop); drop the supervisor's handlers.
        _signal.signal(_signal.SIGTERM, _signal.SIG_DFL)
        _signal.signal(_signal.SIGINT, _signal.SIG_IGN)  # supervisor forwards TERM
        if config.pin and cpus:
            # R-091: pin each worker to one CPU (accept balancing is the
            # kernel's job via SO_REUSEPORT) -- one the process may use.
            _pin_loop_thread(cpus[idx % len(cpus)], cpus, idx)
        reason = _serve_single(
            app,
            host,
            port,
            config,
            reuse_port=True,
            worker_id=idx,
            ssl_ctx=ssl_ctx,
            ready_fd=ready_w,
            lifeline_fd=lifeline,
            unblock_signals=_FORK_BLOCKED_SIGNALS,
            gc_inherited=gc_inherited,
        )
        if reason == "app":
            try:
                os.write(ready_w, b"S")
            except OSError:
                pass
        status = 0
    except BaseException:  # noqa: BLE001 — worker death is the supervisor's signal
        logger.exception("worker %d crashed", idx)
    finally:
        _exit_worker(status)
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


def _read_worker_report(ready_fd) -> tuple[bool, bool]:
    """(became_ready, app_stopped) for a dead worker.

    Non-blocking: the bytes are either already in the pipe or the worker
    died before writing them. Closes the descriptor either way.
    """
    try:
        data = os.read(ready_fd, 16)
    except OSError:  # BlockingIOError included: never wrote anything
        data = b""
    finally:
        try:
            os.close(ready_fd)
        except OSError:
            pass
    return b"R" in data, b"S" in data


def _reap(children):
    """(pid, status) for one of OUR exited children, or (0, 0).

    waitpid(-1) reaped any child of the process -- including ones the
    embedding application started itself, whose exit statuses it then lost
    (Popen.wait() reports 0 for a child already gone; a job that exited 7
    read as a success). A status of None: something else reaped it.
    """
    for pid in list(children):
        try:
            rpid, status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return pid, None
        if rpid:
            return rpid, status
    return 0, 0


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


def _probe_port(host, port) -> None:
    """Refuse to start a pool on a port that something already holds.

    Every worker binds with SO_REUSEPORT, which Linux grants to any socket
    of the same user that sets it too -- so a second pool started on a
    live port (a deploy overlapping the old instance, two services sharing
    a port, the workers of a supervisor that was killed) bound without
    error and the kernel split connections between the two: 36 / 24 of 60
    requests, measured. A bind WITHOUT SO_REUSEPORT fails while any
    listener holds the port, so a throwaway one here turns that into
    EADDRINUSE before anything is forked -- and before every worker ran the
    application's lifespan startup only to find the port taken.
    """
    for ip, flowinfo, scope_id in _bind_candidates(host, port):
        family = _socket.AF_INET6 if ":" in ip else _socket.AF_INET
        try:
            probe = _socket.socket(family, _socket.SOCK_STREAM)
        except OSError as exc:
            if _os_errno(exc) in _UNUSABLE_ADDRESS_ERRNOS:
                continue
            raise
        with probe:
            if family == _socket.AF_INET6:
                probe.setsockopt(_socket.IPPROTO_IPV6, _socket.IPV6_V6ONLY, 1)
            # As the native listener does: a port held only by connections
            # in TIME_WAIT is free to take.
            probe.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
            addr = (ip, port, flowinfo, scope_id) if family == _socket.AF_INET6 else (ip, port)
            try:
                probe.bind(addr)
            except OSError as exc:
                code = _os_errno(exc)
                if code in _UNUSABLE_ADDRESS_ERRNOS:
                    continue
                if code in _ADDRINUSE_ERRNOS:
                    raise OSError(
                        exc.errno,
                        f"{ip}:{port} is already in use -- by another server, or by the "
                        "workers of one whose supervisor died",
                    ) from exc
                raise


def _freeze_for_fork(config: Config) -> bool:
    """Freeze the supervisor's heap right before forking (R-075), as
    CPython's gc.freeze() documentation prescribes for fork without exec.
    True when this froze it; the caller unfreezes after the pool ends.

    Each worker used to run its own full gc.collect() after forking, which
    writes the GC header of every object it inherited and so copies every
    page those objects live on: with ~190 MB of import-time objects, each
    worker of a 4-worker pool held 193 MB of private memory (6 MB with
    gc_mode='default'). Frozen here, the inherited objects sit in the
    permanent generation, which no collection in a worker touches.
    """
    if config.gc_mode not in ("freeze", "disable") or gc.get_freeze_count():
        return False  # 'default' opts out; an existing freeze is the caller's
    gc.collect()
    gc.freeze()
    return True


def _serve_multi(app, host, port, config: Config, n: int, ssl_ctx=None):
    """Supervisor: fork N workers each binding with SO_REUSEPORT (the
    kernel load-balances accepts), restart crashed ones (R-092), forward
    SIGTERM/SIGINT, and drain within ``config.grace`` seconds."""
    if port == 0:
        raise ValueError(
            "workers > 1 requires an explicit port (each worker binds it "
            "with SO_REUSEPORT; port 0 would scatter workers across ports)"
        )
    if threading.current_thread() is not threading.main_thread():
        # The supervisor's job is signal handling, which CPython allows on
        # the main thread only. It used to fork the whole pool first and
        # then call signal.signal(), whose ValueError escaped with every
        # worker already running, serving and unsupervised.
        raise RuntimeError(
            "serve() with workers > 1 must run on the main thread: the pool's "
            "supervisor handles the stop signals"
        )
    _probe_port(host, port)
    cpus = _allowed_cpus()
    logger.info("cadeloop supervisor: %d workers on http://%s:%s", n, host, port)
    # pid -> (idx, spawn_time, ready_fd)
    children: dict[int, tuple[int, float, int]] = {}
    streaks: dict[int, int] = {}
    stopping = False
    force = False
    exit_code = 0

    def _forward(signum, _frame):
        nonlocal stopping, force
        if stopping and signum == _signal.SIGINT:
            force = True  # a second Ctrl-C: stop waiting for the drain
        stopping = True
        for pid in list(children):
            try:
                os.kill(pid, _signal.SIGTERM)
            except ProcessLookupError:
                pass

    lifeline_r = lifeline_w = None
    handlers_installed = False
    old_term = old_int = old_chld = None
    gc_frozen = False

    def spawn(idx):
        # Blocked across fork() AND the bookkeeping: a stop signal handled
        # between the two ran _forward over a `children` that did not have
        # the new worker yet, which was then never told to stop and was
        # SIGKILLed `grace` later without its lifespan shutdown.
        with _signals_blocked(_FORK_BLOCKED_SIGNALS):
            pid, ready_fd = _spawn_worker(
                app,
                host,
                port,
                config,
                idx,
                cpus,
                ssl_ctx=ssl_ctx,
                inherited=[e[2] for e in children.values()] + [lifeline_w],
                lifeline=lifeline_r,
                gc_inherited=gc_frozen,
            )
            children[pid] = (idx, time.monotonic(), ready_fd)

    try:
        # The workers' lifeline: only this process holds the write end, so
        # its death, however it happens, is EOF in every worker.
        lifeline_r, lifeline_w = os.pipe()
        os.set_blocking(lifeline_r, False)
        # Installed before the first fork. It used to come after the whole
        # pool was forked, so a SIGTERM during startup (a service stopped
        # right after it started) found SIGTERM still at SIG_DFL and killed
        # this process outright, leaving the workers forked so far serving
        # the port with no supervisor.
        old_term = _signal.signal(_signal.SIGTERM, _forward)
        old_int = _signal.signal(_signal.SIGINT, _forward)
        handlers_installed = True
        # An embedder that set SIGCHLD to SIG_IGN has the kernel reap
        # children by itself: no death would ever be seen, and when the
        # last worker died waitpid's ECHILD ended serve() as though it had
        # been asked to stop.
        if hasattr(_signal, "SIGCHLD") and _signal.getsignal(_signal.SIGCHLD) == _signal.SIG_IGN:
            old_chld = _signal.signal(_signal.SIGCHLD, _signal.SIG_DFL)
        gc_frozen = _freeze_for_fork(config)
        for idx in range(n):
            if stopping:
                break
            spawn(idx)
        while children and not stopping:
            # WNOHANG, not a blocking wait. A blocking waitpid() is
            # restarted by CPython after a signal handler that does not
            # raise -- and _forward does not raise -- so the `stopping`
            # check above was unreachable until some child happened to
            # exit, and a worker wedged in synchronous application code
            # never exits.
            pid, status = _reap(children)
            if pid == 0:
                # Nothing exited. Sleep briefly rather than spinning; the
                # signal handler runs between iterations either way.
                time.sleep(0.05)
                continue
            idx, spawned, ready_fd = children.pop(pid)
            # Consumes and closes ready_fd, so every reaped child is
            # accounted for on this path.
            became_ready, app_stopped = _read_worker_report(ready_fd)
            if stopping:
                continue
            if status is not None and os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0:
                if app_stopped:
                    # The application stopped its own loop: a request to
                    # stop serving, exactly as with a single process.
                    _forward(None, None)
                    continue
                logger.info("worker %d stopped after a signal sent to it alone — restarting", idx)
                spawn(idx)
                continue
            streaks[idx] = _crash_streak_next(
                streaks.get(idx, 0),
                became_ready=became_ready,
                uptime=time.monotonic() - spawned,
            )
            if streaks[idx] >= _CRASH_STREAK_LIMIT:
                logger.error(
                    "worker %d died %d times in a row %s — giving up",
                    idx,
                    streaks[idx],
                    "without ever serving" if not became_ready else "right after starting",
                )
                exit_code = 1
                _forward(None, None)
                continue
            logger.warning("worker %d died (%s) — restarting", idx, _describe_status(status))
            spawn(idx)
        # Drain: give survivors `grace` seconds, then SIGKILL (R-092). Told
        # once more first: harmless to a worker already stopping, and it
        # covers one forked after the signal that started the shutdown.
        for pid in list(children):
            try:
                os.kill(pid, _signal.SIGTERM)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + config.grace
        while children and time.monotonic() < deadline and not force:
            pid, _status = _reap(children)
            if pid == 0:
                time.sleep(0.05)
                continue
            _close_ready_fd(children.pop(pid, None))
        for pid in list(children):
            logger.warning("worker pid %d exceeded grace=%ss — SIGKILL", pid, config.grace)
            try:
                os.kill(pid, _signal.SIGKILL)
                os.waitpid(pid, 0)
            except (ProcessLookupError, ChildProcessError):
                pass
            _close_ready_fd(children.pop(pid, None))
    except BaseException:
        # A fork failing partway (a process limit, say), at startup or as a
        # replacement, raised with workers already running: same cleanup
        # either way, or they keep serving with no supervisor.
        _kill_children(children)
        raise
    finally:
        for entry in children.values():
            _close_ready_fd(entry)
        for fd in (lifeline_r, lifeline_w):
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
        if handlers_installed:
            # (None: a handler installed from C, which Python cannot put back.)
            _signal.signal(_signal.SIGTERM, _signal.SIG_DFL if old_term is None else old_term)
            _signal.signal(_signal.SIGINT, _signal.SIG_DFL if old_int is None else old_int)
        if old_chld is not None:
            _signal.signal(_signal.SIGCHLD, old_chld)
        if gc_frozen:
            gc.unfreeze()
    if exit_code:
        raise SystemExit(exit_code)


# --------------------------------------------------------------------- #
# spawn worker model — fork-free platforms (Windows), R-090..R-093       #
# --------------------------------------------------------------------- #

# Connections the supervisor queues for one worker beyond what its channel
# holds. When every worker's queue is full the accept thread waits for room
# -- backpressure into the kernel's accept backlog, as a blocking handoff
# gave -- but a little at a time per worker, rotating, so a stalled worker
# can delay a connection and never wedge accepting. Past the wait the
# connection is dropped.
_HANDOFF_QUEUE = 256
_HANDOFF_WAIT = 5.0
# accept() failures that mean "out of a resource", where retrying at once
# only spins: asyncio backs off for ACCEPT_RETRY_DELAY. POSIX values plus
# WSAEMFILE/WSAENOBUFS.
_ACCEPT_RESOURCE_ERRNOS = frozenset(
    {errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM, 10024, 10055}
)
_ACCEPT_RETRY_DELAY = 1.0
# ...and the ones that mean the listener itself is unusable (WSAEBADF,
# WSAEINVAL, WSAENOTSOCK).
_ACCEPT_FATAL_ERRNOS = frozenset(
    {errno.EBADF, errno.EINVAL, errno.ENOTSOCK, 10009, 10022, 10038}
)


class _SpawnWorker:
    """Supervisor's handle on one spawned worker: the process, our end of
    its control/handoff channel, and the thread that writes to it.

    Every write to the channel happens on that thread. They used to happen
    on the supervisor's single accept thread, as blocking writes: a worker
    that stopped reading (stalled, stopped, its pump starved) filled its
    channel and then blocked the accept loop for EVERY worker -- after
    ~280 handoffs to one SIGSTOPped worker of two, the healthy one stopped
    getting connections -- and the STOP frame sent on shutdown blocked on
    the same channel, so grace was never enforced. Now a worker whose
    queue is full is simply skipped.
    """

    __slots__ = ("proc", "sock", "stdin", "idx", "spawned", "broken", "_outq", "_writer",
                 "_stop_requested", "_closing")

    def __init__(self, proc, sock, stdin, idx, spawned):
        self.proc = proc
        self.sock = sock
        self.stdin = stdin
        self.idx = idx
        self.spawned = spawned
        self.broken = False
        self._outq: queue.Queue = queue.Queue(_HANDOFF_QUEUE)
        self._writer = None
        self._stop_requested = False
        self._closing = False

    def alive(self) -> bool:
        return self.proc.poll() is None

    def start_writer(self):
        self._writer = threading.Thread(
            target=self._write_loop, name=f"cadeloop-handoff-{self.idx}", daemon=True
        )
        self._writer.start()

    def offer(self, conn, timeout=None) -> bool:
        """Queue one ACCEPTED connection for this worker (R-090): at once,
        or within ``timeout`` seconds. False when its queue stayed full or
        the worker is gone; the caller still owns ``conn`` then. Once
        queued, it belongs to the writer, which sends it and then closes
        this process's copy.

        The socket has never been associated with a completion port — the
        supervisor's listener is deliberately never registered with one —
        so the receiving worker may legally associate it with its own
        (ADR-25).
        """
        if self.broken or self._stop_requested:
            return False
        try:
            if timeout is None:
                self._outq.put_nowait(conn)
            else:
                self._outq.put(conn, timeout=timeout)
        except queue.Full:
            return False
        return True

    def request_stop(self):
        """Ask for STOP after whatever is already queued -- never blocking:
        the writer delivers it, behind the backlog it belongs after."""
        self._stop_requested = True
        try:
            self._outq.put_nowait(None)
        except queue.Full:
            pass  # the writer is busy; it sends STOP once the queue drains

    def _write_loop(self):
        q = self._outq
        try:
            while True:
                conn = q.get()
                if self._closing:
                    return
                if conn is not None:
                    try:
                        if _WIN:
                            # WSADuplicateSocketW for the worker; the blob is
                            # independent of our handle once made.
                            _send_frame(self, b"CONN " + conn.share(self.proc.pid))
                        else:
                            _send_frame(self, b"CONN", conn.fileno())
                    finally:
                        conn.close()
                if self._stop_requested and q.empty():
                    _send_frame(self, b"STOP")
                    return
        except (OSError, ValueError):
            # The worker is gone (or its channel is): nothing queued for it
            # can be delivered any more.
            self.broken = True
        finally:
            self._drop_queued()

    def _drop_queued(self):
        while True:
            try:
                conn = self._outq.get_nowait()
            except queue.Empty:
                return
            if conn is not None:
                conn.close()

    def close(self):
        # Wake an idle writer so it exits; one still inside a send to a
        # process that just died fails out of it promptly. Give it that
        # moment before closing the channel under it (on Windows close()
        # would block on the writer's buffer lock until it does).
        self._closing = True
        try:
            self._outq.put_nowait(None)
        except queue.Full:
            pass
        if self._writer is not None:
            self._writer.join(1.0)
        if self._writer is None or not self._writer.is_alive():
            for closer in (self.sock, self.stdin):
                try:
                    if closer is not None:
                        closer.close()
                except OSError:
                    pass
        self._drop_queued()


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


def _spawn_shared_worker(spec, config: Config, idx: int, cpus) -> _SpawnWorker:
    """Spawn one worker and hand it its startup header. Unlike the model
    this replaced, no listener crosses the boundary — only connections do."""
    if _WIN:
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
        "pin": cpus[idx % len(cpus)] if config.pin and cpus else None,
        # A worker starts as `python -m cadeloop._winworker`, with only the
        # inherited working directory and PYTHONPATH on its path: an app
        # the supervisor imports from its launcher script's directory (a
        # Windows service runs in System32) failed to import in every
        # worker. multiprocessing's spawn passes sys.path the same way.
        "sys_path": list(sys.path),
    }
    try:
        _send_frame(worker, b"HELLO " + json.dumps(header).encode())
    except (OSError, ValueError):
        proc.kill()
        worker.close()
        raise
    worker.start_writer()
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
    if spec.partition(":")[0] == "__main__":
        _check_app_spec(spec)  # raises: a spawned worker has a different __main__
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
        if hasattr(_socket, "SO_EXCLUSIVEADDRUSE"):
            # Windows lets a later socket that sets SO_REUSEADDR bind over
            # this one and take its connections; the native listener was
            # already made exclusive, this one -- bound here in Python --
            # was not.
            lsock.setsockopt(_socket.SOL_SOCKET, _socket.SO_EXCLUSIVEADDRUSE, 1)
        lsock.bind(sockaddr)
        lsock.listen(1024)
    except OSError:
        lsock.close()
        raise
    cpus = _allowed_cpus()
    logger.info("cadeloop supervisor: %d workers on http://%s:%s (accept+handoff)", n, host, port)

    # `workers` is read by the accept thread and mutated by the
    # supervision loop, so every access goes through this lock. Without it
    # the accept thread could evaluate len(workers), lose the race to a
    # removal, and index out of range — killing the only thread that
    # distributes connections.
    workers_lock = threading.Lock()
    workers = []
    stopping = False
    stop_event = threading.Event()
    accept_failure: list = []
    exit_code = 0

    def _live_workers():
        with workers_lock:
            return list(workers)

    try:
        for idx in range(n):
            w = _spawn_shared_worker(spec, config, idx, cpus)
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
        stop_event.set()
        try:
            # Wakes a blocked accept() on Linux, where close() alone does
            # not (the descriptor stays referenced by the call in progress
            # and the socket keeps listening).
            lsock.shutdown(_socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            lsock.close()  # unblocks the accept loop on Windows
        except OSError:
            pass
        for w in _live_workers():
            w.request_stop()

    def _accept_loop():
        """Distribute connections round-robin over the live workers."""
        turn = 0
        dropped = 0
        failures = 0
        while not stopping:
            try:
                conn, _addr = lsock.accept()
            except OSError as exc:
                if stopping:
                    return  # listener closed: shutting down
                code = _os_errno(exc)
                if code in _ACCEPT_FATAL_ERRNOS:
                    accept_failure.append(exc)
                    return
                # Anything else is transient: a connection reset before we
                # took it (Windows reports WSAECONNRESET from accept()),
                # ECONNABORTED, EPROTO, descriptors running out. Returning
                # on the first one -- as this did for any OSError -- ended
                # accepting for good while the supervisor and its workers
                # stayed up: a silent, total outage that any client
                # resetting early could trigger.
                failures += 1
                if code in _ACCEPT_RESOURCE_ERRNOS:
                    if failures == 1:
                        logger.warning(
                            "accept() failed (%s); retrying in %ss", exc, _ACCEPT_RETRY_DELAY
                        )
                    stop_event.wait(_ACCEPT_RETRY_DELAY)
                elif failures > 100:
                    stop_event.wait(0.01)  # an error that keeps recurring: do not spin
                continue
            failures = 0
            handed = False
            try:
                # One snapshot per connection: the supervision loop may
                # add or remove workers at any moment.
                live = [w for w in _live_workers() if w.alive()]
                for _ in range(len(live)):
                    w = live[turn % len(live)]
                    turn += 1
                    if w.offer(conn):
                        handed = True
                        break
                if not handed and live:
                    # Every queue is full: a burst, or stalled workers.
                    deadline = time.monotonic() + _HANDOFF_WAIT
                    while not handed and not stopping and time.monotonic() < deadline:
                        w = live[turn % len(live)]
                        turn += 1
                        handed = w.offer(conn, timeout=0.05)
                if not handed:
                    dropped += 1
                    if dropped == 1 or dropped % 100 == 0:
                        logger.warning(
                            "no worker could take a connection (%d dropped so far): "
                            "workers dead or not reading their channels",
                            dropped,
                        )
            finally:
                if not handed:
                    conn.close()  # handed: the worker's writer owns it now

    accepter = threading.Thread(target=_accept_loop, name="cadeloop-accept", daemon=True)
    accepter.start()

    streaks: dict[int, int] = {}
    try:
        while _live_workers():
            if stopping:
                # Same reason as the fork supervisor: once shutdown starts,
                # the bounded drain in `finally` owns the deadline. Spinning
                # here waiting for a wedged worker to exit meant grace was
                # only ever enforced after it already had.
                break
            time.sleep(0.2)
            if not accepter.is_alive() and not stopping:
                exc = accept_failure[0] if accept_failure else None
                logger.error("the accept loop stopped (%r); shutting the pool down", exc)
                exit_code = 1
                _stop_all()
                break
            for w in [w for w in _live_workers() if not w.alive()]:
                with workers_lock:
                    if w in workers:
                        workers.remove(w)
                w.close()
                if stopping:
                    continue
                rc = w.proc.returncode
                if rc == 0:
                    # Clean self-exit means stop was requested inside the
                    # worker — treat as a shutdown signal (fork parity).
                    _stop_all()
                    continue
                if rc == _EXIT_RESTART:
                    logger.info(
                        "worker %d stopped after a signal sent to it alone — restarting", w.idx
                    )
                else:
                    # became_ready=None: this model's control channel runs
                    # supervisor -> worker only on Windows, so there is no
                    # readiness frame to read. `_STABLE_SECS` stands in --
                    # which is the point of sharing the rule rather than
                    # keeping a second copy that quietly says something else.
                    streaks[w.idx] = _crash_streak_next(
                        streaks.get(w.idx, 0),
                        became_ready=None,
                        uptime=time.monotonic() - w.spawned,
                    )
                    if streaks[w.idx] >= _CRASH_STREAK_LIMIT:
                        logger.error(
                            "worker %d died %d times in a row without staying up — giving up",
                            w.idx,
                            streaks[w.idx],
                        )
                        exit_code = 1
                        _stop_all()
                        continue
                    logger.warning("worker %d died (status %s) — restarting", w.idx, rc)
                replacement = _spawn_shared_worker(spec, config, w.idx, cpus)
                with workers_lock:
                    workers.append(replacement)
    except KeyboardInterrupt:
        _stop_all()
    finally:
        if not stopping:
            _stop_all()
        deadline = time.monotonic() + config.grace
        try:
            for w in _live_workers():
                budget = max(0.0, deadline - time.monotonic())
                try:
                    w.proc.wait(timeout=budget)
                except subprocess.TimeoutExpired:
                    logger.warning("worker %d exceeded grace=%ss — killing", w.idx, config.grace)
                    w.proc.kill()
                    w.proc.wait()
        except KeyboardInterrupt:
            # A second Ctrl-C: stop waiting for the drain (uvicorn's force quit).
            for w in _live_workers():
                w.proc.kill()
                w.proc.wait()
        finally:
            for w in _live_workers():
                w.close()
            try:
                lsock.close()
            except OSError:
                pass
    if exit_code:
        raise SystemExit(exit_code)
