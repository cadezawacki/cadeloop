"""L3 facade: the ``asyncio.AbstractEventLoop`` subclass (R-013, R-050).

The scheduling hot paths (call_soon / timers / run_forever / time /
call_soon_threadsafe) are implemented natively in ``cadeloop._core`` and
bound directly onto the instance in ``__init__``, so calls bypass Python-level
wrapper frames entirely. This module supplies the rest of the surface:
futures/tasks integration, executors, DNS (R-055), exception-handler
machinery, and asyncgen shutdown — plus explicit, milestone-annotated
``NotImplementedError``s for the I/O surface that arrives with the Windows
transport milestones (M1/M2/M4; see docs/roadmap.md).
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import os
import reprlib
import socket
import subprocess
import sys
import threading
import warnings
import weakref
from asyncio import events, futures, tasks

from . import _core
from .tcp import TcpSurface, _DatagramTransport

__all__ = ["Loop"]

logger = __import__("logging").getLogger("cadeloop")

_MILESTONES = {
    "tcp": "TCP transports arrive with the IOCP backend in milestone M1",
    "http": "the native HTTP/ASGI engine arrives in milestone M2",
    "udp": "UDP endpoints arrive in milestone M4 (R-058)",
    "tls": "the native TLS engine arrives in milestone M4 (R-059)",
    "readiness": "add_reader/add_writer readiness emulation arrives in "
    "milestone M1 and is hardened in M4 (R-057)",
    "sendfile": "loop.sendfile (TransmitFile, R-036) arrives in milestone M1",
}


def _not_yet(feature: str, key: str):
    raise NotImplementedError(
        f"cadeloop: {feature} is not implemented yet — {_MILESTONES[key]}. "
        "See docs/roadmap.md for the milestone plan."
    )


def _run_until_complete_cb(fut):
    if not fut.cancelled():
        exc = fut.exception()
        if isinstance(exc, (SystemExit, KeyboardInterrupt)):
            # Leave it to run_forever to propagate.
            return
    futures._get_loop(fut).stop()


class Loop(TcpSurface, asyncio.AbstractEventLoop):
    """A cadeloop event loop (portable dev backend off-Windows; IOCP/RIO on
    Windows). Drop-in compatible with ``asyncio.run()`` and stdlib tasks."""

    def __init__(
        self,
        *,
        backend: str | None = None,
        spin_us: int = 20,
        high_water: int = 64 * 1024,
        low_water: int = 16 * 1024,
        accept_pool: int = 64,
        rio_cq_size: int = 65536,
        rio_rq_recv: int = 32,
        rio_rq_send: int = 32,
        dns_cache: bool = False,
        dns_cache_ttl: float = 5.0,
    ):
        # Backend resolution: explicit arg > CADELOOP_BACKEND env (lets the
        # whole test/bench suite run against a chosen backend, e.g. "rio"
        # on Windows) > "auto".
        if backend is None:
            backend = os.environ.get("CADELOOP_BACKEND", "auto")
        if backend == "rio":
            # R-020: 'auto' deliberately avoids RIO until it's validated on
            # real hardware (docs/roadmap.md M3) — an explicit request gets
            # a loud, dismissible warning rather than a silent footgun.
            # cadeloop.Config/serve() additionally require
            # CADELOOP_ALLOW_EXPERIMENTAL_RIO=1 for this exact reason; this
            # low-level constructor stays unblocked for RIO diagnosis
            # (tools/windows/rio_smoke.py, rio_bisect.py).
            warnings.warn(
                "cadeloop: backend='rio' is experimental and unvalidated on "
                "real Windows hardware — every machine tested so far has hit "
                "either an OS-level RIO initialization failure or a data-path "
                "stall. 'auto' stays on the hardware-validated IOCP backend.",
                RuntimeWarning,
                stacklevel=2,
            )
        core = _core.CoreLoop(
            backend=backend,
            spin_us=spin_us,
            high_water=high_water,
            low_water=low_water,
            rio_cq_size=rio_cq_size,
            rio_rq_recv=rio_rq_recv,
            rio_rq_send=rio_rq_send,
        )
        self._core = core
        self._accept_pool = accept_pool  # R-032
        self._signal_handlers = {}
        self._console_ctrl_handler_ref = None  # R-052: keeps the ctypes callback alive
        core.set_error_hook(self._on_callback_error)
        core.set_net_error_hook(self._on_net_error)
        core.set_slow_callback_hook(self._on_slow_callback)

        # R-050: native fast paths bound straight onto the instance —
        # attribute lookup finds the bound native method, no Python frame.
        self.call_soon = core.call_soon
        self.call_soon_threadsafe = core.call_soon_threadsafe
        self.call_later = core.call_later
        self.call_at = core.call_at
        self.time = core.time
        self.stop = core.stop
        self.is_running = core.is_running
        self.is_closed = core.is_closed
        self.get_debug = core.get_debug
        self.stats = core.stats  # R-103

        self._task_factory = None
        self._exception_handler = None
        self._default_executor = None
        self._executor_shutdown_called = False
        self._dns_executor = None
        self._dns_cache: dict = {}
        # R-055: off by default (matches the AbstractEventLoop contract —
        # real asyncio.getaddrinfo never caches). cadeloop.Config/serve()
        # default this on (documented: RFC TTLs ignored) since a short
        # server-side cache is a deliberate, reasonable tradeoff there;
        # direct Loop() construction gets stdlib-faithful behavior unless
        # asked for otherwise.
        self._dns_cache_enabled = dns_cache
        self._dns_cache_ttl = dns_cache_ttl
        self._asyncgens = weakref.WeakSet()
        self._asyncgens_shutdown_called = False
        self._coroutine_origin_tracking_enabled = False
        self._coroutine_origin_tracking_saved_depth = 0

        # R-142: honor PYTHONASYNCIODEBUG / -X dev.
        core.set_debug(
            sys.flags.dev_mode or bool(os.environ.get("PYTHONASYNCIODEBUG"))
        )

    # ------------------------------------------------------------------ #
    # lifecycle                                                          #
    # ------------------------------------------------------------------ #

    def run_forever(self):
        self._check_closed()
        if self.is_running():
            raise RuntimeError("This event loop is already running")
        if events._get_running_loop() is not None:
            raise RuntimeError(
                "Cannot run the event loop while another loop is running"
            )
        self._set_coroutine_origin_tracking(self.get_debug())
        old_agen_hooks = sys.get_asyncgen_hooks()
        sys.set_asyncgen_hooks(
            firstiter=self._asyncgen_firstiter_hook,
            finalizer=self._asyncgen_finalizer_hook,
        )
        events._set_running_loop(self)
        wakeup = self._install_signal_wakeup()
        try:
            self._core.run_forever()
        finally:
            self._remove_signal_wakeup(wakeup)
            events._set_running_loop(None)
            sys.set_asyncgen_hooks(*old_agen_hooks)
            self._set_coroutine_origin_tracking(False)

    def _set_coroutine_origin_tracking(self, enabled):
        """R-142: sys.set_coroutine_origin_tracking_depth ports directly
        from base_events._set_coroutine_origin_tracking — debug mode
        previously only flipped the core loop's own debug flag, missing
        the richer "Object created at (most recent call last)"
        coroutine-origin tracebacks real asyncio's debug mode adds."""
        if bool(enabled) == bool(self._coroutine_origin_tracking_enabled):
            return
        if enabled:
            self._coroutine_origin_tracking_saved_depth = sys.get_coroutine_origin_tracking_depth()
            sys.set_coroutine_origin_tracking_depth(10)  # asyncio.constants.DEBUG_STACK_DEPTH
        else:
            sys.set_coroutine_origin_tracking_depth(self._coroutine_origin_tracking_saved_depth)
        self._coroutine_origin_tracking_enabled = enabled

    def _install_signal_wakeup(self):
        """R-052: CPython's C-level signal handler writes one byte to the
        wakeup fd; watching the read end makes a parked kernel poll return
        immediately, so CTRL+C interrupts an idle loop at once. This is
        the proactor CTRL+C fix on Windows, and on POSIX it also closes
        the race of a signal landing between the tick's signal check and
        the park. Main thread only (set_wakeup_fd's own rule)."""
        if threading.current_thread() is not threading.main_thread():
            return None
        import signal as signal_module

        try:
            rsock, csock = socket.socketpair()
            rsock.setblocking(False)
            csock.setblocking(False)
            old_fd = signal_module.set_wakeup_fd(csock.fileno(), warn_on_full_buffer=False)
        except (ValueError, OSError, AttributeError):
            return None

        def drain():
            try:
                while rsock.recv(4096):
                    pass
            except (BlockingIOError, InterruptedError, OSError):
                pass

        try:
            self._core.add_reader(rsock.fileno(), drain)
        except OSError:
            signal_module.set_wakeup_fd(old_fd)
            rsock.close()
            csock.close()
            return None
        return (rsock, csock, old_fd)

    def _remove_signal_wakeup(self, wakeup):
        if wakeup is None:
            return
        import signal as signal_module

        rsock, csock, old_fd = wakeup
        try:
            self._core.remove_reader(rsock.fileno())
        except OSError:
            pass
        try:
            signal_module.set_wakeup_fd(old_fd)
        except (ValueError, OSError):
            pass
        rsock.close()
        csock.close()

    def run_until_complete(self, future):
        self._check_closed()
        new_task = not futures.isfuture(future)
        future = tasks.ensure_future(future, loop=self)
        if new_task:
            # An exception is raised if the future didn't complete, so there
            # is no need to log the "destroy pending task" message.
            future._log_destroy_pending = False
        future.add_done_callback(_run_until_complete_cb)
        try:
            self.run_forever()
        except BaseException:
            if new_task and future.done() and not future.cancelled():
                # The coroutine raised a BaseException. Consume the exception
                # to not log a warning; the caller doesn't have access to the
                # local task.
                future.exception()
            raise
        finally:
            future.remove_done_callback(_run_until_complete_cb)
        if not future.done():
            raise RuntimeError("Event loop stopped before Future completed.")
        return future.result()

    def close(self):
        # Check-before-teardown (base_events.close()'s own order): a
        # close() on a running loop must fail cleanly, not leave the
        # executors permanently shut down as a side effect of a call
        # that itself raised.
        if self.is_running():
            raise RuntimeError("Cannot close a running event loop")
        # Restore process-level signal disposition before tearing the core
        # down. Leaving handlers installed pointed the signal module at a
        # _dispatch that returns immediately once the loop is closed, so
        # SIGINT/SIGTERM were silently swallowed for the rest of the
        # process instead of resuming default behaviour -- and the closed
        # loop stayed referenced from the signal module. Matches what the
        # stdlib Unix loop does in its own close().
        for sig in list(self._signal_handlers):
            try:
                self.remove_signal_handler(sig)
            except (OSError, ValueError, RuntimeError):
                self._signal_handlers.pop(sig, None)
        self._remove_console_ctrl_handler()
        if self._default_executor is not None:
            self._default_executor.shutdown(wait=False)
            self._default_executor = None
        if self._dns_executor is not None:
            self._dns_executor.shutdown(wait=False)
            self._dns_executor = None
        self._core.close()

    def _check_closed(self):
        if self._core.is_closed():
            raise RuntimeError("Event loop is closed")

    def set_debug(self, enabled):
        self._core.set_debug(bool(enabled))
        if self.is_running():
            # Deferred to the loop's own thread (matches base_events):
            # sys.set_coroutine_origin_tracking_depth is process-global
            # state, so mutating it off-thread while the loop runs would
            # race. If not running, run_forever() applies it at startup.
            self.call_soon_threadsafe(self._set_coroutine_origin_tracking, enabled)

    def __repr__(self):
        return (
            f"<cadeloop.Loop running={self.is_running()} "
            f"closed={self.is_closed()} debug={self.get_debug()}>"
        )

    # ------------------------------------------------------------------ #
    # futures & tasks                                                    #
    # ------------------------------------------------------------------ #

    def create_future(self):
        return futures.Future(loop=self)

    def create_task(self, coro, *, name=None, context=None):
        self._check_closed()
        if self._task_factory is None:
            task = tasks.Task(coro, loop=self, name=name, context=context)
            if task._source_traceback:
                del task._source_traceback[-1]
        else:
            if context is None:
                task = self._task_factory(self, coro)
            else:
                task = self._task_factory(self, coro, context=context)
            if name is not None:
                try:
                    set_name = task.set_name
                except AttributeError:
                    pass
                else:
                    set_name(name)
        return task

    def set_task_factory(self, factory):
        if factory is not None and not callable(factory):
            raise TypeError("task factory must be a callable or None")
        self._task_factory = factory

    def get_task_factory(self):
        return self._task_factory

    # ------------------------------------------------------------------ #
    # executors & DNS (R-055)                                            #
    # ------------------------------------------------------------------ #

    def run_in_executor(self, executor, func, *args):
        self._check_closed()
        if asyncio.iscoroutine(func) or asyncio.iscoroutinefunction(func):
            raise TypeError("coroutines cannot be used with run_in_executor()")
        if executor is None:
            executor = self._default_executor
            if self._executor_shutdown_called:
                raise RuntimeError("Executor shutdown has been called")
            if executor is None:
                executor = concurrent.futures.ThreadPoolExecutor(
                    thread_name_prefix="cadeloop"
                )
                self._default_executor = executor
        return futures.wrap_future(executor.submit(func, *args), loop=self)

    def set_default_executor(self, executor):
        if not isinstance(executor, concurrent.futures.ThreadPoolExecutor):
            raise TypeError("executor must be ThreadPoolExecutor instance")
        self._default_executor = executor

    def _dns_pool(self):
        # R-055: fixed internal pool, size = min(8, cpus).
        if self._dns_executor is None:
            self._dns_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=min(8, os.cpu_count() or 1),
                thread_name_prefix="cadeloop-dns",
            )
        return self._dns_executor

    async def getaddrinfo(self, host, port, *, family=0, type=0, proto=0, flags=0):
        key = (host, port, family, type, proto, flags)
        if self._dns_cache_enabled:
            hit = self._dns_cache.get(key)
            if hit is not None and hit[0] > self.time():
                return hit[1]
        result = await futures.wrap_future(
            self._dns_pool().submit(
                socket.getaddrinfo, host, port, family, type, proto, flags
            ),
            loop=self,
        )
        if self._dns_cache_enabled:
            if len(self._dns_cache) > 1024:
                self._dns_cache.clear()
            self._dns_cache[key] = (self.time() + self._dns_cache_ttl, result)
        return result

    async def getnameinfo(self, sockaddr, flags=0):
        return await futures.wrap_future(
            self._dns_pool().submit(socket.getnameinfo, sockaddr, flags),
            loop=self,
        )

    async def shutdown_default_executor(self):
        self._executor_shutdown_called = True
        if self._default_executor is None:
            return
        future = self.create_future()
        thread = threading.Thread(target=self._do_shutdown, args=(future,))
        thread.start()
        try:
            await future
        finally:
            thread.join()

    def _do_shutdown(self, future):
        try:
            self._default_executor.shutdown(wait=True)
            if not self.is_closed():
                self.call_soon_threadsafe(future.set_result, None)
        except Exception as ex:
            if not self.is_closed() and not future.cancelled():
                self.call_soon_threadsafe(future.set_exception, ex)

    # ------------------------------------------------------------------ #
    # async generators                                                   #
    # ------------------------------------------------------------------ #

    def _asyncgen_firstiter_hook(self, agen):
        if self._asyncgens_shutdown_called:
            warnings.warn(
                f"asynchronous generator {agen!r} was scheduled after "
                f"loop.shutdown_asyncgens() call",
                ResourceWarning,
                source=self,
            )
        self._asyncgens.add(agen)

    def _asyncgen_finalizer_hook(self, agen):
        self._asyncgens.discard(agen)
        if not self.is_closed():
            self.call_soon_threadsafe(self.create_task, agen.aclose())

    async def shutdown_asyncgens(self):
        self._asyncgens_shutdown_called = True
        if not len(self._asyncgens):
            return
        closing_agens = list(self._asyncgens)
        self._asyncgens.clear()
        results = await tasks.gather(
            *[ag.aclose() for ag in closing_agens], return_exceptions=True
        )
        for result, agen in zip(results, closing_agens):
            if isinstance(result, Exception):
                self.call_exception_handler(
                    {
                        "message": f"an error occurred during closing of "
                        f"asynchronous generator {agen!r}",
                        "exception": result,
                        "asyncgen": agen,
                    }
                )

    # ------------------------------------------------------------------ #
    # exception handling                                                 #
    # ------------------------------------------------------------------ #

    def set_exception_handler(self, handler):
        if handler is not None and not callable(handler):
            raise TypeError(
                f"A callable object or None is expected, got {handler!r}"
            )
        self._exception_handler = handler

    def get_exception_handler(self):
        return self._exception_handler

    def default_exception_handler(self, context):
        message = context.get("message")
        if not message:
            message = "Unhandled exception in event loop"
        exception = context.get("exception")
        if exception is not None:
            exc_info = (type(exception), exception, exception.__traceback__)
        else:
            exc_info = False
        log_lines = [message]
        for key in sorted(context):
            if key in {"message", "exception"}:
                continue
            log_lines.append(f"{key}: {context[key]!r}")
        logger.error("\n".join(log_lines), exc_info=exc_info)

    def call_exception_handler(self, context):
        if self._exception_handler is None:
            try:
                self.default_exception_handler(context)
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException:
                logger.error(
                    "Exception in default exception handler", exc_info=True
                )
            return
        try:
            self._exception_handler(self, context)
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            try:
                self.default_exception_handler(
                    {
                        "message": "Unhandled error in exception handler",
                        "exception": exc,
                        "context": context,
                    }
                )
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException:
                logger.error(
                    "Exception in default exception handler "
                    "while handling an unexpected error "
                    "in custom exception handler",
                    exc_info=True,
                )

    # Hooks invoked from the native dispatcher.

    def _on_callback_error(self, handle, exc):
        self.call_exception_handler(
            {
                "message": f"Exception in callback {handle!r}",
                "exception": exc,
                "handle": handle,
            }
        )

    def _on_net_error(self, message, exc):
        self.call_exception_handler({"message": message, "exception": exc})

    def _on_slow_callback(self, handle, seconds):
        # R-142: slow-callback warnings in debug mode (>100ms).
        logger.warning(
            "Executing %s took %.3f seconds", reprlib.repr(handle), seconds
        )

    # ------------------------------------------------------------------ #
    # I/O surface — milestone-gated (see docs/roadmap.md)                #
    # ------------------------------------------------------------------ #



    async def create_datagram_endpoint(
        self,
        protocol_factory,
        local_addr=None,
        remote_addr=None,
        *,
        family=0,
        proto=0,
        flags=0,
        reuse_port=None,
        allow_broadcast=None,
        sock=None,
    ):
        """R-058: native datagram endpoint (WSARecvFrom/WSASendTo on IOCP,
        recvfrom/sendto on epoll — no readiness probes, which would
        truncate datagrams on Windows)."""
        import socket as socket_module

        if sock is not None:
            if any((local_addr, remote_addr, family, proto, flags, reuse_port, allow_broadcast)):
                raise ValueError("sock is mutually exclusive with address/options")
            udp_sock = sock
        else:
            fam = family or socket_module.AF_INET
            if local_addr or remote_addr:
                probe = local_addr or remote_addr
                infos = await self.getaddrinfo(
                    probe[0],
                    probe[1],
                    family=family,
                    type=socket_module.SOCK_DGRAM,
                    proto=proto,
                    flags=flags,
                )
                if not infos:
                    raise OSError(f"getaddrinfo({probe!r}) returned empty list")
                fam = infos[0][0]
            udp_sock = socket_module.socket(fam, socket_module.SOCK_DGRAM, proto)
            try:
                if reuse_port:
                    if not hasattr(socket_module, "SO_REUSEPORT"):
                        raise ValueError("reuse_port not supported on this platform")
                    udp_sock.setsockopt(
                        socket_module.SOL_SOCKET, socket_module.SO_REUSEPORT, 1
                    )
                if allow_broadcast:
                    udp_sock.setsockopt(
                        socket_module.SOL_SOCKET, socket_module.SO_BROADCAST, 1
                    )
                if local_addr:
                    udp_sock.bind(local_addr)
                if remote_addr:
                    udp_sock.connect(remote_addr)
            except BaseException:
                udp_sock.close()
                raise
        udp_sock.setblocking(False)
        protocol = protocol_factory()
        transport = _DatagramTransport(self, udp_sock, protocol, remote_addr)
        try:
            transport._open()
        except BaseException:
            if sock is None:
                udp_sock.close()
            raise
        return transport, protocol

    async def sendfile(self, transport, file, offset=0, count=None, *, fallback=True):
        """R-036. Native path: drain the transport's corked writes, then
        os.sendfile straight on the borrowed socket fd (zero-copy).
        TransmitFile on Windows is the remaining native refinement; there
        (and for SSL transports, which have no raw fd) the chunked
        transport-write fallback applies."""
        if transport.is_closing():
            raise RuntimeError("Transport is closing")
        fileno = getattr(transport, "fileno", None)
        can_native = (
            fileno is not None and hasattr(os, "sendfile") and hasattr(file, "fileno")
        )
        if can_native:
            try:
                file.fileno()
            except (OSError, AttributeError, ValueError):
                can_native = False
        if not can_native:
            if not fallback:
                raise asyncio.SendfileNotAvailableError(
                    "sendfile is not available for this transport/file"
                )
            return await self._sendfile_fallback(transport, file, offset, count)
        # No byte may interleave: wait out writes already queued in the
        # engine before bypassing it (concurrent app writes during a
        # sendfile are user error, as in stdlib asyncio).
        while transport.get_write_buffer_size() > 0:
            await tasks.sleep(0.001)
        return await self._sendfile_native_fd(transport.fileno(), file, offset, count)

    async def _sendfile_native_fd(self, fd, file, offset, count):
        in_fd = file.fileno()
        total = 0
        while True:
            blocksize = 256 * 1024 if count is None else min(count - total, 256 * 1024)
            if blocksize <= 0:
                break
            try:
                sent = os.sendfile(fd, in_fd, offset + total, blocksize)
            except BlockingIOError:
                sent = None
            except InterruptedError:
                continue
            if sent == 0:
                break  # end of file
            if sent is not None:
                total += sent
                continue
            # Socket buffer full: wait for writability.
            fut = self.create_future()

            def on_writable():
                if not fut.done():
                    fut.set_result(None)

            self._core.add_writer(fd, on_writable)
            try:
                await fut
            finally:
                self._core.remove_writer(fd)
        # stdlib convention: leave the file object positioned after the
        # bytes sent.
        file.seek(offset + total)
        return total

    async def _sendfile_fallback(self, transport, file, offset, count):
        if offset:
            file.seek(offset)
        total = 0
        while True:
            blocksize = 16384 if count is None else min(count - total, 16384)
            if blocksize <= 0:
                break
            data = file.read(blocksize)
            if not data:
                break
            transport.write(data)
            total += len(data)
            while transport.get_write_buffer_size() > 64 * 1024:
                await tasks.sleep(0.001)
        return total














    def add_signal_handler(self, sig, callback, *args):
        """R-052. The Python-level handler runs on the main thread once
        the parked poll wakes (the run_forever wakeup fd guarantees that
        promptly) and enqueues the callback thread-safely. On Windows the
        deliverable console signals are SIGINT (CTRL+C) and SIGBREAK
        (CTRL+BREAK); SIGTERM is accepted for artificial delivery
        (os.kill / raise_signal) like CPython itself.

        A genuinely external stop request — a process supervisor, Docker,
        or ``Popen.send_signal(SIGTERM)`` — reaches none of these:
        Windows has no real SIGTERM delivery, and ``Popen.send_signal``
        maps SIGTERM straight to ``TerminateProcess`` (uncatchable by any
        process, full stop — no workaround exists or ever will). What
        *can* be caught is ``GenerateConsoleCtrlEvent`` (CTRL_BREAK_EVENT
        et al.), which arrives through ``SetConsoleCtrlHandler``, not
        Python's ``signal`` module — so this installs that handler
        (idempotently, once per loop) and routes CTRL_BREAK/CLOSE/LOGOFF/
        SHUTDOWN events into whichever of SIGTERM/SIGINT/SIGBREAK is
        registered here, giving external supervisors a real graceful-
        shutdown path if they send CTRL_BREAK_EVENT instead of a blind
        kill. cadeloop's own multi-worker supervisor already gets this for
        its spawned children via CREATE_NEW_PROCESS_GROUP + a control
        pipe (server.py); this covers the top-level process an external
        manager controls, which that mechanism does not reach."""
        import signal as signal_module

        if not isinstance(sig, int):
            raise TypeError(f"sig must be an int, not {sig!r}")
        if sys.platform == "win32":
            allowed = {signal_module.SIGINT, signal_module.SIGTERM}
            if hasattr(signal_module, "SIGBREAK"):
                allowed.add(signal_module.SIGBREAK)
            if sig not in allowed:
                raise ValueError(f"signal {sig!r} not supported on this platform")
            self._install_console_ctrl_handler()
        elif sig not in signal_module.valid_signals():
            raise ValueError(f"invalid signal number {sig}")
        if (
            asyncio.iscoroutine(callback)
            or asyncio.iscoroutinefunction(callback)
        ):
            raise TypeError("coroutines cannot be used with add_signal_handler()")
        self._check_closed()
        self._signal_handlers[sig] = (callback, args)

        def _dispatch(signum, frame):
            entry = self._signal_handlers.get(signum)
            if entry is None or self.is_closed():
                return
            cb, cb_args = entry
            self.call_soon_threadsafe(cb, *cb_args)

        signal_module.signal(sig, _dispatch)

    def remove_signal_handler(self, sig):
        import signal as signal_module

        if sig not in self._signal_handlers:
            return False
        del self._signal_handlers[sig]
        if sig == signal_module.SIGINT:
            signal_module.signal(sig, signal_module.default_int_handler)
        else:
            signal_module.signal(sig, signal_module.SIG_DFL)
        return True

    # Console control events SetConsoleCtrlHandler treats as a stop
    # request; CTRL_C_EVENT (0) is deliberately excluded — Python's own
    # SIGINT plumbing already owns that one.
    _CTRL_BREAK_EVENT = 1
    _CTRL_CLOSE_EVENT = 2
    _CTRL_LOGOFF_EVENT = 5
    _CTRL_SHUTDOWN_EVENT = 6
    _STOP_CTRL_EVENTS = frozenset((_CTRL_BREAK_EVENT, _CTRL_CLOSE_EVENT, _CTRL_LOGOFF_EVENT, _CTRL_SHUTDOWN_EVENT))

    def _remove_console_ctrl_handler(self):
        """R-052 (win32 only): unregister the console-control callback so a
        closed loop stops intercepting CTRL_BREAK/CLOSE/LOGOFF/SHUTDOWN."""
        if self._console_ctrl_handler_ref is None:
            return
        try:
            import ctypes

            ctypes.windll.kernel32.SetConsoleCtrlHandler(self._console_ctrl_handler_ref, False)
        except (AttributeError, OSError):
            pass  # not Windows, or the console is already gone
        finally:
            self._console_ctrl_handler_ref = None

    def _install_console_ctrl_handler(self):
        """R-052 (win32 only, idempotent). See add_signal_handler's
        docstring for why this exists alongside the signal-module path."""
        if self._console_ctrl_handler_ref is not None:
            return
        import ctypes
        import signal as signal_module

        handler_routine = ctypes.WINFUNCTYPE(ctypes.c_int, ctypes.c_uint)

        def handler(ctrl_type):
            # Runs on a dedicated OS thread Windows spawns to deliver the
            # event — never this loop's own thread (R-022 discipline).
            if ctrl_type not in self._STOP_CTRL_EVENTS:
                return 0  # not ours: let the next handler (or Python's
                # own SIGINT/SIGBREAK plumbing, for CTRL_C/CTRL_BREAK) act
            for candidate in (
                signal_module.SIGTERM,
                signal_module.SIGINT,
                getattr(signal_module, "SIGBREAK", None),
            ):
                entry = self._signal_handlers.get(candidate) if candidate is not None else None
                if entry is not None:
                    cb, cb_args = entry
                    try:
                        self.call_soon_threadsafe(cb, *cb_args)
                    except RuntimeError:
                        pass  # loop already closing/closed
                    return 1
            return 0  # nothing registered: let Windows apply the default

        self._console_ctrl_handler_ref = handler_routine(handler)
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleCtrlHandler.argtypes = [handler_routine, ctypes.c_int]
        kernel32.SetConsoleCtrlHandler.restype = ctypes.c_int
        if not kernel32.SetConsoleCtrlHandler(self._console_ctrl_handler_ref, 1):
            warnings.warn(
                "cadeloop: SetConsoleCtrlHandler registration failed — "
                "CTRL_CLOSE/LOGOFF/SHUTDOWN events won't trigger graceful "
                "shutdown (CTRL+C/CTRL+BREAK are unaffected).",
                RuntimeWarning,
                stacklevel=3,
            )

    # ---- pipes + subprocess (R-051) ----------------------------------
    # POSIX rides the stdlib 3.11 machinery (this project pins CPython
    # 3.11): the _Unix*PipeTransport / _UnixSubprocessTransport classes
    # drive readiness through the selector-loop-private _add_reader
    # family, which maps 1:1 onto our public watch surface. Windows rides
    # its own _winpipes transports over overlapped ReadFile/WriteFile
    # (IOCP backend) — see connect_read_pipe/connect_write_pipe/
    # _make_subprocess_transport below.

    def _add_reader(self, fd, callback, *args):
        return self.add_reader(fd, callback, *args)

    def _remove_reader(self, fd):
        return self.remove_reader(fd)

    def _add_writer(self, fd, callback, *args):
        return self.add_writer(fd, callback, *args)

    def _remove_writer(self, fd):
        return self.remove_writer(fd)

    async def connect_read_pipe(self, protocol_factory, pipe):
        protocol = protocol_factory()
        waiter = self.create_future()
        if sys.platform == "win32":
            from . import _winpipes

            transport = _winpipes.ReadPipeTransport(self, pipe, protocol, waiter)
        else:
            from asyncio import unix_events

            transport = unix_events._UnixReadPipeTransport(self, pipe, protocol, waiter)
        try:
            await waiter
        except BaseException:
            transport.close()
            raise
        return transport, protocol

    async def connect_write_pipe(self, protocol_factory, pipe):
        protocol = protocol_factory()
        waiter = self.create_future()
        if sys.platform == "win32":
            from . import _winpipes

            transport = _winpipes.WritePipeTransport(self, pipe, protocol, waiter)
        else:
            from asyncio import unix_events

            transport = unix_events._UnixWritePipeTransport(self, pipe, protocol, waiter)
        try:
            await waiter
        except BaseException:
            transport.close()
            raise
        return transport, protocol

    async def _make_subprocess_transport(
        self, protocol, args, shell, stdin, stdout, stderr, bufsize, extra=None, **kwargs
    ):
        if sys.platform == "win32":
            from . import _winpipes

            waiter = self.create_future()
            transp = _winpipes.SubprocessTransport(
                self, protocol, args, shell, stdin, stdout, stderr, bufsize,
                waiter=waiter, extra=extra, **kwargs,
            )
            try:
                await waiter
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException:
                transp.close()
                await transp._wait()
                raise
            return transp

        from asyncio import events as _events
        from asyncio import unix_events

        with _events.get_child_watcher() as watcher:
            if not watcher.is_active():
                raise RuntimeError(
                    "asyncio child watcher is not activated, "
                    "subprocess support is not installed"
                )
            waiter = self.create_future()
            transp = unix_events._UnixSubprocessTransport(
                self,
                protocol,
                args,
                shell,
                stdin,
                stdout,
                stderr,
                bufsize,
                waiter=waiter,
                extra=extra,
                **kwargs,
            )
            watcher.add_child_handler(
                transp.get_pid(), self._child_watcher_callback, transp
            )
            try:
                await waiter
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException:
                transp.close()
                await transp._wait()
                raise
        return transp

    def _child_watcher_callback(self, pid, returncode, transp):
        # Thread-safe: the watcher reaps from its waiter thread.
        self.call_soon_threadsafe(self.call_soon, transp._process_exited, returncode)

    async def subprocess_shell(
        self,
        protocol_factory,
        cmd,
        *,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=False,
        shell=True,
        bufsize=0,
        encoding=None,
        errors=None,
        text=None,
        **kwargs,
    ):
        if not isinstance(cmd, (bytes, str)):
            raise ValueError("cmd must be a string")
        if universal_newlines or text or encoding is not None or errors is not None:
            raise ValueError("text mode is not supported")
        if not shell:
            raise ValueError("shell must be True")
        if bufsize != 0:
            raise ValueError("bufsize must be 0")
        protocol = protocol_factory()
        transport = await self._make_subprocess_transport(
            protocol, cmd, True, stdin, stdout, stderr, bufsize, **kwargs
        )
        return transport, protocol

    async def subprocess_exec(
        self,
        protocol_factory,
        program,
        *args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=False,
        shell=False,
        bufsize=0,
        encoding=None,
        errors=None,
        text=None,
        **kwargs,
    ):
        if universal_newlines or text or encoding is not None or errors is not None:
            raise ValueError("text mode is not supported")
        if shell:
            raise ValueError("shell must be False")
        if bufsize != 0:
            raise ValueError("bufsize must be 0")
        popen_args = (program,) + args
        for arg in popen_args:
            if not isinstance(arg, (str, bytes)):
                raise TypeError(
                    f"program arguments must be a bytes or text string, not {type(arg).__name__}"
                )
        protocol = protocol_factory()
        transport = await self._make_subprocess_transport(
            protocol, popen_args, False, stdin, stdout, stderr, bufsize, **kwargs
        )
        return transport, protocol
