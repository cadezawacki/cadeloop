"""Windows named-pipe transports (R-051): subprocess stdio + connect_read_pipe
/connect_write_pipe/subprocess_exec/subprocess_shell support.

Ported from ``asyncio.proactor_events``'s ``_Proactor*PipeTransport``
classes and ``asyncio.windows_events``'s ``_WindowsSubprocessTransport``,
with the stdlib IocpProactor calls (``self._loop._proactor.recv/send``,
which are deeply coupled to ``_overlapped``/``ProactorEventLoop``
internals we don't have) swapped for cadeloop's own primitives
(``CoreLoop.pipe_register/pipe_read/pipe_write``, backed by overlapped
ReadFile/WriteFile on the IOCP backend — see crates/core/src/backend/
iocp.rs). Process-exit detection uses a dedicated waiter thread blocked
on ``WaitForSingleObject`` instead of stdlib's IOCP-integrated
``RegisterWaitForSingleObject`` path, mirroring this project's own POSIX
child-watcher precedent (``asyncio.ThreadedChildWatcher``) rather than
adding a second wait-for-handle mechanism to the backend.

Win32-only APIs (``_winapi``, ``asyncio.windows_utils``) are imported
lazily inside the functions that need them, so this module stays
importable (for tests, syntax checks) on any platform — actually
constructing a transport still requires running on win32.

``asyncio.base_subprocess.BaseSubprocessTransport`` and the pipe/socket
transport ABCs in ``asyncio.transports`` are pure Python and
platform-agnostic, so they're used directly (the same reuse the POSIX
path already gets from ``asyncio.unix_events``).
"""

from __future__ import annotations

import threading
import warnings
from asyncio import base_subprocess, futures, protocols, transports

__all__ = ["ReadPipeTransport", "WritePipeTransport", "SubprocessTransport"]


class _PipeTransportBase(transports._FlowControlMixin, transports.BaseTransport):
    """Shared state/close machinery for both pipe directions."""

    def __init__(self, loop, handle, protocol, waiter=None, extra=None):
        super().__init__(extra, loop)
        self._extra["pipe"] = handle
        self._handle = handle
        self._fileno = handle.fileno()
        self.set_protocol(protocol)
        self._buffer = None
        self._read_fut = None
        self._write_fut = None
        self._pending_write = 0
        self._conn_lost = 0
        self._closing = False
        self._called_connection_lost = False
        self._eof_written = False
        self._empty_waiter = None
        self._core = loop._core
        self._core.pipe_register(self._fileno)
        self._loop.call_soon(self._protocol.connection_made, self)
        if waiter is not None:
            self._loop.call_soon(futures._set_result_unless_cancelled, waiter, None)

    def __repr__(self):
        info = [self.__class__.__name__]
        if self._handle is None:
            info.append("closed")
        elif self._closing:
            info.append("closing")
        return "<{}>".format(" ".join(info))

    def set_protocol(self, protocol):
        self._protocol = protocol

    def get_protocol(self):
        return self._protocol

    def is_closing(self):
        return self._closing

    def close(self):
        if self._closing:
            return
        self._closing = True
        self._conn_lost += 1
        if not self._buffer and self._write_fut is None:
            self._loop.call_soon(self._call_connection_lost, None)
        if self._read_fut is not None:
            self._read_fut.cancel()
            self._read_fut = None

    def __del__(self, _warn=warnings.warn):
        if self._handle is not None:
            _warn(f"unclosed transport {self!r}", ResourceWarning, source=self)
            self._handle.close()

    def _fatal_error(self, exc, message="Fatal error on pipe transport"):
        if not isinstance(exc, OSError) and self._loop is not None:
            self._loop.call_exception_handler({
                "message": message,
                "exception": exc,
                "transport": self,
                "protocol": self._protocol,
            })
        self._force_close(exc)

    def _force_close(self, exc):
        if self._empty_waiter is not None and not self._empty_waiter.done():
            if exc is None:
                self._empty_waiter.set_result(None)
            else:
                self._empty_waiter.set_exception(exc)
        if self._closing and self._called_connection_lost:
            return
        self._closing = True
        self._conn_lost += 1
        if self._write_fut:
            self._write_fut.cancel()
            self._write_fut = None
        if self._read_fut:
            self._read_fut.cancel()
            self._read_fut = None
        self._pending_write = 0
        self._buffer = None
        self._loop.call_soon(self._call_connection_lost, exc)

    def _call_connection_lost(self, exc):
        if self._called_connection_lost:
            return
        try:
            self._protocol.connection_lost(exc)
        finally:
            if self._handle is not None:
                self._handle.close()
            self._handle = None
            self._called_connection_lost = True

    def get_write_buffer_size(self):
        size = self._pending_write
        if self._buffer is not None:
            size += len(self._buffer)
        return size


class ReadPipeTransport(_PipeTransportBase, transports.ReadTransport):
    """Overlapped ReadFile, re-posted after each completion (R-051)."""

    def __init__(self, loop, handle, protocol, waiter=None, extra=None, buffer_size=65536):
        self._paused = True
        self._pending_data_length = -1
        self._pending_data = b""
        self._buffer_size = buffer_size
        super().__init__(loop, handle, protocol, waiter, extra)
        self._loop.call_soon(self._loop_reading)
        self._paused = False

    def is_reading(self):
        return not self._paused and not self._closing

    def pause_reading(self):
        if self._closing or self._paused:
            return
        self._paused = True

    def resume_reading(self):
        if self._closing or not self._paused:
            return
        self._paused = False
        if self._read_fut is None:
            self._loop.call_soon(self._loop_reading)
        length = self._pending_data_length
        data = self._pending_data
        self._pending_data_length = -1
        self._pending_data = b""
        if length > -1:
            # After _loop_reading() so the protocol can re-pause first.
            self._loop.call_soon(self._data_received, data, length)

    def _eof_received(self):
        try:
            keep_open = self._protocol.eof_received()
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._fatal_error(exc, "Fatal error: protocol.eof_received() call failed.")
            return
        if not keep_open:
            self.close()

    def _data_received(self, data, length):
        if self._paused:
            self._pending_data_length = length
            self._pending_data = data
            return
        if length == 0:
            self._eof_received()
            return
        if isinstance(self._protocol, protocols.BufferedProtocol):
            try:
                protocols._feed_data_to_buffered_proto(self._protocol, data)
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self._fatal_error(exc, "Fatal error: protocol.buffer_updated() call failed.")
                return
        else:
            self._protocol.data_received(data)

    def _loop_reading(self, fut=None):
        length = -1
        data = None
        try:
            if fut is not None:
                self._read_fut = None
                if fut.cancelled():
                    if self._closing:
                        return
                    raise futures.CancelledError
                data = fut.result()  # b"" == EOF (translated from
                length = len(data)  # ERROR_BROKEN_PIPE too, pyshim/net.rs)
                if length == 0:
                    return  # EOF: no reschedule

            if self._closing:
                return

            if not self._paused:
                new_fut = self._loop.create_future()
                self._core.pipe_read(self._fileno, self._buffer_size, new_fut)
                self._read_fut = new_fut
        except OSError as exc:
            self._fatal_error(exc, "Fatal read error on pipe transport")
        except futures.CancelledError:
            if not self._closing:
                raise
        else:
            if not self._paused:
                self._read_fut.add_done_callback(self._loop_reading)
        finally:
            if length > -1:
                self._data_received(data, length)


class _BaseWritePipeTransport(_PipeTransportBase, transports.WriteTransport):
    """Transport for write pipes (subprocess stdin, connect_write_pipe)."""

    def write(self, data):
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"data argument must be a bytes-like object, not {type(data).__name__}")
        if self._eof_written:
            raise RuntimeError("write_eof() already called")
        if self._empty_waiter is not None:
            raise RuntimeError("unable to write; sendfile is in progress")
        if not data:
            return
        if self._conn_lost:
            self._conn_lost += 1
            return

        # IDLE -> WRITING / WRITING -> BACKED UP / BACKED UP (R-035 style
        # corking, minus gather — WriteFile takes one buffer).
        if self._write_fut is None:
            self._loop_writing(data=bytes(data))
        elif not self._buffer:
            self._buffer = bytearray(data)
            self._maybe_pause_protocol()
        else:
            self._buffer.extend(data)
            self._maybe_pause_protocol()

    def _loop_writing(self, fut=None, data=None):
        try:
            if fut is not None and self._write_fut is None and self._closing:
                return
            if fut is not None:
                self._write_fut = None
                self._pending_write = 0
                fut.result()
            if data is None:
                data = self._buffer
                self._buffer = None
            if not data:
                if self._closing:
                    self._loop.call_soon(self._call_connection_lost, None)
                self._maybe_resume_protocol()
            else:
                new_fut = self._loop.create_future()
                self._core.pipe_write(self._fileno, bytes(data), new_fut)
                self._write_fut = new_fut
                self._pending_write = len(data)
                new_fut.add_done_callback(self._loop_writing)
                self._maybe_pause_protocol()
            if self._empty_waiter is not None and self._write_fut is None:
                self._empty_waiter.set_result(None)
        except OSError as exc:
            self._fatal_error(exc, "Fatal write error on pipe transport")

    def can_write_eof(self):
        return True

    def write_eof(self):
        self.close()

    def abort(self):
        self._force_close(None)

    def _make_empty_waiter(self):
        if self._empty_waiter is not None:
            raise RuntimeError("Empty waiter is already set")
        self._empty_waiter = self._loop.create_future()
        if self._write_fut is None:
            self._empty_waiter.set_result(None)
        return self._empty_waiter

    def _reset_empty_waiter(self):
        self._empty_waiter = None


class WritePipeTransport(_BaseWritePipeTransport):
    """Adds the "detect the peer closing" probe read (stdlib parity):
    subprocess stdin pipes are opened DUPLEX (windows_utils.pipe) exactly
    so this small ReadFile can double as a close signal."""

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        probe = self._loop.create_future()
        self._core.pipe_read(self._fileno, 16, probe)
        self._read_fut = probe
        probe.add_done_callback(self._pipe_closed)

    def _pipe_closed(self, fut):
        if fut.cancelled():
            return  # the transport was closed
        if self._closing:
            self._read_fut = None
            return
        self._read_fut = None
        exc = fut.exception()
        if exc is None:
            data = fut.result()
            assert data == b"", data
        if self._write_fut is not None:
            self._force_close(exc or BrokenPipeError())
        else:
            self.close()


def _watch_process_exit(loop, proc, callback):
    """Windows has no SIGCHLD; block on the process HANDLE in a dedicated
    thread (mirrors asyncio.ThreadedChildWatcher's os.waitpid thread on
    POSIX — same architecture, Windows' blocking-wait primitive)."""
    import _winapi

    def waiter():
        _winapi.WaitForSingleObject(int(proc._handle), _winapi.INFINITE)
        rc = proc.poll()
        loop.call_soon_threadsafe(callback, rc)

    threading.Thread(target=waiter, daemon=True, name="cadeloop-subprocess-wait").start()


class SubprocessTransport(base_subprocess.BaseSubprocessTransport):
    def _start(self, args, shell, stdin, stdout, stderr, bufsize, **kwargs):
        from asyncio import windows_utils

        self._proc = windows_utils.Popen(
            args, shell=shell, stdin=stdin, stdout=stdout, stderr=stderr, bufsize=bufsize, **kwargs
        )
        _watch_process_exit(self._loop, self._proc, self._process_exited)
