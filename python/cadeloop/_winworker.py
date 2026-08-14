"""Spawned worker entry for the fork-free worker model (R-090..R-093).

The supervisor (``server._serve_multi_spawn``) owns the listener and the
accept loop; this process never listens. Connections arrive already
accepted, one per control-channel frame, and are adopted into this
worker's own completion port.

That split is forced by Windows, not chosen for symmetry: a file object
binds to exactly one IOCP for life, so a listener duplicated into N
workers can only ever be driven by whichever worker associated it first
— the rest post ops whose completions are delivered to the winner's
port carrying pointers into the loser's address space (ADR-25). Handing
over ACCEPTED sockets instead keeps every socket associated exactly once,
by the process that will actually drive it.

Channel framing (see ``server._send_frame`` for the writing side): each
frame is a length-prefixed command.

* ``STOP`` — drain and exit. EOF (a dead supervisor) means the same.
* ``CONN`` — one accepted connection. On Windows the frame body carries
  ``socket.share()`` bytes; on POSIX the descriptor rides SCM_RIGHTS
  alongside the frame and the body is empty.

The channel is always this process's stdin: a pipe on Windows, an
AF_UNIX SOCK_SEQPACKET socket on POSIX (the supervisor dups its end onto
our fd 0), so the descriptor-passing path has message boundaries to hang
ancillary data on.
"""

from __future__ import annotations

import json
import socket
import sys


def _pin_to_cpu(cpu: int) -> None:
    """R-091 round-robin pinning. sched_setaffinity where it exists;
    SetProcessAffinityMask on Windows."""
    try:
        import os

        if hasattr(os, "sched_setaffinity"):
            os.sched_setaffinity(0, {cpu})
            return
        if sys.platform == "win32":
            import ctypes

            handle = ctypes.c_void_p(-1)  # GetCurrentProcess() pseudo handle
            k32 = ctypes.windll.kernel32
            k32.SetProcessAffinityMask.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
            k32.SetProcessAffinityMask.restype = ctypes.c_int
            k32.SetProcessAffinityMask(handle, 1 << cpu)
    except OSError:
        pass  # pinning is best-effort (R-091)


class Channel:
    """Worker's end of the supervisor channel.

    Windows rides a byte pipe and needs explicit length prefixes; POSIX
    rides SOCK_SEQPACKET, where one ``recv_fds`` yields exactly one frame
    plus any descriptor attached to it. Both expose the same
    ``recv_frame`` contract so the worker loop is platform-blind.
    """

    def __init__(self, sock=None, stream=None):
        self._sock = sock
        self._stream = stream

    @classmethod
    def from_stdin(cls):
        if sys.platform == "win32":
            return cls(stream=sys.stdin.buffer)
        # POSIX: the supervisor dup'd its socketpair end onto our stdin.
        return cls(sock=socket.socket(fileno=0, family=socket.AF_UNIX, type=socket.SOCK_SEQPACKET))

    def _read_exact(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self._stream.read(n - len(buf))
            if not chunk:
                return b""  # EOF
            buf += chunk
        return buf

    def recv_frame(self):
        """Return ``(command, body, fd)``; ``(None, b"", None)`` on EOF."""
        if self._sock is not None:
            data, fds, _flags, _addr = socket.recv_fds(self._sock, 65536, 1)
            if not data:
                return None, b"", None
            payload = data[4:]  # length prefix is redundant here; kept for parity
            fd = fds[0] if fds else None
        else:
            head = self._read_exact(4)
            if not head:
                return None, b"", None
            payload = self._read_exact(int.from_bytes(head, "big"))
            fd = None
        cmd, _, body = payload.partition(b" ")
        return cmd, body, fd

    def close(self):
        try:
            if self._sock is not None:
                self._sock.close()
        except OSError:
            pass


def main() -> int:
    chan = Channel.from_stdin()
    # The header is the one frame that is always plain bytes on both
    # platforms, so it is read through the same path as everything else.
    cmd, body, _fd = chan.recv_frame()
    if cmd != b"HELLO":
        return 1
    header = json.loads(body)
    if header.get("pin") is not None:
        _pin_to_cpu(header["pin"])

    from .config import Config
    from .server import _serve_single, load_app

    app = load_app(header["spec"])
    config = Config(**header["config"])
    _serve_single(
        app,
        "-",  # no listener: connections arrive over the channel
        0,
        config,
        worker_id=header.get("worker_id"),
        control_channel=chan,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
