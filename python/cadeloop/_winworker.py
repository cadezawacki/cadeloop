"""Spawned worker entry for the fork-free worker model (R-090..R-093).

The supervisor (``server._serve_multi_spawn``) writes one JSON header
line followed by ``share_len`` bytes of ``socket.share()`` output
(WSADuplicateSocketW) to our stdin, then keeps the pipe open as the
control channel: b"STOP" (or EOF — a dead supervisor) requests a
graceful drain. Everything else is the ordinary single-worker path.
"""

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
            k32.SetProcessAffinityMask(handle, 1 << cpu)
    except OSError:
        pass  # pinning is best-effort (R-091)


def main() -> int:
    raw = sys.stdin.buffer
    header = json.loads(raw.readline())
    if header["share_len"]:
        share = raw.read(header["share_len"])
        lsock = socket.fromshare(share)  # WSADuplicateSocketW (win32)
    else:
        # POSIX: the listener fd was inherited via pass_fds.
        lsock = socket.socket(fileno=header["listen_fd"])
    if header.get("pin") is not None:
        _pin_to_cpu(header["pin"])

    from .config import Config
    from .server import _serve_single, load_app

    app = load_app(header["spec"])
    config = Config(**header["config"])
    _serve_single(
        app,
        "-",  # host/port unused: the listener is adopted
        0,
        config,
        worker_id=header.get("worker_id"),
        listen_sock=lsock,
        control_reader=raw,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
