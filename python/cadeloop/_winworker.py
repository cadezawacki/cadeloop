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
            k32.SetProcessAffinityMask.restype = ctypes.c_int  # BOOL — was
            # previously left to ctypes' default-int assumption; declaring
            # it explicitly matches loop.py's SetConsoleCtrlHandler call
            # (the codebase's other kernel32 FFI site) rather than leaving
            # this one call implicit. Pinning stays best-effort — a
            # nonzero-vs-zero result isn't acted on — but the return type
            # itself is no longer left to inference.
            k32.SetProcessAffinityMask(handle, 1 << cpu)
    except OSError:
        pass  # pinning is best-effort (R-091)


def _trace(stage: str) -> None:
    # TEMPORARY (see docs/decisions.md): bisecting a Windows-only
    # STATUS_ACCESS_VIOLATION on worker startup that a remote CI run
    # can't hand back a stack trace for. Each line is flushed
    # immediately so it survives a hard crash on the *next* statement.
    print(f"cadeloop._winworker: {stage}", file=sys.stderr, flush=True)


def main() -> int:
    _trace("start")
    raw = sys.stdin.buffer
    header = json.loads(raw.readline())
    _trace("header parsed")
    if header["share_len"]:
        share = raw.read(header["share_len"])
        _trace(f"share bytes read ({len(share)})")
        lsock = socket.fromshare(share)  # WSADuplicateSocketW (win32)
        _trace("fromshare returned")
    else:
        # POSIX: the listener fd was inherited via pass_fds.
        lsock = socket.socket(fileno=header["listen_fd"])
    if header.get("pin") is not None:
        _pin_to_cpu(header["pin"])
        _trace("pinned")

    from .config import Config
    from .server import _serve_single, load_app

    _trace("imports done")
    app = load_app(header["spec"])
    _trace("app loaded")
    config = Config(**header["config"])
    _trace("about to call _serve_single")
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
