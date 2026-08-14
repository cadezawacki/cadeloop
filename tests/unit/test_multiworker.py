"""M3 multi-process worker model (§8, R-090..R-093): SO_REUSEPORT pool
(fork, POSIX-only), supervisor restart, graceful drain, and the
fork-free spawn model (R-090 Windows worker model — also runs on POSIX
via fd inheritance, so it's covered here on every platform, not just
manually via tools/windows/validate.ps1's step 18)."""

import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request

import pytest

APP = """\
import os
async def app(scope, receive, send):
    if scope["type"] != "http":
        return
    await receive()
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": str(os.getpid()).encode()})
"""


def _children_of(pid: int) -> list[int]:
    kids = []
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        try:
            with open(f"/proc/{d}/stat") as f:
                if f.read().split()[3] == str(pid):
                    kids.append(int(d))
        except OSError:
            continue
    return kids


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _get(port: int, timeout: float = 3.0) -> bytes:
    return urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=timeout).read()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork-based worker model + /proc introspection")
def test_worker_pool_balances_restarts_and_drains(tmp_path):
    (tmp_path / "mwapp.py").write_text(APP)
    port = _free_port()
    env = dict(os.environ)
    pkg = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "python"))
    env["PYTHONPATH"] = os.pathsep.join([str(tmp_path), pkg, env.get("PYTHONPATH", "")])
    proc = subprocess.Popen(
        [sys.executable, "-m", "cadeloop", "mwapp:app", "--port", str(port), "--workers", "2"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        # Wait for the pool to come up.
        deadline = time.time() + 10
        while True:
            try:
                _get(port, timeout=1)
                break
            except Exception:  # noqa: BLE001 — startup poll
                if time.time() > deadline:
                    proc.kill()
                    raise AssertionError(f"pool never came up: {proc.stdout.read()[-2000:]}") from None
                time.sleep(0.1)

        kids = _children_of(proc.pid)
        assert len(kids) == 2, f"expected 2 workers, saw {kids}"

        # SO_REUSEPORT balancing: over enough requests both pids answer.
        pids = {_get(port).decode() for _ in range(40)}
        assert len(pids) == 2, f"kernel balanced onto {pids} only"

        # R-092 supervision: SIGKILL a worker; a replacement must appear
        # and the pool must keep serving.
        os.kill(kids[0], signal.SIGKILL)
        deadline = time.time() + 5
        while time.time() < deadline:
            now = _children_of(proc.pid)
            if len(now) == 2 and set(now) != set(kids):
                break
            time.sleep(0.1)
        else:
            raise AssertionError(f"worker not restarted: {_children_of(proc.pid)}")
        assert _get(port)  # still serving

        # Graceful drain: SIGTERM stops supervisor + all workers, exit 0.
        proc.send_signal(signal.SIGTERM)
        out, _ = proc.communicate(timeout=15)
        assert proc.returncode == 0, out[-2000:]
        assert "restarting" in out  # the supervision event was logged
        assert _children_of(proc.pid) == []
    finally:
        if proc.poll() is None:
            proc.kill()


async def _reject_port_zero_app(scope, receive, send):  # pragma: no cover - never runs
    pass


def test_multiworker_rejects_port_zero():
    import cadeloop

    # An import-string app spec (not a bare callable) so this exercises the
    # port==0 check inside _serve_multi/_serve_multi_spawn on every
    # platform — a bare callable would instead hit the (platform-specific,
    # separately tested) "requires an app import string" rejection on
    # non-fork platforms before ever reaching the port check.
    with pytest.raises(ValueError, match="explicit port"):
        cadeloop.serve("test_multiworker:_reject_port_zero_app", "127.0.0.1", 0, workers=2)


def test_spawn_worker_pool_serves_and_stops(tmp_path):
    """The fork-free spawn model (R-090, Windows worker model): one
    supervisor-bound listener handed to spawned workers. On POSIX the
    handoff is fd inheritance instead of WSADuplicateSocketW, so the
    whole supervisor/worker/control-pipe path runs here too."""
    (tmp_path / "mwapp.py").write_text(APP)
    port = _free_port()
    env = dict(os.environ)
    pkg = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "python"))
    env["PYTHONPATH"] = os.pathsep.join([str(tmp_path), pkg, env.get("PYTHONPATH", "")])
    # TEMPORARY (ADR-24): bisecting the Windows worker-model crash. The
    # supervisor process's env propagates to its spawned workers (they
    # inherit os.environ — _spawn_shared_worker's Popen call passes no
    # explicit env=), so setting this once here reaches the worker
    # process the crash actually happens in.
    env["CADELOOP_TRACE_TICK"] = "1"
    driver = (
        "from cadeloop.server import _serve_multi_spawn\n"
        "from cadeloop.config import Config\n"
        f"_serve_multi_spawn('mwapp:app', '127.0.0.1', {port}, "
        "Config(workers=2, grace=5.0), 2)\n"
    )
    proc = subprocess.Popen([sys.executable, "-c", driver], env=env)
    try:
        deadline = time.monotonic() + 10
        pids = set()
        while time.monotonic() < deadline:
            try:
                pids.add(_get(port))
                break
            except OSError:
                time.sleep(0.2)
        else:
            raise AssertionError("spawn worker pool never started serving")
        for _ in range(7):
            pids.add(_get(port))
        assert len(pids) >= 1  # accept distribution is the kernel's call
        assert all(p.isdigit() for p in (b.decode() for b in pids))
        # Supervisor death must cascade: workers see control-pipe EOF and
        # drain out; the port must stop answering.
        proc.terminate()
        proc.wait(timeout=10)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            try:
                _get(port, timeout=0.5)
                time.sleep(0.2)
            except OSError:
                break
        else:
            raise AssertionError("workers kept serving after supervisor death")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
