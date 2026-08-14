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

import cadeloop
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
        # Distribution is now OURS, not the kernel's: the supervisor accepts
        # centrally and hands each connection to a worker round-robin
        # (ADR-25), so 8 fresh connections across 2 live workers must land
        # on both. Under the shared-listener model this could only be
        # asserted as ">= 1" because the kernel chose.
        assert len(pids) >= 2, f"handoff did not distribute across workers: {pids}"
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


def test_spawn_supervisor_binds_the_requested_family(monkeypatch):
    """The spawn supervisor hardcoded AF_INET, so an IPv6 host failed at
    bind even though the single-worker and fork paths accept it. Reported
    by Codex review on PR #1 (UC-009)."""
    from cadeloop import server

    if not socket.has_ipv6:
        pytest.skip("no IPv6 on this host")
    try:  # has_ipv6 only reports build support; containers often lack it
        socket.socket(socket.AF_INET6, socket.SOCK_STREAM).close()
    except OSError:
        pytest.skip("IPv6 unavailable in this environment")

    seen = {}
    real_socket = socket.socket

    class _Boom(RuntimeError):
        pass

    def fake_spawn(*a, **kw):
        raise _Boom("stop right after the bind")

    def spy(family, type_, proto=0, *a, **kw):
        seen.setdefault("family", family)
        return real_socket(family, type_, proto, *a, **kw)

    monkeypatch.setattr(server._socket, "socket", spy)
    monkeypatch.setattr(server, "_spawn_shared_worker", fake_spawn)
    with pytest.raises(_Boom):
        server._serve_multi_spawn(
            "test_multiworker:_reject_port_zero_app", "::1", 8123, server.Config(workers=2), 2
        )
    assert seen["family"] == socket.AF_INET6


def test_spawn_startup_failure_cleans_up_started_workers(monkeypatch):
    """A failure partway through startup used to leave the children
    already spawned running and the listener bound, because the cleanup
    block had not been entered yet. Reported by Codex review on PR #1
    (UC-008)."""
    from cadeloop import server

    stopped = []
    spawned = []

    class FakeProc:
        returncode = None

        def __init__(self, idx):
            self.idx = idx

        def wait(self, timeout=None):
            stopped.append(self.idx)
            return 0

        def kill(self):
            stopped.append(("kill", self.idx))

    class FakeWorker:
        def __init__(self, idx):
            self.idx = idx
            self.proc = FakeProc(idx)
            self.spawned = time.monotonic()

        def alive(self):
            return True

        def close(self):
            pass

    def fake_spawn(spec, config, idx, ncpu):
        if idx == 1:
            raise RuntimeError("second spawn failed")
        w = FakeWorker(idx)
        spawned.append(w)
        return w

    monkeypatch.setattr(server, "_spawn_shared_worker", fake_spawn)
    monkeypatch.setattr(server, "_send_frame", lambda w, *a, **k: None)

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    with pytest.raises(RuntimeError, match="second spawn failed"):
        server._serve_multi_spawn(
            "test_multiworker:_reject_port_zero_app",
            "127.0.0.1",
            port,
            server.Config(workers=2),
            2,
        )
    assert [w.idx for w in spawned] == [0]
    assert 0 in stopped, "worker 0 was left running after the startup failure"
    # And the listener is gone, so the port is immediately rebindable.
    with socket.socket() as s2:
        s2.bind(("127.0.0.1", port))


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork supervisor is POSIX-only")
def test_replacement_fork_failure_stops_the_survivors(monkeypatch):
    """A worker dies, the replacement fork fails (a process or memory
    limit), and the exception unwound through a `finally` that only
    restored signal dispositions -- leaving every surviving worker
    running, listening and unsupervised while the caller was told the
    server had failed."""
    from cadeloop import server as srv

    spawned = []
    killed = []

    def fake_spawn(app, host, port, config, idx, ncpu, ssl_ctx=None):
        if len(spawned) >= 2:
            raise OSError(11, "Resource temporarily unavailable")
        pid = 101 + len(spawned)
        spawned.append(pid)
        # (pid, ready_fd): the supervisor reads the fd to tell "died
        # before ever serving" from "crashed while serving". A real pipe,
        # not a placeholder, so the close paths are exercised too.
        r, w = os.pipe()
        os.close(w)  # never written: this worker never reaches serving
        os.set_blocking(r, False)
        return pid, r

    reaped = [False]

    def fake_waitpid(pid, flags=0):
        if pid == -1:
            if reaped[0]:
                raise ChildProcessError
            reaped[0] = True
            return (101, 256)  # non-zero exit -> restart path
        return (pid, 0)

    monkeypatch.setattr(srv, "_spawn_worker", fake_spawn)
    monkeypatch.setattr(srv.os, "waitpid", fake_waitpid)
    monkeypatch.setattr(srv.os, "kill", lambda pid, sig: killed.append((pid, sig)))

    with pytest.raises(OSError):
        srv._serve_multi(_noop_app, "127.0.0.1", 8123, cadeloop.Config(), 2)

    assert (102, signal.SIGKILL) in killed, (
        f"surviving worker 102 was left running; kills seen: {killed}"
    )


async def _noop_app(scope, receive, send):  # pragma: no cover - never run
    pass


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork worker model")
def test_a_slow_failing_worker_still_trips_the_crash_loop_guard(monkeypatch):
    """The guard asked only whether a worker died within _CRASH_FAST_SECS
    of being FORKED. An application that fails slowly -- a database
    connection timing out after five seconds is the ordinary case -- reset
    the streak on every death, so the pool restarted it forever and never
    gave up. "Died without ever serving" is the signal that matters, and
    it does not depend on how long the failure took."""
    from cadeloop import server as srv

    spawned = []
    pending = []

    def fake_spawn(app, host, port, config, idx, ncpu, ssl_ctx=None):
        pid = 200 + len(spawned)
        spawned.append(pid)
        r, w = os.pipe()
        os.close(w)  # never written: the worker dies before serving
        os.set_blocking(r, False)
        pending.append(pid)
        return pid, r

    def fake_waitpid(pid, flags=0):
        if pid == -1:
            if not pending:
                raise ChildProcessError
            # Each death looks SLOW: well past _CRASH_FAST_SECS since fork.
            return (pending.pop(0), 256)
        return (pid, 0)

    # Every reap reports a death older than the fast-crash window.
    clock = [0.0]

    def fake_monotonic():
        clock[0] += srv._CRASH_FAST_SECS * 3
        return clock[0]

    monkeypatch.setattr(srv, "_spawn_worker", fake_spawn)
    monkeypatch.setattr(srv.os, "waitpid", fake_waitpid)
    monkeypatch.setattr(srv.os, "kill", lambda pid, sig: None)
    monkeypatch.setattr(srv.time, "monotonic", fake_monotonic)

    with pytest.raises(SystemExit):
        srv._serve_multi(_noop_app, "127.0.0.1", 8124, cadeloop.Config(grace=0.0), 1)

    # Bounded: the guard gave up instead of respawning without end.
    assert len(spawned) <= srv._CRASH_STREAK_LIMIT + 1, (
        f"respawned {len(spawned)} times; the crash-loop guard never engaged"
    )
