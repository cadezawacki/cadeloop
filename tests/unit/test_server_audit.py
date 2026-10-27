"""Regression tests for the server / supervisor / CLI / queue audit.

Each test names the finding it pins (S1..S30 in the audit report) and
fails on the code before the fix. Process-model tests drive real worker
pools in subprocesses -- the bugs live in signal delivery, fork and
process lifetime, which an in-process fake cannot reproduce faithfully --
with ephemeral ports, generous timeouts and a kill-everything cleanup.
"""

from __future__ import annotations

import asyncio
import glob
import logging
import os
import signal
import socket
import subprocess
import sys
import textwrap
import threading
import time
import urllib.request
import weakref

import cadeloop
import pytest

PKG_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(cadeloop.__file__)))
needs_fork = pytest.mark.skipif(not hasattr(os, "fork"), reason="fork worker model (POSIX)")
needs_proc = pytest.mark.skipif(not os.path.isdir("/proc/self/task"), reason="/proc introspection")

# One app for the process-model tests. Lifespan shutdown leaves a marker
# file per worker in MARK_DIR; /slow keeps a request in flight across a
# shutdown; /boom crashes the worker the way a segfaulting extension does.
APP = """
import asyncio, json, os
MARK = os.environ.get("MARK_DIR")

def mark(name):
    if MARK:
        open(os.path.join(MARK, f"{name}-{os.getpid()}"), "w").close()

async def app(scope, receive, send):
    if scope["type"] == "lifespan":
        while True:
            m = await receive()
            if m["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            else:
                mark("shutdown")
                await send({"type": "lifespan.shutdown.complete"})
                return
    if scope["type"] != "http":
        return
    await receive()
    path = scope["path"]
    if path == "/slow":
        await asyncio.sleep(1.5)
    elif path == "/boom":
        os._exit(1)
    elif path == "/print":
        print("printed-by", os.getpid())
    elif path == "/stop":
        asyncio.get_running_loop().stop()
    elif path == "/aff":
        inner = await asyncio.get_running_loop().run_in_executor(
            None, lambda: sorted(os.sched_getaffinity(0))
        )
        body = json.dumps({"loop": sorted(os.sched_getaffinity(0)), "executor": inner})
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": body.encode()})
        return
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": str(os.getpid()).encode()})
"""


def _env(**extra):
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in (PKG_ROOT, extra.pop("PYTHONPATH", None), env.get("PYTHONPATH")) if p
    )
    env.update(extra)
    return env


def _run_py(code, *, timeout=60, cwd=None, **env):
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        env=_env(**env),
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


# --------------------------------------------------------------------- #
# cadeloop.Queue (S2, S14, S28)                                          #
# --------------------------------------------------------------------- #


def test_s2_queue_is_usable_from_a_thread_other_than_its_creator():
    """S2: FastQueue was `unsendable`; under panic=abort any call from a
    thread other than the creating one -- a module-level queue driven by a
    loop on a worker thread, or a monitor thread reading qsize() -- killed
    the interpreter with SIGABRT. asyncio.Queue allows both."""
    proc = _run_py(
        """
        import asyncio, gc, threading
        import cadeloop
        q = cadeloop.Queue()                    # built on the main thread
        out = []
        def loop_thread():
            async def main():
                q.put_nowait(1)
                return await q.get()
            out.append(asyncio.run(main()))
        t = threading.Thread(target=loop_thread); t.start(); t.join()
        t = threading.Thread(target=lambda: out.append(q.qsize())); t.start(); t.join()
        made = []
        t = threading.Thread(target=lambda: made.append(cadeloop.Queue())); t.start(); t.join()
        made.clear(); gc.collect()              # last reference dropped on another thread
        print("ok", out)
        """
    )
    assert proc.returncode == 0, (proc.returncode, proc.stderr[-1500:])
    assert "ok [1, 0]" in proc.stdout
    assert "unsendable" not in proc.stderr, proc.stderr[-1500:]


def test_s14_a_second_loop_is_refused_like_asyncio_queue():
    """S14: the queue never bound to a loop, so a getter left parked by an
    earlier (closed) loop swallowed the wake meant for a live getter on
    the next loop, which then hung with an item in the queue. asyncio.Queue
    binds to the first loop that parks and refuses any other."""
    q = cadeloop.Queue()
    loop1 = cadeloop.new_event_loop()
    holder = {}

    async def first():
        holder["t"] = loop1.create_task(q.get())  # referenced: no GC rescue
        await asyncio.sleep(0.01)

    loop1.run_until_complete(first())
    loop1.close()

    async def second():
        g = asyncio.ensure_future(q.get())
        await asyncio.sleep(0.01)
        q.put_nowait("item")
        return await asyncio.wait_for(g, 5.0)

    loop2 = cadeloop.new_event_loop()
    try:
        with pytest.raises(RuntimeError, match="bound to a different event loop"):
            loop2.run_until_complete(second())
    finally:
        loop2.close()


def test_s14_a_failed_wake_is_passed_to_the_next_waiter():
    """S14: wake_next treated a waiter whose set_result failed (its loop
    already closed) as woken and stopped, so the next, live waiter was
    never resolved."""

    class DeadLoopWaiter:
        def done(self):
            return False

        def set_result(self, _value):
            raise RuntimeError("Event loop is closed")

    loop = cadeloop.new_event_loop()
    try:
        fq = cadeloop._core.FastQueue(0)
        live = loop.create_future()
        fq.park_getter(DeadLoopWaiter())
        fq.park_getter(live)
        fq.put_nowait("x")
        assert live.done(), "the live waiter behind a failed wake was never resolved"
    finally:
        loop.close()


def test_s28_queue_parity_with_asyncio_queue():
    """S28: weakref support, generic aliasing, FIFO join wake order and
    stdlib-shaped maxsize values."""
    q = cadeloop.Queue()
    assert weakref.ref(q)() is q
    assert cadeloop.Queue[int] is not None
    for maxsize, fills_at in ((2.5, 3), (2**63, None), (-1, None), (0.5, 1)):
        q = cadeloop.Queue(maxsize)
        assert q.maxsize == maxsize
        if fills_at is not None:
            for i in range(fills_at):
                q.put_nowait(i)
            assert q.full()
        else:
            for i in range(10):
                q.put_nowait(i)
            assert not q.full()

    async def join_order():
        q = cadeloop.Queue()
        q.put_nowait(1)
        order = []

        async def joiner(i):
            await q.join()
            order.append(i)

        tasks = [asyncio.ensure_future(joiner(i)) for i in range(4)]
        await asyncio.sleep(0)
        q.get_nowait()
        q.task_done()
        await asyncio.gather(*tasks)
        return order

    loop = cadeloop.new_event_loop()
    try:
        assert loop.run_until_complete(join_order()) == [0, 1, 2, 3]
    finally:
        loop.close()


# --------------------------------------------------------------------- #
# process-model helpers                                                  #
# --------------------------------------------------------------------- #

posix_only = pytest.mark.skipif(sys.platform == "win32", reason="POSIX process groups and signals")


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _get(port, path="/", timeout=5.0):
    return urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=timeout).read()


def _wait_up(port, deadline=30.0):
    end = time.monotonic() + deadline
    while True:
        try:
            return _get(port, timeout=1.0)
        except Exception:  # noqa: BLE001 — still starting
            if time.monotonic() > end:
                raise AssertionError(f"nothing answered on port {port}") from None
            time.sleep(0.05)


def _wait_all_serving(srv, n, deadline=30.0):
    """Until each of the n workers has answered a request itself -- past
    its startup, with its own signal handlers in place."""
    seen = set()
    end = time.monotonic() + deadline
    while len(seen) < n:
        if time.monotonic() > end:
            raise AssertionError(f"only {len(seen)} of {n} workers ever answered")
        try:
            seen.add(int(_get(srv.port, timeout=2.0)))
        except Exception:  # noqa: BLE001 — still starting
            time.sleep(0.05)
    return seen


def _proc_stat(pid):
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()
    except OSError:
        return None


def _alive(pid):
    st = _proc_stat(pid)
    return st is not None and st[0] != "Z"


def _children_of(pid):
    return [
        int(d)
        for d in os.listdir("/proc")
        if d.isdigit() and (st := _proc_stat(d)) and st[1] == str(pid) and st[0] != "Z"
    ]


def _group_members(pgid):
    return [
        int(d)
        for d in os.listdir("/proc")
        if d.isdigit() and (st := _proc_stat(d)) and st[2] == str(pgid) and st[0] != "Z"
    ]


def _until(predicate, timeout=15.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def _in_flight(port, n, path="/slow"):
    results = []

    def one():
        try:
            results.append(_get(port, path, timeout=20))
        except Exception as exc:  # noqa: BLE001 — the assertion reports it
            results.append(exc)

    threads = [threading.Thread(target=one) for _ in range(n)]
    for t in threads:
        t.start()
    time.sleep(0.4)  # let them reach the server
    return threads, results


class _Server:
    """A server process -- and its whole process group -- under test.
    ``code`` runs with PORT substituted; otherwise the CLI runs APP."""

    def __init__(self, tmp_path, args=(), *, code=None, env=None, cwd=None, port=None):
        (tmp_path / "auditapp.py").write_text(APP)
        self.marks = tmp_path / "marks"
        self.marks.mkdir(exist_ok=True)
        self.port = port or _free_port()
        if code is not None:
            cmd = [sys.executable, "-c", textwrap.dedent(code).replace("PORT", str(self.port))]
        else:
            cmd = [sys.executable, "-m", "cadeloop", "auditapp:app", "--port", str(self.port), *args]
        self.proc = subprocess.Popen(
            cmd,
            env=_env(PYTHONPATH=str(tmp_path), MARK_DIR=str(self.marks), **(env or {})),
            cwd=str(cwd or tmp_path),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        self._out = []
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()

    def _read(self):
        for line in self.proc.stdout:
            self._out.append(line)

    @property
    def output(self):
        return "".join(self._out)

    def workers(self):
        return _children_of(self.proc.pid)

    def shutdown_markers(self):
        return len(glob.glob(str(self.marks / "shutdown-*")))

    def wait(self, timeout=30):
        rc = self.proc.wait(timeout)
        self._reader.join(5)
        return rc

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        self.proc.wait(10)
        self._reader.join(5)
        self.proc.stdout.close()


SPAWN_POOL = """
    from cadeloop.server import _serve_multi_spawn
    from cadeloop.config import Config
    _serve_multi_spawn("auditapp:app", "127.0.0.1", PORT, Config(workers=2, grace=GRACE), 2)
"""


# --------------------------------------------------------------------- #
# S1: stop signals during the drain                                      #
# --------------------------------------------------------------------- #


@posix_only
@needs_fork
@needs_proc
def test_s1_ctrl_c_to_the_process_group_drains_every_worker(tmp_path):
    """S1: Ctrl-C reaches the terminal's whole process group, so each worker
    got the SIGINT itself (its handler overrode the SIG_IGN the supervisor
    gave it) and then the supervisor's forwarded SIGTERM -- which hit SIG_DFL
    because the handlers were restored before the drain. Measured: 0-1 of 4
    workers ran lifespan.shutdown and every in-flight request failed."""
    with _Server(tmp_path, ["-w", "2", "--grace", "10"]) as srv:
        _wait_all_serving(srv, 2)
        threads, results = _in_flight(srv.port, 4)
        os.killpg(srv.proc.pid, signal.SIGINT)
        for t in threads:
            t.join(30)
        assert srv.wait() == 0, srv.output
    assert all(isinstance(r, bytes) for r in results), results
    assert srv.shutdown_markers() == 2, srv.output


@posix_only
@needs_fork
@needs_proc
def test_s1_a_repeated_sigterm_during_the_drain_is_not_fatal(tmp_path):
    """S1, deterministically: systemd's KillMode=control-group SIGTERMs
    every process of the unit, then the supervisor forwards its own. The
    supervisor is held stopped so that forwarded TERM lands while the
    workers are already draining."""
    with _Server(tmp_path, ["-w", "2", "--grace", "10"]) as srv:
        kids = sorted(_wait_all_serving(srv, 2))
        threads, results = _in_flight(srv.port, 4)
        os.kill(srv.proc.pid, signal.SIGSTOP)
        for pid in [*kids, srv.proc.pid]:
            os.kill(pid, signal.SIGTERM)
        time.sleep(0.5)
        os.kill(srv.proc.pid, signal.SIGCONT)
        for t in threads:
            t.join(30)
        assert srv.wait() == 0, srv.output
    assert all(isinstance(r, bytes) for r in results), results
    assert srv.shutdown_markers() == 2, srv.output


@posix_only
@needs_proc
def test_s1_single_process_second_sigterm_during_the_drain(tmp_path):
    """S1: a lone server's drain ran with SIGTERM back at SIG_DFL; a second
    SIGTERM killed it (exit -15) before lifespan.shutdown."""
    with _Server(tmp_path, ["--grace", "10"]) as srv:
        _wait_up(srv.port)
        threads, results = _in_flight(srv.port, 3)
        srv.proc.send_signal(signal.SIGTERM)
        time.sleep(0.4)
        srv.proc.send_signal(signal.SIGTERM)
        for t in threads:
            t.join(30)
        assert srv.wait() == 0, srv.output
    assert all(isinstance(r, bytes) for r in results), results
    assert srv.shutdown_markers() == 1


def test_s1_repeated_requests_escalate_only_for_a_person_in_a_single_process():
    from cadeloop.server import _Shutdown

    class FakeLoop:
        stops = 0

        def stop(self):
            self.stops += 1

    worker_loop = FakeLoop()
    sd = _Shutdown(worker_loop, interactive=False)
    sd.phase = "serving"
    sd.on_signal(signal.SIGTERM)
    assert sd.reason == "signal" and worker_loop.stops == 1
    sd.phase = "draining"
    sd.on_signal(signal.SIGTERM)
    sd.on_signal(signal.SIGINT)
    assert worker_loop.stops == 1 and not sd.hurry, "a pool worker's supervisor owns the deadline"

    lone_loop = FakeLoop()
    sd = _Shutdown(lone_loop, interactive=True)
    sd.phase = "serving"
    sd.on_signal(signal.SIGINT)
    sd.phase = "draining"
    sd.on_signal(signal.SIGTERM)
    assert not sd.hurry, "a repeated SIGTERM never escalates"
    sd.on_signal(signal.SIGINT)
    assert sd.hurry, "a second Ctrl-C stops waiting for in-flight requests"
    sd.phase = "closing"
    sd.on_signal(signal.SIGINT)
    assert lone_loop.stops == 2, "a third abandons the lifespan wait"


# --------------------------------------------------------------------- #
# S3, S7, S11, S27: the supervisor's signals and children                #
# --------------------------------------------------------------------- #


@posix_only
@needs_fork
@needs_proc
def test_s3_workers_stop_when_their_supervisor_is_killed(tmp_path):
    """S3: nothing told a fork worker its supervisor was gone; after a
    SIGKILL (or the OOM killer) the workers served the port for good."""
    with _Server(tmp_path, ["-w", "2"]) as srv:
        kids = sorted(_wait_all_serving(srv, 2))
        os.kill(srv.proc.pid, signal.SIGKILL)
        srv.proc.wait(10)
        assert _until(lambda: not any(_alive(k) for k in kids)), "workers outlived their supervisor"
        assert srv.shutdown_markers() == 2, "they should stop gracefully, not just die"


@posix_only
@needs_fork
@needs_proc
def test_s3_sigterm_while_the_pool_is_forking_leaves_no_orphans(tmp_path):
    """S3: the supervisor's handlers were installed only after every worker
    was forked; a SIGTERM in between killed it outright (SIG_DFL) and the
    workers forked so far kept serving with no supervisor."""
    code = """
        import os, signal
        forks = [0]
        def before():
            forks[0] += 1
            if forks[0] == 2:
                os.kill(os.getpid(), signal.SIGTERM)
        os.register_at_fork(before=before)
        import cadeloop
        cadeloop.serve("auditapp:app", "127.0.0.1", PORT, workers=3, grace=5.0)
    """
    with _Server(tmp_path, code=code) as srv:
        assert srv.wait() == 0, srv.output
        assert _until(lambda: not _group_members(srv.proc.pid), 10), "orphaned workers"


@posix_only
@needs_fork
@needs_proc
def test_s7_a_pool_from_a_non_main_thread_is_refused_before_forking(tmp_path):
    """S7: the pool was forked first and signal.signal() raised after, so
    serve(workers=2) from a thread raised while two workers kept serving."""
    code = """
        import os, threading, time, cadeloop
        err = {}
        def run():
            try:
                cadeloop.serve("auditapp:app", "127.0.0.1", PORT, workers=2)
            except BaseException as e:
                err["e"] = e
        t = threading.Thread(target=run)
        t.start()
        t.join(30)
        time.sleep(0.5)
        print("raised", type(err.get("e")).__name__)
    """
    with _Server(tmp_path, code=code) as srv:
        assert srv.wait() == 0
        assert "raised RuntimeError" in srv.output, srv.output
        assert not _group_members(srv.proc.pid)


@posix_only
@needs_fork
@needs_proc
def test_s11_a_stop_signal_racing_a_restart_still_reaches_the_new_worker(tmp_path):
    """S11: a signal handled between fork() and recording the child ran
    _forward without it; the new worker was never told to stop and was
    SIGKILLed `grace` later without its lifespan shutdown."""
    code = """
        import os, signal
        forks = [0]
        def after_in_parent():
            forks[0] += 1
            if forks[0] == 3:  # 1-2: the pool; 3: the replacement
                os.kill(os.getpid(), signal.SIGTERM)
        os.register_at_fork(after_in_parent=after_in_parent)
        import cadeloop
        cadeloop.serve("auditapp:app", "127.0.0.1", PORT, workers=2, grace=8.0)
    """
    with _Server(tmp_path, code=code) as srv:
        os.kill(min(_wait_all_serving(srv, 2)), signal.SIGKILL)
        t0 = time.monotonic()
        assert srv.wait() == 0, srv.output
        elapsed = time.monotonic() - t0
    assert "exceeded grace" not in srv.output, srv.output
    assert elapsed < 6.0, f"{elapsed:.1f}s: the replacement waited out grace"
    assert srv.shutdown_markers() == 2


@posix_only
@needs_fork
@needs_proc
def test_s27_workers_are_still_supervised_when_sigchld_is_ignored(tmp_path):
    """S27: with SIGCHLD at SIG_IGN the kernel reaps children itself, so
    no death was ever seen: nothing was restarted, and once the last
    worker died serve() returned as though asked to stop."""
    code = """
        import signal, cadeloop
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        cadeloop.serve("auditapp:app", "127.0.0.1", PORT, workers=2)
    """
    with _Server(tmp_path, code=code) as srv:
        victim = min(_wait_all_serving(srv, 2))
        os.kill(victim, signal.SIGKILL)
        assert _until(lambda: len(srv.workers()) == 2 and victim not in srv.workers()), srv.output
        assert srv.proc.poll() is None


@posix_only
@needs_fork
def test_s15_the_supervisor_reaps_only_its_own_children(tmp_path):
    """S15: waitpid(-1) reaped the embedding application's own children;
    Popen.wait() then reported 0 for a job that had exited 7."""
    code = """
        import os, signal, subprocess, threading, cadeloop
        job = subprocess.Popen(["sh", "-c", "sleep 0.3; exit 7"])
        threading.Timer(2.5, lambda: os.kill(os.getpid(), signal.SIGTERM)).start()
        cadeloop.serve("auditapp:app", "127.0.0.1", PORT, workers=2, grace=3.0)
        print("job exit", job.wait())
    """
    with _Server(tmp_path, code=code) as srv:
        assert srv.wait() == 0, srv.output
    assert "job exit 7" in srv.output, srv.output


# --------------------------------------------------------------------- #
# S6, S12, S13, S22: pool behaviour                                      #
# --------------------------------------------------------------------- #


@posix_only
@needs_fork
@needs_proc
def test_s6_a_second_pool_on_a_live_port_is_refused(tmp_path):
    """S6: every worker binds with SO_REUSEPORT, so a second pool on the
    same port started without error and the kernel split the traffic."""
    with _Server(tmp_path, ["-w", "2"]) as first:
        _wait_up(first.port)
        with _Server(tmp_path, ["-w", "2"], port=first.port) as second:
            rc = second.wait(30)
            assert rc != 0 and "already in use" in second.output, second.output
        owners = {_get(first.port) for _ in range(20)}
        assert owners <= {str(p).encode() for p in first.workers()}


@posix_only
@needs_fork
def test_s12_worker_output_is_flushed_when_a_worker_exits(tmp_path):
    """S12: workers ended with os._exit() and never flushed stdio, so with
    stdout on a pipe (systemd, docker) everything they printed was lost."""
    with _Server(tmp_path, ["-w", "2"], env={"PYTHONUNBUFFERED": ""}) as srv:
        _wait_up(srv.port)
        for _ in range(4):
            _get(srv.port, "/print")
        srv.proc.send_signal(signal.SIGTERM)
        assert srv.wait() == 0
    assert srv.output.count("printed-by") == 4, srv.output


@posix_only
@needs_fork
@needs_proc
def test_s13_one_off_crashes_on_healthy_workers_do_not_stop_the_pool(tmp_path):
    """S13: one crash streak for the whole pool, and a worker that had
    served still counted unless it lived 30 s: five one-off crashes (a
    request that segfaults an extension, retried) stopped the server."""
    with _Server(tmp_path, ["-w", "3"]) as srv:
        _wait_all_serving(srv, 3)
        for _ in range(6):
            time.sleep(1.2)  # every worker has served past the fast-crash window
            try:
                _get(srv.port, "/boom", timeout=3)
            except Exception:  # noqa: BLE001 — the worker died mid-request
                pass
        assert srv.proc.poll() is None, srv.output
        assert _get(srv.port)


def test_s13_the_restart_rule_resets_after_a_worker_served():
    from cadeloop import server as srv

    n = srv._crash_streak_next
    assert n(3, became_ready=True, uptime=srv._CRASH_FAST_SECS + 0.5) == 1
    assert n(3, became_ready=True, uptime=srv._CRASH_FAST_SECS / 2) == 4
    assert n(3, became_ready=False, uptime=1000.0) == 4
    assert n(3, became_ready=None, uptime=srv._STABLE_SECS - 1) == 4


@posix_only
@needs_fork
@needs_proc
def test_s22_signalling_one_worker_replaces_it(tmp_path):
    """S22: any clean worker exit stopped the pool, so recycling one
    bloated worker with SIGTERM shut the whole server down."""
    with _Server(tmp_path, ["-w", "2"]) as srv:
        victim = min(_wait_all_serving(srv, 2))
        os.kill(victim, signal.SIGTERM)
        assert _until(lambda: len(srv.workers()) == 2 and victim not in srv.workers()), srv.output
        assert srv.proc.poll() is None
        assert _get(srv.port)


@posix_only
@needs_fork
def test_s22_the_application_stopping_its_loop_still_stops_the_pool(tmp_path):
    with _Server(tmp_path, ["-w", "2"]) as srv:
        _wait_up(srv.port)
        try:
            _get(srv.port, "/stop", timeout=3)
        except Exception:  # noqa: BLE001 — the loop stopped under the request
            pass
        assert srv.wait() == 0, srv.output


# --------------------------------------------------------------------- #
# S4, S5, S9, S16: the spawn (Windows) model, driven on POSIX            #
# --------------------------------------------------------------------- #


@posix_only
def test_s4_a_transient_accept_error_does_not_end_accepting(tmp_path):
    """S4: any OSError from accept() ended the accept thread for good while
    the supervisor and workers stayed up. Windows raises WSAECONNRESET from
    accept() for a client that resets early; injected here."""
    code = """
        import socket
        real = socket.socket.accept
        calls = [0]
        def flaky(self):
            calls[0] += 1
            if calls[0] == 2:
                raise ConnectionResetError(10054, "reset before accept (injected)")
            return real(self)
        socket.socket.accept = flaky
    """
    code = textwrap.dedent(code) + textwrap.dedent(SPAWN_POOL.replace("GRACE", "2.0"))
    with _Server(tmp_path, code=code) as srv:
        _wait_up(srv.port)
        try:
            _get(srv.port, timeout=2)
        except Exception:  # noqa: BLE001 — the connection the error was for
            pass
        for _ in range(3):
            assert _get(srv.port).isdigit()


@posix_only
@needs_proc
def test_s5_a_stalled_spawn_worker_blocks_neither_accepting_nor_shutdown(tmp_path):
    """S5: handoffs were blocking writes on the single accept thread. One
    worker that stopped reading filled its channel and stopped accepting
    for every worker; the STOP frame then blocked on the same channel, so
    Ctrl-C never reached the grace deadline."""
    with _Server(tmp_path, code=SPAWN_POOL.replace("GRACE", "2.0")) as srv:
        stalled, healthy = sorted(_wait_all_serving(srv, 2))
        os.kill(stalled, signal.SIGSTOP)
        held = []
        try:
            for _ in range(1200):  # past the stalled worker's channel + queue
                s = socket.create_connection(("127.0.0.1", srv.port), timeout=5)
                s.sendall(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
                held.append(s)
            # The healthy worker gets the next connection once it has caught
            # up with the burst (it is behind, not stuck).
            answer = []

            def answered():
                try:
                    answer.append(_get(srv.port, timeout=5))
                    return True
                except OSError:
                    return False

            assert _until(answered, 20), "the healthy worker never got a connection"
            assert answer[-1] == str(healthy).encode()
            t0 = time.monotonic()
            srv.proc.send_signal(signal.SIGINT)
            srv.wait(30)
            assert time.monotonic() - t0 < 10, "shutdown did not honour grace=2.0"
        finally:
            try:
                os.kill(stalled, signal.SIGCONT)
            except ProcessLookupError:
                pass
            for s in held:
                s.close()


@posix_only
@needs_proc
def test_s16_supervisor_death_does_not_cut_a_draining_spawn_worker(tmp_path):
    """S16: EOF on the channel called loop.stop() whatever the worker was
    doing; a worker already draining on its own SIGTERM had its drain cut
    short when the supervisor died, failing every in-flight request."""
    with _Server(tmp_path, code=SPAWN_POOL.replace("GRACE", "8.0")) as srv:
        _wait_up(srv.port)
        threads, results = _in_flight(srv.port, 4)
        os.killpg(srv.proc.pid, signal.SIGTERM)
        for t in threads:
            t.join(30)
    assert all(isinstance(r, bytes) for r in results), results


@posix_only
def test_s9_spawned_workers_import_from_the_supervisors_path(tmp_path):
    """S9: a spawned worker is `python -m cadeloop._winworker` and saw only
    its working directory and PYTHONPATH: an app the supervisor imported
    from its launcher's directory (a Windows service runs in System32)
    failed to import in every worker."""
    svc = tmp_path / "svc"
    svc.mkdir()
    (svc / "svcapp.py").write_text(APP)
    (svc / "run.py").write_text(
        "import sys\n"
        "from cadeloop.server import _serve_multi_spawn\n"
        "from cadeloop.config import Config\n"
        "_serve_multi_spawn('svcapp:app', '127.0.0.1', int(sys.argv[1]), "
        "Config(workers=2, grace=2.0), 2)\n"
    )
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, str(svc / "run.py"), str(port)],
        env=_env(),
        cwd=str(elsewhere),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        _wait_up(port, deadline=15)
    finally:
        os.killpg(proc.pid, signal.SIGKILL)
        _, err = proc.communicate(timeout=10)
    assert "ModuleNotFoundError" not in err, err[-1500:]


# --------------------------------------------------------------------- #
# S10, S18, S19: GC and CPU placement of forked workers                  #
# --------------------------------------------------------------------- #

BIG_HEAP_APP = """
import os
HEAP = [[i, str(i)] for i in range(400_000)]  # ~60 MB of import-time objects

async def app(scope, receive, send):
    if scope["type"] != "http":
        return
    await receive()
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": str(os.getpid()).encode()})
"""


def _private_dirty_mb(pid):
    with open(f"/proc/{pid}/smaps_rollup") as f:
        for line in f:
            if line.startswith("Private_Dirty:"):
                return int(line.split()[1]) / 1024
    return 0.0


@posix_only
@needs_fork
@pytest.mark.skipif(not os.path.exists("/proc/self/smaps_rollup"), reason="smaps_rollup")
def test_s10_workers_share_the_supervisors_heap(tmp_path):
    """S10: each worker ran its own full gc.collect() after fork, writing the
    GC header of every inherited object and so copying the whole heap: 193
    MB private per worker for a ~190 MB heap. Frozen in the supervisor
    before forking, the inherited objects are never touched."""
    (tmp_path / "bigapp.py").write_text(BIG_HEAP_APP)
    with _Server(tmp_path, code="""
        import cadeloop
        cadeloop.serve("bigapp:app", "127.0.0.1", PORT, workers=2)
    """) as srv:
        _wait_up(srv.port)
        time.sleep(1.0)
        private = [_private_dirty_mb(pid) for pid in srv.workers()]
    assert private and max(private) < 30, f"worker private memory (MB): {private}"


@posix_only
@needs_fork
@needs_proc
@pytest.mark.skipif(not hasattr(os, "sched_getaffinity") or len(os.sched_getaffinity(0)) < 3,
                    reason="needs 3+ CPUs to restrict")
def test_s18_s19_pinning_stays_inside_the_affinity_mask_and_off_helper_threads(tmp_path):
    """S18: workers were pinned to idx % os.cpu_count() -- outside a taskset
    or cpuset mask -- and workers=0 counted the machine, not the mask.
    S19: pinning the worker pinned every thread it later started too."""
    allowed = sorted(os.sched_getaffinity(0))[-2:]
    code = f"""
        import os, cadeloop
        os.sched_setaffinity(0, {set(allowed)!r})
        cadeloop.serve("auditapp:app", "127.0.0.1", PORT, workers=0, pin=True)
    """
    with _Server(tmp_path, code=code) as srv:
        _wait_up(srv.port)
        assert len(srv.workers()) == 2, srv.output
        import json

        seen = [json.loads(_get(srv.port, "/aff")) for _ in range(10)]
    for report in seen:
        assert len(report["loop"]) == 1 and report["loop"][0] in allowed, report
        assert report["executor"] == allowed, report


def test_s19_pinning_is_opt_in():
    assert cadeloop.Config().pin is False


# --------------------------------------------------------------------- #
# S17, S21, S23, S26, S29: the single-worker lifecycle (in process)      #
# --------------------------------------------------------------------- #


def _serve_in_thread(app, config, **kw):
    """Run _serve_single on a thread (no signal handlers there) and return
    (thread, result) -- result gets "reason" or "error"."""
    from cadeloop.server import _serve_single

    result = {}

    def run():
        try:
            result["reason"] = _serve_single(app, "127.0.0.1", kw.pop("port", 0), config, **kw)
        except BaseException as exc:  # noqa: BLE001
            result["error"] = exc

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t, result


def test_s17_leftover_tasks_and_async_generators_are_finalized():
    """S17: teardown went straight to loop.close(): a background task's
    `finally` and an async generator's cleanup never ran."""
    ran = []

    async def ticker():
        try:
            while True:
                yield 1
                await asyncio.sleep(0.01)
        finally:
            ran.append("agen")

    async def background():
        try:
            async for _ in ticker():
                pass
        finally:
            ran.append("task")

    async def app(scope, receive, send):
        if scope["type"] == "lifespan":
            await receive()
            lp = asyncio.get_running_loop()
            lp.create_task(background())
            await send({"type": "lifespan.startup.complete"})
            lp.call_later(0.3, lp.stop)
            await receive()
            await send({"type": "lifespan.shutdown.complete"})

    t, result = _serve_in_thread(app, cadeloop.Config(grace=1.0))
    t.join(30)
    assert not t.is_alive() and "error" not in result, result
    assert sorted(ran) == ["agen", "task"], ran


def test_s26_serve_restores_the_callers_current_event_loop():
    """S26: serve() set the thread's current loop to None on return,
    clobbering the embedder's own."""
    out = {}

    async def app(scope, receive, send):
        if scope["type"] == "lifespan":
            await receive()
            await send({"type": "lifespan.startup.complete"})
            asyncio.get_running_loop().call_later(0.1, asyncio.get_running_loop().stop)
            await receive()
            await send({"type": "lifespan.shutdown.complete"})

    def run():
        from cadeloop.server import _serve_single

        mine = asyncio.new_event_loop()
        asyncio.set_event_loop(mine)
        try:
            _serve_single(app, "127.0.0.1", 0, cadeloop.Config(grace=0.0))
            out["same"] = asyncio.get_event_loop_policy().get_event_loop() is mine
        finally:
            asyncio.set_event_loop(None)
            mine.close()

    t = threading.Thread(target=run)
    t.start()
    t.join(30)
    assert out.get("same") is True


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)


@pytest.fixture()
def cadeloop_logs():
    cap = _Capture()
    lg = logging.getLogger("cadeloop")
    old = lg.level
    lg.addHandler(cap)
    lg.setLevel(logging.DEBUG)
    try:
        yield cap.records
    finally:
        lg.removeHandler(cap)
        lg.setLevel(old)


def test_s21_a_startup_that_raises_after_receiving_startup_is_not_silent(cadeloop_logs):
    """S21a: it was taken for "no lifespan support" and logged at DEBUG;
    the server served without what startup was meant to build."""

    async def app(scope, receive, send):
        if scope["type"] == "lifespan":
            await receive()
            asyncio.get_running_loop().call_later(0.2, asyncio.get_running_loop().stop)
            raise ConnectionRefusedError("database unreachable")

    t, result = _serve_in_thread(app, cadeloop.Config(grace=0.0))
    t.join(30)
    warned = [r for r in cadeloop_logs if r.levelno >= logging.WARNING and "lifespan" in r.getMessage()]
    assert warned and warned[0].exc_info, [r.getMessage() for r in cadeloop_logs]


def test_s21_no_shutdown_message_after_startup_failed():
    """S21b: an app that reported startup.failed was still sent
    lifespan.shutdown (uvicorn does not)."""
    got = []

    async def app(scope, receive, send):
        if scope["type"] == "lifespan":
            await receive()
            await send({"type": "lifespan.startup.failed", "message": "bad config"})
            got.append((await receive())["type"])

    t, result = _serve_in_thread(app, cadeloop.Config(grace=0.0))
    t.join(30)
    assert isinstance(result.get("error"), RuntimeError) and "bad config" in str(result["error"])
    assert got == [], got


def test_s21_a_failing_shutdown_hook_is_reported_once(cadeloop_logs):
    """S21c: an exception from the shutdown hook went down the
    crash-while-serving path: "lifespan task crashed", a loop.stop() under
    shutdown()'s wait, and a false "lifespan shutdown timed out"."""

    async def app(scope, receive, send):
        if scope["type"] == "lifespan":
            await receive()
            await send({"type": "lifespan.startup.complete"})
            asyncio.get_running_loop().call_later(0.2, asyncio.get_running_loop().stop)
            await receive()
            try:
                raise OSError("pool.close() failed")
            except OSError as exc:
                await send({"type": "lifespan.shutdown.failed", "message": repr(exc)})
                raise

    t, result = _serve_in_thread(app, cadeloop.Config(grace=0.0))
    t.join(30)
    messages = [r.getMessage() for r in cadeloop_logs if r.levelno >= logging.WARNING]
    assert any("lifespan shutdown failed" in m for m in messages), messages
    assert not any("crashed" in m or "timed out" in m for m in messages), messages


def test_s23_warmup_freezes_what_the_first_requests_built():
    """S23: `warmup` was documented and validated but read by nothing; the
    heap was frozen before the first request, so caches applications build
    lazily stayed in the collected generations (a full collection walking a
    300k-entry cache took ~80 ms; frozen, 0.4 ms)."""
    import gc

    if gc.get_freeze_count():
        pytest.skip("test needs a clean permanent generation")
    cache = {}
    seen = {}

    async def app(scope, receive, send):
        if scope["type"] != "http":
            return
        await receive()
        if scope["path"] == "/stop":
            seen["frozen"] = gc.get_freeze_count()
            asyncio.get_running_loop().stop()
        elif not cache:
            cache.update({i: [i] for i in range(50_000)})
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    port = _free_port()
    t, result = _serve_in_thread(app, cadeloop.Config(grace=0.0, warmup=5), port=port)
    try:
        _wait_up(port)
        startup_frozen = gc.get_freeze_count()
        for _ in range(8):
            _get(port)
        time.sleep(0.2)
        try:
            _get(port, "/stop", timeout=3)
        except Exception:  # noqa: BLE001
            pass
    finally:
        t.join(30)
    assert seen["frozen"] - startup_frozen >= 50_000, (startup_frozen, seen)
    assert gc.get_freeze_count() == 0, "serve() must still unfreeze on the way out"


def test_s23_reuse_scope_is_gone():
    with pytest.raises(TypeError):
        cadeloop.Config(reuse_scope=True)


def test_s29_every_resolved_address_gets_a_listener(monkeypatch):
    """S29: binding stopped at the first candidate, so a name resolving to
    several addresses (localhost on a dual-stack host) got one listener.
    An address the host cannot use at all (::1 without IPv6) is skipped."""
    from cadeloop import server as srv

    monkeypatch.setattr(
        srv,
        "_bind_candidates",
        lambda host, port: [("127.0.0.1", 0, 0), ("127.0.0.2", 0, 0), ("::1", 0, 0)],
    )
    loop = cadeloop.new_event_loop()
    try:

        async def app(scope, receive, send):  # pragma: no cover - never called
            pass

        lifespan = srv._Lifespan(app, loop)
        lids, first = srv._listen_all(
            loop, "multi", 0, app, lifespan, cadeloop.Config(), reuse_port=False, ssl_ctx=None
        )
        try:
            assert len(lids) >= 2
            for ip in ("127.0.0.1", "127.0.0.2"):
                socket.create_connection((ip, first[1]), timeout=2).close()  # same port on both
        finally:
            for lid in lids:
                loop._core.listener_close(lid)
    finally:
        loop.close()


# --------------------------------------------------------------------- #
# S20: access log                                                        #
# --------------------------------------------------------------------- #


class _StubCore:
    def __init__(self):
        self.sinks = []

    def set_access_log(self, sink):
        self.sinks.append(sink)


class _StubLoop:
    def __init__(self):
        self._core = _StubCore()
        self.soon = []

    def is_closed(self):
        return False

    def call_later(self, _delay, _cb):
        return type("H", (), {"cancel": lambda self: None})()

    def call_soon(self, cb, *args):
        self.soon.append((cb, args))


def test_s20_no_per_request_sink_while_the_access_logger_is_disabled():
    """S20: with access_log on but its logger not enabled for INFO (the
    README's own default) the engine still called a sink per request just
    to discard the record: -26% throughput. Nothing is installed now."""
    from cadeloop.server import _AccessLog, _RequestHook

    lg = logging.getLogger("cadeloop.access.test-s20")
    lg.setLevel(logging.WARNING)
    log = _AccessLog(lg)
    try:
        hook = _RequestHook(_StubLoop())
        hook.attach_access_log(log)
        hook.start()
        assert hook._loop._core.sinks in ([], [None]), hook._loop._core.sinks
        lg.setLevel(logging.INFO)
        hook._refresh()  # what the once-a-second timer does
        assert hook._loop._core.sinks[-1] == log.sink
        hook.stop()
        assert hook._loop._core.sinks[-1] is None
    finally:
        log.close()


def test_s20_the_warmup_counter_uninstalls_itself():
    from cadeloop.server import _RequestHook

    loop = _StubLoop()
    warm = []
    hook = _RequestHook(loop)
    hook.count_requests(3, lambda: warm.append(True))
    hook.start()
    sink = loop._core.sinks[-1]
    for _ in range(3):
        sink(None, "GET", b"/", 200, 0.1)
    for cb, args in loop.soon:
        cb(*args)
    assert warm == [True] and loop._core.sinks[-1] is None


def test_s20_access_log_records_reach_the_handler_with_ipv6_brackets():
    from cadeloop.server import _AccessLog

    lg = logging.getLogger("cadeloop.access.test-s20-emit")
    lg.setLevel(logging.INFO)
    lg.propagate = False
    cap = _Capture()
    lg.addHandler(cap)
    log = _AccessLog(lg)
    log.sink(("::1", 5555), "GET", b"/x", 200, 1.5)
    log.sink(("127.0.0.1", 6666), "POST", b"/y", 201, 2.5)
    log.close()
    lines = [r.getMessage() for r in cap.records]
    assert lines == ['[::1]:5555 "GET /x" 200 1.50ms', '127.0.0.1:6666 "POST /y" 201 2.50ms'], lines


# --------------------------------------------------------------------- #
# S8, S24, S25: CLI and Config                                           #
# --------------------------------------------------------------------- #


@posix_only
def test_s8_the_console_script_imports_the_app_from_the_current_directory(tmp_path):
    """S8: the installed `cadeloop` script runs with its own directory on
    sys.path, not the current one, so `cadeloop myapp:app` -- the README's
    first example -- could not import myapp."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    script = bindir / "cadeloop"
    script.write_text(
        f"#!{sys.executable}\n"
        "import re, sys\n"
        "from cadeloop.__main__ import main\n"
        "if __name__ == '__main__':\n"
        "    sys.argv[0] = re.sub(r'(-script\\.pyw|\\.exe)?$', '', sys.argv[0])\n"
        "    sys.exit(main())\n"
    )
    script.chmod(0o755)
    project = tmp_path / "project"
    project.mkdir()
    (project / "myapp.py").write_text(APP)
    port = _free_port()
    proc = subprocess.Popen(
        [str(script), "myapp:app", "--port", str(port)],
        env=_env(),
        cwd=str(project),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        _wait_up(port, deadline=15)
    finally:
        os.killpg(proc.pid, signal.SIGKILL)
        _, err = proc.communicate(timeout=10)
    assert "ModuleNotFoundError" not in err


@pytest.mark.parametrize(
    "kwargs, exc",
    [
        ({"access_log": "false"}, TypeError),
        ({"pin": "false"}, TypeError),
        ({"immediate_flush": "false"}, TypeError),
        ({"workers": "4"}, TypeError),
        ({"workers": True}, TypeError),
        ({"grace": "10"}, TypeError),
        ({"accept_pool": 1.5}, TypeError),
        ({"stats_endpoint": True}, TypeError),
        ({"max_body": 2**64}, ValueError),
        ({"rio_cq_size": 2**32}, ValueError),
    ],
)
def test_s24_config_rejects_wrong_types_and_native_overflows(kwargs, exc):
    """S24: a string "false" was stored and read as true (access_log="false"
    turned the access log on), and values past the native integer types
    failed only inside startup, after lifespan startup had run."""
    with pytest.raises(exc):
        cadeloop.Config(**kwargs)


def test_s24_config_normalises_numbers():
    c = cadeloop.Config(grace=3, workers=2)
    assert c.grace == 3.0 and isinstance(c.grace, float) and c.workers == 2


def test_s25_cli_flags_match_the_readme(monkeypatch):
    """S25: --no-access-log did not exist although every boolean was
    documented with a --no- form; --gc-mode accepted any string and
    --port any integer, both failing later with a traceback."""
    import contextlib
    import io

    import cadeloop.__main__ as cli

    seen = {}
    monkeypatch.setattr(cli, "serve", lambda app, host, port, **kw: seen.update(kw, port=port))
    monkeypatch.setattr(cli, "load_app", lambda spec: None)
    assert cli.main(["m:a", "--no-access-log"]) == 0 and seen["access_log"] is False
    assert cli.main(["m:a", "--access-log"]) == 0 and seen["access_log"] is True
    for bad in (["--gc-mode", "bogus"], ["--port", "70000"]):
        with pytest.raises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            cli.main(["m:a", *bad])


# --------------------------------------------------------------------- #
# S30: smaller leaks and rough edges                                     #
# --------------------------------------------------------------------- #


@posix_only
@needs_fork
def test_s30_a_failed_fork_does_not_leak_the_readiness_pipe(monkeypatch):
    from cadeloop import server as srv

    def no_fork():
        raise OSError(11, "Resource temporarily unavailable")

    before = len(os.listdir("/proc/self/fd")) if os.path.isdir("/proc/self/fd") else None
    monkeypatch.setattr(srv.os, "fork", no_fork)
    with pytest.raises(OSError):
        srv._spawn_worker(None, "127.0.0.1", 1, cadeloop.Config(), 0, [0])
    if before is not None:
        assert len(os.listdir("/proc/self/fd")) == before


def test_s30_the_spawn_supervisor_does_not_import_the_app(monkeypatch, tmp_path):
    """S30c: the spawn supervisor never serves, but serve() imported the app
    in it anyway -- a full extra copy of its import-time state."""
    from cadeloop import server as srv

    (tmp_path / "heavy_never_imported.py").write_text("raise RuntimeError('imported')\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delattr(srv.os, "fork", raising=False)
    calls = []
    monkeypatch.setattr(srv, "_serve_multi_spawn", lambda spec, *a: calls.append(spec))
    srv.serve("heavy_never_imported:app", workers=2)
    assert calls == ["heavy_never_imported:app"]
    assert "heavy_never_imported" not in sys.modules
    with pytest.raises(ValueError, match="__main__"):
        srv.serve("__main__:app", workers=2)


def test_s30_worker_exit_statuses_are_decoded_in_logs():
    from cadeloop.server import _describe_status

    assert _describe_status(256) == "exit code 1"
    assert _describe_status(9) == "killed by SIGKILL"
    assert "unknown" in _describe_status(None)
