"""Regression tests for the scheduling-core and epoll-backend audit.

Each test pins down a defect that was observable from Python: reference
cycles through loop handles that the collector could never free, timer
handles misreporting their deadline, the native scheduling entry points'
argument checking, an accept-error storm under descriptor exhaustion, a
SIGPIPE that killed processes whose embedder restored the default
disposition, a clock that was neither time.monotonic() nor current inside
a tick, callback ordering and stop() semantics that differed from
asyncio's, scheduling calls from other threads that raised, and
reader/writer watches that ran twice, ran after removal, or went deaf
after their fd number was recycled.
"""

import asyncio
import gc
import os
import socket
import subprocess
import sys
import textwrap
import threading
import time
import weakref

import cadeloop
import pytest

# The cadeloop the test process imported (not a hard-coded tree), so the
# subprocesses below exercise the same build as the in-process tests.
PYPATH = os.path.dirname(os.path.dirname(os.path.abspath(cadeloop.__file__)))


@pytest.fixture()
def loop():
    lp = cadeloop.new_event_loop()
    yield lp
    if not lp.is_closed():
        lp.close()


class _Owner:
    """Holds a handle to one of its own bound methods: the cycle
    object -> handle -> bound method -> object."""

    def tick(self, *args):
        pass


def _collect_cycles(make, n=50):
    refs = []
    for _ in range(n):
        o = _Owner()
        make(o)
        refs.append(weakref.ref(o))
        del o
    return refs


def _alive(refs):
    gc.collect()
    gc.collect()
    return sum(r() is not None for r in refs)


# --------------------------------------------------------------------- #
# handle reference cycles                                               #
# --------------------------------------------------------------------- #


def test_fired_timer_handle_cycle_is_collected(loop):
    refs = _collect_cycles(lambda o: setattr(o, "h", loop.call_later(0, o.tick)))
    loop.run_until_complete(asyncio.sleep(0.01))
    assert _alive(refs) == 0


def test_cancelled_timer_handle_cycle_is_collected(loop):
    def make(o):
        o.h = loop.call_later(3600, o.tick)
        o.h.cancel()

    refs = _collect_cycles(make)
    # No tick needed: cancel() releases the handle's references at once,
    # as asyncio's Handle.cancel() does -- not when the heap tombstone
    # eventually surfaces.
    assert _alive(refs) == 0


def test_dispatched_call_soon_handle_cycle_is_collected(loop):
    # The object is both the callback's self and an argument.
    refs = _collect_cycles(lambda o: setattr(o, "h", loop.call_soon(o.tick, o)))
    loop.run_until_complete(asyncio.sleep(0))
    assert _alive(refs) == 0


def test_threadsafe_handle_cycle_is_collected(loop):
    refs = _collect_cycles(lambda o: setattr(o, "h", loop.call_soon_threadsafe(o.tick)))
    loop.run_until_complete(asyncio.sleep(0))
    assert _alive(refs) == 0


def test_pending_handles_released_at_close(loop):
    refs = _collect_cycles(lambda o: setattr(o, "h", loop.call_later(3600, o.tick)))
    loop.close()
    assert _alive(refs) == 0


def test_abandoned_unclosed_loop_is_collected():
    """A loop dropped without close() whose pending timers reference the
    loop: the cycle runs through the native loop state, which the
    collector can only see through CoreLoop's traverse."""
    box = [cadeloop.new_event_loop()]
    lref = weakref.ref(box[0])

    def make(o):
        o.loop = box[0]
        o.h = box[0].call_later(3600, o.tick)

    refs = _collect_cycles(make)
    with pytest.warns(ResourceWarning, match="unclosed event loop"):
        box.clear()  # the last strong reference outside the cycle
        gc.collect()
        gc.collect()
    assert lref() is None
    assert _alive(refs) == 0


def test_removed_reader_handle_releases_callback(loop):
    a, b = socket.socketpair()
    try:
        refs = []
        for _ in range(10):
            o = _Owner()
            o.sock = a
            loop.add_reader(a.fileno(), o.tick)
            refs.append(weakref.ref(o))
            del o
            loop.remove_reader(a.fileno())
        assert _alive(refs) == 0
    finally:
        a.close()
        b.close()


def test_handle_cancelling_itself_mid_dispatch(loop):
    """The callback releases its own handle's references while running:
    dispatch must hold its own strong references for the call."""
    out = []

    class Self:
        def run(self):
            self.h.cancel()
            out.append(self)  # self is still alive and usable

    s = Self()
    s.h = loop.call_soon(s.run)
    del s
    loop.run_until_complete(asyncio.sleep(0))
    assert len(out) == 1 and out[0].h.cancelled()


def test_cancelled_handle_repr_in_debug_mode(loop):
    loop.set_debug(True)

    def my_callback():
        pass

    h = loop.call_soon(my_callback)
    h.cancel()
    # Debug mode keeps the released callback's repr, as asyncio keeps the
    # callback itself.
    assert "cancelled" in repr(h) and "my_callback" in repr(h)


# --------------------------------------------------------------------- #
# native scheduling entry points                                        #
# --------------------------------------------------------------------- #


def test_timer_handle_when_is_the_exact_float(loop):
    when = loop.time() + 1.2345678901
    assert loop.call_at(when, lambda: None).when() == when

    async def inside():
        # asyncio's contract: when() == time() + delay, time() read by the
        # call itself.
        t0 = loop.time()
        w = loop.call_later(0.75, lambda: None).when()
        t1 = loop.time()
        return t0 + 0.75 <= w <= t1 + 0.75

    assert loop.run_until_complete(inside())


def test_call_soon_argument_errors(loop):
    with pytest.raises(TypeError, match="missing 1 required positional argument: 'callback'"):
        loop.call_soon()
    with pytest.raises(TypeError, match="unexpected keyword argument 'bogus'"):
        loop.call_soon(print, bogus=1)
    with pytest.raises(TypeError, match="missing 1 required positional argument: 'callback'"):
        loop.call_later(1)
    with pytest.raises(TypeError):
        loop.call_later("soon", print)


def test_call_soon_many_args_and_context(loop):
    import contextvars

    var = contextvars.ContextVar("v")
    ctx = contextvars.copy_context()
    ctx.run(var.set, "in-ctx")
    out = []
    loop.call_soon(lambda *a: out.append(a), 1, 2, 3, 4, 5)
    loop.call_soon(lambda a, b: out.append((a, b)), "x", ())  # () as a real 2nd arg
    loop.call_soon(lambda: out.append(var.get(None)), context=ctx)
    loop.call_later(0, lambda: out.append("timer"), context=ctx)
    loop.run_until_complete(asyncio.sleep(0.01))
    assert out == [(1, 2, 3, 4, 5), ("x", ()), "in-ctx", "timer"]


def test_cancel_after_fire_reports_no_tombstone(loop):
    """asyncio's TimerHandle._scheduled: cancelling a timer that already
    fired must not be counted as a heap tombstone."""
    hs = [loop.call_later(0, lambda: None) for _ in range(100)]
    loop.run_until_complete(asyncio.sleep(0.01))
    for h in hs:
        h.cancel()
    loop.run_until_complete(asyncio.sleep(0))
    assert loop.stats()["timers_len"] == 0


# --------------------------------------------------------------------- #
# process-level behaviour (subprocesses)                                #
# --------------------------------------------------------------------- #


def _run_py(code, timeout=60):
    env = dict(os.environ, PYTHONPATH=PYPATH)
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout,
    )


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="RLIMIT_NOFILE semantics")
def test_accept_backs_off_under_descriptor_exhaustion():
    """At its descriptor limit the listener used to re-post failing
    accepts in a hot loop: ~100k 'Accept failed' reports per second and a
    full core. It must report sparingly, idle, and recover once
    descriptors free up."""
    p = _run_py(
        """
        import asyncio, logging, os, resource, socket, threading, time
        import cadeloop

        errors = []
        class H(logging.Handler):
            def emit(self, rec):
                errors.append(rec.getMessage())
        # asyncio's logger (where the default exception handler reports),
        # and cadeloop's (where older builds reported).
        for name in ("asyncio", "cadeloop"):
            logging.getLogger(name).addHandler(H())

        async def main():
            loop = asyncio.get_running_loop()
            held = []
            class P(asyncio.Protocol):
                def connection_made(self, t):
                    held.append(t)
            srv = await loop.create_server(P, "127.0.0.1", 0)
            port = srv.sockets[0].getsockname()[1]
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            nfd = len(os.listdir("/proc/self/fd"))
            resource.setrlimit(resource.RLIMIT_NOFILE, (nfd + 4, hard))
            clients = []
            def connect():
                for _ in range(8):
                    try:
                        clients.append(socket.create_connection(("127.0.0.1", port), timeout=1))
                    except OSError:
                        break
            t = threading.Thread(target=connect); t.start(); t.join()
            c0 = time.process_time()
            await asyncio.sleep(1.2)
            cpu = time.process_time() - c0
            resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
            deadline = time.monotonic() + 5
            while len(held) < len(clients) and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            print(len(errors), round(cpu, 2), len(held), len(clients))
            for c in clients:
                c.close()
            srv.close()

        cadeloop.run(main())
        """
    )
    assert p.returncode == 0, p.stderr
    errors, cpu, accepted, clients = p.stdout.split()
    assert clients != "0"
    assert int(errors) <= 4, f"accept-error storm: {errors} reports"
    assert float(cpu) < 0.5, f"spun a core while starved: {cpu}s"
    assert accepted == clients, "listener did not recover after the limit lifted"


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")
def test_write_to_reset_peer_with_default_sigpipe_does_not_kill():
    """The epoll send path used writev, which raises SIGPIPE on a socket
    the peer reset. Only Python's own startup ignores that signal; an
    application that restores SIG_DFL (common for CLI tools under `| head`)
    was killed outright by a client disconnecting mid-response."""
    p = _run_py(
        """
        import asyncio, signal, socket, struct
        import cadeloop
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)

        async def main():
            loop = asyncio.get_running_loop()
            done = loop.create_future()
            class P(asyncio.Protocol):
                def connection_made(self, t):
                    self.t = t
                def data_received(self, data):
                    for _ in range(64):
                        self.t.write(b"x" * 65536)
                def connection_lost(self, exc):
                    if not done.done():
                        done.set_result(exc)
            srv = await loop.create_server(P, "127.0.0.1", 0)
            port = srv.sockets[0].getsockname()[1]
            c = socket.create_connection(("127.0.0.1", port))
            # RST on close: the server's pending writes hit a reset socket.
            c.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            c.sendall(b"go")
            await asyncio.sleep(0.05)
            c.close()
            exc = await asyncio.wait_for(done, 5)
            print("survived", type(exc).__name__)
            srv.close()

        cadeloop.run(main())
        """
    )
    assert p.returncode == 0, f"rc={p.returncode} stderr={p.stderr[-500:]}"
    assert "survived" in p.stdout


# --------------------------------------------------------------------- #
# the loop clock                                                        #
# --------------------------------------------------------------------- #


def test_loop_time_is_time_monotonic(loop):
    """asyncio and uvloop report time.monotonic(): deadlines computed
    from either clock (call_at, asyncio.timeout_at) must mean the same
    instant. cadeloop's clock started at 0 for every loop."""
    a = time.monotonic()
    t = loop.time()
    b = time.monotonic()
    if sys.platform.startswith("linux"):
        assert a <= t <= b  # the very same clock
    else:
        assert abs(t - a) < 0.05  # aligned at loop creation


def test_call_at_a_time_monotonic_deadline_fires(loop):
    fut = loop.create_future()
    loop.call_at(time.monotonic() + 0.05, fut.set_result, None)
    t0 = time.monotonic()
    loop.run_until_complete(asyncio.wait_for(fut, 5))
    assert 0.04 <= time.monotonic() - t0 < 2


def test_loop_time_is_fresh_inside_a_callback(loop):
    async def main():
        t0 = loop.time()
        time.sleep(0.05)  # noqa: ASYNC251 -- blocking work is the point
        return loop.time() - t0

    # A per-tick value made every delta measured inside a callback 0.
    assert loop.run_until_complete(main()) >= 0.045


def test_sleep_after_blocking_work_lasts_its_delay(loop):
    async def main():
        time.sleep(0.2)  # noqa: ASYNC251 -- blocking work is the point
        t0 = time.monotonic()
        await asyncio.sleep(0.1)
        return time.monotonic() - t0

    # The deadline used to be computed from the tick's start: the 200ms
    # of blocking work had already "used up" the sleep.
    assert loop.run_until_complete(main()) >= 0.09


def test_past_due_timers_run_in_deadline_order(loop):
    out = []
    loop.call_later(-1, out.append, "a")
    loop.call_later(-2, out.append, "b")
    loop.call_later(0, out.append, "c")
    loop.run_until_complete(asyncio.sleep(0.01))
    assert out == ["b", "a", "c"]  # by when(), as asyncio's heap orders them


# --------------------------------------------------------------------- #
# callback ordering and stop()                                          #
# --------------------------------------------------------------------- #


def test_call_soon_threadsafe_and_call_soon_share_one_fifo(loop):
    out = []
    loop.call_soon_threadsafe(out.append, 1)
    loop.call_soon(out.append, 2)
    loop.call_soon_threadsafe(out.append, 3)

    async def main():
        loop.call_soon_threadsafe(out.append, 4)
        loop.call_soon(out.append, 5)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    loop.run_until_complete(main())
    assert out == [1, 2, 3, 4, 5]


def test_stop_lets_the_current_iteration_finish(loop):
    """asyncio runs every callback that was ready when the iteration
    began; stop() takes effect after it. A 128-callback batch cap
    returned after 127 of these 300."""
    out = []
    loop.call_soon(loop.stop)
    for i in range(300):
        loop.call_soon(out.append, i)
    loop.run_forever()
    assert out == list(range(300))


# --------------------------------------------------------------------- #
# scheduling from other threads                                         #
# --------------------------------------------------------------------- #


def _in_thread(fn, delay=0.05):
    def run():
        time.sleep(delay)
        fn()

    t = threading.Thread(target=run)
    t.start()
    return t


def test_set_result_from_another_thread_wakes_the_loop(loop):
    """Not thread-safe by asyncio's book, but accepted outside debug mode
    -- and common in callback-style wrappers. It used to raise
    RuntimeError AFTER marking the future done, losing its callbacks: the
    awaiting task never woke."""

    async def main():
        fut = loop.create_future()
        t = _in_thread(lambda: fut.set_result(42))
        t0 = time.monotonic()
        r = await asyncio.wait_for(fut, 10)
        t.join()
        return r, time.monotonic() - t0

    r, elapsed = loop.run_until_complete(main())
    assert r == 42
    assert elapsed < 5  # the parked loop was woken, not left to its timeout


def test_call_later_and_create_task_from_another_thread(loop):
    async def main():
        fut = loop.create_future()

        async def coro():
            return "task"

        tasks = []

        def foreign():
            loop.call_later(0.02, fut.set_result, "timer")
            tasks.append(loop.create_task(coro()))

        t = _in_thread(foreign)
        r = await asyncio.wait_for(fut, 10)
        t.join()
        return r, await tasks[0]

    assert loop.run_until_complete(main()) == ("timer", "task")


def test_debug_mode_rejects_foreign_thread_scheduling(loop):
    loop.set_debug(True)
    errors = []

    async def main():
        def foreign():
            for call in (
                lambda: loop.call_soon(print),
                lambda: loop.call_later(1, print),
                lambda: loop.call_at(loop.time() + 1, print),
            ):
                try:
                    call()
                except RuntimeError as e:
                    errors.append(str(e))

        t = _in_thread(foreign, delay=0)
        while t.is_alive():
            await asyncio.sleep(0.01)

    loop.run_until_complete(main())
    assert len(errors) == 3 and all("Non-thread-safe operation" in e for e in errors)


# --------------------------------------------------------------------- #
# add_reader / add_writer                                               #
# --------------------------------------------------------------------- #


def _outlast_the_poll_skip_window(loop):
    # A callback in the same iteration as the readiness event that keeps
    # the loop busy past the 250us poll-skip window, so the next iteration
    # really polls -- which is when a watch run one iteration late got
    # queued a second time.
    loop.call_soon(time.sleep, 0.002)


def test_one_shot_writer_runs_once(loop):
    a, b = socket.socketpair()
    try:
        calls = []

        def on_writable():
            calls.append(1)
            loop.remove_writer(a.fileno())

        loop.add_writer(a.fileno(), on_writable)
        _outlast_the_poll_skip_window(loop)
        loop.run_until_complete(asyncio.sleep(0.05))
        assert calls == [1]
    finally:
        a.close()
        b.close()


def test_reader_that_drains_its_socket_is_not_called_again(loop):
    a, b = socket.socketpair()
    a.setblocking(False)
    try:
        got, errors = [], []

        def on_readable():
            try:
                got.append(a.recv(100))
            except BlockingIOError as e:
                errors.append(e)

        loop.add_reader(a.fileno(), on_readable)
        b.send(b"hello")
        _outlast_the_poll_skip_window(loop)
        loop.run_until_complete(asyncio.sleep(0.05))
        loop.remove_reader(a.fileno())
        # One readiness event, one call: the watch used to run a tick late,
        # after a second poll had queued it again.
        assert got == [b"hello"] and not errors
    finally:
        a.close()
        b.close()


def test_reader_removed_by_an_earlier_callback_does_not_run(loop):
    pairs = [socket.socketpair() for _ in range(2)]
    try:
        calls = []
        fds = [a.fileno() for a, _ in pairs]

        def make(i):
            def cb():
                calls.append(i)
                for fd in fds:
                    loop.remove_reader(fd)

            return cb

        for i, fd in enumerate(fds):
            loop.add_reader(fd, make(i))
        for _, b in pairs:
            b.send(b"x")
        loop.run_until_complete(asyncio.sleep(0.05))
        assert len(calls) == 1
    finally:
        for a, b in pairs:
            a.close()
            b.close()


def test_removed_and_replaced_watch_handles_are_cancelled(loop):
    a, b = socket.socketpair()
    try:
        fd = a.fileno()
        h1 = loop._add_reader(fd, lambda: None)
        h2 = loop._add_reader(fd, lambda: None)
        assert h1.cancelled() and not h2.cancelled()
        assert loop.remove_reader(fd)
        assert h2.cancelled()
    finally:
        a.close()
        b.close()


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="epoll fd semantics")
def test_fd_closed_before_remove_reader_can_be_watched_again(loop):
    """epoll forgets a registration when its file is closed. remove_reader
    after close() raised and kept the table entry, and a new file behind
    the recycled fd number was then never registered: its reader never
    fired."""
    a, b = socket.socketpair()
    fd = a.fileno()
    loop.add_reader(fd, lambda: None)
    a.close()
    assert loop.remove_reader(fd) is True
    _check_recycled_fd_fires(loop, fd, b)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="epoll fd semantics")
def test_watch_replaced_on_a_recycled_fd_is_registered(loop):
    """The same, without the remove_reader: close, recycle, add again."""
    a, b = socket.socketpair()
    fd = a.fileno()
    loop.add_reader(fd, lambda: None)
    a.close()
    _check_recycled_fd_fires(loop, fd, b)


def _check_recycled_fd_fires(loop, fd, *close_after):
    """Put a new file behind `fd` (the kernel usually hands the number
    out again by itself; dup2 makes sure) and check a reader on it fires."""
    c, d = socket.socketpair()
    extra = None
    if c.fileno() != fd:
        os.dup2(c.fileno(), fd)
        extra = fd
    try:
        fired = loop.create_future()
        loop.add_reader(fd, lambda: fired.done() or fired.set_result(True))
        d.send(b"x")
        assert loop.run_until_complete(asyncio.wait_for(fired, 5))
        loop.remove_reader(fd)
    finally:
        if extra is not None:
            os.close(extra)
        for s_ in (c, d, *close_after):
            s_.close()


# --------------------------------------------------------------------- #
# logging, messages, handle introspection                              #
# --------------------------------------------------------------------- #


def _boom(x):
    raise ValueError(x)


def test_callback_errors_are_reported_in_asyncio_format(loop, caplog):
    loop.call_soon(_boom, 1)
    with caplog.at_level("ERROR", logger="asyncio"):
        loop.run_until_complete(asyncio.sleep(0.01))
    recs = [r for r in caplog.records if r.name == "asyncio"]
    assert recs, "the default handler must log to asyncio's logger"
    msg = recs[0].getMessage()
    # asyncio's Handle._run: "Exception in callback <func>(<args>) at <file>:<line>"
    assert msg.startswith("Exception in callback _boom(1) at "), msg
    assert __file__.rstrip("c") in msg


def test_handle_repr_and_private_attributes_match_asyncio(loop):
    h = loop.call_soon(_boom, 1)
    assert h._callback is _boom and h._args == (1,)
    assert repr(h).startswith("<Handle _boom(1) at ")
    t = loop.call_at(12345.5, _boom, 2)
    assert repr(t).startswith("<TimerHandle when=12345.5 _boom(2) at ")
    t.cancel()
    assert repr(t) == "<TimerHandle cancelled when=12345.5>"
    assert t._callback is None and t._args is None
    h.cancel()


def test_debug_mode_source_traceback_points_at_the_caller(loop):
    loop.set_debug(True)
    lineno = sys._getframe().f_lineno + 1
    h = loop.call_soon(_boom, 1)
    frame = h._source_traceback[-1]
    assert (frame[0], frame[1], frame[2]) == (
        __file__.rstrip("c"),
        lineno,
        "test_debug_mode_source_traceback_points_at_the_caller",
    )
    assert f"created at {frame[0]}:{lineno}" in repr(h)
    h.cancel()
    # debug cancel() keeps the formatted callback, as asyncio's _repr does
    assert repr(h).startswith("<Handle cancelled _boom(1) at ")


# --------------------------------------------------------------------- #
# subclassing                                                           #
# --------------------------------------------------------------------- #


def test_subclass_overrides_of_the_fast_paths_are_called():
    calls = []

    class MyLoop(cadeloop.Loop):
        def call_soon(self, callback, *args, context=None):
            calls.append("call_soon")
            return super().call_soon(callback, *args, context=context)

        def call_later(self, delay, callback, *args, context=None):
            calls.append("call_later")
            return super().call_later(delay, callback, *args, context=context)

        def time(self):
            calls.append("time")
            return super().time()

    lp = MyLoop()
    try:
        lp.run_until_complete(asyncio.sleep(0.001))
        assert {"call_soon", "call_later", "time"} <= set(calls)
        # The methods it did not override still take the native fast path.
        assert type(lp.__dict__["call_at"]).__name__ == "builtin_function_or_method"
        assert "call_soon" not in lp.__dict__
    finally:
        lp.close()
