"""Regression tests for the scheduling-core and epoll-backend audit.

Each test pins down a defect that was observable from Python: reference
cycles through loop handles that the collector could never free, timer
handles misreporting their deadline, the native scheduling entry points'
argument checking, an accept-error storm under descriptor exhaustion, and
a SIGPIPE that killed processes whose embedder restored the default
disposition.
"""

import asyncio
import gc
import os
import socket
import subprocess
import sys
import textwrap
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
        # Within a tick time() is the tick's cached clock, so asyncio's
        # contract -- when() == time() + delay -- is exact.
        t = loop.time()
        return loop.call_later(0.75, lambda: None).when() == t + 0.75

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
        logging.getLogger("cadeloop").addHandler(H())

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
