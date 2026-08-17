"""M0 loop semantics: call_soon / timers / lifecycle / threadsafe wakeup."""

import asyncio
import contextvars
import sys
import threading
import time as _time

import cadeloop
import pytest


@pytest.fixture()
def loop():
    lp = cadeloop.new_event_loop()
    yield lp
    if not lp.is_closed():
        lp.close()


def run_briefly(loop, seconds=0.05):
    loop.call_later(seconds, loop.stop)
    loop.run_forever()


# --------------------------------------------------------------------- #
# call_soon                                                             #
# --------------------------------------------------------------------- #


def test_call_soon_fifo_order(loop):
    out = []
    for i in range(10):
        loop.call_soon(out.append, i)
    run_briefly(loop, 0.01)
    assert out == list(range(10))


def test_call_soon_multiple_args(loop):
    out = []
    loop.call_soon(lambda *a: out.append(a), 1, "x", None)
    run_briefly(loop, 0.01)
    assert out == [(1, "x", None)]


def test_call_soon_returns_cancellable_handle(loop):
    out = []
    h = loop.call_soon(out.append, 1)
    assert not h.cancelled()
    h.cancel()
    assert h.cancelled()
    run_briefly(loop, 0.01)
    assert out == []


def test_call_soon_non_callable_raises(loop):
    # Debug-gated, matching CPython: base_events.call_soon runs
    # _check_callback only under `if self._debug`.
    loop.set_debug(True)
    try:
        with pytest.raises(TypeError):
            loop.call_soon(42)
    finally:
        loop.set_debug(False)


def test_call_soon_rejects_coroutine_function(loop):
    """A bare coroutine function passed to call_soon was previously
    accepted silently (is_callable() is True for it) and, when the
    handle ran, just created-and-dropped a coroutine object instead of
    failing fast with the same TypeError real asyncio raises."""

    async def coro_fn():
        pass

    class Obj:
        async def coro_method(self):
            pass

    # Debug-gated, exactly as CPython gates it: base_events.call_soon runs
    # _check_callback only under `if self._debug`. Running it on every
    # schedule made cadeloop stricter than the stdlib it mirrors and cost
    # two attribute lookups on the loop's busiest path.
    loop.set_debug(True)
    with pytest.raises(TypeError, match="coroutines cannot be used with call_soon"):
        loop.call_soon(coro_fn)
    with pytest.raises(TypeError, match="coroutines cannot be used with call_soon_threadsafe"):
        loop.call_soon_threadsafe(Obj().coro_method)
    with pytest.raises(TypeError, match="a callable object was expected"):
        loop.call_soon(42)
    # A sync function/method must still be accepted normally.
    loop.call_soon(lambda: None)
    loop.set_debug(False)


def test_call_soon_after_close_raises():
    loop = cadeloop.new_event_loop()
    loop.close()
    with pytest.raises(RuntimeError, match="Event loop is closed"):
        loop.call_soon(print)


def test_callback_scheduled_during_dispatch_runs_next_tick(loop):
    order = []

    def first():
        order.append("first")
        loop.call_soon(lambda: order.append("nested"))

    loop.call_soon(first)
    loop.call_soon(lambda: order.append("second"))
    run_briefly(loop, 0.02)
    assert order == ["first", "second", "nested"]


# --------------------------------------------------------------------- #
# timers                                                                #
# --------------------------------------------------------------------- #


def test_call_later_ordering_and_delay(loop):
    out = []
    start = loop.time()
    loop.call_later(0.06, out.append, 2)
    loop.call_later(0.02, out.append, 1)
    loop.call_later(0.1, loop.stop)
    loop.run_forever()
    assert out == [1, 2]
    assert loop.time() - start >= 0.09


def test_call_later_negative_delay_fires_immediately(loop):
    out = []
    loop.call_later(-5, out.append, "now")
    run_briefly(loop, 0.01)
    assert out == ["now"]


def test_timer_handle_when_and_cancel(loop):
    h = loop.call_later(60, print)
    assert h.when() == pytest.approx(loop.time() + 60, abs=0.5)
    assert not h.cancelled()
    h.cancel()
    assert h.cancelled()
    h.cancel()  # idempotent
    assert h.cancelled()


def test_call_at(loop):
    out = []
    loop.call_at(loop.time() + 0.02, out.append, "at")
    run_briefly(loop, 0.05)
    assert out == ["at"]


def test_many_cancelled_timers_compact(loop):
    handles = [loop.call_later(3600, print) for _ in range(1000)]
    for h in handles:
        h.cancel()
    out = []
    loop.call_later(0.01, out.append, "live")
    run_briefly(loop, 0.03)
    assert out == ["live"]
    assert loop.stats()["timers_len"] <= 500


def test_equal_deadline_timers_fifo(loop):
    out = []
    when = loop.time() + 0.02
    for i in range(5):
        loop.call_at(when, out.append, i)
    run_briefly(loop, 0.05)
    assert out == list(range(5))


# --------------------------------------------------------------------- #
# time                                                                  #
# --------------------------------------------------------------------- #


def test_time_advances_when_not_running(loop):
    t1 = loop.time()
    _time.sleep(0.02)
    t2 = loop.time()
    assert t2 > t1


def test_time_cached_within_tick(loop):
    """R-061: loop.time() is stable within one callback."""
    samples = []

    def cb():
        samples.append(loop.time())
        samples.append(loop.time())

    loop.call_soon(cb)
    run_briefly(loop, 0.01)
    assert samples[0] == samples[1]


# --------------------------------------------------------------------- #
# lifecycle                                                             #
# --------------------------------------------------------------------- #


def test_stop_before_run_forever_stops_after_one_tick(loop):
    out = []
    loop.call_soon(out.append, 1)
    loop.stop()
    loop.run_forever()  # must not hang
    assert out == [1]
    assert not loop.is_running()


def test_reentrant_run_forever_raises(loop):
    errors = []

    def reenter():
        try:
            loop.run_forever()
        except RuntimeError as e:
            errors.append(str(e))
        loop.stop()

    loop.call_soon(reenter)
    loop.run_forever()
    assert errors and "already running" in errors[0]


def test_close_while_running_raises(loop):
    errors = []
    # Prime a default executor so we can prove it survives a failed
    # close() below (R-102: close() must check is_running() before
    # touching the executors, not after — otherwise a close() that
    # itself raises still leaves them permanently shut down).
    loop.run_until_complete(loop.run_in_executor(None, lambda: None))
    executor_before = loop._default_executor
    assert executor_before is not None

    def do_close():
        try:
            loop.close()
        except RuntimeError as e:
            errors.append(str(e))
        loop.stop()

    loop.call_soon(do_close)
    loop.run_forever()
    assert errors and "Cannot close a running event loop" in errors[0]
    assert loop._default_executor is executor_before
    assert (
        loop.run_until_complete(loop.run_in_executor(None, lambda: "still works"))
        == "still works"
    )


def test_close_is_idempotent():
    loop = cadeloop.new_event_loop()
    loop.close()
    loop.close()
    assert loop.is_closed()


def test_run_until_complete_result_and_exception(loop):
    async def ok():
        await asyncio.sleep(0.01)
        return "done"

    async def boom():
        raise ValueError("boom")

    assert loop.run_until_complete(ok()) == "done"
    with pytest.raises(ValueError, match="boom"):
        loop.run_until_complete(boom())


def test_run_until_complete_stopped_early(loop):
    async def forever():
        await asyncio.sleep(3600)

    loop.call_later(0.02, loop.stop)
    with pytest.raises(RuntimeError, match="stopped before Future completed"):
        loop.run_until_complete(forever())


def test_is_running_reflects_state(loop):
    states = []
    loop.call_soon(lambda: states.append(loop.is_running()))
    assert not loop.is_running()
    run_briefly(loop, 0.01)
    assert states == [True]
    assert not loop.is_running()


# --------------------------------------------------------------------- #
# threadsafe                                                            #
# --------------------------------------------------------------------- #


def test_call_soon_threadsafe_wakes_parked_loop(loop):
    out = []

    def producer():
        _time.sleep(0.05)
        loop.call_soon_threadsafe(out.append, "from-thread")
        loop.call_soon_threadsafe(loop.stop)

    t = threading.Thread(target=producer)
    t.start()
    start = _time.monotonic()
    loop.run_forever()
    elapsed = _time.monotonic() - start
    t.join()
    assert out == ["from-thread"]
    assert elapsed < 2.0, "wakeup latency should be far below park timeout"


def test_call_soon_threadsafe_many_producers(loop):
    out = []
    n_threads, per_thread = 8, 200

    def producer(tid):
        for i in range(per_thread):
            loop.call_soon_threadsafe(out.append, (tid, i))

    threads = [threading.Thread(target=producer, args=(t,)) for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    run_briefly(loop, 0.1)
    assert len(out) == n_threads * per_thread
    # Per-producer FIFO must hold.
    for tid in range(n_threads):
        seq = [i for (t, i) in out if t == tid]
        assert seq == sorted(seq)


# --------------------------------------------------------------------- #
# context & exception handling                                          #
# --------------------------------------------------------------------- #


def test_contextvars_copied_at_schedule_time(loop):
    var = contextvars.ContextVar("v", default="default")
    seen = []
    var.set("scheduled")
    loop.call_soon(lambda: seen.append(var.get()))
    var.set("changed-later")
    run_briefly(loop, 0.01)
    assert seen == ["scheduled"]


def test_explicit_context_kwarg(loop):
    var = contextvars.ContextVar("v", default="default")
    ctx = contextvars.copy_context()
    seen = []
    var.set("outside-ctx")
    loop.call_soon(lambda: seen.append(var.get()), context=ctx)
    run_briefly(loop, 0.01)
    assert seen == ["default"]


def test_callback_exception_goes_to_exception_handler(loop):
    captured = []
    loop.set_exception_handler(lambda lp, ctx: captured.append(ctx))

    def bad():
        raise RuntimeError("kaboom")

    loop.call_soon(bad)
    run_briefly(loop, 0.02)
    assert len(captured) == 1
    assert isinstance(captured[0]["exception"], RuntimeError)
    assert "kaboom" in str(captured[0]["exception"])
    assert "handle" in captured[0]


def test_exception_handler_get_set(loop):
    assert loop.get_exception_handler() is None
    handler = lambda lp, ctx: None  # noqa: E731
    loop.set_exception_handler(handler)
    assert loop.get_exception_handler() is handler
    with pytest.raises(TypeError):
        loop.set_exception_handler(42)


def test_cancelled_executor_shutdown_resolves_without_error(loop):
    """Cancelling the task awaiting shutdown_default_executor() cancels
    its future; when the shutdown thread then finishes, set_result on
    the cancelled future raised InvalidStateError through the exception
    handler. CPython schedules _set_result_unless_cancelled instead.
    Reported on PR #1."""
    errors = []
    loop.set_exception_handler(lambda lp, ctx: errors.append(ctx))

    async def main():
        release = threading.Event()
        job = loop.run_in_executor(None, release.wait)
        t = asyncio.ensure_future(loop.shutdown_default_executor())
        await asyncio.sleep(0.05)  # shutdown thread started, future awaited
        # cancel() reaches the awaited future synchronously; release the
        # executor before t resumes -- its finally join()s the shutdown
        # thread on the loop thread, which deadlocks if the job is still
        # holding the executor open.
        t.cancel()
        release.set()
        try:
            await t
        except asyncio.CancelledError:
            pass
        await asyncio.sleep(0.2)  # the thread's set_result callback runs
        await job

    loop.run_until_complete(main())
    assert errors == [], errors


def test_exception_handler_raising_system_exit_stops_the_loop(loop):
    """CPython parity: call_exception_handler deliberately re-raises
    SystemExit/KeyboardInterrupt from a custom handler, and the loop
    must let them unwind run_forever -- not demote them to an
    unraisable warning and keep running. Reported on PR #1."""

    def handler(lp, ctx):
        raise SystemExit(3)

    loop.set_exception_handler(handler)
    loop.call_soon(lambda: 1 / 0)
    loop.call_later(2, loop.stop)  # fallback so a broken loop still exits
    with pytest.raises(SystemExit):
        loop.run_forever()


def test_pure_scheduling_elides_kernel_polls(loop):
    """R-150: with no op in flight and no fd registered (the signal-wakeup
    pipe is exempt), busy ticks must not touch the kernel at all."""
    n = [0]

    def chain():
        n[0] += 1
        if n[0] < 2000:
            loop.call_soon(chain)
        else:
            loop.stop()

    loop.call_soon(chain)
    loop.run_forever()
    st = loop.stats()
    assert st["polls_elided"] > 0, st
    # Elision must never suppress parked polls: a timer-driven park still
    # wakes on time.
    t0 = loop.time()
    loop.call_later(0.05, loop.stop)
    loop.run_forever()
    assert loop.time() - t0 >= 0.04


def test_loop_keeps_running_after_callback_exception(loop):
    loop.set_exception_handler(lambda lp, ctx: None)
    out = []
    loop.call_soon(lambda: 1 / 0)
    loop.call_soon(out.append, "survived")
    run_briefly(loop, 0.02)
    assert out == ["survived"]


# --------------------------------------------------------------------- #
# tasks & executors                                                     #
# --------------------------------------------------------------------- #


def test_create_task_and_gather(loop):
    async def work(n):
        await asyncio.sleep(0.01)
        return n * 2

    async def main():
        ts = [loop.create_task(work(i), name=f"w{i}") for i in range(5)]
        assert ts[0].get_name() == "w0"
        return await asyncio.gather(*ts)

    assert loop.run_until_complete(main()) == [0, 2, 4, 6, 8]


def test_task_factory(loop):
    made = []

    def factory(lp, coro, context=None):
        t = asyncio.Task(coro, loop=lp, context=context)
        made.append(t)
        return t

    assert loop.get_task_factory() is None
    loop.set_task_factory(factory)
    assert loop.get_task_factory() is factory

    async def main():
        await loop.create_task(asyncio.sleep(0))

    loop.run_until_complete(main())
    assert made


def test_run_in_executor(loop):
    def blocking(x):
        _time.sleep(0.01)
        return threading.current_thread().name, x

    async def main():
        name, val = await loop.run_in_executor(None, blocking, 7)
        assert val == 7
        assert name != threading.current_thread().name
        return "ok"

    assert loop.run_until_complete(main()) == "ok"
    loop.run_until_complete(loop.shutdown_default_executor())


def test_getaddrinfo_numeric(loop):
    async def main():
        return await loop.getaddrinfo("127.0.0.1", 80, type=1)

    res = loop.run_until_complete(main())
    assert res and res[0][4][0] == "127.0.0.1"
    assert loop.run_until_complete(main()) == res


def test_getaddrinfo_no_cache_by_default():
    """R-055: a plain Loop() must match the AbstractEventLoop contract
    (real asyncio.getaddrinfo never caches) — caching is an opt-in
    cadeloop.Config/serve() default, not a Loop()-level default."""
    lp = cadeloop.new_event_loop()
    try:
        assert lp._dns_cache_enabled is False
        calls = []
        real = __import__("socket").getaddrinfo

        def counting(*a, **kw):
            calls.append(1)
            return real(*a, **kw)

        import unittest.mock

        with unittest.mock.patch("socket.getaddrinfo", counting):
            lp.run_until_complete(lp.getaddrinfo("127.0.0.1", 80, type=1))
            lp.run_until_complete(lp.getaddrinfo("127.0.0.1", 80, type=1))
        assert len(calls) == 2, "getaddrinfo was cached despite dns_cache defaulting to off"
    finally:
        lp.close()


def test_getaddrinfo_cache_opt_in():
    """dns_cache=True (cadeloop.Config's own default) makes a second
    identical lookup within the TTL hit the cache instead of resolving
    again."""
    from cadeloop.loop import Loop

    lp = Loop(dns_cache=True, dns_cache_ttl=5.0)
    try:
        calls = []
        real = __import__("socket").getaddrinfo

        def counting(*a, **kw):
            calls.append(1)
            return real(*a, **kw)

        import unittest.mock

        with unittest.mock.patch("socket.getaddrinfo", counting):
            lp.run_until_complete(lp.getaddrinfo("127.0.0.1", 80, type=1))
            lp.run_until_complete(lp.getaddrinfo("127.0.0.1", 80, type=1))
        assert len(calls) == 1, "second lookup within the TTL should have hit the cache"
    finally:
        lp.close()


def test_asyncio_run_with_policy_installed():
    old_policy = asyncio.get_event_loop_policy()
    try:
        cadeloop.install()

        async def main():
            assert isinstance(asyncio.get_running_loop(), cadeloop.Loop)
            await asyncio.sleep(0)
            return "policy-ok"

        assert asyncio.run(main()) == "policy-ok"
    finally:
        asyncio.set_event_loop_policy(old_policy)


def test_async_generator_cleanup():
    finalized = []

    async def agen():
        try:
            yield 1
            yield 2
        finally:
            finalized.append(True)

    async def main():
        g = agen()
        assert await g.__anext__() == 1
        # abandon the generator; Runner's shutdown_asyncgens must close it

    cadeloop.run(main())
    assert finalized == [True]


def test_debug_flag(loop):
    assert loop.get_debug() is False
    loop.set_debug(True)
    assert loop.get_debug() is True
    loop.set_debug(False)


def test_debug_enables_coroutine_origin_tracking(loop):
    """set_debug(True) previously only flipped the core loop's own debug
    flag, missing sys.set_coroutine_origin_tracking_depth entirely (no
    richer "Object created at" tracebacks in debug mode, unlike real
    asyncio)."""
    saved = sys.get_coroutine_origin_tracking_depth()
    try:

        async def toggle():
            loop.set_debug(True)
            await asyncio.sleep(0.01)  # let the threadsafe call_soon land
            assert sys.get_coroutine_origin_tracking_depth() > 0
            loop.set_debug(False)
            await asyncio.sleep(0.01)
            assert sys.get_coroutine_origin_tracking_depth() == saved

        loop.run_until_complete(toggle())
        # run_forever()'s own finally: also resets it even without an
        # explicit set_debug(False) — verify the not-currently-running
        # entry path (run_forever applies it at startup) too.
        loop.set_debug(True)
        assert sys.get_coroutine_origin_tracking_depth() == saved  # not running yet
        run_briefly(loop, 0.01)
        assert sys.get_coroutine_origin_tracking_depth() == saved  # reset in the finally
    finally:
        loop.set_debug(False)
        sys.set_coroutine_origin_tracking_depth(saved)


def test_stats_shape(loop):
    run_briefly(loop, 0.01)
    stats = loop.stats()
    for key in (
        "backend",
        "ticks",
        "polls",
        "completions",
        "callbacks_dispatched",
        "timers_fired",
        "xthread_items",
        "spin_hits",
        "ready_len",
        "timers_len",
    ):
        assert key in stats
    assert stats["ticks"] > 0


def test_sigint_interrupts_idle_park_promptly():
    # R-052: with no timers scheduled the loop parks indefinitely; the
    # run_forever wakeup fd must surface a signal immediately anyway.
    import signal
    import threading
    import time as time_mod

    lp = cadeloop.new_event_loop()
    try:
        killer = threading.Timer(0.3, signal.raise_signal, args=(signal.SIGINT,))
        killer.start()
        t0 = time_mod.monotonic()
        try:
            lp.run_forever()  # parks with NOTHING scheduled
            raise AssertionError("run_forever returned without KeyboardInterrupt")
        except KeyboardInterrupt:
            pass
        elapsed = time_mod.monotonic() - t0
        assert elapsed < 2.0, f"SIGINT took {elapsed:.2f}s to interrupt an idle park"
        killer.cancel()
    finally:
        lp.close()


def test_signal_handler_fires_during_idle_park():
    # A handled signal (no KeyboardInterrupt) must also wake the park and
    # run its callback promptly (R-052).
    import signal
    import sys
    import threading
    import time as time_mod

    sig = signal.SIGBREAK if sys.platform == "win32" else signal.SIGUSR1
    lp = cadeloop.new_event_loop()
    hits = []
    try:
        lp.add_signal_handler(sig, lambda: (hits.append(1), lp.stop()))
        threading.Timer(0.3, signal.raise_signal, args=(sig,)).start()
        t0 = time_mod.monotonic()
        lp.run_forever()
        elapsed = time_mod.monotonic() - t0
        assert hits == [1]
        assert elapsed < 2.0, f"handled signal took {elapsed:.2f}s"
        lp.remove_signal_handler(sig)
    finally:
        lp.close()


# --------------------------------------------------------------------- #
# native create_task / create_future fast paths (R-050)                 #
# --------------------------------------------------------------------- #


def test_create_task_fast_path_is_a_real_asyncio_task(loop):
    """The vectorcall path must build the same objects the Python path
    did -- a real asyncio.Task owned by the facade, not by the core."""

    async def work():
        return 7

    async def main():
        t = loop.create_task(work())
        assert isinstance(t, asyncio.Task)
        assert t.get_loop() is loop
        assert await t == 7

    loop.run_until_complete(main())
    f = loop.create_future()
    assert isinstance(f, asyncio.Future)
    assert f.get_loop() is loop
    f.set_result(None)


def test_create_task_forwards_name_and_context(loop):
    """name= / context= are only threaded into the vectorcall when
    supplied, so both must still land."""
    var = contextvars.ContextVar("v", default="default")
    ctx = contextvars.copy_context()
    ctx.run(var.set, "from-ctx")
    seen = []

    async def work():
        seen.append(var.get())

    async def main():
        t = loop.create_task(work(), name="named", context=ctx)
        assert t.get_name() == "named"
        await t

    loop.run_until_complete(main())
    assert seen == ["from-ctx"]


def test_create_task_rejects_unknown_kwarg(loop):
    """An unmodelled keyword falls back to the facade, which forwards it
    to asyncio.Task -- so the error is the stdlib's, not a cadeloop
    'unexpected keyword argument create_task()' from the shim."""

    async def work():
        pass

    coro = work()
    try:
        with pytest.raises(TypeError):
            loop.create_task(coro, no_such_kwarg=1)
    finally:
        coro.close()


def test_create_task_on_closed_loop_raises(loop):
    async def work():
        pass

    coro = work()
    loop.close()
    try:
        with pytest.raises(RuntimeError, match="closed"):
            loop.create_task(coro)
    finally:
        coro.close()


def test_task_factory_takes_back_the_fast_path(loop):
    """Setting a factory must unbind the native fast path, and clearing
    it must put the fast path back -- otherwise the factory is silently
    ignored (or permanently sticky)."""
    made = []

    def factory(lp, coro, context=None):
        t = asyncio.Task(coro, loop=lp, context=context)
        made.append(t)
        return t

    async def one():
        await loop.create_task(asyncio.sleep(0))

    loop.set_task_factory(factory)
    loop.run_until_complete(one())
    # run_until_complete wraps its own coroutine in a task too, so the
    # factory sees two -- the count matters only as a delta.
    with_factory = len(made)
    assert with_factory >= 1

    loop.set_task_factory(None)
    loop.run_until_complete(one())
    assert len(made) == with_factory, "factory still used after set_task_factory(None)"


def test_subclass_override_is_not_shadowed_by_the_fast_path():
    """Binding a native method onto the instance would shadow a subclass
    override. A subclass has every right to expect its own method to be
    called, so the fast path must stand down for it."""
    calls = []

    class MyLoop(cadeloop.Loop):
        def create_task(self, coro, *, name=None, context=None, **kw):
            calls.append("task")
            return super().create_task(coro, name=name, context=context, **kw)

        def create_future(self):
            calls.append("future")
            return super().create_future()

    lp = MyLoop()
    try:
        fut = lp.create_future()
        fut.set_result(None)
        lp.run_until_complete(lp.create_task(asyncio.sleep(0)))
    finally:
        lp.close()
    assert calls == ["future", "task"]


def test_debug_mode_still_trims_the_source_traceback(loop):
    """Debug mode routes back through the facade so that the
    `del task._source_traceback[-1]` frame trim still happens -- the
    native path has no Python frame to trim."""

    async def work():
        pass

    loop.set_debug(True)
    try:
        t = loop.create_task(work())
        assert t._source_traceback
        # The trimmed entry is the facade's own create_task line; what is
        # left must end at this test function.
        assert t._source_traceback[-1].name == (
            "test_debug_mode_still_trims_the_source_traceback"
        )
        loop.run_until_complete(t)
    finally:
        loop.set_debug(False)


def test_closed_loop_is_collectable():
    """`Loop.__dict__` holds the core and the core holds bound methods of
    the Loop (the error hooks, and the owner reference the native
    create_task fast path needs). Without a tp_traverse on the core the
    collector could not see that cycle, so every closed loop stayed
    allocated for the life of the process."""
    import gc
    import weakref

    refs = []
    for _ in range(3):
        lp = cadeloop.Loop()
        refs.append(weakref.ref(lp))
        lp.close()
        del lp
    gc.collect()
    gc.collect()
    alive = sum(1 for r in refs if r() is not None)
    assert alive == 0, f"{alive}/{len(refs)} closed loops were never collected"


def test_a_context_that_cannot_be_entered_fails_one_callback_not_the_loop():
    """Entering a handle's Context can fail on the caller's account -- the
    plain way being to run the loop itself inside that same Context, which
    leaves it already entered when the handle tries. That returned Err
    from run_handle, which the dispatcher treats as fatal, so it unwound
    run_forever and stopped the loop. The identical mistake made *inside*
    a callback is reported and survived; asyncio's Handle._run stops the
    loop only for KeyboardInterrupt/SystemExit."""
    import contextvars

    lp = cadeloop.new_event_loop()
    ctx = contextvars.copy_context()
    hits = []
    errors = []
    lp.set_exception_handler(lambda _loop, c: errors.append(c.get("exception")))
    try:
        lp.call_soon(lambda: hits.append("a"), context=ctx)
        lp.call_soon(lambda: hits.append("b"))
        lp.call_soon(lp.stop)
        # run_forever executes INSIDE ctx, so ctx is already entered when
        # the first handle tries to enter it.
        ctx.run(lp.run_forever)
    finally:
        lp.close()

    assert errors and isinstance(errors[0], RuntimeError), errors
    assert "already entered" in str(errors[0])
    # The loop kept going: the callback after the failing one still ran.
    assert hits == ["b"], hits


def test_slow_callback_duration_is_a_real_knob(caplog):
    """The native dispatcher had 100ms baked in, so asyncio's standard
    `loop.slow_callback_duration` did nothing when set -- and the facade
    did not carry the attribute at all, so merely reading it raised
    AttributeError. Reported by Codex on PR #1."""
    import logging

    lp = cadeloop.new_event_loop()
    lp.set_debug(True)
    caplog.set_level(logging.WARNING, logger="cadeloop")
    try:
        assert lp.slow_callback_duration == 0.1  # asyncio's default
        lp.slow_callback_duration = 0.005
        assert lp.slow_callback_duration == 0.005

        def slow():
            _time.sleep(0.05)  # 10x the configured threshold
            lp.stop()

        lp.call_soon(slow)
        lp.run_forever()
    finally:
        lp.close()

    # Reported through the logger, which is where _on_slow_callback sends
    # it -- not the exception handler.
    assert any("took" in r.getMessage() for r in caplog.records), caplog.text
    with pytest.raises(ValueError):
        lp2 = cadeloop.new_event_loop()
        try:
            lp2.slow_callback_duration = 0
        finally:
            lp2.close()


def test_run_until_complete_rejects_before_scheduling_the_coroutine():
    """The running-loop check happened inside run_forever(), by which
    point ensure_future() had already created and queued a Task. So a
    coroutine the caller was told had been REJECTED went on executing as
    an unobserved background task. base_events checks first; now so does
    this. Reported by Codex on PR #1."""
    lp = cadeloop.new_event_loop()
    ran = []

    async def should_not_run():
        ran.append(1)  # pragma: no cover - the point is that it does not

    async def inner():
        # Re-entering the running loop must refuse, and must not have
        # scheduled anything on the way to refusing.
        coro = should_not_run()
        with pytest.raises(RuntimeError, match="already running"):
            lp.run_until_complete(coro)
        # Never scheduled -- that is the fix under test -- so close it
        # here, or it is collected with a "never awaited" RuntimeWarning.
        coro.close()

    try:
        lp.run_until_complete(inner())
        # Give anything that was wrongly scheduled a chance to run.
        lp.run_until_complete(asyncio.sleep(0.05))
    finally:
        lp.close()

    assert ran == [], "the rejected coroutine ran anyway, as a background task"
