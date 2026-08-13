"""M0 loop semantics: call_soon / timers / lifecycle / threadsafe wakeup."""

import asyncio
import contextvars
import threading
import time as _time

import pytest

import cadeloop


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
    with pytest.raises(TypeError):
        loop.call_soon(42)


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

    def do_close():
        try:
            loop.close()
        except RuntimeError as e:
            errors.append(str(e))
        loop.stop()

    loop.call_soon(do_close)
    loop.run_forever()
    assert errors and "Cannot close a running event loop" in errors[0]


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
    # Cached second call returns identical result (R-055).
    assert loop.run_until_complete(main()) == res


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
