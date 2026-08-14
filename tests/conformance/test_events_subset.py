"""Conformance subset (R-120): call_soon/timer semantics modeled on
CPython's ``test.test_asyncio.test_events`` / ``test_base_events``
assertions, rewritten self-contained so they run on distro Pythons that
don't ship the ``test`` package. The full-suite runner is
``run_cpython_suite.py`` (used on CI images that include the test suite).
"""

import asyncio
import threading
import time

import cadeloop
import pytest


@pytest.fixture()
def loop():
    lp = cadeloop.new_event_loop()
    asyncio.set_event_loop(lp)
    yield lp
    asyncio.set_event_loop(None)
    if not lp.is_closed():
        lp.close()


# Mirrors EventLoopTestsMixin.test_call_later
def test_call_later(loop):
    results = []

    def callback(arg):
        results.append(arg)
        loop.stop()

    loop.call_later(0.1, callback, "hello world")
    t0 = time.monotonic()
    loop.run_forever()
    t1 = time.monotonic()
    assert results == ["hello world"]
    assert 0.08 <= t1 - t0 <= 0.8


# Mirrors EventLoopTestsMixin.test_call_soon
def test_call_soon(loop):
    results = []

    def callback(arg1, arg2):
        results.append((arg1, arg2))
        loop.stop()

    loop.call_soon(callback, "hello", "world")
    loop.run_forever()
    assert results == [("hello", "world")]


# Mirrors EventLoopTestsMixin.test_call_soon_threadsafe
def test_call_soon_threadsafe(loop):
    results = []

    def callback(arg):
        results.append(arg)
        if len(results) >= 2:
            loop.stop()

    def run_in_thread():
        loop.call_soon_threadsafe(callback, "hello")

    t = threading.Thread(target=run_in_thread)
    loop.call_later(0.1, callback, "world")
    t.start()
    loop.run_forever()
    t.join()
    assert results == ["hello", "world"]


# Mirrors EventLoopTestsMixin.test_run_until_complete
def test_run_until_complete(loop):
    delay = 0.1
    t0 = loop.time()
    loop.run_until_complete(asyncio.sleep(delay))
    dt = loop.time() - t0
    assert dt >= delay - 0.05


# Mirrors EventLoopTestsMixin.test_run_until_complete_stopped
def test_run_until_complete_stopped(loop):
    async def cb():
        loop.stop()
        await asyncio.sleep(0.1)

    task = cb()
    with pytest.raises(RuntimeError):
        loop.run_until_complete(task)


# Mirrors test_base_events timer semantics
def test_call_later_cancel(loop):
    results = []
    h = loop.call_later(0.05, results.append, "cancelled")
    h.cancel()
    loop.call_later(0.1, loop.stop)
    loop.run_forever()
    assert results == []
    assert h.cancelled()


def test_call_soon_handle_cancel_after_schedule(loop):
    results = []
    h = loop.call_soon(results.append, "cancelled")
    loop.call_soon(results.append, "kept")
    h.cancel()
    loop.call_later(0.05, loop.stop)
    loop.run_forever()
    assert results == ["kept"]


# Mirrors BaseEventLoopTests.test_run_once_in_executor / callback ordering
def test_callback_ordering_stable(loop):
    results = []
    for i in range(100):
        loop.call_soon(results.append, i)
    loop.call_soon(loop.stop)
    loop.run_forever()
    assert results == list(range(100))


# Mirrors BaseEventLoopTests.test__run_once_schedule_handle behavior:
# a timer scheduled in the past still runs via the ready queue.
def test_past_deadline_timer_runs(loop):
    results = []
    loop.call_at(loop.time() - 1, results.append, "past")
    loop.call_soon(loop.stop)
    loop.run_forever()
    # One extra tick may be needed; run again if empty.
    if not results:
        loop.call_soon(loop.stop)
        loop.run_forever()
    assert results == ["past"]


# Mirrors RunningLoopTests
def test_get_running_loop_inside_and_outside(loop):
    with pytest.raises(RuntimeError):
        asyncio.get_running_loop()

    seen = []

    async def main():
        seen.append(asyncio.get_running_loop())

    loop.run_until_complete(main())
    assert seen == [loop]
    with pytest.raises(RuntimeError):
        asyncio.get_running_loop()


# Mirrors test_events handle-repr smoke assertions
def test_handle_reprs(loop):
    h = loop.call_soon(print)
    assert "print" in repr(h)
    h.cancel()
    assert "cancelled" in repr(h)
    th = loop.call_later(60, print)
    assert "TimerHandle" in type(th).__name__ or "print" in repr(th)


# Mirrors EventLoopTestsMixin.test_run_forever_pre_stopped semantics
def test_run_forever_when_stopped_twice(loop):
    loop.stop()
    loop.run_forever()  # returns
    out = []
    loop.call_soon(out.append, "second-run")
    loop.call_soon(loop.stop)
    loop.run_forever()  # runs normally again
    assert out == ["second-run"]
