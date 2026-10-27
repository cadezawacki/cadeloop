"""Regression tests for the server / supervisor / CLI / queue audit.

Each test names the finding it pins (S1..S30 in the audit report) and
fails on the code before the fix. Process-model tests drive real worker
pools in subprocesses -- the bugs live in signal delivery, fork and
process lifetime, which an in-process fake cannot reproduce faithfully --
with ephemeral ports, generous timeouts and a kill-everything cleanup.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import textwrap
import weakref

import cadeloop
import pytest

PKG_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(cadeloop.__file__)))


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
