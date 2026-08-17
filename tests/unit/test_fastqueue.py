"""cadeloop.Queue (R-150): asyncio.Queue-compatible FIFO with native hot
paths. Semantics ported from CPython 3.11 asyncio/queues.py -- these
tests pin the ported behavior, cancellation recovery included."""

import asyncio

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


def test_nowait_fifo_and_exception_types(loop):
    q = cadeloop.Queue(maxsize=2)
    assert q.maxsize == 2 and q.empty() and not q.full() and q.qsize() == 0
    q.put_nowait(1)
    q.put_nowait(2)
    assert q.full() and q.qsize() == 2
    # asyncio's own exception classes, so except clauses written for the
    # stdlib queue keep working.
    with pytest.raises(asyncio.QueueFull):
        q.put_nowait(3)
    assert q.get_nowait() == 1
    assert q.get_nowait() == 2
    with pytest.raises(asyncio.QueueEmpty):
        q.get_nowait()


def test_blocking_roundtrip_preserves_order(loop):
    async def main():
        q = cadeloop.Queue(maxsize=8)
        out = []

        async def producer():
            for i in range(500):
                await q.put(i)
            await q.put(None)

        async def consumer():
            while True:
                v = await q.get()
                if v is None:
                    return
                out.append(v)

        await asyncio.gather(producer(), consumer())
        return out

    assert loop.run_until_complete(main()) == list(range(500))


def test_cancelled_getter_does_not_eat_the_wake(loop):
    """queues.py's except-block recovery: a getter whose future was
    resolved and whose task was then cancelled must pass the wake (and
    the item) to the next parked getter, not lose both."""

    async def main():
        q = cadeloop.Queue()
        g1 = asyncio.ensure_future(q.get())
        g2 = asyncio.ensure_future(q.get())
        await asyncio.sleep(0.01)  # both parked, g1 first in line
        q.put_nowait("x")  # resolves g1's waiter
        g1.cancel()  # lands before g1 consumes; recovery must wake g2
        with pytest.raises(asyncio.CancelledError):
            await g1
        assert await asyncio.wait_for(g2, 2) == "x"

    loop.run_until_complete(main())


def test_cancelled_parked_getter_before_any_item(loop):
    async def main():
        q = cadeloop.Queue()
        g1 = asyncio.ensure_future(q.get())
        g2 = asyncio.ensure_future(q.get())
        await asyncio.sleep(0.01)
        g1.cancel()
        with pytest.raises(asyncio.CancelledError):
            await g1
        q.put_nowait(41)
        assert await asyncio.wait_for(g2, 2) == 41

    loop.run_until_complete(main())


def test_cancelled_putter_recovery(loop):
    async def main():
        q = cadeloop.Queue(maxsize=1)
        q.put_nowait("a")
        p1 = asyncio.ensure_future(q.put("b"))
        p2 = asyncio.ensure_future(q.put("c"))
        await asyncio.sleep(0.01)  # both parked
        assert q.get_nowait() == "a"  # frees a slot, wakes p1
        p1.cancel()  # resolved-then-cancelled: wake must pass to p2
        with pytest.raises(asyncio.CancelledError):
            await p1
        await asyncio.wait_for(p2, 2)
        assert q.get_nowait() == "c"

    loop.run_until_complete(main())


def test_join_and_task_done(loop):
    async def main():
        q = cadeloop.Queue()
        await q.join()  # zero unfinished: returns immediately
        q.put_nowait(1)
        q.put_nowait(2)
        done = []

        async def worker():
            for _ in range(2):
                await q.get()
                q.task_done()
            done.append(True)

        await asyncio.gather(q.join(), worker())
        assert done == [True]
        with pytest.raises(ValueError):
            q.task_done()

    loop.run_until_complete(main())


def test_queue_is_loop_agnostic():
    """The queue parks on whatever loop is running -- it works unchanged
    on the stdlib loop."""
    lp = asyncio.new_event_loop()

    async def main():
        q = cadeloop.Queue(maxsize=4)

        async def producer():
            for i in range(100):
                await q.put(i)
            await q.put(None)

        async def consumer():
            out = []
            while True:
                v = await q.get()
                if v is None:
                    return out
                out.append(v)

        _, out = await asyncio.gather(producer(), consumer())
        return out

    try:
        assert lp.run_until_complete(main()) == list(range(100))
    finally:
        lp.close()
