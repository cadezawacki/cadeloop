"""cadeloop.Queue: an asyncio.Queue-compatible FIFO whose hot paths are a
single native call each (R-150 perf).

The stdlib queue's put/get spend their time in pure-Python bookkeeping
(~10 operations per op); here the entire fast path -- capacity check,
deque move, counter, waiter wake -- is one call into the native core, and
the coroutine wrappers exist only so ``await`` rides CPython's optimized
generator-return path (a custom awaitable's materialized StopIteration
costs more than the coroutine it replaces; measured, not guessed).

Parking, cancellation recovery, and join()/task_done() semantics are
ported from CPython 3.11 asyncio/queues.py -- see fastqueue.rs for the
line-by-line mapping. FIFO only: the LIFO/priority subclass hooks the
stdlib gets from overridable _put/_get would put Python back on the hot
path, which is the entire cost being removed.
"""

from asyncio import events

from . import _core

__all__ = ["Queue"]

_MISSING = object()


class Queue:
    """Drop-in for asyncio.Queue (FIFO), native hot paths."""

    __slots__ = ("_q",)

    def __init__(self, maxsize=0):
        self._q = _core.FastQueue(maxsize)

    # -- inspection (delegated, one native call each) -------------------

    @property
    def maxsize(self):
        return self._q.maxsize

    def qsize(self):
        return self._q.qsize()

    def empty(self):
        return self._q.empty()

    def full(self):
        return self._q.full()

    def put_nowait(self, item):
        self._q.put_nowait(item)

    def get_nowait(self):
        return self._q.get_nowait()

    def task_done(self):
        self._q.task_done()

    def __repr__(self):
        return f"<cadeloop.Queue maxsize={self._q.maxsize} qsize={self._q.qsize()}>"

    # -- awaitable surface ----------------------------------------------

    async def put(self, item):
        q = self._q
        while not q.try_put(item):
            fut = events.get_running_loop().create_future()
            q.park_putter(fut)
            try:
                await fut
            except BaseException:
                q.putter_recovery(fut)
                raise

    async def get(self):
        q = self._q
        v = q.get_or(_MISSING)
        while v is _MISSING:
            fut = events.get_running_loop().create_future()
            q.park_getter(fut)
            try:
                await fut
            except BaseException:
                q.getter_recovery(fut)
                raise
            v = q.get_or(_MISSING)
        return v

    async def join(self):
        await self._q.join()
