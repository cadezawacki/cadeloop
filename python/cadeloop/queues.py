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

import math
import numbers
import operator
import types
from asyncio import events, mixins

from . import _core

__all__ = ["Queue"]

_MISSING = object()
_I64_MAX = 2**63 - 1


def _native_maxsize(maxsize):
    """The capacity the native queue enforces for a stdlib-style maxsize.

    asyncio.Queue keeps maxsize as given and only ever evaluates
    ``maxsize <= 0`` and ``qsize() >= maxsize``; the native side needs an
    i64 that answers both the same way. Anything at or above 2**63 cannot
    be reached by a real queue, and a non-integral bound is reached at its
    ceiling.
    """
    try:
        n = operator.index(maxsize)
    except TypeError:
        if not isinstance(maxsize, numbers.Real):
            raise TypeError(f"maxsize must be a number, not {type(maxsize).__name__}") from None
        x = float(maxsize)
        if not math.isfinite(x) or x <= 0:
            return 0  # NaN, +-inf and <= 0 are never "full" in the stdlib either
        return min(math.ceil(x), _I64_MAX)
    return max(min(n, _I64_MAX), -_I64_MAX)


class Queue:
    """Drop-in for asyncio.Queue (FIFO), native hot paths."""

    # __weakref__: asyncio.Queue is weak-referenceable, and pub/sub fan-out
    # keeps its subscribers' queues in a WeakSet.
    __slots__ = ("_q", "_maxsize", "_loop", "_join_loop", "__weakref__")

    __class_getitem__ = classmethod(types.GenericAlias)

    def __init__(self, maxsize=0):
        self._q = _core.FastQueue(_native_maxsize(maxsize))
        self._maxsize = maxsize
        # Bound on first park, like asyncio.mixins._LoopBoundMixin. join()
        # waits on the stdlib queue's internal Event, which binds on its
        # own -- hence a second slot rather than one shared binding.
        self._loop = None
        self._join_loop = None

    # -- inspection (delegated, one native call each) -------------------

    @property
    def maxsize(self):
        return self._maxsize

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
        return f"<cadeloop.Queue maxsize={self._maxsize!r} qsize={self._q.qsize()}>"

    def _bind(self, loop, attr="_loop"):
        # asyncio.mixins._LoopBoundMixin._get_loop. Without it a waiter
        # left parked by an earlier loop the queue outlived silently
        # swallowed the next wake, and the live waiter behind it hung with
        # an item in the queue; the stdlib refuses the second loop instead.
        if getattr(self, attr) is None:
            with mixins._global_lock:
                if getattr(self, attr) is None:
                    setattr(self, attr, loop)
        if loop is not getattr(self, attr):
            raise RuntimeError(f"{self!r} is bound to a different event loop")

    # -- awaitable surface ----------------------------------------------

    async def put(self, item):
        q = self._q
        while not q.try_put(item):
            loop = events.get_running_loop()
            if loop is not self._loop:
                self._bind(loop)
            fut = loop.create_future()
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
            loop = events.get_running_loop()
            if loop is not self._loop:
                self._bind(loop)
            fut = loop.create_future()
            q.park_getter(fut)
            try:
                await fut
            except BaseException:
                q.getter_recovery(fut)
                raise
            v = q.get_or(_MISSING)
        return v

    async def join(self):
        q = self._q
        if q.unfinished():
            loop = events.get_running_loop()
            if loop is not self._join_loop:
                self._bind(loop, "_join_loop")
            fut = loop.create_future()
            q.park_joiner(fut)
            try:
                await fut
            finally:
                # Event.wait()-style removal: a cancelled or timed-out join
                # must not stay parked until the count reaches zero -- on a
                # queue that never drains, repeated timed-out joins would
                # retain one future each, without bound.
                q.unjoin(fut)
