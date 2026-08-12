"""Event loop policy and installation helpers (R-100)."""

from __future__ import annotations

import asyncio

from .loop import Loop

__all__ = ["EventLoopPolicy", "install", "new_event_loop", "run"]


class EventLoopPolicy(asyncio.events.BaseDefaultEventLoopPolicy):
    """``asyncio.set_event_loop_policy(cadeloop.EventLoopPolicy())``."""

    _loop_factory = Loop


def new_event_loop() -> Loop:
    """Create a fresh cadeloop event loop."""
    return Loop()


def install() -> None:
    """Set cadeloop as the process-wide asyncio policy (uvloop/winloop
    convention)."""
    asyncio.set_event_loop_policy(EventLoopPolicy())


def run(main, *, debug=None):
    """``cadeloop.run(coro)`` — like ``asyncio.run`` but always on a
    cadeloop loop, regardless of the installed policy."""
    with asyncio.Runner(loop_factory=new_event_loop, debug=debug) as runner:
        return runner.run(main)
