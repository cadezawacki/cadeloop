"""Event loop policy and installation helpers (R-100)."""

from __future__ import annotations

import asyncio

from .loop import Loop

__all__ = ["EventLoopPolicy", "install", "new_event_loop", "run"]


# On POSIX the child-watcher machinery that asyncio.create_subprocess_*
# depends on lives in the Unix policy, not in BaseDefaultEventLoopPolicy.
# Deriving from the base class there produced a policy whose loops could
# not spawn subprocesses at all once install() was called -- direct-loop
# tests kept passing only because they ran under the process's original
# Unix policy.
_BasePolicy = getattr(asyncio, "DefaultEventLoopPolicy", asyncio.events.BaseDefaultEventLoopPolicy)


class EventLoopPolicy(_BasePolicy):  # type: ignore[misc,valid-type]
    """``asyncio.set_event_loop_policy(cadeloop.EventLoopPolicy())``.

    Inherits the platform default policy so POSIX keeps its child-watcher
    implementation (`get_child_watcher`/`set_child_watcher`), and only the
    loop factory is replaced.
    """

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
