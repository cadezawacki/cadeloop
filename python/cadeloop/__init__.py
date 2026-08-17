"""cadeloop — maximum-performance asyncio event loop + ASGI stack.

Usage (R-004, adjusted project name)::

    import asyncio, cadeloop
    cadeloop.install()                # or:
    asyncio.set_event_loop_policy(cadeloop.EventLoopPolicy())

    loop = cadeloop.new_event_loop()
    cadeloop.run(main())

    cadeloop.serve(app, "0.0.0.0", 8000)     # ASGI server (milestone M2)
"""

from ._core import __version__ as _core_version
from .config import Config
from .loop import Loop
from .policy import EventLoopPolicy, install, new_event_loop, run
from .server import serve

__version__ = _core_version
__all__ = [
    "Config",
    "EventLoopPolicy",
    "Loop",
    "install",
    "new_event_loop",
    "run",
    "serve",
    "__version__",
]
