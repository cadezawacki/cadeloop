# How cadeloop works

A short tour for people who use asyncio but have never looked inside an
event loop. Each page builds on the one before and takes about five minutes
to read. No Rust or C knowledge needed.

1. [What an event loop does](1-event-loops.md): why one thread can serve
   thousands of connections, and what `await` actually does.
2. [How the operating system reports I/O](2-talking-to-the-os.md):
   readiness on Linux, completion on Windows, and why the difference
   mattered for Python on Windows.
3. [Inside cadeloop](3-inside-cadeloop.md): the layers, one tick step by
   step, spinning, write corking, timers and buffers.
4. [The HTTP server](4-the-http-server.md): where a request's time goes,
   and what `cadeloop.serve()` takes out of Python.
5. [How cadeloop compares](5-how-it-compares.md): asyncio, uvloop,
   winloop, rloop and rsloop as loops; uvicorn, hypercorn and granian as
   servers.
6. [Measuring performance](6-measuring.md): what to measure, the traps
   that make numbers meaningless, and how to benchmark your own app.

The diagrams are drawn by [`diagrams.py`](diagrams.py) in this folder, in a
light and a dark version each; run it after changing one.
