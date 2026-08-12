# Architecture

Layering per spec §3 (R-020..R-022):

```
L4  Python user code / ASGI app
L3  python/cadeloop: Loop facade, EventLoopPolicy, Config, CLI     [Python]
L2  crates/pyshim: CoreLoop/Handle bindings, HTTP+ASGI engine (M2) [Rust/PyO3]
L1  crates/core::reactor: completion dispatch, timer heap, queues  [Rust]
L0  crates/core::backend: IOCP | RIO (M3) | portable-dev           [Rust]
```

## Crate split

`cadeloop-core` has **no Python dependency**. Everything is generic over an
opaque callback token `T`; the pyshim instantiates `T = Py<PyAny>`. This
keeps L0/L1 state machines unit-testable on any host and concentrates every
GIL assumption in one pyshim module (`gil_boundary`, R-010).

## Threading & GIL model (R-021)

One reactor thread per loop == the thread that called `run_forever`. Each
tick:

1. **GIL held**: drain cross-thread queue, absorb timer cancellations,
   pop expired timers into the ready queue, compute poll timeout
   (0 if ready work exists; else min(next deadline, 100ms) — R-030).
2. **GIL released** (the only release point): backend poll —
   spin-then-park (R-060), then `GetQueuedCompletionStatusEx` batch 256.
3. **GIL re-acquired once per batch** (not per event): translate
   completions, then dispatch up to 128 ready callbacks (R-054) via
   `PyObject_VectorcallMethod` on `context.run(callback, *args)`.

### The state-cell protocol (why no lock)

Loop state lives in an `UnsafeCell` guarded by thread-affinity + the GIL
rather than a mutex (see `gil_boundary.rs`). Holding a lock across the
GIL-released poll would deadlock with any GIL-holding thread blocking on
that lock (poll thread needs the GIL back; GIL holder waits on the lock).
The affinity protocol raises asyncio's standard "non-thread-safe operation"
error instead of blocking, making the deadlock structurally impossible.
Cross-thread paths (`call_soon_threadsafe`, `stop`, `time`, `is_running`)
touch only atomics and the lock-free MPSC queue (R-022).

### Python-reference hygiene

No Python reference is ever dropped inside the state critical section: a
decref can run `__del__`/GC, which may re-enter the loop and alias the
`&mut` state. Discarded tokens (cancelled timers, close-time drains) are
routed to a *graveyard* vector and dropped after the critical section ends.

## Memory (R-070..R-075)

* mimalloc is the extension's global allocator (Python's allocator
  untouched).
* Buffer slabs: 2 MiB regions (VirtualAlloc, large pages attempted,
  silent fallback), size classes {4K, 16K, 64K}, per-class freelists,
  thread-affine (no locks), refcounted slots (kernel op + each exported
  memoryview hold refs), debug poisoning `0xDD`. RIO registers each region
  exactly once (cookie stored per region).

## Op lifecycle (R-037)

Every kernel op owns a pinned slab slot (`OVERLAPPED` first field,
container-of recovery, generation-checked). State machine
`Free -> Posted -> {Completed | Cancelled -> Completed} -> Free`; a slot
recycles only after its completion is reaped, `CancelIoEx` tolerates
`ERROR_NOT_FOUND`, and stale completions are debug-fatal / release-ignored.

## Portable dev backend

Non-Windows hosts get a condvar-based completion queue good for timers,
`call_soon`, and `call_soon_threadsafe` — enough to develop and
conformance-test L1/L3 anywhere. It implements no socket ops and is never
published (wheels are cp311-win_amd64 only, R-110). `backend="auto"|"iocp"|"rio"`
all resolve to it off-Windows.

## Facade fast paths (R-050)

`Loop.__init__` binds the native methods (`call_soon`, `call_later`,
`call_at`, `time`, `stop`, ...) directly onto the instance, so hot calls hit
the PyO3 method with no Python wrapper frame. The facade supplies the
long-tail surface: futures/tasks, executors, DNS pool + 5s cache (R-055),
exception-handler machinery, asyncgen shutdown, and milestone-gated
`NotImplementedError`s for I/O that hasn't landed.
