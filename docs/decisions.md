# Design decisions (ADR log)

## ADR-1: Project name `cadeloop`
Owner renamed the spec's `iceloop` to `cadeloop` (matches the repository).
All R-004 API mechanics unchanged: `cadeloop.EventLoopPolicy()`,
`cadeloop.new_event_loop()`, `cadeloop.serve(...)`, `python -m cadeloop`,
env prefix `CADELOOP_`.

## ADR-2: Python-free core crate
`cadeloop-core` is generic over the callback token; all PyO3 code lives in
`cadeloop-pyshim`. Consequences: L0/L1 state machines proptest on any host;
R-010's `gil_boundary` isolation is structural, not a convention.

## ADR-3: Portable dev backend
The spec targets Windows-only production backends, but development and this
repo's own CI need runnable loop semantics everywhere. A condvar-based
completion queue implements poll/wakeup only (no sockets). It is explicitly
not a supported production target and is unreachable from shipped wheels.

## ADR-4: Thread-affinity state cell instead of a lock (pyshim)
A mutex held across the GIL-released poll deadlocks with GIL-holding
threads that touch the loop. Instead: `UnsafeCell` + owner-thread protocol
that *raises* asyncio's "non-thread-safe operation" error from foreign
threads while running. Cross-thread APIs are atomics + lock-free queue
only. See `gil_boundary.rs` for the soundness argument and the nogil
migration note.

## ADR-5: Graveyard protocol for Python refs
Nothing inside the state critical section may drop a `Py<T>` (decref can
run `__del__`/GC → re-entrancy → aliasing the `&mut` state). Heap/reactor
APIs therefore never drop tokens; they hand them back (`take_graveyard`,
`clear_pending`) for disposal outside the critical section.

## ADR-6: PyO3 pinned to 0.24
Spec floor is 0.21; 0.24 is the newest line vetted for this codebase's
patterns (frozen classes, `as_super`, vectorcall ffi). Upgrades are
deliberate bumps with CI proof, not floating.

## ADR-7: M0 facade completes the loop surface in Python
Hot paths (call_soon/timers/time/run) are native and bound directly onto
instances; the long tail (executors, exception handlers, asyncgens) is
Python for correctness-first iteration speed. Native migration happens
per-path when profiles justify it (R-050 lists the required fast paths —
all scheduling ones are already native).

## ADR-8: `panic = "abort"` in release (R-111)
Mandated by spec; in a Python extension this means any release-mode Rust
panic kills the process. Policy: no panics on user-reachable paths
(`debug_assert` + release-tolerant fallbacks, e.g. stale-completion drops);
CI runs the test suite in both profiles.

## ADR-9: Windows verification from a Linux dev box
This tree was authored on Linux: Windows code paths are gated behind
`cfg(windows)` and verified with `cargo check/clippy --target
x86_64-pc-windows-msvc` (zero warnings) plus PYO3_CROSS checks for the
extension. Functional Windows verification happens in the GitHub Actions
matrix (build, tests, conformance on windows-2022/2025). Until those
runners execute the IOCP paths, treat them as compile-verified, not
behavior-verified.

## ADR-10: Benchmark honesty
Only the L1 scheduling layer is benchmarkable off-Windows. The README's
Linux report is labeled as such: it compares scheduling primitives against
stdlib/uvloop/rloop on the portable backend and makes no claim about the
R-003 acceptance targets, which are Windows two-machine runs (R-131).

## ADR-11: Linux is a first-class dev/test target (epoll backend)
Owner directives — drop-in coverage of the full asyncio surface, Linux
benchmarking — override the spec's "no Linux support" non-goal (§15). The
epoll backend emulates the IOCP completion-style op API (proactor-over-
epoll), so ONE Rust transport layer serves both platforms and every
transport test runs in this repo's Linux CI. Production wheels remain
cp311-win_amd64; Linux perf headroom (EPOLLET, io_uring) is documented,
not chased.

## ADR-12: Transports live in Rust
A Python transport layer over sock_* futures would concede uvloop's whole
advantage. Transports are Rust state driven during completion translation;
protocol callbacks are cached bound methods; events cross the state-cell
boundary as pre-built payloads (see ADR-5's graveyard rule, now extended
to buffers).

## ADR-13: pause_reading never cancels the in-flight recv
Cancelling creates completion/slot-reuse races (a cancelled op's completion
may already be queued carrying data) and costs syscalls. Pausing merely
stops re-posting; the in-flight result is delivered (asyncio's pause is
advisory) and reading resumes for free. Belt-and-braces: kernel ops hold a
buffer-slot refcount released only when their completion is reaped, so no
teardown ordering can recycle a slot the kernel may still write (R-073).

## ADR-14: The GIL is released only for polls that can block
R-021 mandates GIL-released polling; a zero-timeout reap cannot block, and
the save/restore costs ~100ns/tick. Ticks with pending ready work poll
non-blocking WITH the GIL held. This plus the epoll no-op-poll fast path
and single-clock-read rule keeps the M1 tick at M0's scheduling cost
(verified: call_soon_chain 3.26M ops/s before/after).

## ADR-15: TLS ships now via stdlib sslproto
R-059 explicitly allows "a compatibility path built on ssl.MemoryBIO in
Python (correctness first)" — that is asyncio.sslproto.SSLProtocol over
our transports, exactly uvloop's approach. HTTPS works today; the native
OpenSSL-BIO engine remains the M4 performance item.

## ADR-16: pyo3 freelist rejected after measurement
rsloop uses `#[pyclass(freelist=1024)]` on its handle types. Measured
here, pyo3's freelist LOCKS (a `parking_lot`-style mutex on every
alloc/dealloc): call_soon went 343ns → 781ns. Reverted; plain mimalloc
allocation wins. Lesson recorded because the flag looks like a free win.

## ADR-17: HTTP parsing is llhttp-in-cell; ASGI dispatch is phase 2
The M2 engine parses inside the state cell (vendored llhttp 9.2.1,
strict mode, C + Rust accumulators — no Python execution, satisfying the
gil_boundary contract) and queues completed requests. Python runs only in
phase 2: scope dict, app coroutine, send/receive. Response bytes are
serialized in Rust and enter the same corked write queue as transport
writes (R-035/R-084), so the wire path materializes zero Python objects.
Malformed input never reaches Python at all (R-086: 400/413/414/431
answered in-cell).

## ADR-18: A request finishes when its app coroutine returns
Not when the response completes. Consequences: (a) post-response code —
Starlette BackgroundTask — runs before the next pipelined request, same
serialization uvicorn provides; (b) the pipelined pump stays iterative
(step never re-enters the pump, so a burst of N sync requests costs no
recursion); (c) response-completion bookkeeping needs no per-request
sequence numbers. The cost — a slow background task delays the next
request on THAT connection — matches uvicorn's observable behavior.

## ADR-19: Eager AppTask registers as asyncio.current_task()
The R-056 eager path steps app coroutines without allocating asyncio
Tasks. anyio (Starlette's task groups, StreamingResponse) weakrefs and
interrogates `current_task()` — with None it crashes. AppTask therefore
calls `_asyncio._enter_task`/`_leave_task` around every step and exposes
the Task surface anyio touches (weakref slot, cancel→awaited-future
forwarding, done/get_loop/get_name/uncancel/cancelling). Verified against
Starlette streaming + background tasks. `eager_tasks=False` remains the
full-fidelity escape hatch (§16).

## ADR-20: aiofastnet findings — stacked transports beat proactor emulation
aiofastnet (Cython transports patched onto any base loop via add_reader)
was benched standalone AND stacked on cadeloop. On echo-rtt the stack
"aiofastnet-cadeloop" beat cadeloop's own native transports by ~12%
(42.8K vs 38.3K msg/s): on the epoll dev backend, a reader-callback
transport recv()s inline in the poll tick, while our proactor emulation
pays a completion-slot repost hop. Two conclusions recorded: (1) the M2
native HTTP engine bypasses that hop entirely (parse happens on the recv
completion in-cell); (2) an inline-recv-on-readable fast path for the
epoll backend is queued as M2.5 — it does not affect Windows/IOCP, where
completions are the native kernel interface, and it validates that
cadeloop's scheduler core composes: aiofastnet-on-cadeloop was the
fastest non-native stack measured.
