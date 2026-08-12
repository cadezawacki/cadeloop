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
