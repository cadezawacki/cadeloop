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

**Addendum (drop-in-completeness audit, post-M5):** the verification
above was accurate but incomplete — it never exercised anyio's
thread-offload path or contextvars isolation, and both were broken.
anyio's `WorkerThread` (the target of `anyio.to_thread.run_sync`, which
Starlette's `run_in_threadpool` and every FastAPI plain `def` route or
sync `Depends()` go through) resolves the "root task" to
`current_task()` and then reads `root_task._loop` and calls
`root_task.add_done_callback(...)` — AppTask had neither, so every sync
route crashed with AttributeError. Separately, `step_inner` drove
coroutines with no `PyContext_Enter`/`Exit` boundary at all (unlike
`handles.rs::run_handle`'s existing discipline for plain callbacks), so
a contextvar set during one request stayed visible to a later or
concurrently-interleaved request on the same worker — silent state
corruption for Sentry/OpenTelemetry/structlog/correlation-ID patterns.
Both fixed: AppTask now has a real `_loop` getter and Future-style
`add_done_callback`/`remove_done_callback`, and captures+enters a
`contextvars.Context.copy()` per step, mirroring `run_handle` exactly.
See `test_fastapi_sync_route` and `test_contextvar_isolation_*` in
tests/unit/test_http_engine.py.

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

## ADR-21: The aiofastnet mirror — lazy epoll interest + drained-recv
ADR-20 identified the gap; this closes it. Per ping-pong message the old
proactor emulation paid recv(success) + epoll_ctl(DEL) + recv(EAGAIN) +
epoll_ctl(ADD). Now: op completions leave the kernel mask armed (the
same-tick re-post finds desired == kernel and issues nothing), and a
short read marks the fd drained so the next post parks without the
speculative recv. An event with no consumer is the single disarm point,
so LT epoll cannot storm. Two hazards this created and their fixes, both
regression-tested: (1) fd-number reuse after close and (2) same-socket
re-register (connect → attach) could leave a stale mask that either
silently skips a needed epoll_ctl (hang) or EEXISTs — register_socket
now resets entry AND kernel registration, and watches sync eagerly
(their removal path is the one that races user-space close). Result:
one recv syscall per message; echo-rtt +28% (35.1K → 45.0K msg/s),
matching the aiofastnet-stacked configuration that exposed the gap.

## ADR-22: The rloop mirror — one state-cell entry per tick
rloop's call_soon_chain lead was tick anatomy, not call_soon itself
(their schedule path is nearly identical, context copy included). Our
tick entered the state cell three times (poll phase, translate phase,
batch take); a queue-depth-1 chain pays full tick overhead per callback.
Pure-scheduling ticks now do flush + prepare + poll + translate + batch
take under ONE claim. Interrupt safety: reactor.unpop_ready returns an
undispatched batch tail to the queue front, so KeyboardInterrupt (from
the throttled signal check or a callback) loses nothing and preserves
FIFO. call_soon_chain 3.15 → 3.64 M ops/s; threadsafe 2.58 → 3.55.
rloop's remaining threadsafe edge comes from reusing a loop-init context
snapshot instead of copying the caller's context per call — declined:
call_soon_threadsafe capturing the calling thread's context is
observable drop-in behavior (R-013).

## ADR-23: RIO is a hybrid over IOCP, implemented ahead of its hardware
Owner-directed: implement RIO (R-040..R-044) even though this
environment cannot execute it. Shape: RIO has no accept/connect/cancel
and no readiness story, so the backend wraps the IOCP backend — AcceptEx
(accept sockets now created WSA_FLAG_REGISTERED_IO), ConnectEx, probes,
and the PQCS wakeup stay IOCP; recv/send flow through per-socket request
queues into ONE completion queue whose notification posts to the same
IOCP port (KEY_RIO), so the loop keeps a single park point and the spin
phase drains RIODequeueCompletion syscall-free (R-041/R-060). Sockets
that cannot take an RQ (foreign fds) silently keep IOCP ops — mixed mode
is by design. Recv buffers resolve ptr → (RIO_BUFFERID, offset) against
slab regions registered once via the R-043 hook; sends copy into
registered 64 KiB staging slots (RIO takes one registered buffer per
request; oversize payloads ride the existing partial-send resumption).
CQ capacity is reserved at RQ creation and grown by doubling — overflow
is a visible creation-time refusal, never silent loss (§16). cancel()
marks the op and lets closesocket flush the completion, translated to
WSA_OPERATION_ABORTED for R-037 parity. RIO op ids carry a namespace bit
so the two slabs share net.ops safely. Discipline for untestable code:
everything that can be wrong in pure logic (region map, staging ledger,
CQ ledger) lives in platform-free rio_util with Linux-run unit tests;
the FFI file is thin, msvc-cross-checked at zero warnings, and
`backend="auto"` keeps resolving to IOCP until Windows-hardware
validation flips it (the recorded M3 gate). One side effect: the local
msvc cross-check had silently broken when M2 vendored llhttp (no MSVC C
compiler on Linux); build.rs now skips the C build for cross-CHECKS
(which never link) and real Windows builds compile it natively.

## ADR-24: first real Windows CI results — open investigation, worker crash
CI's `.github/workflows/ci.yml` had a YAML syntax error (an unquoted
`name:` containing `: `) that silently produced zero jobs on every run
since before this repo's CI history begins — every prior "tests pass"
claim was validated only by local runs, never by GitHub Actions. Fixing
it (and a follow-on `cargo fmt` drift + a `not_unsafe_ptr_arg_deref`
clippy finding on the new pipe ops) let real Windows jobs execute for
the first time. Two more real, CI-only findings surfaced and were fixed
straightforwardly: `build-windows` never copied the built extension
into the source tree (unlike `test-linux`), breaking subprocess-spawning
tests that point PYTHONPATH at the repo; and the lint job's Windows
clippy cross-check needs `llvm-dlltool` (via `python3-dll-a`, a
`pyo3-ffi` build dependency for `PYO3_CROSS`) which isn't on
`ubuntu-latest` by default — confirmed by inspecting the actual
installed file list that rustup's `llvm-tools`/`llvm-tools-preview`
component does NOT ship it (only `llvm-ar`/`nm`/`objcopy`/etc); it comes
from the OS's own `llvm` package.

Still open: `test_spawn_worker_pool_serves_and_stops` (the WSADuplicate-
SocketW fork-free spawn model, R-090) crashes a freshly spawned worker
with `STATUS_ACCESS_VIOLATION` (0xC0000005) — seen on windows-2025 alone
in one run, then on BOTH windows-2022 and windows-2025 in the next,
which rules out a single-runner fluke. No stack trace or crash dump is
obtainable from a remote CI log, so static auditing (pipe-read framing
via `BufferedReader.read()` — confirmed correct by direct simulation off
this repo; `_pin_to_cpu`'s `SetProcessAffinityMask` ctypes call — missing
`restype`, hardened, did not stop the recurrence; `SetConsoleCtrlHandler`'s
callback-trampoline lifetime in loop.py — looks correctly kept alive via
a persistent `self` attribute) has not found a conclusive root cause.
`_winworker.py` now has `_trace()` stderr markers (flushed after every
statement in `main()`) as a temporary bisection aid — pytest inherits
and captures the spawned worker's stderr, so the next CI run's failure
log should show the last stage reached before the crash. Remove the
markers once the crash site is identified and fixed.

A second Windows-only bug was found blocking the same investigation:
`test_starlette_routes_and_streaming`'s `/bg` sub-test (a Starlette
`BackgroundTask` response) hung past pytest-timeout's 120s ceiling on
`build-windows`, main thread stuck inside `run_forever()` — no
completion ever woke the reactor. It had been showing up almost as
often as the worker crash and, on at least one run, prevented the
suite from ever reaching the worker-crash test at all. Confirmed
locally (after finding and deleting a stale pre-session
`_core.cpython-311-x86_64-linux-gnu.so` that Python's import machinery
was preferring over every fresh rebuild — the reason earlier local
verification of this same session's Rust-level tracing attempts had
been silently testing nothing) that the equivalent request completes
in ONE synchronous step on Linux for a body this small — no
suspend/resume needed at all. Added flushed `eprintln!` markers in
`http.rs` at the request-driving choke points (initial step, every
resume, coroutine-finished, request-finish start/end).

With both sets of markers in place, the run immediately after landed
the first-ever fully green `build-windows` on BOTH runners — every
step, including the CPython conformance suite — and reached `stress`
(passed) and `benchmark-regression` (new failure) for the first time,
since both are gated on `needs: build-windows`. Neither the crash nor
the hang reproduced — but neither is confirmed fixed, since both are
intermittent (the crash has since recurred on one runner while the
other stayed green, and has hit worker 0 and worker 1 on different
runs), so treat them as "not reproduced in this run", never "fixed".

`benchmark-regression`'s failure was FIRST attributed here to the
tracing markers' own overhead. That attribution was wrong, and the
correction matters because it points at a broken gate rather than a
real regression:

* Every regressed entry is a *scheduling* microbenchmark
  (`call_soon_chain`, `timer_fire`, `task_spawn`, `future_chain`,
  `gather_fanin`, `queue_pingpong`, ...). None of them issue an HTTP
  request or spawn a worker, so they never execute a single line of
  the instrumented paths — the markers cannot be the cause.
* The gate runs `--suite sched --loops cadeloop` on a shared
  GitHub-hosted runner and compares against `bench/baselines/
  windows-sched.json`, which was recorded on the owner's own Windows
  machine (it still carries `asyncio`/`winloop`/`rsloop` contenders,
  hence the run's "missing from current run" lines for all three).
  Dedicated local hardware vs. a shared virtualized runner differs by
  far more than the 5% threshold permanently, so the gate as
  configured can never pass in CI regardless of code quality.

The fix is to record a CI-hardware baseline (or scope the gate to
nightly on a consistent runner), NOT to chase a phantom regression.
A pyo3 0.24 -> 0.29 cost cannot be ruled out from this data alone, but
the hardware mismatch already explains the full magnitude.

Separately, the `http.rs` markers were a real defect while they
existed: unlike the `coreloop.rs` tick tracing (env-gated behind
`CADELOOP_TRACE_TICK`) and the Python-side markers (scoped to worker
processes), they were UNCONDITIONAL on the request hot path — four
flushed stderr lines per request. Measured locally at 2000 requests:
41.8k req/s with them vs 70.7k without when stderr is captured the way
pytest/CI captures it. They have been removed; the /bg hang had not
recurred in the runs after they landed, so they were costing ~40% of
request throughput while yielding no new signal. If that hang returns,
re-add them env-gated like the tick tracing rather than unconditional.
