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

**Addendum (later review pass): reversed.** The parity claim was wrong.
uvicorn (h11 and httptools alike) starts the next request on a connection,
and closes a close-delimited response, at `on_response_complete`, while the
previous request's task carries on with its background work. Measured
against uvicorn 0.54: behind a 1.5 s BackgroundTask the next keep-alive
request took 1.5 s here and 0.0 s there, and an HTTP/1.0 streaming response
reached EOF after 1.52 s against 0.02 s. The dead-peer sweep (H4) also
cancelled background work once its client had read the response and gone.
A request's turn on its connection now ends when its response completes.
An eager driver that suspends after that point, or a non-eager Task whose
final `send()` has gone out, becomes a *detached* request (`net::Detached`,
keyed by object). The connection is released through `finish_request`, so
the keep-alive or close decision and the pipeline pump run at once. A
detached request's completion is told apart from its connection's later
requests by the request epoch that `AppTask` and `HttpTaskDone` now carry.
A graceful drain waits for detached requests, `_cancel_requests` and
`close()` cancel them, and the sweep never reclaims one whose response was
complete. The same mechanism now keeps a request whose connection was torn
down under it (RST, EPIPE). Before, its eager driver was dropped with the
entry, so it could never be cancelled or finalized. Such a request is
reclaimed after the dead-peer deadline, like a parked request whose client
has gone. Consequence (b) still holds: the pump never recurses.

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
(accept sockets now created WSA_FLAG_REGISTERED_IO), ConnectEx, readiness
watches (probes then, AFD polls since ADR-30) and the PQCS wakeup stay IOCP; recv/send flow through per-socket request
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

## ADR-25: R-090's shared-listener worker model is unsound on Windows
The spawn-worker `STATUS_ACCESS_VIOLATION` (ADR-24) is not a stray bug —
it is the shared-listener design meeting a Windows invariant. Traced to
ground with `CADELOOP_TRACE_TICK` markers in `CoreLoop::tick`: the last
line before every crash is `about to poll` with no matching `poll
returned`, so the fault is inside `reactor.poll()`; and every tick up to
that point reports `drained 0 completions`, so the crashing worker never
reaped a single completion of its own.

Mechanism. `_serve_multi_spawn` binds ONE listener and hands it to N
workers via `WSADuplicateSocketW`. Every duplicate names the same
underlying file object, and on Windows a file object binds to exactly
one completion port for life. So worker A's `CreateIoCompletionPort`
succeeds and worker B's fails with `ERROR_INVALID_PARAMETER` (87) —
where `iocp.rs::register_socket` swallows it as "already associated,
treat as success". That heuristic was written for the legitimate
same-process R-033 socket-recycling case and cannot distinguish
"already associated with MY port" from "already associated with ANOTHER
PROCESS's port". B is waved through, posts `AcceptEx`, and its
completions are delivered to A's port carrying an `lpOverlapped` valid
only in B's address space. A's `translate_entry` casts it to `*IocpOp`
and reads `(*op_ptr).id` — the access violation. Every observed symptom
follows: the crash sits inside `poll` (which calls `translate_entry`);
the non-owning worker drains nothing forever; which worker dies flips
run to run with the association race; only the spawn model is affected,
because it is the only place a socket crosses a process boundary; and
runs go green whenever no connection lands before the test's timeout,
so no foreign completion is ever dequeued.

`CADELOOP_TRACE_ASSOC` now reports the 87-swallow when it happens, to
confirm this end-to-end on CI hardware rather than by inference alone.

Consequences, once confirmed:

* `register_socket` must stop treating 87 as success for a socket it did
  not itself associate. Swallowing it converts an unfixable
  misconfiguration into a memory-safety violation; failing loudly at
  listener setup turns the same condition into an actionable Python
  exception.
* `translate_entry` should not blindly trust `lpOverlapped`. The slab's
  generation check already rejects stale ids, but only AFTER the
  dereference that faults. Validating the pointer against the slab's own
  allocation range before casting makes a foreign completion survivable.
* R-090 itself needs a different Windows design. Duplicating a listener
  into N IOCP-owning processes cannot work. The workable shape is the
  supervisor owning the accept loop and handing each ACCEPTED socket to
  a worker (`WSADuplicateSocketW` per connection, before that socket is
  associated anywhere), which costs a handoff per connection but obeys
  the one-object-one-port rule. Until then, `workers > 1` on Windows is
  not safe to advertise.

## ADR-26: R-090 becomes supervisor-accept + per-connection handoff
ADR-25's fix. The shared-listener model is removed, not patched: no
listener crosses a process boundary any more. The supervisor binds,
listens, and runs the accept loop itself, then hands each ACCEPTED
connection to a worker round-robin; the worker adopts it into its own
completion port via the new `CoreLoop.http_adopt`.

The listener is deliberately never registered with a completion port —
the supervisor accepts with a plain blocking `accept()` on a dedicated
thread. That is load-bearing, not incidental: it is what keeps the
sockets it yields unassociated, and therefore legal for the receiving
worker to associate. Registering the supervisor's listener with an IOCP
to accept "more efficiently" would reintroduce the exact bug, one level
down, on every connection.

Transport of the connection is the one platform split, behind a single
frame format (4-byte length + `CMD [body]`):

* Windows — the child's stdin pipe; the body carries `socket.share()`
  (WSADuplicateSocketW) bytes.
* POSIX — an AF_UNIX **SOCK_SEQPACKET** pair dup'd onto the child's
  stdin; the descriptor rides SCM_RIGHTS attached to the frame. Seqpacket
  rather than stream because ancillary data needs a message boundary to
  hang on, and a buffered reader would otherwise consume the bytes an fd
  was attached to.

Using the same model on POSIX rather than keeping the old shared-listener
path there is the point: Windows is the only platform where this bug
bites and the slowest place to test, so the handoff path now runs in
Linux CI on every commit. `test_spawn_worker_pool_serves_and_stops` also
asserts distribution properly now — round-robin is ours, so 8 connections
across 2 workers MUST reach both, where the kernel-scheduled model could
only assert ">= 1". The fork model (`_serve_multi`, SO_REUSEPORT) is
untouched and remains the POSIX default, so the hot Linux path pays
nothing for this.

Cost: one handoff per connection on the fork-free path. That buys
correctness on Windows, where the alternative does not work at all.

Confirmed on Windows CI: `test_spawn_worker_pool_serves_and_stops`
exercises the whole supervisor/worker/control-pipe path (8 connections
distributed across 2 workers, then supervisor death cascading to both)
and has passed on windows-2022 and windows-2025 across every run since.
Together with the /bg hang not reproducing, that met the bar the markers
were waiting on, and they have been removed.

Removed, not vindicated: the mechanism behind the /bg hang was never
captured from a trace. `5d96fcb` fixes a real bug matching its signature
and the hang stopped, which is empirical settlement rather than proof.
The one piece kept is the op-target breakdown, now permanent as
`stats()["ops_by_target"]` — it is what distinguishes "a connect
completion was never delivered" from "a recv was orphaned", and a loop
stuck either way looks healthy on every other counter.

## ADR-27: Handles release their references instead of being GC-tracked
A handle that is part of a reference cycle -- `self._t =
loop.call_later(t, self._tick)`, the shape of every protocol that keeps its
own timer -- was uncollectable: handles were opaque to the collector, so
every object that armed a timer on itself leaked for the life of the
process, fired or cancelled. asyncio and uvloop GC-track their handles;
doing the same here measured ~35% slower on loops with many callbacks or
timers in flight, because every collection traverses every pending handle.

Instead a handle drops its callback, arguments and context the moment the
loop gives it up: after a one-shot dispatch, on `cancel()` (what asyncio's
`Handle.cancel()` does anyway), when a watch is removed or displaced, and
at close. A handle that still references anything is therefore reachable
through its loop, which reports exactly those pending handles from its own
`tp_traverse` (only while its state is quiescent: under-reporting is safe,
inconsistent reporting within one pass is not) and releases them from
`tp_clear`, so even an abandoned, unclosed loop whose timers point back at
it is collected. The handle types stay out of the GC: 56 and 72 bytes, the
pymalloc classes they always occupied.

## ADR-28: The loop clock is time.monotonic()'s, read fresh where users see it
R-061 asked for a clock cached per tick. In practice that broke asyncio's
contract twice: `loop.time()` started at 0 per loop, so deadlines computed
from `time.monotonic()` (`call_at`, `asyncio.timeout_at`) were meaningless;
and the value was frozen for the whole tick, so a `call_later` made after a
callback had blocked for 200ms counted those 200ms against its delay, and
`loop.time()` deltas taken inside a callback were 0.

Ticks are now `CLOCK_MONOTONIC` on Linux (the clock behind
`time.monotonic()`, a vDSO read) and QPC aligned to `time.monotonic()` at
loop creation on Windows. `loop.time()` and every `call_later` read it
fresh, as asyncio does; the reactor keeps one reading per tick for timer
expiry and poll timeouts, taken lazily on the tick's first use, so a
pure-scheduling tick reads no clock at all. Cost: ~23ns per `call_later`
(what asyncio and uvloop pay); the lazy tick reading paid it back on the
callback paths (+4%).

## ADR-29: Other threads may call into a loop that is not parked
asyncio's loops accept `call_soon`, `call_later`, `create_task` and
`Future.set_result` from other threads outside debug mode; uvloop does too.
The state cell refused every call from a thread other than the running
loop's -- and `set_result` raised only AFTER marking the future done, so
its callbacks were lost and the awaiting task never woke.

The cell now admits any GIL holder unless a borrow is live. The only borrow
that spans a GIL release is the loop's own poll, so a foreign thread is
refused exactly while the loop is parked; the scheduling calls then hand
their callback or timer over the lock-free queue (R-022) and wake the loop,
and anything else raises asyncio's non-thread-safe error. Debug mode keeps
asyncio's `_check_thread` refusal. The same rule gave `call_soon_threadsafe`
and `call_soon` one FIFO: whenever the state is reachable both push onto the
ready queue directly (asyncio's single `_ready` deque); the cross-thread
queue, drained a tick later, had run `call_soon_threadsafe(a);
call_soon(b)` as b, a, and paid an eventfd write even on the loop's thread.
A re-entered borrow is refused too, which turns a would-be aliased `&mut`
into a RuntimeError.

## ADR-30: IOCP readiness is IOCTL_AFD_POLL, not zero-byte probes
`add_reader`/`add_writer` on IOCP were emulated with zero-byte overlapped
reads and writes on the watched socket. On UDP a zero-byte read CONSUMED
the datagram it was meant to detect and a zero-byte write sent an empty
datagram; on a listener or a connecting socket the probe failed at once and
was re-posted in a busy loop; and the probe tied the socket to the loop's
completion port for good.

Watches are now `IOCTL_AFD_POLL` requests issued through a helper socket
the backend owns -- the undocumented-but-stable mechanism libuv, mio and
wepoll are built on -- so the watched socket is never associated with the
port, read or written, and readiness has `select()` semantics: a listener
is readable when a connection is waiting, a connecting socket writable once
the connect resolves. Removing a watch cancels and reaps its request; the
backend's `Drop` reaps whatever is still in flight before freeing the slab
the kernel writes into. Compile-verified on Linux; the Windows tests
(`test_windows_audit.py`, the `#[cfg(windows)]` Rust tests) run in Windows
CI.

## ADR-31: Callbacks share one context snapshot instead of a copy apiece
asyncio's `Handle` copies the current `contextvars.Context` when it is
created, and cadeloop did the same: every pending callback held a
GC-tracked context. A 200k-callback burst kept 200k of them alive, and the
young collections they set off -- and the full collections their promotion
fed -- were most of what the burst cost. rsloop skips the copy only when
the context is empty, which helps benchmarks and not applications:
anything that calls `decimal.getcontext()`, as most web stacks end up
doing, leaves a variable in it.

A context is only a holder for its variables, an immutable map (CPython's
HAMT) that a copy shares with its source. A callback scheduled without an
explicit context now keeps a *snapshot*: a context nothing ever enters,
shared by every callback scheduled while the current context's map is that
same object. At dispatch the callback runs in a scratch context holding the
snapshot's variables, which is reused for the next callback only while
that is unobservable (nothing else holds it, and the callback set nothing
in it), so each callback still runs in a context of its own. Explicit
contexts, readiness watches (asyncio runs every call of one watch in the
same context) and signal handlers keep asyncio's semantics exactly; the
parity suite runs the context tests against both loops.

Comparing maps by identity reads `PyContext.ctx_vars`, and finding the
current map without copying the context first reads
`PyThreadState.context` -- private fields of structs CPython 3.11 ships in
its headers. Both layouts are verified through public APIs at import
(entering and exiting a probe context, setting a probe variable, matching
`interp`, `dict` and `id` against their accessors); a mismatch falls back
to sharing empty contexts only (found with `len()`) and to copying first.
Against the previous release: call_soon_burst 3.2x, timer_schedule_cancel
1.95x, timer_fire 1.6x, threadsafe_throughput 1.4x, call_soon_chain +5%.

## ADR-32: I/O before due timers; transport events routed at dispatch
The reactor moved expired timers into the ready batch before polling, so an
iteration ran its due timers ahead of the I/O its poll reported. asyncio's
`_run_once` runs the previous iteration's callbacks, then the I/O
callbacks, then the timers due, and current libuv (so uvloop) also polls
before it runs timers. The order decides races: uvicorn's keep-alive
timeout, due in the iteration in which the connection's next request
arrived, closed the connection and lost the request every time, where
asyncio and uvloop deliver the request and its `data_received` cancels the
timer. Expired timers now wait in a queue of their own and run at the
iteration's tail, after the transport events.

Transport events were also bound when their completions were translated,
before any of the iteration's callbacks had run: each carried the protocol
method it would call. A callback that closed, aborted or paused the
transport earlier in the same iteration, or swapped its protocol with
`set_protocol()` (as `start_tls` does), did not stop it: data and EOF
reached protocols after `close()`/`abort()` and while paused, datagrams
after a datagram transport's close, `resume_writing` after `abort()`, and
the old protocol got bytes meant for the new one -- a TLS ClientHello
handed to the plaintext protocol stalled `start_tls` until its timeout.
Inbound events now name their transport, and dispatch decides: dropped once
it is closing or gone, held while reading is paused (EOF too, delivered
after the held bytes by `resume_reading`), otherwise delivered to the
current protocol; `resume_writing` goes only to the protocol that was
paused.

## ADR-33: Large outbound payloads are sent from the application's bytes object
The HTTP engine copied every response body behind its head into one
buffer: one allocation and one memcpy per body message. For a small body
that is the right shape -- one queue entry, one iovec, and consecutive
responses coalesce. For a large one the copy was the cost: on 64 KiB
responses the memcpy and the buffer it filled were a tenth of the
server's CPU, to move bytes that were already sitting still in an
immutable object the application had just handed over.

A body of 16 KiB or more in a `bytes` object is now queued by reference:
the head (and any chunk-size line) as an owned buffer, the body as a
`WriteBuf::Bytes` holding the object, then the chunk's CRLF or the
terminating chunk -- all three under one flush decision, so a response
still leaves in one send. That is the retention `Transport.write()` has
always used for large writes (R-074), so partial sends, cancellation and
teardown already handle it; the reference is released through the
graveyard, never in the cell. Every framing comes out byte for byte as
before (content-length, caller-declared length, chunked, chunked with
trailers, HTTP/1.0 close-delimited, HEAD). TLS connections keep the
copy: their output is encrypted out of a staging buffer. Other buffer
types (`bytearray`, `memoryview`) are copied, as they must be.

`websocket.send` had the same shape twice over: the message was extracted
into a `String` or `Vec`, then copied again into its frame. The frame is
now built from the message's own storage, and a `bytes` payload of 16 KiB
or more is sent by reference behind a header-only buffer.

Inbound, WebSocket unmasking ran a byte at a time (`byte ^= mask[i & 3]`,
which the compiler leaves scalar) and was a third of the server's CPU on
64 KiB frames; it now XORs eight bytes at a time, which vectorizes.

Linux, loopback, one session, interleaved runs against the previous build
(medians): 64 KiB responses 32.1 K -> 48.9 K req/s; 1 MiB streamed in
16 KiB chunks 2.2 K -> 2.8 K req/s (between +8% and +29% over three
runs); WebSocket echo 14.0 K -> 21.5 K msg/s at 64 KiB and 51.4 K ->
66.8 K at 16 KiB. Small requests and messages are unchanged within noise.

## ADR-34: On epoll, a plain protocol's large reads become bytes at dispatch
A read was turned into a `bytes` object as its completion was translated,
in the cell, and its receive re-posted into the same slot. With several
large reads in one tick that allocated all of their objects back to back
before the first was delivered, and -- for a protocol that writes them
straight back -- freed them together a tick later, when the sends'
completions were reaped. The allocator never saw one of those blocks come
back before it was asked for the next, so each tick took fresh memory:
past glibc's trim threshold the heap was handed back to the kernel and
faulted in again, and every copy landed cache-cold. On an echo of 64 KiB
messages that was most of the distance to uvloop, which reads, delivers,
writes and frees one message before it touches the next.

On the readiness backend a read of 32 KiB or more now travels as a
BufferedProtocol's does: the event takes the slot, and the bytes object
is made when the event is dispatched, in the cell entry that routes it,
which gives the slot back in the same breath. Two things make that pay:

* The next receive is posted by the dispatch, after the delivery. The
  slot is back in the pool by then and is the one that receive is given,
  so a connection keeps reading into one warm buffer. Posted at translate
  instead, every connection alternated between two, and a tick with more
  large reads than the pool keeps resident (256) paged slots out and
  faulted them back in: measured at 512 connections, 16% *slower* than
  before the change. Nothing is lost by waiting -- this backend reads
  only inside a poll, and the next poll comes after the dispatch either
  way -- and `post_recv` declines by itself whatever the delivery did in
  between (paused, closed, raised, or had the receive posted already by a
  `resume_reading()` from another callback).
* A write large enough to be flushed as it is made (R-035) has its send
  settled at once when the backend finished it inside the post
  (`IoBackend::take_staged_op`), so the buffers it was sent from are
  released before `write()` returns instead of a tick later. Only that
  one completion is applied: applying everything staged from inside a
  write would let one callback read ahead on every other connection.

Each message is then allocated, filled, delivered, sent and freed before
the next, which reuses the same block. The completion backends keep their
path (`IoBackend::completes_inline` is false there): on IOCP the kernel
fills a posted buffer on its own, a receive posted early does overlap
with the callbacks, and none of this has been measured on Windows.

Echo of 64 KiB messages, measured as above: 38.2 K -> 43.6 K msg/s at 8
connections, 22.3 K -> 32.0 K at 512 (in the same runs, uvloop: 44.9 K
and 38.9 K; rloop: 52.4 K at 8). What remains is the copy out of the slot, and at high
connection counts the slots themselves, one per connection where uvloop
reads everything through a single buffer: rloop receives straight into
the bytes object it delivers. Doing the same here means reading at
dispatch rather than in the poll -- the readiness-native read path ADR-20
sketched, which would also free an idle connection's posted buffer -- and
is left for a change of its own.

## ADR-35: The probe after a full read is adaptive (epoll)
A read that fills its buffer is followed by an inline `recv` at the next
post, on the reasoning that a full buffer means more is waiting
(ADR-21). It also means a peer sending messages of exactly the buffer's
size, one at a time -- and there every probe came back EAGAIN: a wasted
syscall per message, for good. After two such misses in a row the probe is
skipped and the receive parks; level-triggered epoll reports the socket at
the next poll if data was there after all, so nothing can be lost, only
found one poll later. One full read in sixteen is still probed, and a hit
puts the socket back to probing every time, so a stream is recognised
within sixteen reads. Worth about 1% on the 64 KiB echo -- a syscall, no
more -- and nothing measurable elsewhere.

A send of a single buffer -- most of them -- goes through `send()` rather
than `sendmsg()`: the same call without the msghdr and iovec the kernel
copies in and validates first.

## ADR-36: Commands on what the loop owns are handed to its thread
ADR-29 let other threads into the loop whenever it was not parked, sent the
scheduling calls through the cross-thread queue when it was, and had
"anything else" raise asyncio's non-thread-safe error. Anything else
included every transport method, on the reasoning that calling one from
another thread is the application's mistake to be told about.

It is not always the application's call. The garbage collector runs
finalizers on whichever thread it is collecting on, and asyncio's
`StreamWriter.__del__` closes the transport it held: a writer collected by
a worker thread, with the loop waiting for I/O, printed "Exception ignored
in StreamWriter.__del__ ... RuntimeError: Non-thread-safe operation (the
loop is waiting for I/O...)" -- and the connection was never closed, since
the object that was closing it had just gone. Over TLS the same finalizer
runs sslproto's shutdown, which writes the close_notify to the transport
under it and pauses or resumes its reading on the way. And the calls an
application does make itself were refused or not according to whether the
loop happened to be parked at that instant: `transport.close()` from a main
thread shutting down a loop on another raised some of the time, and
`Server.close()` wrapped its native close in `except RuntimeError: pass`
("loop already closed"), so the server reported itself closed and its port
went on accepting. asyncio takes all of these.

A command on something the loop owns -- a stream transport's `close`,
`abort`, `_force_close`, `write`, `writelines`, `write_eof`,
`pause_reading` and `resume_reading`, a datagram transport's `close` and
`abort`, a server's `close` -- made from another thread while the loop is
running, or inside a live borrow of its state, is now handed to the loop's
thread (`net::HandedOver`): queued the way `call_soon_threadsafe` queues,
waking the loop if it is parked, and carried out in the order made. The
test is the caller's thread, not whether the state happens to be
reachable. Reachable, a write used to run on the calling thread, which put
it ahead of an earlier one still waiting in the queue, and called
`pause_writing()` or `connection_lost()` on a thread protocols never
expect to be called on; with one thread carrying every command out, order
holds and protocol callbacks stay where they belong. On the loop's own
thread the check is two loads and a compare, and nothing else changes.

What a call can tell from its arguments is still told to the caller: a
`write()` of the wrong type raises there, as does one after
`write_eof()`. A mutable buffer is copied when the call is made.
`is_closing()` is true from the moment a handed-over `close()` returns.
Configuration and queries that need the state -- `set_protocol`,
`set_write_buffer_limits`, `get_protocol`, `is_reading` -- still raise
from another thread while the loop is parked; those are not calls a
finalizer makes, and the error's advice (`call_soon_threadsafe`) is right
for them.

## ADR-37: The access log is written without the interpreter
R-140's access log calls a Python sink once per finished request; the sink
appends to a deque and a daemon thread turns each record into a
`logger.info()`. That keeps a blocking handler off the loop thread, but not
off the GIL: the writer needs it for every `LogRecord`, every format and
every handler call, and takes it from the loop to get it. Measured on the
plain-text benchmark (one worker, 64 connections, loopback): 136k
requests/s without the log, 26k with it logging to a file -- 37 us of
logging for a request that costs 7.

The log can be written with no Python at all, because everything in a
record is already native: the method is a static string, the target the
parser's own bytes, the status and the duration numbers. `accesslog`
(`cadeloop_core`, so it is tested without an interpreter) formats a
finished request straight into a byte buffer inside the state cell -- text,
or one JSON object per line -- and the tick's first phase hands the buffer
to a writer thread that owns the file and never takes the GIL. 128k
requests/s with it on: half a microsecond a request.

The decisions inside that:

- **A thread, not an inline write.** A regular file takes a write into the
  page cache in microseconds, but standard output may be a pipe to
  something slow, and a disk can stall. The loop never waits on either.
- **Bounded, and honest about it.** What the writer has not written is
  capped at 8 MiB; past that, records are dropped and counted and the count
  is written into the log when the writer catches up (and reported by
  `stats()`), as the logging path does.
- **Whole lines per write, appended.** Several workers share a file without
  a lock, and a rotator's rename is noticed by comparing the path's file
  with the open one once a second.
- **Escaped.** A target is the client's bytes: anything outside printable
  ASCII, and the quote and backslash, are written as escapes, so a request
  cannot forge a second line or a status.
- **Opt-in beside the logging path, not instead of it.** `access_log=True`
  keeps its meaning and its handlers; `access_log_file` is the new setting.

Python still gets the records, through the *tap*: a callable given the
finished requests as a list of tuples -- the logging sink's five values --
once per tick rather than once per request. The first request to finish in
a tick schedules the delivery as an ordinary loop callback, so it runs
after everything else that tick finishes, and a tap that raises is reported
like any callback. Measured with a tap that counts: 140k requests/s, inside
the noise of no logging. The tap's callable is stored next to the loop's
own bound method, which is a reference cycle through the loop, so
`__traverse__` reports both and `__clear__` lets the log go.

## ADR-38: TLS on the loop's own transports is a port of sslproto, not a new design
R-059 left client-side `ssl=`, `start_tls` and `create_server(ssl=)` on
`asyncio.sslproto`, as uvloop's first versions did. Measured on a TLS echo
(one worker, 64 connections, loopback) that path was slower than uvloop's:
76k messages/s against 83k at 1 KiB, 30k against 46k at 16 KiB, and 256 KiB
of memory for every connection, because sslproto allocates and zero-fills a
receive buffer of that size in its constructor. Profiled, the interpreter
was 37% of the server's CPU (15% under uvloop, whose sslproto is compiled):
a dozen Python frames around four C calls per message.

There were two ways to remove it. One was to extend the HTTP engine's TLS
(ADR-11's phase-2 `tls_ingest`/`tls_flush_conn`) into the transport itself.
That code is small because the engine's needs are small: it serves, it
never connects; a failed handshake is dropped without telling anyone; there
is no handshake or shutdown timeout, no flow control toward an application
protocol, no `start_tls`, no buffered protocols. Everything it lacks is
exactly what makes a general TLS transport hard, and what libraries depend
on down to the exception type.

The other was to port sslproto as it is: the same two objects (a protocol
for the transport underneath, a transport for the application), the same
five states, one function per method under the same name, driving the same
`SSLObject` over the same pair of `MemoryBIO`s made by the caller's own
`SSLContext`. That is what `crates/pyshim/src/tls.rs` is. TLS itself stays
entirely OpenSSL's through Python's `ssl` module -- verification, protocol
and cipher policy, SNI, sessions -- so nothing security-relevant is
reimplemented; what moves to Rust is the bookkeeping between the calls.
Being a protocol and a transport in the public sense, it sits on any
transport and touches none of the native transport's internals: the plain
TCP path is unchanged, and TLS inside TLS (an HTTPS proxy) works because
the port's transport is itself something `start_tls` accepts.

A port can be held to its original, and this one is, three ways: the
generated scenarios of test_differential.py run with the port on cadeloop's
side and sslproto on asyncio's (600 seeds: equal traces); the parity suite
runs every test in a third column on a `native_tls` loop; and CPython's own
test_sslproto, test_ssl, test_events and test_streams pass against it (two
skips, both `isinstance(transport, asyncio.Transport)`, which no native
transport satisfies).

Where it differs from sslproto, on purpose:

- It is a plain `data_received` protocol, not a buffered one: ciphertext
  goes from the transport's bytes into the incoming BIO, so there is no
  buffer to own. 2,000 connections: 83 MiB instead of 540.
- `SSLObject.read()` is asked for 16 KiB, one record's worth, not 256 KiB;
  `_ssl` allocates what it is asked for on every call.
- After a read that returned data it does not call again just to be told
  there is nothing (`SSLWantReadError`) when the incoming BIO is empty and
  the SSL object holds no decrypted bytes. That exception was 1.4 us of a
  4 us message (119k messages/s against 100k without the check).
- `close()` and `abort()` from another thread are handed to the loop's
  (ADR-36) instead of running the shutdown on the collector's thread.
- It does not have sslproto's buffered-protocol bug. `_do_read__buffered`
  fills the application's buffer through `buf[offset:]`; for a bytearray
  that is a copy, so every record after the first of a read is lost and
  the application is told stale bytes are its data (CPython 3.11-3.13,
  found by the generated scenarios). The port slices a memoryview.

Result: 111k messages/s at 1 KiB (+47%, and 35% ahead of uvloop), 37k at
16 KiB (+22%; uvloop is still ahead there, where what is left is the
MemoryBIO's own copying and the allocator, see the profile in the commit).

It ships off by default (`Loop(native_tls=True)`, `Config.native_tls`,
`CADELOOP_NATIVE_TLS=1`). sslproto has years of production behind it and
this has none; the tests say they agree, and a release as an option is how
that gets confirmed before it becomes what everyone runs.

## ADR-39: Request headers: fewer allocations in the parser, and no object cache
A request with a browser's header set costs twice what a bare one does:
3.0 us of server CPU for `GET /` with a Host header alone, 5.8 us with
eighteen more (pipelined, so the kernel is out of the picture). Counted in
instructions, each header is about 1,100 in the engine -- llhttp's
byte-at-a-time state machine, two `Vec`s grown from nothing for the name
and the value, a list of them that is reallocated and moved three or four
times on its way to nineteen entries -- and about 500 in the interpreter
for the two `bytes` and the tuple the scope gets.

The plan was to cache those three objects: a fixed-size table of recently
seen `(name, value)` tuples, handed out again instead of rebuilt, with
values kept only for a list of header names that carry no credentials. It
was built and measured, and it made things slower: 2% fewer instructions,
because the allocator calls it saved are real, and 3-7% more CPU per
request, because what it saved was nearly free. A small `bytes` or a
two-tuple comes off pymalloc's free list and goes back to it; the block a
request frees is the block the next one gets, still in the first-level
cache. A table of older objects is a set of cache lines that are not. An
object cache has to beat an allocator that is already a perfect cache for
this pattern, and it cannot. It is not in the tree.

What is: the parser sizes its header list from the connection's previous
request (a client repeats its header set; eight for the first request, at
most 64 reserved), and gives each name and value one allocation of exactly
its length instead of the generic growth path. Same data structures, same
objects in the scope, nothing kept between requests. Nineteen headers:
53.3k instructions a request from 56.6k, 5.37 us from 5.80 (-7.5%), and
10% more requests a second on the pipelined benchmark; eight headers, -6%;
one header, no change.

What is left is llhttp itself, about 9 instructions per byte of request
head. A vectorised header scanner would cut most of that, and would also
replace the one strict, widely reviewed component that decides where a
request ends. Not a trade to make for the difference.

