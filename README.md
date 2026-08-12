# cadeloop

A maximum-performance asyncio event loop + ASGI stack for **Windows /
CPython 3.11**, with a Rust core (IOCP today, Registered I/O planned):
a drop-in `asyncio.AbstractEventLoop` aiming to outperform
winloop/uvicorn-class stacks on throughput and tail latency.

```python
import asyncio, cadeloop

cadeloop.install()          # asyncio.set_event_loop_policy(cadeloop.EventLoopPolicy())
asyncio.run(main())         # runs on cadeloop

# or explicitly:
cadeloop.run(main())
loop = cadeloop.new_event_loop()

# ASGI server (native engine lands in milestone M2):
cadeloop.serve(app, "0.0.0.0", 8000, workers=4)
# CLI: python -m cadeloop app:app --port 8000 --workers 4
```

## Status

Milestone **M0 complete** (scheduling core, loop facade, conformance
subset, CI); **M1 in progress** (IOCP kernel backend implemented and
compile-verified for Windows; TCP transports next). See
[docs/roadmap.md](docs/roadmap.md) and
[docs/requirements-traceability.md](docs/requirements-traceability.md)
for the full R-xxx status map.

| Layer | State |
|---|---|
| L1 scheduling (call_soon, timers, threadsafe wakeup, tasks) | ✅ working + tested (Windows & dev backend) |
| L0 IOCP backend (AcceptEx/ConnectEx/DisconnectEx, cancel-safe op slab) | ✅ implemented, compile-verified; exercised by transports in M1 |
| L0 RIO backend | probe only (M3) |
| TCP transports / `create_server` / `create_connection` | M1 |
| Native HTTP/1.1 + ASGI 3.0 engine, `cadeloop.serve` | M2 |
| TLS, WebSockets, UDP, `add_reader` hardening | M4 |
| Multi-worker (`WSADuplicateSocketW`, core pinning) | M3 |

On non-Windows hosts the package runs on a **portable dev backend**
(timers/callbacks only, no sockets) so the scheduling layer can be
developed and tested anywhere; shipped wheels are `cp311-win_amd64`.

## Linux scheduling microbenchmarks (dev backend)

> **What this is — and is not.** The project's acceptance benchmarks
> (R-003) are Windows two-machine TCP/HTTP runs against winloop and
> uvicorn (per R-131, loopback numbers are never authoritative). Those
> land with M1/M2. What *can* be measured today, on any OS, is the L1
> scheduling core: callback dispatch, the timer heap, coroutine stepping,
> and cross-thread wakeups. This section reports exactly that — cadeloop's
> Rust scheduling core vs vanilla asyncio, uvloop, and rloop on Linux.
> No socket I/O is involved; treat these as component benchmarks, not
> end-to-end claims.

**Environment:** Linux 6.18 x86-64, Intel Xeon @ 2.10 GHz (4 vCPU),
CPython 3.11.15. Contenders: stdlib asyncio (SelectorEventLoop) 3.11.15,
uvloop 0.22.1 (libuv/Cython), rloop 0.3.1 (Rust, experimental),
cadeloop @ this tree (portable dev backend, `latency_mode="balanced"`).
Methodology per R-130: fresh subprocess per run, 3 warmup + 5 measured
runs, medians reported (`bench/harness/harness.py --suite sched`); raw
JSON in [`bench/baselines/linux-sched-dev.json`](bench/baselines/linux-sched-dev.json).

Median throughput, millions of ops/second (higher is better; parentheses =
speedup vs stdlib asyncio):

| benchmark | cadeloop | asyncio (stdlib) | uvloop | rloop |
|---|---|---|---|---|
| call_soon_chain (dispatch latency chain) | **3.27 (5.4x)** | 0.61 | 1.96 (3.2x) | 4.80 (7.9x) |
| call_soon_burst (bulk schedule+drain) | **3.14 (3.3x)** | 0.97 | 1.20 (1.2x) | 3.71 (3.8x) |
| timer_schedule_cancel (heap churn) | **2.19 (3.7x)** | 0.59 | 0.50 (0.8x) | 1.70 (2.9x) |
| timer_fire (heap throughput) | **1.63 (4.7x)** | 0.35 | 1.28 (3.7x) | 1.66 (4.8x) |
| sleep0_chain (coroutine stepping) | **1.45 (3.3x)** | 0.44 | 1.20 (2.7x) | 2.22 (5.0x) |
| task_spawn (Task create+finish) | 0.29 (1.2x) | 0.23 | 0.29 (1.3x) | 0.31 (1.3x) |
| threadsafe_throughput (cross-thread wakeup) | **3.37 (22x)** | 0.15 | 1.82 (12x) | 5.10 (33x) |

Honest reading of the numbers:

- **vs stdlib asyncio**: cadeloop wins every benchmark, 1.2x–22x. The
  largest gaps are exactly where the Rust core replaces Python machinery:
  callback dispatch, the timer heap (tombstone cancellation + 4-ary heap
  vs re-heapify), and the lock-free `call_soon_threadsafe` path.
- **vs uvloop**: cadeloop is faster on 6 of 7 (1.2x–4.4x), tied on
  `task_spawn`. `timer_schedule_cancel` is the standout (4.4x): uvloop
  pays libuv timer setup/teardown per handle, cadeloop pays one heap push
  plus an atomic tombstone.
- **vs rloop**: rloop (also Rust) is faster on the pure-dispatch
  microbenches (chain, sleep0, threadsafe), cadeloop on timer churn;
  the rest are close. rloop is an instructive ceiling for Python-visible
  dispatch overhead — but it targets Unix (no Windows support), while
  every cadeloop design decision (IOCP completion model, pinned OVERLAPPED
  slabs, RIO-registered buffers) is in service of the Windows I/O targets.
- `task_spawn` clusters at ~0.3M/s for all four loops because all of them
  (today) create stdlib `asyncio.Task` objects — that cost is Python-side
  and is precisely what the M2 eager-task fast path (R-056: zero-Task
  completions for never-suspending handlers) attacks.

Reproduce: `python bench/harness/harness.py --suite sched --loops
cadeloop,asyncio,uvloop,rloop --markdown` (needs `pip install uvloop
rloop`).

## Architecture

```
L4  Python user code / ASGI app
L3  python/cadeloop — Loop facade, policy, Config, CLI      [Python]
L2  crates/pyshim   — PyO3 bindings; HTTP+ASGI engine (M2)  [Rust]
L1  crates/core     — reactor: timers, queues, dispatch      [Rust]
L0  crates/core     — IOCP | RIO (M3) | portable-dev         [Rust]
```

Highlights (details in [docs/architecture.md](docs/architecture.md)):

- **One thread, one GIL release point per tick**: kernel poll runs with
  the GIL released and re-acquires once per completion batch, not per
  event; callbacks dispatch in batches of 128 via vectorcall.
- **Cancel-safe overlapped I/O**: every kernel op lives in a pinned slab
  slot with a `{Free, Posted, Completed, Cancelled}` state machine —
  property-tested — so `CancelIoEx` races can't use-after-free.
- **Deadlock-free by construction**: no lock is held across the
  GIL-released poll; loop state is guarded by a thread-affinity protocol
  that raises (as asyncio specifies) instead of blocking.
- **Buffer slabs**: 2 MiB regions, {4/16/64 KiB} classes, refcounted slots
  shared between kernel ops and exported memoryviews, debug poisoning,
  one-time RIO registration hooks.

## Development

```bash
# Rust core (any OS — includes Windows-target cross-check):
cargo test -p cadeloop-core
cargo check -p cadeloop-core --target x86_64-pc-windows-msvc

# Extension + Python suite (Linux dev backend or Windows):
cargo build -p cadeloop-pyshim --release
cp target/release/lib_core.so python/cadeloop/_core.so   # Linux dev shortcut
pip install pytest && PYTHONPATH=python pytest tests/unit tests/conformance

# Wheel (the real packaging path):
pip install maturin && maturin build --release

# CPython asyncio conformance suite (runs where the `test` pkg exists):
python tests/conformance/run_cpython_suite.py
```

Repo layout follows the spec (R-114): `crates/core`, `crates/pyshim`,
`python/cadeloop`, `vendor/llhttp`, `tests/{unit,conformance,stress}`,
`bench/{echo,http,sched,harness}`, `docs/`.

## License

MIT (no GPL dependencies — enforced by `cargo deny`).
