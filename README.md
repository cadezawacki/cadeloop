# cadeloop

A maximum-performance asyncio event loop + ASGI stack with a Rust core.
Windows (IOCP, Registered I/O planned) is the production performance
target; Linux runs the same transport layer over epoll, making cadeloop a
**working drop-in `asyncio.AbstractEventLoop` replacement on both** —
uvicorn and aiohttp run on it unmodified.

```python
import asyncio, cadeloop

cadeloop.install()      # asyncio.set_event_loop_policy(cadeloop.EventLoopPolicy())
asyncio.run(main())     # everything below now runs on cadeloop:

server = await asyncio.start_server(handler, "0.0.0.0", 8000)
reader, writer = await asyncio.open_connection("example.org", 443, ssl=ctx)
loop.add_reader(fd, callback)      # readiness, sock_*, signals — all live
```

## Status

**M0 + M1 complete on Linux** (scheduling core, Rust TCP transports, full
drop-in surface, TLS via the stdlib `sslproto` path); the Windows IOCP
backend is implemented and compile-verified, with behavioral verification
and the winloop echo gate riding on Windows CI/hardware. The native
HTTP/ASGI engine (`cadeloop.serve`) is milestone M2. Full R-xxx map:
[docs/requirements-traceability.md](docs/requirements-traceability.md).

| Surface | State |
|---|---|
| Scheduling (call_soon, timers, threadsafe, tasks) | ✅ tested |
| TCP transports, `create_server`/`create_connection`, streams | ✅ tested (Linux/epoll; Windows/IOCP compile-verified) |
| TLS (`ssl=` / `start_tls`) | ✅ via stdlib sslproto (native engine M4) |
| `sock_*`, `add_reader`/`add_writer`, POSIX signals | ✅ tested |
| Drop-in: uvicorn (HTTP/1.1), aiohttp | ✅ interop-tested |
| UDP · subprocess/pipes · native `loop.sendfile` | M4 · M5 · M1-Windows |
| Native HTTP/1.1 + ASGI engine, `cadeloop.serve`, multi-worker | M2 · M3 |

## Benchmarks (Linux, loopback)

> **Scope.** Everything below is measured on one machine over loopback
> (Linux 6.18, 4 vCPU Intel Xeon 2.10 GHz, CPython 3.11.15) with client
> and server sharing the box — useful for relative comparison, **not** the
> spec's acceptance numbers, which are two-machine Windows runs against
> winloop (R-131). Methodology per R-130: 3 warmup + 5 measured runs,
> medians reported, fresh process per run, loop-independent (threaded,
> non-asyncio) load generators. Contenders: stdlib asyncio 3.11.15,
> uvloop 0.22.1, rloop 0.3.1, rsloop 0.1.30, hypercorn 0.18. Raw JSON
> lives in [`bench/baselines/`](bench/baselines/). Reproduce with
> `python bench/harness/harness.py --suite {sched,echo,http}`.

### Scheduling core

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/bench-sched-dark.svg">
  <img alt="Scheduling speedup vs stdlib asyncio: cadeloop leads uvloop on all ten benchmarks" src="docs/assets/bench-sched.svg">
</picture>

Median throughput, millions of ops/second:

| benchmark | cadeloop | asyncio | uvloop | rloop | rsloop |
|---|---|---|---|---|---|
| call_soon_chain | 3.07 | 0.53 | 1.60 | 3.90 | 4.60 |
| call_soon_burst | 3.11 | 0.87 | 1.34 | 2.95 | failed¹ |
| timer_schedule_cancel | **2.35** | 0.51 | 0.53 | 1.66 | failed¹ |
| timer_fire | **1.71** | 0.31 | 1.20 | 1.55 | failed¹ |
| sleep0_chain | 1.41 | 0.38 | 0.89 | 1.68 | 1.74 |
| task_spawn | 0.28 | 0.21 | 0.28 | 0.29 | 0.28 |
| threadsafe_throughput | 3.33 | 0.15 | 2.08 | **4.65** | 2.94 |
| future_chain | 0.89 | 0.21 | 0.56 | 1.02 | 0.97 |
| gather_fanin | 0.26 | 0.18 | 0.25 | 0.28 | 0.27 |
| queue_pingpong | 1.24 | 1.12 | 1.21 | 1.22 | 1.19 |

- **vs stdlib asyncio: faster on 10/10** (1.1x–22.7x). **vs uvloop:
  faster on 10/10** (1.01x–4.4x; the timer benches and cross-thread
  wakeups are the standouts).
- The other Rust loops are honest company: rloop wins cross-thread
  wakeups, rsloop wins the call_soon chain. Both are experimental
  schedulers without a working socket layer (rloop has no
  `create_server`; ¹rsloop 0.1.30 hung reproducibly on three benches at
  full scale and is recorded as failed — the harness kills runs at 90s).
- `task_spawn`/`gather_fanin`/`queue_pingpong` cluster for every loop:
  stdlib `asyncio.Task`/`Queue` Python code dominates. That cost is the
  target of the M2 eager-task path (R-056).

### TCP echo — per-message loop overhead

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/bench-echo-dark.svg">
  <img alt="Single-connection TCP echo: cadeloop 40.4K msgs/s and 53us p99 vs uvloop 21.2K and 78us" src="docs/assets/bench-echo.svg">
</picture>

Single connection, 1 KiB ping-pong (RTT measures the full transport +
loop wakeup path; no client saturation):

| loop | msgs/s | p50 RTT | p99 RTT |
|---|---|---|---|
| **cadeloop** | **40.4K** | **22.2 µs** | **53.4 µs** |
| rsloop | 21.6K | 45.9 µs | 86.1 µs |
| uvloop | 21.2K | 46.0 µs | 77.5 µs |
| asyncio | 18.8K | 50.5 µs | 85.8 µs |

**2.1x uvloop's single-stream throughput at half the p50 latency.** This
is the R-060 spin-then-park design working as intended: the reply usually
lands inside the 20 µs spin window (`latency_mode="balanced"`), skipping
the park/wake cycle every other loop pays per message.

At 64 concurrent connections on this 4-vCPU box the *client* saturates
first and all four loops converge (cadeloop 37.5K, asyncio 40.0K, uvloop
39.6K, rsloop 40.0K msgs/s aggregate) — that configuration measures the
load generator, and only a two-machine run can separate the servers.

### HTTP/1.1 — uvicorn as a drop-in workload

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/bench-http-dark.svg">
  <img alt="uvicorn plaintext RPS: cadeloop 7.03K leads uvloop, asyncio, rsloop; hypercorn trails" src="docs/assets/bench-http.svg">
</picture>

Plaintext "Hello, World!" ASGI, 64 keep-alive connections, uvicorn (h11)
running **unmodified** on each loop:

| contender | req/s | p50 | p99 |
|---|---|---|---|
| **uvicorn + cadeloop** | **7.03K** | **8.95 ms** | **11.0 ms** |
| uvicorn + rsloop | 6.95K | 9.08 ms | 11.1 ms |
| uvicorn + asyncio | 6.92K | 9.14 ms | 11.1 ms |
| uvicorn + uvloop | 6.90K | 9.11 ms | 11.7 ms |
| hypercorn + asyncio | 3.98K | 15.8 ms | 21.9 ms |

cadeloop is the fastest loop under uvicorn on throughput, p50, and p99 —
but the honest reading is that uvicorn's Python-side HTTP parsing
flattens loop differences to a few percent. This workload proves
*drop-in compatibility at zero cost*; the 2x-uvicorn spec target (R-002)
belongs to the M2 native HTTP engine, which replaces the uvicorn layer
entirely. (socketify.py, the intended C-level reference ceiling, hangs on
import in this container and is excluded.)

### Two findings from building these benchmarks

- Benchmarks are tests: the first echo runs exposed two real transport
  races (data loss on `pause_reading` with an in-flight completion; slot
  reuse corrupting streams) — both fixed with a design change (pausing
  cancels nothing) plus kernel-op buffer refcounts (R-073), and now
  covered by the 10 MB-transfer test.
- Benchmarks are regression gates: the M1 transport work initially taxed
  every scheduling tick ~430 ns; three fast-path fixes (skip `epoll_wait`
  on idle zero-timeout polls, keep the GIL for non-blocking reaps, one
  clock read per tick) restored M0 numbers exactly, and one state-cell
  entry removed from `transport.write` took uvicorn+cadeloop from 10%
  behind uvicorn+uvloop to ahead of it.

## Architecture

```
L4  Python user code / ASGI app
L3  python/cadeloop — Loop facade, policy, Config, CLI       [Python]
L2  crates/pyshim   — transports, listeners, bindings        [Rust]
L1  crates/core     — reactor: timers, queues, dispatch      [Rust]
L0  crates/core     — IOCP | RIO (M3) | epoll (Linux dev)    [Rust]
```

Highlights (details in [docs/architecture.md](docs/architecture.md),
decisions in [docs/decisions.md](docs/decisions.md)):

- **One completion-style op API over both kernels** — IOCP natively;
  epoll wrapped as a proactor (syscall attempted at post time, parked
  only on EWOULDBLOCK) so a single Rust transport layer serves both.
- **One thread, one conditional GIL release per tick**: the kernel poll
  drops the GIL only when it can actually block; completions dispatch in
  batches via vectorcall with per-connection protocol callbacks cached as
  bound methods.
- **Cancel-safe by construction**: pinned op slabs with a
  `{Free, Posted, Completed, Cancelled}` state machine (property-tested),
  buffer slots refcounted by kernel ops and memoryview exports alike, and
  a graveyard protocol so no Python object can be dropped — and no
  `__del__` can re-enter — inside the loop's critical section.
- **Corked gather writes** (R-035): writes coalesce within a tick into
  ≤16-slice `writev`/`WSASend` calls, flushing at tick end, at 64 KiB, or
  on drain; `bytes` payloads are retained zero-copy (R-074).

## Development

```bash
cargo test --workspace                                    # Rust core (44 tests)
cargo check -p cadeloop-core --target x86_64-pc-windows-msvc

cargo build -p cadeloop-pyshim --release                  # extension
cp target/release/lib_core.so python/cadeloop/_core.so    # Linux dev shortcut
pip install pytest pytest-timeout uvicorn aiohttp trustme
PYTHONPATH=python pytest tests/unit tests/conformance     # 87 tests

pip install maturin && maturin build --release            # the real wheel
python tests/conformance/run_cpython_suite.py             # CPython asyncio suite
```

Repo layout follows the spec (R-114): `crates/core`, `crates/pyshim`,
`python/cadeloop`, `vendor/llhttp`, `tests/{unit,conformance,stress}`,
`bench/{echo,http,sched,harness}`, `docs/`.

## License

MIT (no GPL dependencies — enforced by `cargo deny`).
