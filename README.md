# cadeloop

A maximum-performance asyncio event loop + ASGI stack with a Rust core.
Windows (IOCP, Registered I/O planned) is the production performance
target; Linux runs the same transport layer over epoll, making cadeloop a
**working drop-in `asyncio.AbstractEventLoop` replacement on both** —
uvicorn and aiohttp run on it unmodified — plus a **native HTTP/1.1 +
ASGI 3.0 server** (`cadeloop.serve`) whose parsing, scope construction,
and response serialization all happen in Rust.

```python
import asyncio, cadeloop

cadeloop.install()      # asyncio.set_event_loop_policy(cadeloop.EventLoopPolicy())
asyncio.run(main())     # everything below now runs on cadeloop:

server = await asyncio.start_server(handler, "0.0.0.0", 8000)
reader, writer = await asyncio.open_connection("example.org", 443, ssl=ctx)
loop.add_reader(fd, callback)      # readiness, sock_*, signals — all live
```

```bash
# the native ASGI server (llhttp in Rust, ~5x uvicorn on loopback):
python -m cadeloop myapp:app --port 8000
```

## Status

**M0 + M1 + M2 complete on Linux** (scheduling core, Rust TCP
transports, full drop-in surface, TLS via the stdlib `sslproto` path,
native HTTP/ASGI engine with Starlette/FastAPI verified); the Windows
IOCP backend is implemented and compile-verified, with behavioral
verification and the winloop gates riding on Windows CI/hardware. Full
R-xxx map:
[docs/requirements-traceability.md](docs/requirements-traceability.md).

| Surface | State |
|---|---|
| Scheduling (call_soon, timers, threadsafe, tasks) | ✅ tested |
| TCP transports, `create_server`/`create_connection`, streams | ✅ tested (Linux/epoll; Windows/IOCP compile-verified) |
| TLS (`ssl=` / `start_tls`) | ✅ via stdlib sslproto (native engine M4) |
| `sock_*`, `add_reader`/`add_writer`, POSIX signals | ✅ tested |
| Drop-in: uvicorn (HTTP/1.1), aiohttp | ✅ interop-tested |
| Native HTTP/1.1 + ASGI engine (`cadeloop.serve`, CLI, lifespan) | ✅ tested (Starlette/FastAPI, keep-alive/pipelining, chunked, limits) |
| UDP · subprocess/pipes · native `loop.sendfile` | M4 · M5 · M1-Windows |
| Multi-worker (§8) · WebSockets · native TLS | M3 · M4 · M4 |

## Benchmarks (Linux, loopback)

> **Scope.** Everything below is measured on one machine over loopback
> (Linux 6.18, 4 vCPU Intel Xeon 2.10 GHz, CPython 3.11.15) with client
> and server sharing the box — useful for relative comparison, **not** the
> spec's acceptance numbers, which are two-machine Windows runs against
> winloop (R-131). Methodology per R-130: 3 warmup + 5 measured runs,
> medians reported, fresh process per run, loop-independent (threaded,
> non-asyncio) load generators. Contenders: stdlib asyncio 3.11.15,
> uvloop 0.22.1, rloop 0.3.1, rsloop 0.1.30, aiofastnet 1.0.5 (both
> standalone on asyncio and stacked on cadeloop), hypercorn 0.18, and
> cadeloop's own native ASGI server. Raw JSON lives in
> [`bench/baselines/`](bench/baselines/). Reproduce with
> `python bench/harness/harness.py --suite {sched,echo,http}`.

### Scheduling core

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/bench-sched-dark.svg">
  <img alt="Scheduling speedup vs stdlib asyncio: cadeloop faster than asyncio on all ten benchmarks and ahead of uvloop on nine" src="docs/assets/bench-sched.svg">
</picture>

Median throughput, millions of ops/second:

| benchmark | cadeloop | asyncio | uvloop | rloop | rsloop |
|---|---|---|---|---|---|
| call_soon_chain | 3.15 | 0.52 | 1.60 | 3.92 | 4.47 |
| call_soon_burst | 3.06 | 0.85 | 1.17 | 3.01 | failed¹ |
| timer_schedule_cancel | **2.20** | 0.50 | 0.50 | 1.68 | failed¹ |
| timer_fire | **1.73** | 0.31 | 1.18 | 1.56 | failed¹ |
| sleep0_chain | 1.45 | 0.41 | 0.87 | 1.55 | 1.65 |
| task_spawn | 0.25 | 0.20 | 0.27 | 0.26 | 0.26 |
| threadsafe_throughput | 2.58 | 0.14 | 1.84 | **4.24** | 2.93 |
| future_chain | 0.93 | 0.23 | 0.49 | 0.95 | 0.90 |
| gather_fanin | **0.27** | 0.17 | 0.25 | 0.26 | 0.24 |
| queue_pingpong | 1.13 | 1.02 | 1.11 | 1.14 | 1.14 |

- **vs stdlib asyncio: faster on 10/10** (1.1x–18.4x). **vs uvloop:
  faster on 9/10** (1.02x–4.4x; the timer benches and cross-thread
  wakeups are the standouts; `task_spawn` landed at 0.93x uvloop this
  run — within run-to-run noise, and reported as measured).
- The other Rust loops are honest company: rloop wins cross-thread
  wakeups, rsloop wins the call_soon chain. Both are experimental
  schedulers without a working socket layer (rloop has no
  `create_server`; ¹rsloop 0.1.30 hung reproducibly on three benches at
  full scale and is recorded as failed — the harness kills runs at 90s).
- `task_spawn`/`gather_fanin`/`queue_pingpong` cluster for every loop:
  stdlib `asyncio.Task`/`Queue` Python code dominates. The M2 native
  server escapes exactly this tax with the eager-task path (R-056) —
  see the HTTP section.

### TCP echo — per-message loop overhead

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/bench-echo-dark.svg">
  <img alt="Single-connection TCP echo: aiofastnet-on-cadeloop 38.8K and cadeloop 35.1K msgs/s vs uvloop 20.7K; cadeloop p99 67us vs uvloop 81us" src="docs/assets/bench-echo.svg">
</picture>

Single connection, 1 KiB ping-pong (RTT measures the full transport +
loop wakeup path; no client saturation):

| loop | msgs/s | p50 RTT | p99 RTT |
|---|---|---|---|
| aiofastnet **on cadeloop** | **38.8K** | **22.5 µs** | 69.3 µs |
| **cadeloop** | 35.1K | 23.1 µs | **67.0 µs** |
| rsloop | 21.3K | 45.5 µs | 91.7 µs |
| uvloop | 20.7K | 46.3 µs | 80.7 µs |
| aiofastnet (on asyncio) | 19.8K | 48.7 µs | 86.7 µs |
| asyncio | 19.0K | 50.3 µs | 86.9 µs |

**1.7x uvloop's single-stream throughput at half the p50 latency.** This
is the R-060 spin-then-park design working as intended: the reply usually
lands inside the 20 µs spin window (`latency_mode="balanced"`), skipping
the park/wake cycle every other loop pays per message.

The aiofastnet rows are the interesting control experiment. aiofastnet
patches only the networking calls (Cython transports over
`add_reader`) and keeps the host loop's scheduler. On stdlib asyncio it
buys ~4%; **stacked on cadeloop it is the fastest stack measured** —
~10% over cadeloop's own Rust transports. That isolates a real cost in
our epoll dev backend's proactor emulation (a completion-slot re-post
hop per read that a readiness-callback transport doesn't pay), now
queued as an M2.5 fast path (ADR-20). It also demonstrates the drop-in
claim from an unusual angle: a third-party Cython transport layer runs
unmodified *on top of* cadeloop's scheduler and wins. Windows/IOCP is
unaffected — completions are the kernel's native interface there.

At 64 concurrent connections on this 4-vCPU box the *client* saturates
first and every contender converges into the 38.0–41.1K msgs/s band —
that configuration measures the load generator, and only a two-machine
run can separate the servers.

### HTTP/1.1 — the native engine vs everything else

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/bench-http-dark.svg">
  <img alt="HTTP plaintext RPS: cadeloop-native 35.1K req/s at 7.8ms p99 vs the uvicorn pack at ~6.6-6.8K and hypercorn 3.98K" src="docs/assets/bench-http.svg">
</picture>

Plaintext "Hello, World!" ASGI, 64 keep-alive connections. `cadeloop-native`
is `cadeloop.serve()` — the M2 engine: llhttp parses inside the Rust
core, the scope is built natively, the app coroutine is stepped eagerly
(no asyncio Task for a request that never suspends, R-056), and the
response is serialized in Rust straight into the corked write queue. The
uvicorn rows run uvicorn (h11) **unmodified** on each loop:

| contender | req/s | p50 | p99 |
|---|---|---|---|
| **cadeloop native** (`cadeloop.serve`) | **35.1K** | **1.26 ms** | **7.82 ms** |
| uvicorn + rsloop | 6.83K | 9.27 ms | 11.1 ms |
| uvicorn + asyncio | 6.71K | 9.29 ms | 12.5 ms |
| uvicorn + aiofastnet-cadeloop | 6.68K | 9.37 ms | 12.0 ms |
| uvicorn + uvloop | 6.64K | 9.33 ms | 12.3 ms |
| uvicorn + aiofastnet | 6.60K | 9.42 ms | 13.1 ms |
| uvicorn + cadeloop | 6.57K | 9.53 ms | 12.3 ms |
| hypercorn + asyncio | 3.98K | 15.8 ms | 24.8 ms |

**5.3x uvicorn+uvloop's throughput at 7.4x lower p50 latency** — the
spec's ≥2x-uvicorn target (R-002) cleared with headroom on this box
(the acceptance measurement itself remains a two-machine Windows run,
R-131). Two honest notes: the entire uvicorn pack sits within ±2% —
h11's Python-side parsing flattens *any* loop's advantage, which is why
the native engine exists — and this is the same app, same client, same
methodology, so the 5x is pure server-stack difference, not tuning.
The engine passes the same ASGI suites as the drop-in path: Starlette
(including streaming responses and background tasks) and FastAPI run on
it unmodified (R-123). (socketify.py, the intended C-level reference
ceiling, hangs on import in this container and is excluded.)

### Three findings from building these benchmarks

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
- Benchmarks are competitive analysis: benching aiofastnet *stacked on*
  cadeloop (echo table above) exposed a ~10% transport-layer cost ours
  pays on the epoll dev backend and handed us the M2.5 fix for free;
  benching rsloop's `#[pyclass(freelist)]` trick the same way showed it
  *doubling* call_soon cost under pyo3 (the freelist locks) — adopted
  findings and rejected ones both end up as ADRs (16, 20).

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
cargo test --workspace                                    # Rust core (53 tests)
cargo check -p cadeloop-core --target x86_64-pc-windows-msvc

cargo build -p cadeloop-pyshim --release                  # extension
cp target/release/lib_core.so python/cadeloop/_core.so    # Linux dev shortcut
pip install pytest pytest-timeout uvicorn aiohttp trustme starlette fastapi
PYTHONPATH=python pytest tests/unit tests/conformance     # 110 tests

pip install maturin && maturin build --release            # the real wheel
python tests/conformance/run_cpython_suite.py             # CPython asyncio suite
```

Repo layout follows the spec (R-114): `crates/core`, `crates/pyshim`,
`python/cadeloop`, `vendor/llhttp`, `tests/{unit,conformance,stress}`,
`bench/{echo,http,sched,harness}`, `docs/`.

## License

MIT (no GPL dependencies — enforced by `cargo deny`).
