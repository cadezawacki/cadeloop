# Requirements traceability

Status: ✅ implemented · 🔶 partial · 📅 planned (milestone) · 📝 documented decision

| Req | Summary | Status | Where |
|---|---|---|---|
| R-001 | Beat winloop/stdlib on throughput + tail latency | 📅 M1 gate | bench/echo |
| R-002 | Native HTTP/ASGI beating uvicorn | 📅 M2 gate | bench/http |
| R-003 | Acceptance ratios (1.15x echo / 2.0x RPS / 0.6x p99) | 📅 M1–M3 gates | bench/harness |
| R-004 | Public API (policy, factory, serve, CLI) | ✅ (name: `cadeloop` per owner) | python/cadeloop |
| R-010 | CPython 3.11 only; GIL isolation module | ✅ | pyproject.toml; crates/pyshim/src/gil_boundary.rs |
| R-011 | Win32/Winsock2 only; msquic out of scope | ✅ | crates/core/src/backend |
| R-012 | Rust core via PyO3 ≥0.21 + maturin | ✅ (pyo3 0.24) | crates/pyshim |
| R-013 | Genuine AbstractEventLoop subclass | ✅ full surface incl. transports/TLS/sock_*/readers (drop-in: uvicorn+aiohttp interop tests) | python/cadeloop/loop.py, tcp.py |
| R-014 | MIT; no GPL deps; allowed core deps | ✅ | LICENSE, deny.toml |
| R-020 | IoBackend trait + backend selection | ✅ | backend/mod.rs |
| R-021 | One reactor thread; GIL released per-batch poll | ✅ | reactor.rs, coreloop.rs |
| R-022 | Lock-free MPSC + PQCS wakeup | ✅ | ready.rs, iocp.rs |
| R-030 | GQCSEx batch 256; timeout clamp [0,100ms] | ✅ | iocp.rs, reactor.rs |
| R-031 | FILE_SKIP_* modes + inline success + LSP guard | ✅ | iocp.rs `register_socket` |
| R-032 | AcceptEx pool + GetAcceptExSockaddrs + update-context | ✅ pool (accept_pool outstanding, replenish-on-complete); GetAcceptExSockaddrs deferred (getpeername used) | iocp.rs, net.rs |
| R-033 | DisconnectEx(TF_REUSE_SOCKET) + 4096-cap pool | ✅ | iocp.rs |
| R-034 | ConnectEx + bind-before-connect + update-context | ✅ | iocp.rs |
| R-035 | Gather WSASend ≤16 WSABUFs; corking | ✅ tick-end cork flush, ≥64 KiB early flush | iocp.rs, net.rs, coreloop.rs |
| R-036 | TransmitFile sendfile + pathsend | 📅 M1 | — |
| R-037 | Pinned OVERLAPPED slab + state machine + CancelIoEx | ✅ (proptested) | opslab.rs, iocp.rs |
| R-038 | TCP_NODELAY, loopback fast path, TFO | ✅ helpers | iocp.rs |
| R-040..044 | RIO backend | 🔶 implemented: CQ+IOCP-notify (R-041), per-socket RQs (R-042), slab-region registration + send staging (R-043), 1024-batch dequeue + doubling CQ growth (R-044); bookkeeping unit-tested on Linux, FFI cross-compile-verified; Windows behavioral validation 📅 (gates auto-selection) | rio.rs, rio_util.rs |
| R-050 | AbstractEventLoop surface + native fast paths | ✅ scheduling + TCP/server/sock_*/readers native; remaining gates: datagrams (M4), subprocess/pipes (M5), sendfile (R-036) | coreloop.rs, net.rs, loop.py |
| R-051 | Subprocess gated (default off) | ✅ gate + message | config.py, loop.py |
| R-052 | SIGINT/SIGBREAK handlers | 🔶 POSIX ✅ (EINTR-prompt); Windows SetConsoleCtrlHandler 📅 M4 | loop.py |
| R-053 | 4-ary timer heap, tombstones, >50% compaction | ✅ (proptested) | timer.rs |
| R-054 | 128-batch dispatch, vectorcall, bound-method cache | ✅ per-connection protocol methods cached at setup | ready.rs, handles.rs, net.rs |
| R-055 | DNS thread pool min(8,cpus) + 5s LRU cache | ✅ | loop.py |
| R-056 | Eager task fast path, zero-Task completions | ✅ `PyIter_Send` stepping; non-suspending requests allocate no Task/Future; AppTask registers via `_enter_task` (anyio-compatible); `eager_tasks=False` stdlib path | http.rs (pyshim), config.py |
| R-057 | add_reader/add_writer emulation | ✅ Linux (native LT interest); IOCP zero-byte probes compile-verified (harden M4) | epoll.rs, iocp.rs, coreloop.rs |
| R-058 | UDP endpoints | 📅 M4 | loop.py (gated) |
| R-059 | Native TLS (memory BIOs), SSLContext compat | 🔶 sslproto MemoryBIO fallback ✅ (spec-sanctioned, tested); native engine 📅 M4 | tcp.py |
| R-060 | Spin-then-park + latency_mode presets | ✅ | reactor.rs, config.py |
| R-061 | QPC clock, cached per tick | ✅ (via std Instant=QPC; documented) | time.rs |
| R-070 | mimalloc global allocator | ✅ | pyshim/src/lib.rs |
| R-071 | Slabs {4K,16K,64K}, VirtualAlloc, large pages, thread-affine | ✅ | buffers.rs |
| R-072 | Zero-copy memoryview via BufferedProtocol | 🔶 BufferedProtocol supported (single copy into app buffer); recv-into-app-buffer zero-copy 📅 | net.rs |
| R-073 | Slot lifetime refcounts + poison | ✅ (proptested; kernel ops hold slot refs released at completion reap) | buffers.rs, net.rs |
| R-074 | Write retention semantics (bytes zero-copy, bytearray copy) | ✅ (bytes-backed memoryview currently copied — documented deviation) | net.rs |
| R-075 | gc.freeze warmup policy | 🔶 freeze/disable applied post-lifespan-startup in serve(); per-request warmup counter 📅 | server.py |
| R-080 | llhttp strict, limits (64K headers/100 count/8K URL) | ✅ in-callback enforcement → 414/431/413; request-line/idle timeouts 📅 | core/http.rs, vendor/llhttp |
| R-081 | ASGI 3.0 scope + lifespan | ✅ native scope (percent-decode, latin-1 fallback, client/server tuples, lifespan state copy); lifespan `auto` in facade | http.rs (pyshim), server.py |
| R-082 | Header-name/method interning | ✅ interned common methods + scope keys | http.rs (pyshim) |
| R-083 | Per-connection cached receive/send; scope reuse opt-in | 🔶 receive/send cached per conn ✅; `reuse_scope` 📅 | http.rs (pyshim) |
| R-084 | Native response assembly, Date cache, framing at first body chunk | ✅ zero Python objects on the wire path; per-second civil-from-days Date; HEAD suppression; HTTP/1.0 close-delimited | http.rs (pyshim) |
| R-085 | Keep-alive + pipelined drain, strict ordering | ✅ iterative pump; request finishes at coro return (background-task safe) | http.rs (pyshim) |
| R-086 | Error paths: in-cell 4xx, 500-or-abort, disconnect futures | ✅ | core/http.rs, http.rs (pyshim) |
| R-087 | WebSockets | 📅 M4 | — |
| R-088 | Fallback to pure-Python h11 path | ➖ not planned (native engine + uvicorn interop cover it) | — |
| R-090..093 | Worker model, affinity, supervision | 🔶 Linux ✅: SO_REUSEPORT pool, supervisor restart + fast-crash cutoff, grace drain, sched_setaffinity pinning; Windows (WSADuplicateSocketW) 📅 M3 | server.py, tests/unit/test_multiworker.py |
| R-100 | new_event_loop/EventLoopPolicy/install/run | ✅ | policy.py |
| R-101 | serve() + CLI 1:1 cfg mapping | ✅ functional: native listener, lifespan, signals, gc policy, multi-worker pool | server.py, __main__.py |
| R-102 | Config: all tunables, TypeError on unknown, from_env | ✅ | config.py |
| R-103 | loop.stats() introspection | ✅ (M0 counters incl. syscalls_saved_inline plumbing) | coreloop.rs |
| R-110 | maturin cp311-win_amd64 wheels; v2/v3 variants | 🔶 packaging ✅; v3 variant + publish 📅 M5 | pyproject.toml, ci.yml |
| R-111 | LTO/opt/codegen/panic; PGO wheels | 🔶 profile ✅; PGO 📅 M5 | Cargo.toml |
| R-112 | Vendored llhttp + static OpenSSL | 🔶 llhttp pin/fetch ✅; OpenSSL 📅 M4 | vendor/llhttp |
| R-113 | CI matrix: build/unit/conformance/stress/bench-regression/lints | ✅ wired | .github/workflows/ci.yml |
| R-114 | Repo layout | ✅ | (tree) |
| R-120 | CPython suite subsets + shrinking skip-list | ✅ runner + skiplist; suites broaden per milestone | tests/conformance |
| R-121 | Rust unit ≥80% on L0/L1 state machines; proptests | ✅ proptests for R-037/R-053/R-073; coverage tracked in CI later | crates/core |
| R-122 | Edge-case matrix | 🔶 half-close, backpressure water marks, abort, refused, close-with-pending, cancel races ✅ (Linux); RST/drip-feed/slowloris + Windows runs 📅 | tests/unit/test_transports.py |
| R-123 | ASGI compliance + Starlette/FastAPI | ✅ real-socket suites: scope, streaming (anyio task groups), background tasks, FastAPI validation | tests/unit/test_http_engine.py |
| R-124 | Interop smoke (aiohttp/asyncpg/aiofastnet) | 🔶 uvicorn + aiohttp ✅; aiofastnet stacked-on-cadeloop benched ✅; asyncpg 📅 M4 | tests/unit/test_interop.py, bench/ |
| R-130 | Harness: warmups, medians, JSON baselines | ✅ | bench/harness |
| R-131 | bombardier/rewrk/custom client; two-machine authority | 🔶 loopback echo/HTTP harness ✅ (labeled non-authoritative); two-machine + bombardier/rewrk 📅 Windows | bench/ |
| R-132 | Comparison matrix | ✅ Linux subset: {cadeloop, asyncio, uvloop, rloop, rsloop, aiofastnet, aiofastnet-cadeloop} × {sched, echo, http} + cadeloop-native/hypercorn; Windows contenders (winloop) 📅 | bench |
| R-133 | Metrics incl. ETW/VTune traces on perf PRs | 📅 M1 | bench/http/README |
| R-140 | Structured logging; opt-in access log | 🔶 logger ✅ ("cadeloop"); ring-buffer access log 📅 M2 | loop.py |
| R-141 | stats endpoint | 📅 M2 (config ✅) | config.py |
| R-142 | PYTHONASYNCIODEBUG, slow-callback warnings, op asserts | ✅ | loop.py, coreloop.rs, opslab.rs |

Deviations from spec text (owner-approved or forced):
1. **Name**: `iceloop` → `cadeloop` (owner request; R-004 mechanics unchanged).
2. **Linux epoll backend** (ADR-11, owner-directed): full drop-in surface on
   Linux for development, testing, and benchmarking; production wheels
   remain cp311-win_amd64. (The condvar-only portable backend remains for
   other Unixes.)
3. **CI runners**: spec's "windows-11" label doesn't exist on GitHub-hosted
   runners; using windows-2022 + windows-2025, self-hosted Win11 slot noted
   in ci.yml.
4. **R-074**: bytes-backed memoryviews are copied (not zero-copy retained)
   pending a cheap detection path; plain `bytes` follow the spec rule.
5. **R-057 on Windows**: probe-based emulation is compile-verified only
   until Windows CI runs it (Linux uses native epoll interest).
