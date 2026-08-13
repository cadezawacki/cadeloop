# Roadmap (spec §14 milestones)

Gates per milestone: conformance skip-list shrinks monotonically, no
benchmark regression >5%, soak clean (R-113, §14 exit criteria).

## M0 — Skeleton ✅ (this tree)

- [x] Repo layout (R-114), MIT licensing (R-014), maturin packaging (R-110)
- [x] L1 reactor: timer heap (R-053), ready/MPSC queues (R-022/R-054),
      cached clock (R-061), poll-timeout policy (R-030), spin-then-park
      (R-060)
- [x] PyO3 loop passing the call_soon/timer conformance subset; CI wired
      (Windows build/test/conformance + Linux dev-backend jobs)
- [x] Facade: policy/install/run (R-100), Config (R-102), serve()/CLI
      signatures (R-101), stats() (R-103)

## M1 — TCP transports (Linux-verified; Windows echo gate pending)

- [x] L0 IOCP backend primitives (R-030..R-038) — cross-checked
- [x] L0 epoll dev backend (same completion API; Linux drop-in, ADR-11)
- [x] TCP transports in Rust: cached protocol callbacks (R-054), pipelined
      recv, corked gather writes (R-035), water-mark backpressure,
      BufferedProtocol, write-retention rule for bytes (R-074),
      per-op buffer refcounts (R-073)
- [x] add_reader/add_writer + sock_* + POSIX signals (R-057 emulation on
      IOCP is compile-verified; Windows CI exercises it)
- [x] TLS via stdlib sslproto MemoryBIO path (R-059 fallback; native M4)
- [x] Drop-in proof: uvicorn (HTTP/1.1) + aiohttp run unmodified
- [x] bench/echo + bench/http suites (loopback; R-130 methodology)
- [ ] `loop.sendfile` via TransmitFile (R-036) — sock_sendfile fallback ✅
- [ ] Windows behavioral verification + edge-case matrix completion
      (R-122: RST paths, drip-feed, slowloris timing) on Windows CI
- [ ] Echo ≥1.15x winloop on two-machine Windows hardware (the M1 gate,
      R-131 — cannot be measured from this Linux environment)

## M2 — HTTP/ASGI engine (Linux-verified; ≥2.0x gate beaten at 5x)

- [x] Vendored llhttp 9.2.1 in strict mode (R-080), limits enforced
      in-callback (414/431/413), malformed answered fully in-cell (R-086)
- [x] ASGI 3.0 scope (R-081) built natively: interned methods/keys
      (R-082), percent-decoded path w/ latin-1 fallback, lifespan state
      shallow-copied per scope; lifespan protocol in the facade
      (uvicorn-style `auto`)
- [x] Native response assembly (R-084): head buffered until the first
      body chunk decides content-length vs chunked framing; per-second
      Date cache; `server: cadeloop`; HEAD body suppression; HTTP/1.0
      close-delimited streaming
- [x] Keep-alive + strict pipelined ordering, iterative pump (R-085)
- [x] Eager task fast path (R-056): `PyIter_Send` stepping, zero
      Tasks/Futures for non-suspending requests, singleton completed
      awaitable; suspensions get an `AppTask` registered via
      `_enter_task` so `asyncio.current_task()`/anyio task groups work
      (Starlette StreamingResponse + BackgroundTask verified);
      `cfg.eager_tasks=False` stdlib-Task escape hatch (§16)
- [x] receive() disconnect futures (no busy-wait disconnect listeners)
- [x] gc.freeze at post-startup (R-075; per-request warmup counter TBD)
- [x] Starlette/FastAPI real-socket suites green (R-123)
- [x] Loopback plaintext: 35.1K req/s vs uvicorn+uvloop 6.7K (~5x) — the
      authoritative ≥2.0x uvicorn+winloop gate still needs Windows
      hardware (R-131)
- [ ] Request-line/keep-alive idle timeouts (R-080 timers; config wired)
- [ ] Access log (R-140) on the native engine

## M2.5 — transport-layer follow-ups (from competitive analysis) ✅

- [x] epoll lazy interest disarm: op completions keep the kernel mask
      armed (the same-tick re-post then needs no epoll_ctl); unconsumed
      events disarm — the steady-state DEL/ADD pair per message is gone
- [x] Drained-socket heuristic: a short recv parks the next post directly
      (no speculative recv that would EAGAIN); a full-buffer read keeps
      the inline attempt for streaming
- [x] Result: one recv syscall per echo message; echo-rtt 35.1K → 45.0K
      msg/s (+28%), p99 67 → 50µs — native transports now match the
      aiofastnet-on-cadeloop stack that exposed the gap (ADR-20/21)
- [x] Single-cell tick (rloop tick-anatomy): pure-scheduling ticks enter
      the state cell once; call_soon_chain 3.15 → 3.64 M ops/s,
      threadsafe 2.58 → 3.55; lossless batch unwind on interrupts

## M3 — RIO backend; latency targets; multi-worker

- [x] Worker model, Linux flavor (§8): SO_REUSEPORT pool (kernel accept
      balancing), supervisor restart with fast-crash cutoff, SIGTERM
      forward + grace drain, round-robin CPU pinning (R-090..R-093);
      end-to-end tested via the CLI (balancing, SIGKILL restart, drain)
- [ ] Windows worker model: WSADuplicateSocketW handle passing (fork-free)
- [ ] RIO CQ/RQ machinery (R-040..R-044) on the existing probe + buffer
      registration hooks
- [ ] p99 ≤ 0.6x uvicorn+winloop at 80% saturation (R-003 — Windows
      two-machine measurement)

## M4 — TLS, WebSockets, UDP, readiness hardening

- [ ] Native OpenSSL memory-BIO TLS engine, stdlib SSLContext extraction +
      MemoryBIO fallback (R-059)
- [ ] RFC 6455 WebSockets + permessage-deflate (R-087)
- [ ] UDP endpoints incl. RIO datagrams (R-058); asyncpg green via R-057
- [ ] SIGINT/SIGBREAK handlers (R-052)

## M5 — 1.0

- [ ] Native subprocess (R-051)
- [ ] PGO-published wheels (R-111), -v3 wheel variant (R-110)
- [ ] Docs complete; conformance skip-list at its floor

## Explicit non-goals (v1, §15)

HTTP/2, HTTP/3/msquic, Linux/macOS *production* support (the portable
backend is dev/test-only), PyPy, Python != 3.11, kernel drivers/DPDK,
reverse-proxy features, Windows 7/8, ARM64 (M6 candidate).
