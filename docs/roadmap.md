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

## M2 — HTTP/ASGI engine; plaintext ≥2.0x uvicorn+winloop

- [ ] Vendored llhttp in strict mode (R-080), limits enforced
- [ ] ASGI 3.0 scope/lifespan (R-081), header interning (R-082), scope
      reuse opt-in (R-083), Date cache + gather-write responses (R-084),
      keep-alive pipelined-drain (R-085), error paths (R-086)
- [ ] Eager task fast path (R-056): zero-Task completions on 3.11 via
      `PyIter_Send` stepping; `cfg.eager_tasks` escape hatch (§16)
- [ ] gc.freeze warmup policy (R-075)
- [ ] Starlette/FastAPI real-socket suites green (R-123)

## M3 — RIO backend; latency targets; multi-worker

- [ ] RIO CQ/RQ machinery (R-040..R-044) on the existing probe + buffer
      registration hooks
- [ ] p99 ≤ 0.6x uvicorn+winloop at 80% saturation (R-003)
- [ ] Worker model: WSADuplicateSocketW handle passing, affinity pinning,
      supervisor restart/drain (R-090..R-093)

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
