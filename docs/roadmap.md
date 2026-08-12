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

## M1 — IOCP transports; echo ≥1.15x winloop (two-machine)

- [x] L0 IOCP backend primitives (R-030..R-038) — this tree, cross-checked
- [ ] TCP transports: server (AcceptEx pool w/ replenish, R-032) + client
      (ConnectEx), corking/gather writes (R-035), backpressure water marks,
      buffered-protocol zero-copy receive (R-072), write-retention rule
      (R-074)
- [ ] `loop.sendfile` via TransmitFile (R-036)
- [ ] add_reader/add_writer emulation (R-057)
- [ ] Edge-case matrix automation (R-122): cancel-in-flight × ops,
      half-close, RST, drip-feed, backpressure, close-with-pending-ops
- [ ] bench/echo client + two-machine methodology (R-131)

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
