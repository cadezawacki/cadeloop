# bench/http — HTTP/ASGI benchmarks (M2)

The R-003/R-132 HTTP acceptance matrix: cadeloop vs {uvicorn+winloop,
uvicorn+stdlib, hypercorn, socketify.py (reference ceiling)} across
{plaintext "Hello World" ASGI, 4 KiB JSON, TLS plaintext, 64 KiB response,
streamed 1 MiB}.

Lands with the M2 native HTTP engine. Load generators: `bombardier` and
`rewrk` (R-131), orchestrated by `bench/harness` (3 warmup + 5 measured
runs, median + p99, JSON baselines; RPS, p50/p99/p999, CPU%, syscalls/req
via ETW, allocations/req via `loop.stats()`, RSS — R-133).

Perf-optimization PRs must attach ETW/WPA or VTune traces (R-133).
