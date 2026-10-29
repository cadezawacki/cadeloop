# bench/windows — native Windows benchmarks

The suite behind the Windows numbers in the [README](../../README.md#benchmarks-windows)
and [docs/benchmarks.md](../../docs/benchmarks.md#windows). Everything here
runs real Windows executables against a real CPython 3.11 extension: no WSL,
no containers, no cross-compiled results.

| file | what it is |
|---|---|
| `../loadgen/` | Native load generator (Go, stdlib only): HTTP/1.1 keep-alive, pipelined, open-loop and connection-per-request; TCP echo; WebSocket echo; idle-connection holding. Validates every response. |
| `run_suite.py` | Orchestrator: fresh server per contender per rep, readiness by a real request, server process-tree sampling, rotated contender order, one JSON line per run. |
| `servers.py` | Launches each contender on the same app (`app.py`) or the same echo protocol. |
| `app.py` | The one ASGI app every HTTP/WebSocket contender serves. |
| `latency.py` | Timer overshoot and cross-thread wakeup latency per loop. |
| `report.py` | Aggregates runs into tables, a summary JSON and the light/dark SVG charts. |
| `winproc.py` | Process/thread/TCP accounting through ctypes (no elevation needed). |

## Running it

```powershell
go build -C bench\loadgen -o $env:TEMP\loadgen.exe .
cargo build --release -p cadeloop-pyshim
# stage the package under test: python\cadeloop + target\release\_core.dll -> <dir>\cadeloop\_core.pyd
python bench\windows\run_suite.py --suite http,echo,ws,tls,idle `
    --pkg candidate=<dir> [--pkg baseline=<dir2>] --reps 3 --duration 5 --warmup 1.5 `
    --loadgen $env:TEMP\loadgen.exe --out results\run1 --cert cert.pem key.pem
python bench\windows\latency.py --pkg candidate=<dir> --out results\timers.json
python bench\windows\report.py results\run1 --summary summary.json --markdown tables.md `
    --charts docs\assets --timers results\timers.json
```

`--pkg name=<dir>` puts `<dir>` first on `PYTHONPATH` for that contender's
server, so two builds can be compared side by side. Each staged package
should carry an `IDENTITY.txt` (revision + sha256 of `_core.pyd`); the
runner copies it into `meta.json`.

## What the runner guarantees

* **The binary under test is the one measured.** Each server is a fresh
  process with an explicit `PYTHONPATH`; nothing is pip-installed. A venv's
  `python.exe` on Windows is a launcher that starts the real interpreter as
  a child, so CPU, memory, handle and context-switch figures are summed over
  the server's whole process tree, never read off the launcher alone.
* **Readiness is a real response.** No startup sleeps: a server is loaded
  only after an HTTP request through it (or an echo round trip) succeeds.
* **Warmup is excluded.** The load generator measures a window after its
  warmup; requests sent before it or completed after it are not counted, and
  the server-CPU figure covers the same window.
* **Failures are visible.** Wrong status, wrong length, wrong body, framing
  errors, timeouts and resets are counted per kind and reported, never
  folded into throughput; a server that dies mid-run is flagged.
* **Percentiles are never averaged.** Every run keeps its full latency
  histogram (log-linear, ≤1.6% bucket error); a table's percentiles come
  from the merged histogram of all reps. Throughput is the median rep, with
  its min–max shown.
* **Contender order rotates every rep**, so slow drift in the machine's
  state does not favour whoever runs first.

## Load generator notes

`loadgen` timestamps with QueryPerformanceCounter. Go's own monotonic clock
on Windows reads the kernel's interrupt time, which advances only at the
timer interrupt: ~50 µs round trips measured as exactly 0 or a whole
0.5–15.6 ms tick until this was fixed. In open-loop mode (`-rate`,
`-phases`) latency runs from each request's *intended* send time, so a
server stall shows up as queueing delay instead of silently lowering the
offered load. Churn mode (`-churn`) closes with RST (linger 0) so neither
side accumulates TIME_WAIT and the 16,384-port dynamic range is never the
bottleneck.

It was validated before use against a reference server with server-side
counters: request counts match to within the requests in flight at the
stop, a wrong body is reported as an error on every request, chunked and
content-length framing both parse, a 5 ms server delay reads as a 5–6 ms
p50, and an injected 1 s stall appears as ~1 s of queueing in the
open-loop series. Latencies from it and from an independent Python client
agree on the same server.

## Reading the numbers

These are **loopback** results: client and server share one machine, so
they compete for CPU, and loopback exercises no NIC, RSS or interrupt
moderation. They compare servers under identical conditions; they are not
capacity numbers. Per-request server CPU (user + kernel of the server's
process tree, divided by successful requests) is the figure least affected
by sharing the box.

Host security software matters more on Windows than one might expect.
With network-filtering products active (here: AdGuard's and a VPN client's
WFP drivers), loopback TCP on the test machine topped out near 14 K round
trips/s *for any server* -- two independent servers loaded at once still
shared the same 14 K -- and single-connection RTT was ~100 µs instead of
~20 µs. The published numbers were taken with those two products and
Dropbox stopped in every session, and Bitdefender's protection also
disabled for the contender session. cadeloop 1.0.1 ran in every session
as a control and measured within 2% each time. With network filters
running, expect every server's loopback numbers to be capped well below
these.
