# bench/suite: the benchmark suite

The suite behind the benchmark numbers in the [README](../../README.md#benchmarks),
for both Windows and Linux. It runs real servers as separate processes against
a native load generator, with no containers and no cross-compiled results.

| file | what it is |
|---|---|
| `../loadgen/` | The load generator (Go, standard library only): HTTP/1.1 keep-alive, pipelined, open-loop and connection-per-request; TCP echo; WebSocket echo; idle connections. It checks every response. |
| `run_suite.py` | Runs the suites: a fresh server per contender per rep, readiness by a real request, the server's process tree sampled throughout, contender order rotated every rep, one JSON line per run. |
| `servers.py` | Starts each contender on the same app (`app.py`) or the same echo protocol. |
| `app.py` | The ASGI app every HTTP and WebSocket contender serves. |
| `latency.py` | Timer overshoot and cross-thread wake-up latency for each event loop. |
| `report.py` | Turns runs into a summary JSON, Markdown tables, the README's table specs and the light/dark SVG charts. |
| `winproc.py`, `linproc.py` | Process, thread and TCP accounting: ctypes on Windows, `/proc` on Linux. Neither needs administrator rights. |

The contenders follow the platform. Both get cadeloop (`serve()`, and as the
loop under uvicorn), uvicorn on asyncio, and hypercorn. Windows adds uvicorn on
winloop and, for the loop-only tests, winloop and rsloop. Linux adds uvicorn on
uvloop, uvicorn's standard stack (httptools and websockets on uvloop), and
granian, and for the loop-only tests uvloop, rloop and rsloop.

## Running it

Build the load generator and the extension, and stage the build under test:
a directory holding `cadeloop/` with the compiled extension inside, plus an
`IDENTITY.txt` naming the revision and the extension's sha256 (the runner
copies it into `meta.json`).

```bash
go build -C bench/loadgen -o loadgen .
cargo build --release -p cadeloop-pyshim
mkdir -p stage/candidate && cp -r python/cadeloop stage/candidate/
cp target/release/lib_core.so stage/candidate/cadeloop/_core.so     # _core.pyd on Windows
pip install uvicorn uvloop httptools websockets wsproto hypercorn granian rloop rsloop
```

Then the suites. TLS needs a certificate and key:

```bash
python bench/suite/run_suite.py --suite http,ws,tls,echo --reps 3 --duration 5 --warmup 1.5 \
    --pkg candidate=stage/candidate --loadgen bench/loadgen/loadgen --out results/main \
    --cert cert.pem key.pem
python bench/suite/run_suite.py --suite idle,overload --idle-counts 1000,5000 \
    --overload-contenders "cadeloop[candidate],uvicorn+cadeloop,uvicorn+uvloop,uvicorn+asyncio" \
    --pkg candidate=stage/candidate --loadgen bench/loadgen/loadgen --out results/mem
python bench/suite/latency.py --pkg candidate=stage/candidate --out results/timers.json
PYTHONPATH=stage/candidate python bench/harness/harness.py --suite sched \
    --loops cadeloop,uvloop,rloop,rsloop,asyncio --out results/sched.json
python bench/suite/report.py results/main results/mem --summary summary.json --markdown tables.md \
    --tables readme-tables.json --charts docs/assets --timers results/timers.json --sched results/sched.json
python bench/harness/tables.py readme-tables.json --outdir docs/assets --readme README.md
```

On a machine with few cores, add `--loadgen-procs 2` so the load generator
leaves cores for the server; the Linux numbers were taken that way, on 4
vCPUs.

Two builds can be compared side by side by passing `--pkg` twice, for
example `--pkg baseline=<dir> --pkg candidate=<dir2>`, and `--cadeloop-only`
skips the third-party contenders. `report.py --with-baseline` then shows both
builds in the charts.

## What the runner guarantees

* **The binary under test is the one measured.** Each server is a fresh
  process with an explicit `PYTHONPATH`; nothing is installed. CPU, memory,
  handle and context-switch figures are summed over the server's whole
  process tree. On Windows that matters because a venv's `python.exe` is a
  launcher that starts the real interpreter as a child; granian and
  multi-worker servers have child processes everywhere.
* **Readiness is a real response.** There are no startup sleeps: a server is
  loaded only after an HTTP request (or an echo round trip) through it
  succeeds.
* **Warmup is excluded.** The load generator measures a window after its
  warmup, and the server CPU figure covers the same window.
* **Failures are visible.** Wrong status, wrong length, wrong body, framing
  errors, timeouts and resets are counted per kind and reported, never folded
  into throughput, and a server that dies mid-run is flagged.
* **Percentiles are never averaged.** Every run keeps its full latency
  histogram (log-linear, at most 1.6% bucket error), and a table's
  percentiles come from the merged histogram of all reps. Throughput is the
  median rep, with its range kept.
* **Contender order rotates every rep**, so slow drift in the machine does
  not favour whoever runs first.

## Load generator notes

On Windows, `loadgen` timestamps with QueryPerformanceCounter. Go's own
monotonic clock there reads the kernel's interrupt time, which advances only
at the timer interrupt, so ~50 µs round trips measured as exactly 0 or a
whole 0.5 to 15.6 ms tick until this was fixed. Elsewhere Go's monotonic
clock has nanosecond resolution and is used directly.

In open-loop mode (`-rate`, `-phases`) latency runs from each request's
intended send time, so a server stall shows up as queueing delay instead of
silently lowering the offered load. Churn mode (`-churn`) closes with RST
(linger 0), so neither side piles up TIME_WAIT sockets and the ephemeral
port range is never the bottleneck.

It was validated before use against a reference server with server-side
counters: request counts match to within the requests in flight at the stop,
a wrong body is reported as an error on every request, chunked and
content-length framing both parse, a 5 ms server delay reads as a 5 to 6 ms
p50, and an injected 1 s stall appears as about 1 s of queueing in the
open-loop series. Latencies from it and from an independent Python client
agree on the same server.

## Reading the numbers

These are loopback results: client and server share one machine, so they
compete for CPU, and loopback exercises no network card, RSS or interrupt
moderation. They compare servers under identical conditions; they are not
capacity numbers. Server CPU per request (user plus kernel time of the
server's process tree, divided by successful requests) is the figure least
affected by sharing the machine.

Memory is reported per platform's own measure. On Windows it is private
bytes (commit charge). On Linux it is unique memory (USS: private resident
pages), which leaves out pages a process shares, such as those a forked
worker shares with its parent, and pages committed but never touched.

On Windows, host security software matters more than one might expect.
With network-filtering products active (here: AdGuard's and a VPN client's
WFP drivers), loopback TCP on the test machine topped out near 14 K round
trips per second for any server, and single-connection round trips took
about 100 µs instead of about 20 µs. The published Windows numbers were
taken with those two products and Dropbox stopped in every session, and
Bitdefender's protection also disabled for the contender session. With
network filters running, expect every server's loopback numbers to be
capped well below these.

On a cloud VM, absolute numbers drift between sessions: the same echo
benchmark on the same VM type measured 66% apart in two sessions. Compare only
numbers from one session, which is what the rotation within a session is
for.
