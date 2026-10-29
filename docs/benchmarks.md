# Benchmark methodology

Headline numbers live in the [README](../README.md#benchmarks). This file
holds the methodology, the raw commands, and the measurement mistakes worth
knowing about — including two that silently flattened an earlier draft of
the README table into a near-tie.

The Windows methodology is [below](#windows); its harness and full notes are in
[bench/windows/](../bench/windows/README.md). Older Windows/IOCP runs are kept in
[docs/README.md](README.md#benchmarks-windows-11-loopback).

Charts are generated from the same JSON the tables quote:

```bash
python bench/harness/charts.py --sched /tmp/sched.json \
    --sched-ranked /tmp/sched.json --http-ranked /tmp/http-wrk.json \
    --outdir docs/assets
```

## Environment

| | |
|---|---|
| OS | Linux 6.18.44 |
| CPU | 4 vCPU Intel Xeon @ 2.10 GHz |
| Python | CPython 3.11.17 |
| libc | glibc 2.39 |
| cadeloop backend | `epoll-dev` (the Linux dev backend, not the Windows/IOCP production target) |

Contenders: uvloop 0.23.0, rloop 0.5.0, rsloop 0.1.66, uvicorn 0.54.0 (h11 0.16
and httptools 0.9), granian 2.8.5, hypercorn 0.18.0.

Everything runs over loopback with the load generator sharing the same four
cores as the server. Absolute throughput is therefore conservative; the
numbers are for relative comparison on identical conditions, not capacity
planning.

## Running them

```bash
pip install uvloop rloop rsloop "uvicorn[standard]" hypercorn granian
sudo apt-get install wrk

# HTTP/ASGI — wrk-driven
PYTHONPATH=$PWD/python python bench/http/run_wrk.py

# scheduling core
PYTHONPATH=$PWD/python python bench/harness/harness.py --suite sched \
    --loops cadeloop,asyncio,uvloop,rloop,rsloop
```

**HTTP**: `wrk -t2 -c64`, 3s warmup + 3 measured 10s runs, median reported,
single worker per contender, fresh server process per contender. Workload is
the plaintext `Hello, World!` ASGI app in `bench/http/app.py`. `run_wrk.py`
records non-2xx responses and socket errors per run, so a contender that is
fast because it is answering wrongly shows up rather than averaging in.

**Scheduling**: 3 warmup + 5 measured runs, one fresh process per
(loop, benchmark) pair. The README figures merge three full passes, so each
is the median of 15 runs: on a shared 4-vCPU VM a single pass's spread
(slowest to fastest run) reached 30–60% on several benchmarks. No sockets and no external load
generator, so these are free of trap 1 below.

`queue_pingpong` and `queue_pingpong_native` are the same workload on
`asyncio.Queue` and on `cadeloop.Queue`. The latter is loop-agnostic and runs
under every contender, so the pair measures the queue implementation rather
than the loop.

## Trap 1 — the load generator was the bottleneck

The repo also ships `harness.py --suite http`, whose load generator is
`bench/http/client.py`: 64 Python threads in a single process.

Splitting the same offered load across two client *processes*, against an
unchanged server, nearly doubles the measured total:

| clients | connections | measured total |
|---|---|---|
| 1 process | 64 | 23.3 K req/s |
| 2 processes | 32 each | 43.9 K req/s |

That is a GIL ceiling in the client, not a limit in the server. Under that
generator, cadeloop, granian and uvicorn+httptools all reported 20–25 K req/s
and looked interchangeable. Under `wrk` — C, with its own event loop — they
separate by roughly 4×.

`bench/http/run_wrk.py` exists for this reason. **Do not use
`harness.py --suite http` to compare fast servers.**

The TCP echo suite (`--suite echo`) has the same ceiling — 28.5 K msg/s from
one client process, 47.2 K across two — so its numbers are omitted from the
README rather than published as a loop comparison. Fixing that client is
open work.

## Trap 2 — asking uvicorn for a loop does not get you that loop

Since 0.35, uvicorn selects its event loop *by class* through a loop-factory
table:

```python
# uvicorn/loops/asyncio.py
def asyncio_loop_factory(use_subprocess: bool = False):
    if sys.platform == "win32" and not use_subprocess:
        return asyncio.ProactorEventLoop
    return asyncio.SelectorEventLoop
```

`asyncio.set_event_loop_policy()` has no effect on this. The older idiom —
install a policy, then run `uvicorn.run(..., loop="asyncio")` — silently runs
the **stdlib** loop while labelling the result with whichever loop you
installed. There is also no `--loop cadeloop`; uvicorn's `--loop` accepts only
`auto`, `asyncio`, and `uvloop`.

This is what made every `uvicorn + h11` row land on the same ~6.5 K req/s and
support a tidy but false conclusion ("the loop doesn't matter"). Correcting it
moved real numbers (both columns measured on the build current at the time,
so they compare the two *methods*, not two builds — the README's figures are
from a later run and differ slightly):

| row | with the policy idiom | driving the loop directly |
|---|---|---|
| uvicorn + httptools on uvloop | 26.5 K | **43.7 K** |
| uvicorn + h11 on cadeloop | 6.6 K | **10.0 K** |
| uvicorn + h11 on uvloop | 6.6 K | **9.3 K** |
| uvicorn + h11 on asyncio | 6.5 K | 6.6 K *(unchanged — it was always stdlib)* |

`bench/http/server.py` now passes `loop="none"` and drives
`uvicorn.Server.serve()` from a loop it constructed itself, which is also the
only way to run uvicorn on cadeloop in an application.

## rloop 0.3.1 aborts under sustained load

Fixed upstream: rloop 0.5.0 completes the full protocol and has an HTTP row
again. Kept for the record.

rloop served ~7.1 K req/s for two consecutive 3s runs and then killed the
process on a Rust panic:

```
thread '<unnamed>' panicked at src/event_loop.rs:417:44:
called `Option::unwrap()` on a `None` value
```

It could not complete the 3×10s protocol, so it has no HTTP row. This is an
upstream bug, not a harness failure. Its scheduling numbers were collected
before sustained socket load was applied and are unaffected.

## Result history

Numbers are re-measured whenever the engine changes, so the README's table and
these notes move together. Figures quoted inside "Trap 1" and "Trap 2" are kept
at the values from the run that exposed each problem, because the point of
those tables is the delta between two measurement methods rather than the
current throughput.

## Known limitations

- Single box, loopback only. The spec's acceptance numbers (R-131) are
  two-machine Windows runs and are not these.
- Linux measures the `epoll` dev backend. The production target is
  Windows/IOCP.
- Four cores shared between client and server compresses every result;
  contenders that scale better with cores are penalised equally.
- One workload (plaintext `Hello, World!`). Large bodies, TLS, and
  WebSockets are not represented in these numbers.

## Windows

The Windows numbers in the README come from a separate, native harness:
[`bench/windows/`](../bench/windows/README.md) drives each server with
[`bench/loadgen`](../bench/loadgen/), a Go load generator built for this
because a Python client is GIL-bound (trap 1) and `wrk` does not run on
Windows. It speaks HTTP/1.1 (keep-alive, pipelined, open-loop, and one
connection per request), TCP echo and WebSocket, validates every response,
and keeps each run's full latency histogram.

### Trap 3 — Go's clock on Windows

Go's monotonic clock on Windows reads the kernel's interrupt time, which
advances only at the timer interrupt. The first validation run of the load
generator reported echo round trips of exactly 0 or ~0.5 ms; they were
~100 µs. Every latency it records is now a QueryPerformanceCounter delta,
cross-checked against an independent Python client on the same server.

### Trap 4 — the host's network filters set the ceiling

With AdGuard's and a VPN client's WFP filtering drivers active, loopback TCP
on the test machine saturated at ~14 K round trips/s *system-wide*: one
server or two independent servers loaded at once, the total was the same,
and stdlib asyncio, winloop and cadeloop all measured ~14.3 K msg/s on 64
echo connections. Single-connection RTT was ~100 µs. With those products,
Dropbox and Bitdefender's protection paused, the same Go echo server
reached 201 K msg/s and cadeloop's single-connection RTT was 21 µs. A
benchmark that cannot separate servers is measuring the host; check by
loading two servers at once.

### Trap 5 — a venv's python.exe is not the server

On Windows a venv's `python.exe` is a launcher that starts the real
interpreter as a child. Reading CPU or memory off the `Popen` handle
measures the launcher. The runner sums everything over the server's
process tree (and spawn-model workers are grandchildren).

### Windows environment and method

| | |
|---|---|
| OS | Windows 11 Home 10.0.26200, x64, hypervisor present (HVCI on) |
| CPU | Intel Core Ultra 7 265K, 20 cores, 32 GB |
| Python | CPython 3.11.9 (64-bit, MSC v.1938) |
| Rust | 1.96.0, `x86_64-pc-windows-msvc`, release profile (fat LTO, `codegen-units=1`, `panic=abort`), mimalloc |
| Contenders | uvicorn 0.52.2 (h11, wsproto), hypercorn 0.18.0, winloop 0.6.3, rsloop 0.1.30, stdlib asyncio (proactor) |
| Load | loadgen (Go 1.25.6), same machine, loopback |
| Host state | AdGuard, ProtonVPN and Dropbox stopped in every session; Bitdefender protection disabled for the contender session. cadeloop 1.0.1 ran in each session as a control: 73.7 K, 74.3 K, 74.8 K and 73.4 K req/s plaintext |

Each contender serves the same ASGI app (`bench/windows/app.py`) from a
fresh process per rep; three reps with the contender order rotated; 1.5 s
warmup, 5 s measured. Throughput is the median rep. Percentiles come from
the merged histogram of all reps' observations. "Server CPU / request" is
user + kernel time of the server's process tree over the measured window
divided by successful requests -- the number least affected by the client
sharing the machine.

Results: summarised in the [README](../README.md#windows-11-iocp), every
table in full in [benchmarks-windows.md](benchmarks-windows.md), raw data in
[bench/baselines/windows-2026-10/](../bench/baselines/windows-2026-10/).
