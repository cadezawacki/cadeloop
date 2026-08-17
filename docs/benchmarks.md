# Benchmark methodology

Headline numbers live in the [README](../README.md#benchmarks). This file
holds the methodology, the raw commands, and the measurement mistakes worth
knowing about — including two that silently flattened an earlier draft of
the README table into a near-tie.

The Windows/IOCP benchmark history is separate, in
[docs/README.md](README.md#benchmarks-windows-11-loopback).

## Environment

| | |
|---|---|
| OS | Linux 6.18.5 |
| CPU | 4 vCPU Intel Xeon @ 2.80 GHz |
| Python | CPython 3.11.15 |
| libc | glibc 2.39 |
| cadeloop backend | `epoll-dev` (the Linux dev backend, not the Windows/IOCP production target) |

Contenders: uvloop 0.22.1, rloop 0.3.1, rsloop 0.1.36, uvicorn 0.52.3 (h11
and httptools), granian 2.8.1, hypercorn 0.18.

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
(loop, benchmark) pair, medians reported. No sockets and no external load
generator, so these are free of trap 1 below.

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
moved real numbers:

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

rloop served ~7.1 K req/s for two consecutive 3s runs and then killed the
process on a Rust panic:

```
thread '<unnamed>' panicked at src/event_loop.rs:417:44:
called `Option::unwrap()` on a `None` value
```

It could not complete the 3×10s protocol, so it has no HTTP row. This is an
upstream bug, not a harness failure. Its scheduling numbers were collected
before sustained socket load was applied and are unaffected.

## Known limitations

- Single box, loopback only. The spec's acceptance numbers (R-131) are
  two-machine Windows runs and are not these.
- Linux measures the `epoll` dev backend. The production target is
  Windows/IOCP.
- Four cores shared between client and server compresses every result;
  contenders that scale better with cores are penalised equally.
- One workload (plaintext `Hello, World!`). Large bodies, TLS, and
  WebSockets are not represented in these numbers.
