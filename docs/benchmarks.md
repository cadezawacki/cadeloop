# Benchmark methodology

The headline numbers are in the [README](../README.md#benchmarks), and every
number as text is in [benchmarks-linux.md](benchmarks-linux.md) and
[benchmarks-windows.md](benchmarks-windows.md). This page covers how they
were measured, on what, and the measurement mistakes worth knowing about.

Both platforms use the same suite, [`bench/suite`](../bench/suite/README.md),
and the same load generator, [`bench/loadgen`](../bench/loadgen/). A newcomer's
introduction to measuring servers is in
[docs/learn/6-measuring.md](learn/6-measuring.md).

## Method

- Every server serves the same ASGI app ([`bench/suite/app.py`](../bench/suite/app.py))
  in one worker process. The TCP echo tests run the same `asyncio.Protocol`
  on each event loop.
- Each run starts a fresh server, waits until a real request through it
  succeeds, then loads it for a 1.5 s warmup and a 5 s measured window.
- Each workload is run three times, with the order of servers rotated
  between runs. Throughput is the median run; latency percentiles come from
  the three runs' latency histograms merged.
- Server CPU per request is user plus kernel time of the server's whole
  process tree over the measured window, divided by successful requests. It
  is the number least affected by the load generator sharing the machine.
- Every response is checked (status, framing, length, and body where known).
  Failures are counted by kind and reported, never folded into throughput.
  No published run had a failed request.
- The timer test (`bench/suite/latency.py`) and the scheduling
  microbenchmarks (`bench/harness/harness.py --suite sched`: 3 warmup and
  5 measured runs, a fresh process per run) have no network or load
  generator at all. On Linux the scheduling numbers are the median of 15
  runs from three full passes, because one pass varies too much on a
  shared VM (one pass's runs of a single benchmark spanned 50 K to 95 K).

## Linux environment

| | |
|---|---|
| Machine | cloud VM, 4 vCPU Intel Xeon @ 2.10 GHz, 16 GB |
| OS | Linux 6.18.44, glibc 2.39 |
| Python | CPython 3.11.17 |
| cadeloop | revision `808d72d`, release build (Rust 1.97, fat LTO, `codegen-units=1`, `panic=abort`, mimalloc); `epoll` backend |
| Contenders | uvicorn 0.54.0 (h11 0.16.0, httptools 0.9.0, wsproto 1.3.2, websockets 17.3), hypercorn 0.18.0, granian 2.8.7, uvloop 0.23.0, rloop 0.5.0, rsloop 0.1.66, stdlib asyncio (selector) |
| Load | bench/loadgen built with Go 1.24, on the same VM, limited to 2 threads (`-procs 2`) so the server keeps the other cores |

uvicorn appears three ways: on asyncio and on uvloop with the h11 parser and
wsproto (the same configuration as on Windows), and as `uvicorn[standard]`,
the configuration most Linux deployments use: httptools and websockets on
uvloop. granian runs one worker with its defaults and uvloop.

The VM is shared hardware, and its speed drifts between sessions: the same
echo benchmark measured 66% apart in two sessions. Every Linux number in the
README comes from one session, run back to back, so they compare with each
other but not with numbers from another day.

## Windows environment

| | |
|---|---|
| OS | Windows 11 Home 10.0.26200, x64, hypervisor present (HVCI on) |
| CPU | Intel Core Ultra 7 265K, 20 cores, 32 GB |
| Python | CPython 3.11.9 (64-bit, MSC v.1938) |
| Rust | 1.96.0, `x86_64-pc-windows-msvc`, release profile (fat LTO, `codegen-units=1`, `panic=abort`), mimalloc |
| Contenders | uvicorn 0.52.2 (h11, wsproto), hypercorn 0.18.0, winloop 0.6.3, rsloop 0.1.30, stdlib asyncio (proactor) |
| Load | loadgen (Go 1.25.6), same machine, loopback |
| Host state | AdGuard, ProtonVPN and Dropbox stopped in every session; Bitdefender protection disabled for the contender session |

Rows come from four sessions on one machine with the same configuration. The
same cadeloop build ran in every session as a control and measured within 2%
each time, so the sessions are comparable. Raw data:
[bench/baselines/windows-2026-10/](../bench/baselines/windows-2026-10/).

## Trap 1: the load generator was the bottleneck

The repo also has `harness.py --suite http`, whose load generator is
`bench/http/client.py`: 64 Python threads in a single process. Splitting
the same offered load across two client processes, against an unchanged
server, nearly doubled the measured total:

| clients | connections | measured total |
|---|---|---|
| 1 process | 64 | 23.3 K req/s |
| 2 processes | 32 each | 43.9 K req/s |

That is the GIL limiting the client, not the server. Under that generator,
cadeloop, granian and uvicorn with httptools all measured 20 to 25 K req/s
and looked interchangeable; under a native load generator they separate by
roughly 4×. The suite uses `bench/loadgen` (Go) for this reason. Do not use
`harness.py --suite http` or `--suite echo` to compare fast servers.

## Trap 2: asking uvicorn for a loop does not get you that loop

Since 0.35, uvicorn picks its event loop by class through a loop-factory
table:

```python
# uvicorn/loops/asyncio.py
def asyncio_loop_factory(use_subprocess: bool = False):
    if sys.platform == "win32" and not use_subprocess:
        return asyncio.ProactorEventLoop
    return asyncio.SelectorEventLoop
```

`asyncio.set_event_loop_policy()` has no effect on this. The older idiom,
installing a policy and then running `uvicorn.run(..., loop="asyncio")`,
silently runs the standard library loop while the results get labelled with
whichever loop you installed. There is also no `--loop cadeloop`; uvicorn's
`--loop` accepts only `auto`, `asyncio` and `uvloop`.

This made every uvicorn row with h11 land on the same ~6.5 K req/s in an
early run, and supported a tidy, false conclusion ("the loop doesn't
matter"). Measured both ways on the same build:

| row | with the policy idiom | driving the loop directly |
|---|---|---|
| uvicorn + httptools on uvloop | 26.5 K | **43.7 K** |
| uvicorn + h11 on cadeloop | 6.6 K | **10.0 K** |
| uvicorn + h11 on uvloop | 6.6 K | **9.3 K** |
| uvicorn + h11 on asyncio | 6.5 K | 6.6 K *(unchanged: it was always the stdlib loop)* |

`bench/suite/servers.py` passes `loop="none"` and drives
`uvicorn.Server.serve()` from a loop it built itself, which is also the way
to run uvicorn on cadeloop in an application.

## Trap 3: Go's clock on Windows

Go's monotonic clock on Windows reads the kernel's interrupt time, which
advances only at the timer interrupt. The first validation run of the load
generator reported echo round trips of exactly 0 or ~0.5 ms; they were
~100 µs. On Windows every latency it records is now a
QueryPerformanceCounter delta, cross-checked against an independent Python
client on the same server.

## Trap 4: the host's network filters set the ceiling

With AdGuard's and a VPN client's WFP filtering drivers active, loopback TCP
on the Windows test machine saturated at ~14 K round trips per second
system-wide: one server or two independent servers loaded at once, the total
was the same, and stdlib asyncio, winloop and cadeloop all measured ~14.3 K
msg/s on 64 echo connections. Single-connection round trips took ~100 µs.
With those products, Dropbox and Bitdefender's protection paused, the same
Go echo server reached 201 K msg/s and cadeloop's single-connection round
trip was 21 µs. A benchmark that cannot separate servers is measuring the
host; check by loading two servers at once.

## Trap 5: the process you started is not always the server

On Windows a venv's `python.exe` is a launcher that starts the real
interpreter as a child, so CPU or memory read off the `Popen` handle measures
the launcher. granian starts its workers as child processes on every
platform, and multi-worker servers have children too. The suite sums every
figure over the server's whole process tree.

## rloop 0.3.1 aborted under sustained load

Fixed upstream; rloop 0.5.0, the version measured now, completes every test.
Kept for the record: rloop 0.3.1 served ~7.1 K req/s for two consecutive 3 s
runs and then killed the process on a Rust panic
(`called Option::unwrap() on a None value` in `src/event_loop.rs`).

## Known limitations

- Single machine, loopback only. Client and server share the CPUs, and there
  is no network card in the path.
- Linux numbers are from a 4-vCPU VM with the load generator limited to two
  threads; servers that scale with more cores, granian among them, are held
  back by the machine as much as anyone.
- Memory is measured differently per platform: private bytes (commit charge)
  on Windows, unique resident memory (USS) on Linux.
