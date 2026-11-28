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
  in one worker process; the FastAPI tests serve
  [`bench/suite/fapp.py`](../bench/suite/fapp.py) the same way. The TCP echo
  tests run the same `asyncio.Protocol` on each event loop, plain and behind
  `create_server(ssl=...)`.
- Each run starts a fresh server, waits until a real request through it
  succeeds, then loads it for a 1.5 s warmup and a 5 s measured window.
- Each workload is run several times, with the order of servers rotated
  between runs: nine times on Linux, three on Windows. Throughput is the
  median run; latency percentiles come from all the runs' latency
  histograms merged.
- Server CPU per request is user plus kernel time of the server's whole
  process tree over the measured window, divided by successful requests. It
  is the number least affected by the load generator sharing the machine.
- Every response is checked (status, framing, length, and body where known).
  Failures are counted by kind and reported, never folded into throughput.
  No published run had a failed request.
- The timer test (`bench/suite/latency.py`) and the scheduling
  microbenchmarks (`bench/harness/harness.py --suite sched`, a fresh
  process per run) have no network or load generator at all. On Linux both
  take the median of nine runs with the loops interleaved, run by run, so
  that no loop has the machine's good minute to itself; on Windows the
  scheduling numbers are 5 measured runs after 3 warmup runs.

## How sure the numbers are

The Linux numbers were first published from three runs per workload. Looked
at again, that data could not carry what the tables did with it: over its
117 cells the three runs of one server on one workload spanned 9% of their
median at the middle cell, 18% at one cell in ten, and 32% at the worst, and for 24 of the 98 pairs
of neighboring places in the tables the two servers' ranges overlapped,
so the order printed was not known. The November 2026 Linux run is built to
say how far each number can be trusted:

- **Nine runs, and an interval for every median.** The interval is the
  second lowest to the second highest of the nine runs. Whatever the runs'
  distribution, the true median lies between those two 96.1% of the time
  (it is outside only if eight or more of nine independent runs fall on one
  side of it). Three runs cannot give more than a 75% interval, which is
  why the Windows tables, still from three runs, show a range and make no
  such claim.
- **The suites are interleaved.** The nine runs of a workload are spread
  over the whole session in three passes of three, so a slow quarter of an
  hour on the VM lands on every server and shows up in the intervals.
- **The same build is measured twice.** cadeloop runs under two names,
  `control` and `candidate`, through every workload. The two are the same
  files, so whatever separates them is noise: the smallest difference this
  session can show between any two servers. Over the 28 workloads the two
  medians differed by 1.4% at the middle one, 3.7% at one in ten and 5.5% at
  the worst, and their intervals overlapped every time
  ([the table](benchmarks-linux.md#how-far-to-trust-these-numbers)).
- **Places are shared when the runs cannot tell.** In the README's tables
  a server is ahead of another only when its whole interval is ahead of the
  other's. Two servers whose intervals overlap share a place, as do two
  whose printed values are equal.
- **A result capped by the load generator is marked.** When the load
  generator used 90% or more of the CPU it was allowed, the server was not
  the limit. The cell is marked `≥` and treated as a lower bound: it can be
  placed ahead of a server it clearly beat, never behind one.
- **The host's interference is recorded.** Every run records the share of
  CPU time the hypervisor withheld from the VM while it ran.

In this session one run of a workload differed from the next by 4.3% on the
typical cell (the coefficient of variation; 6.5% at one cell in ten), and
the interval of a median came out at ±4.1% of it (±6.5% at one cell in ten).
Idle-connection memory and the overload run are single measurements and
carry no interval.

What this does not cover: all nine runs are from one session on one VM.
The intervals say how well that session pins its own medians down, not how
the numbers would move on another machine or another day. They also treat
the nine runs as independent, which runs taken minutes apart on a drifting
machine are only roughly; spreading them over the session is what keeps
that honest.

## Linux environment

| | |
|---|---|
| Machine | cloud VM, 4 vCPU Intel Xeon @ 2.10 GHz, 16 GB |
| OS | Linux 6.18.44, glibc 2.39 |
| Python | CPython 3.11.18 |
| cadeloop | revision `2e2edee`, release build (Rust 1.100, fat LTO, `codegen-units=1`, `panic=abort`, mimalloc); `epoll` backend |
| Contenders | uvicorn 0.54.0 (h11 0.16.0, httptools 0.9.0, wsproto 1.3.2, websockets 17.4), hypercorn 0.18.0, granian 2.8.9, uvloop 0.23.0, rloop 0.5.0, rsloop 0.1.69, stdlib asyncio (selector) |
| FastAPI tests | FastAPI 0.165.0, Starlette 1.10.0, pydantic 2.14.4 |
| TLS | TLS 1.3 under an ECDSA P-256 certificate; the handshake-per-request test also under an RSA 2048 one |
| Load | bench/loadgen built with Go 1.24, on the same VM, limited to 2 threads (`-procs 2`) so the server keeps the other cores |

uvicorn appears three ways: on asyncio and on uvloop with the h11 parser and
wsproto (the same configuration as on Windows), and as `uvicorn[standard]`,
the configuration most Linux deployments use: httptools and websockets on
uvloop. granian runs one worker with its defaults and uvloop.

The VM is shared hardware, and its speed drifts between sessions: the same
echo benchmark measured 66% apart in two sessions. Every Linux number in the
README comes from one session of under four hours in November 2026 (raw
data: [bench/baselines/linux-2026-11/](../bench/baselines/linux-2026-11/)),
so they compare with each other but not with numbers from another day. That
includes the October numbers they replace
([bench/baselines/linux-2026-10/](../bench/baselines/linux-2026-10/)).
Between the two sessions granian's plaintext figure did not move (48.7 K
requests per second both times), while every server that runs Python code
for each request got 24 to 44% faster, uvicorn on asyncio included, whose
own code did not change. What changed under them is most likely the
interpreter, a different build of CPython 3.11. Either way, a figure here
against one there says nothing about what changed in cadeloop.

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
silently runs the standard library loop while the results get labeled with
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

## Trap 6: the allocator's first large free

The first TLS echo comparison across loops had uvloop at 28 K messages per
second where a hand-written script on the same machine measured 85 K. The
code was the same; the processes differed in what they had imported.

glibc serves an allocation of 128 KiB or more with its own `mmap()` and
returns it with `munmap()`, until the program frees one such block. From
then on the threshold has moved up and blocks of that size come from the
heap. Python's `ssl` module allocates a 256 KiB buffer for every read that
asyncio's and uvloop's TLS make. Until a process has freed its first large
block, a TLS server therefore pays an `mmap`, an `mremap` and a `munmap`
per read, with the page faults that go with them; `strace` showed those
calls on every read in the slow process and none in the fast one. Which
state a server starts in is an accident: importing one module with a large
compiled file is enough to flip it.

A real application reads files and bodies larger than 128 KiB as a matter
of course, so the settled state is the one worth measuring. Every server
in the suite now allocates and frees one 8 MiB block at startup
(`settle_allocator` in [`bench/suite/servers.py`](../bench/suite/servers.py));
`BENCH_COLD_ALLOCATOR=1` leaves the allocator as the process found it. The
published HTTP-over-TLS rows were not affected: those servers had all
reached the settled state on their own.

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
- With two threads, the load generator cannot outrun the fastest echo
  workloads: every loop's row with 16 messages in flight, and cadeloop's
  connection-per-request HTTP row, are lower bounds (marked `≥`).
- The Windows numbers have not been re-measured with this method. They are
  still the October 2026 medians of three runs, ranked by the medians alone,
  on a build that predates the work measured in the Linux tables.
