# cadeloop

**A drop-in `asyncio` event loop with a Rust core — and a native HTTP/1.1 + ASGI server built on top of it.**

[![PyPI](https://img.shields.io/pypi/v/cadeloop.svg)](https://pypi.org/project/cadeloop/)
[![Python](https://img.shields.io/pypi/pyversions/cadeloop.svg)](https://pypi.org/project/cadeloop/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Platforms](https://img.shields.io/badge/platform-Windows%20x64%20%7C%20Linux%20x64-lightgrey.svg)](#installation)

```python
import asyncio, cadeloop

cadeloop.install()          # every asyncio API below now runs on cadeloop
asyncio.run(main())
```

Two lines, and your existing `asyncio` code runs on a Rust reactor. Nothing else
changes: `asyncio.start_server`, `open_connection`, `add_reader`, `sock_*`,
subprocesses, signals, and third-party libraries like **uvicorn** and **aiohttp**
all work unmodified.

When you want more than a faster loop, skip the Python HTTP stack entirely:

```bash
pip install cadeloop
cadeloop myapp:app --port 8000 --workers 4
```

`cadeloop.serve()` parses HTTP, builds the ASGI scope, and serializes responses
**in Rust**. Your `async def app(scope, receive, send)` is the only Python left on
the request path — which is why it serves **2.4× a tuned uvicorn**
(httptools + uvloop), **1.5× granian**, and **15.8× uvicorn + h11 on stdlib
asyncio**. Every number is [measured below](#benchmarks), on a stated machine,
with a reproduction command.

---

## Table of contents

- [Why cadeloop](#why-cadeloop)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Benchmarks](#benchmarks)
- [Usage guide](#usage-guide)
  - [1. As a drop-in event loop](#1-as-a-drop-in-event-loop)
  - [2. As an ASGI server](#2-as-an-asgi-server-cadeloopserve)
  - [3. From the command line](#3-from-the-command-line)
  - [`Loop()` — every constructor argument](#loop--every-constructor-argument)
  - [`Config` — every tunable](#config--every-tunable)
  - [`serve()` — every argument](#serve--every-argument)
  - [CLI flag reference](#cli-flag-reference)
  - [Environment variables](#environment-variables)
  - [`loop.stats()` — introspection](#loopstats--introspection)
- [Use cases and recipes](#use-cases-and-recipes)
- [Compatibility](#compatibility)
- [Architecture](#architecture)
- [Development](#development)
- [License](#license)

---

## Why cadeloop

Most async Python performance work stops at the event loop. cadeloop goes one
layer further and moves the *protocol* into Rust as well:

| | what runs in Python | what runs in Rust |
|---|---|---|
| stdlib `asyncio` | loop, transports, protocols, HTTP | — |
| uvloop / rloop / rsloop | transports, protocols, HTTP | loop |
| **cadeloop (drop-in mode)** | protocols, HTTP | loop, transports |
| **cadeloop (`serve()`)** | your ASGI app, and nothing else | loop, transports, HTTP parse + scope + serialize |

Three properties fall out of that design:

**Windows is a first-class target, not an afterthought.** cadeloop is built on
IOCP — the completion-based API Windows actually wants you to use — with a
Registered I/O backend implemented behind it. uvloop does not support Windows at
all. If you deploy Python services on Windows, this is the point.

**One transport layer, two kernels.** Linux `epoll` is wrapped as a proactor
(the syscall is attempted at post time and only parked on `EWOULDBLOCK`), so the
same Rust transport code serves both platforms and behaviour does not fork by OS.

**Drop-in means drop-in.** cadeloop implements the full
`asyncio.AbstractEventLoop` surface and is verified against **CPython's own
asyncio conformance suite**, not just its own tests. If it does not behave like
the stdlib loop, that is a bug.

> **Project status: alpha (0.0.x).** The scheduling core, transports, TLS, UDP,
> WebSockets, the HTTP/ASGI engine, and multi-worker serving are implemented and
> tested. The API may still move before 1.0. See
> [docs/README.md](docs/README.md) for the full engineering record, milestone
> status, and the hardening log.

---

## Installation

```bash
pip install cadeloop
```

### Requirements

| | requirement | notes |
|---|---|---|
| Python | **CPython 3.11.x only** | Not abi3 — 3.10 and 3.12 will not install. |
| OS | Windows 10/11, or Linux | |
| CPU | x86-64 (Intel **or** AMD) | `amd64` is the instruction set, unrelated to GPUs. |

Wheels are published for **Windows x64** and **Linux x64 (manylinux2014,
glibc ≥ 2.17)**. No GPU, no compiler, and no Rust toolchain are needed to
install a wheel.

### Platforms without a wheel

There is no aarch64 wheel yet — Apple Silicon, Windows on ARM, Raspberry Pi, and
AWS Graviton will fall through to the source distribution, which needs a Rust
toolchain:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
pip install cadeloop --no-binary cadeloop
```

The Rust sources gate on operating system rather than CPU architecture, so an
aarch64 build is expected to work — but it is not built or tested in CI, so
treat it as unsupported rather than proven.

### The PGO and `x86-64-v3` wheels

Windows wheels are built with **profile-guided optimization**: an instrumented
build runs the project's own scheduling and HTTP workload, and the final wheel is
compiled against those profiles. That is the wheel `pip` installs by default —
you get it automatically.

Each [GitHub Release](https://github.com/cadezawacki/cadeloop/releases) also
carries a second Windows wheel compiled for the **x86-64-v3** microarchitecture
level (AVX2, BMI1/BMI2, FMA, F16C). It is deliberately **not on PyPI**: it has no
runtime CPU check, so on a pre-Haswell (2013) / pre-Zen-1 (2017) processor it
does not degrade gracefully — it dies with an illegal-instruction fault on
import. Install it by explicit URL only, and only when you control the hardware:

```bash
pip install https://github.com/cadezawacki/cadeloop/releases/download/<tag>/<v3-wheel>
```

### Verifying an install

```bash
python -c "import cadeloop; l = cadeloop.new_event_loop(); print(cadeloop.__version__, l.stats()['backend']); l.close()"
# 0.0.1 iocp     (Windows)
# 0.0.1 epoll-dev (Linux)
```

---

## Quick start

### Speed up code you already have

```python
import asyncio, cadeloop

cadeloop.install()                 # process-wide policy swap

async def main():
    reader, writer = await asyncio.open_connection("example.org", 80)
    writer.write(b"GET / HTTP/1.0\r\nHost: example.org\r\n\r\n")
    await writer.drain()
    print(await reader.read())
    writer.close()

asyncio.run(main())                # runs on cadeloop
```

### Run an existing ASGI app faster

```bash
cadeloop myapp:app --port 8000           # drop uvicorn entirely
```

Or keep uvicorn and swap only the loop underneath it — see
[Keeping uvicorn, gaining the loop](#keeping-uvicorn-gaining-the-loop).

### Serve from Python

```python
import cadeloop
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

async def hello(request):
    return JSONResponse({"hello": "world"})

app = Starlette(routes=[Route("/", hello)])

if __name__ == "__main__":
    cadeloop.serve(app, "0.0.0.0", 8000, workers=4)
```

---

## Benchmarks

> **Read this first.** Every number below is **Linux over loopback on a
> 4-vCPU box**, with the load generator sharing those cores with the server.
> That makes absolute throughput conservative and the results useful for
> *relative* comparison only. Linux also runs cadeloop's **`epoll` dev
> backend** — the production target is Windows/IOCP, whose numbers live in
> [docs/README.md](docs/README.md) and are not re-run here.
>
> Environment: Linux 6.18.5, 4 vCPU Intel Xeon @ 2.80 GHz, CPython 3.11.15,
> glibc 2.39. Contenders: uvloop 0.22.1, rloop 0.3.1, rsloop 0.1.36,
> uvicorn 0.52.3 (h11 and httptools), granian 2.8.1, hypercorn 0.18.

### HTTP / ASGI — requests per second

Load generator: **`wrk`** (C, its own event loop), `-t2 -c64`, 3s warmup +
3 measured 10s runs, median reported. Single worker for every contender.
Workload: the plaintext `Hello, World!` ASGI app in
[`bench/http/app.py`](bench/http/app.py).

| server | HTTP parsing | event loop | req/s | p50 | p99 |
|---|---|---|---:|---:|---:|
| **cadeloop** (`serve()`) | **Rust** | cadeloop | **104.7 K** | **0.54 ms** | **1.36 ms** |
| granian | Rust (hyper) | its own | 68.1 K | 0.89 ms | 1.99 ms |
| uvicorn + httptools | C | uvloop | 43.7 K | 1.34 ms | 2.91 ms |
| uvicorn + httptools | C | asyncio | 25.5 K | 2.37 ms | 4.20 ms |
| uvicorn + h11 | Python | **cadeloop** | 10.0 K | 6.00 ms | 9.69 ms |
| uvicorn + h11 | Python | uvloop | 9.3 K | 6.34 ms | 13.46 ms |
| uvicorn + h11 | Python | asyncio | 6.6 K | 9.50 ms | 11.94 ms |
| uvicorn + h11 | Python | rsloop | 6.4 K | 9.82 ms | 14.39 ms |
| uvicorn + h11 | Python | rloop | *crashed — see below* | | |
| hypercorn | Python | asyncio | 4.1 K | 15.01 ms | 20.58 ms |

Relative to `cadeloop.serve()`: **1.5× granian**, **2.4× a tuned uvicorn**
(httptools + uvloop), **4.1× uvicorn + httptools on stdlib asyncio**, and
**15.8× uvicorn + h11 on stdlib asyncio**.

**Both layers matter, and they are worth very different amounts.**

*Swapping the loop is real but bounded.* Holding uvicorn+h11 fixed, cadeloop
beats stdlib asyncio by **1.51×** (10.0 K vs 6.6 K) and edges uvloop by 1.07×.
With the faster C parser the loop matters more, not less: httptools on uvloop
is **1.71×** the same stack on asyncio.

*Moving the protocol out of Python is worth more.* `cadeloop.serve()` is
**10.5× uvicorn-on-cadeloop** — same loop, same machine, same app. The
difference is entirely that h11 is no longer parsing in Python.

So the honest opponents for `serve()` are not the h11 rows; they are
**granian** (1.5×) and **uvicorn + httptools + uvloop** (2.4×) — the stacks
that already moved parsing out of Python.

> **rloop 0.3.1 crashed under this benchmark.** It served ~7.1 K req/s for two
> consecutive 3s runs and then aborted the process on a Rust panic —
> `called Option::unwrap() on a None value` at `src/event_loop.rs:417:44` —
> so it could not complete the 3×10s protocol. Reported as measured rather
> than dropped; this is an upstream bug, not a harness failure, and its
> scheduling numbers below were collected before load was applied.

### Scheduling core — millions of ops/second

In-process microbenchmarks: no sockets, no external load generator. 3 warmup
+ 5 measured runs, fresh process per run, medians reported.

| benchmark | cadeloop | asyncio | uvloop | rloop | rsloop | vs asyncio | vs uvloop |
|---|---:|---:|---:|---:|---:|---:|---:|
| `call_soon_burst` | **2.492** | 0.698 | 0.901 | 2.303 | 1.334 | 3.6× | 2.8× |
| `call_soon_chain` | 3.213 | 0.515 | 1.376 | 3.807 | **4.086** | 6.2× | 2.3× |
| `future_chain` | 0.855 | 0.227 | 0.499 | **0.924** | 0.804 | 3.8× | 1.7× |
| `gather_fanin` | **0.257** | 0.160 | 0.232 | 0.240 | 0.229 | 1.6× | 1.1× |
| `queue_pingpong` | **1.163** | 1.013 | 1.129 | 1.144 | 1.084 | 1.1× | 1.0× |
| `sleep0_chain` | 1.269 | 0.393 | 0.744 | **1.424** | 1.361 | 3.2× | 1.7× |
| `task_spawn` | **0.261** | 0.195 | 0.238 | 0.245 | 0.252 | 1.3× | 1.1× |
| `threadsafe_throughput` | 3.123 | 0.142 | 1.542 | **3.939** | 2.659 | 22.0× | 2.0× |
| `timer_fire` | **1.547** | 0.248 | 0.980 | 1.279 | 0.660 | 6.2× | 1.6× |
| `timer_schedule_cancel` | **1.836** | 0.467 | 0.386 | 1.343 | 0.995 | 3.9× | 4.8× |

cadeloop beats stdlib asyncio and uvloop on all ten. Against the other Rust
loops it is more even — **fastest on 6 of 10**, with rloop ahead on
`call_soon_chain`, `future_chain`, `sleep0_chain` and `threadsafe_throughput`,
and rsloop ahead on `call_soon_chain`. Treat these as a profile of where the
scheduling core is strong, not as a ranking.

### Reproducing these

```bash
pip install uvloop rloop rsloop "uvicorn[standard]" hypercorn granian
sudo apt-get install wrk

# HTTP/ASGI (wrk-driven)
PYTHONPATH=$PWD/python python bench/http/run_wrk.py

# scheduling core
PYTHONPATH=$PWD/python python bench/harness/harness.py --suite sched \
    --loops cadeloop,asyncio,uvloop,rloop,rsloop
```

### Two measurement traps these numbers avoid

Both were found while producing this table, and both had silently flattened an
earlier draft of it into a near-tie.

**1. The load generator must not be the bottleneck.** The repo also ships
`harness.py --suite http`, whose client is 64 Python threads in one process.
Splitting the same offered load across two client *processes* nearly doubles
the measured total against an unchanged server — 23.3 K → 43.9 K req/s — which
is a GIL ceiling in the client, not a limit in the server. Under that
generator cadeloop, granian and uvicorn+httptools all report 20–25 K req/s and
look interchangeable; under `wrk` they separate by 4×.
[`bench/http/run_wrk.py`](bench/http/run_wrk.py) exists for this reason.

**2. Asking uvicorn for a loop does not get you that loop.** Since 0.35
uvicorn selects its loop *by class* through a loop-factory table:
`--loop asyncio` returns `SelectorEventLoop` no matter what
`asyncio.set_event_loop_policy()` was set to. Benchmarking a policy swap that
way measures the stdlib loop under someone else's label — it is what made
every `uvicorn + h11` row land on the same 6.5 K, and it hid a genuine 1.7×
between uvloop and asyncio on the httptools rows. The harness now passes
`loop="none"` and drives `uvicorn.Server.serve()` from a loop it constructed
itself.

The TCP echo suite (`--suite echo`) still has the trap-1 client ceiling
(28.5 K → 47.2 K msg/s across two processes), so its numbers are omitted here
rather than published as a loop comparison.

---

## Usage guide

cadeloop has three entry points, and they stack: a **loop**, a **server** built
on that loop, and a **CLI** wrapping that server.

### 1. As a drop-in event loop

There are four ways in, depending on how much of the process you want to claim.

#### `cadeloop.install()` — process-wide

Sets cadeloop as the asyncio policy for the whole process. Everything that later
calls `asyncio.run()`, `asyncio.new_event_loop()`, or `get_event_loop()` gets a
cadeloop loop. This is the uvloop/winloop convention.

```python
import asyncio, cadeloop

cadeloop.install()
asyncio.run(main())
```

Call it **once, at startup, before any loop is created**. Libraries that
constructed a loop earlier keep the one they have.

#### `cadeloop.run(coro, *, debug=None)` — one call, no global state

Like `asyncio.run()`, but always uses a cadeloop loop regardless of the installed
policy. Use this when you do not want to change process-wide behaviour — in a
library, a test, or an app that shares a process with something else.

| argument | type | default | meaning |
|---|---|---|---|
| `main` | coroutine | *required* | The coroutine to run to completion. |
| `debug` | `bool \| None` | `None` | Enables asyncio debug mode. `None` leaves the loop's own default (which honours `PYTHONASYNCIODEBUG` and `-X dev`). |

```python
import cadeloop

result = cadeloop.run(main())
cadeloop.run(main(), debug=True)     # slow-callback warnings, origin tracking
```

#### `cadeloop.new_event_loop()` — an explicit loop object

Returns a `Loop` with default settings. You own its lifecycle.

```python
import cadeloop

loop = cadeloop.new_event_loop()
try:
    loop.run_until_complete(main())
finally:
    loop.close()
```

#### `cadeloop.EventLoopPolicy()` — for frameworks that want a policy

```python
import asyncio, cadeloop

asyncio.set_event_loop_policy(cadeloop.EventLoopPolicy())
```

The policy subclasses the *platform default* policy rather than the abstract
base, so POSIX keeps its child-watcher machinery and
`asyncio.create_subprocess_exec()` keeps working after the swap.

#### Tuning the loop directly

For a tuned loop, construct `Loop` yourself — see
[`Loop()` — every constructor argument](#loop--every-constructor-argument):

```python
import asyncio, cadeloop

def loop_factory():
    return cadeloop.Loop(spin_us=200, dns_cache=True)

with asyncio.Runner(loop_factory=loop_factory) as runner:
    runner.run(main())
```

---

### 2. As an ASGI server (`cadeloop.serve`)

`serve()` runs an ASGI 3.0 application on the native engine: HTTP parsing (llhttp
in Rust), scope construction, and response serialization never enter Python.

```python
import cadeloop

async def app(scope, receive, send):
    if scope["type"] != "http":
        return
    await receive()
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [(b"content-type", b"text/plain")],
    })
    await send({"type": "http.response.body", "body": b"Hello, World!"})

cadeloop.serve(app, "0.0.0.0", 8000)
```

`serve()` **blocks** until the server stops — via `SIGINT`/`SIGTERM`, or
`loop.stop()` from inside a handler.

The `app` argument may be a callable or a `"module:attribute"` string. Pass the
**string** when using `workers > 1` on Windows: the fork-free worker model
re-imports the app in each child, and a resolved callable cannot cross that
boundary.

```python
cadeloop.serve("myapp:app", "0.0.0.0", 8000, workers=4)
```

#### TLS

```python
import ssl, cadeloop

ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ctx.load_cert_chain("cert.pem", "key.pem")

cadeloop.serve(app, "0.0.0.0", 8443, ssl=ctx)
```

TLS is terminated natively in Rust (a memory-BIO `wrap_bio` path), so encrypted
traffic keeps the same zero-Python parsing path. `serve()` forces the context's
ALPN list to `["http/1.1"]` — the engine speaks HTTP/1.1 only, and a context
advertising `h2` would let a client negotiate a protocol the server then rejects
on every request.

#### WebSockets

WebSockets (RFC 6455) are handled by the same native engine; your app receives
`websocket` scopes as usual.

```python
async def app(scope, receive, send):
    if scope["type"] == "websocket":
        await receive()                                   # websocket.connect
        await send({"type": "websocket.accept"})
        while True:
            msg = await receive()
            if msg["type"] == "websocket.disconnect":
                break
            await send({"type": "websocket.send", "text": msg["text"]})
```

#### Lifespan

The ASGI lifespan protocol is supported and detected automatically. An app
without lifespan support is served normally.

```python
async def app(scope, receive, send):
    if scope["type"] == "lifespan":
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await pool.connect()
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await pool.close()
                await send({"type": "lifespan.shutdown.complete"})
                return
```

On shutdown, a quarter of the grace budget (capped at 5s) is reserved for
`lifespan.shutdown` so connection draining cannot consume the whole window and
leave shutdown hooks unrun.

#### Multiple workers

```python
cadeloop.serve("myapp:app", "0.0.0.0", 8000, workers=0)   # 0 = one per CPU
```

Two process models, chosen automatically:

- **POSIX** — `fork` + `SO_REUSEPORT`; each worker has its own listener and the
  kernel balances accepts.
- **Windows** — `spawn` with a shared listener handed to children via
  `WSADuplicateSocketW`.

Workers are supervised and restarted, with a fast-crash cutoff so a worker that
dies immediately and repeatedly does not restart forever. `pin=True` (the
default) pins each worker to a core.

---

### 3. From the command line

```bash
cadeloop myapp:app --host 0.0.0.0 --port 8000 --workers 4
python -m cadeloop myapp:app --port 8000        # equivalent
```

Every `Config` field is exposed as a flag — see the
[CLI flag reference](#cli-flag-reference).

```bash
# low-latency single worker, no body cap, stats on 9001
cadeloop myapp:app --latency-mode spin --max-body none --stats-endpoint 9001

# throughput-tuned pool with a short keep-alive
cadeloop myapp:app -w 8 --latency-mode throughput --keepalive-idle 15
```

---

### `Loop()` — every constructor argument

`cadeloop.Loop(**kwargs)` builds a loop directly. All arguments are
**keyword-only**.

| argument | type | default | what it does |
|---|---|---|---|
| `backend` | `str \| None` | `None` → `CADELOOP_BACKEND` env, else `"auto"` | Reactor to use: `"auto"`, `"iocp"`, `"rio"`, `"epoll"`. `"auto"` picks IOCP on Windows and epoll on Linux. `"rio"` is experimental and warns. |
| `spin_us` | `int` | `20` | Microseconds to spin before making a blocking kernel wait. Higher values trade CPU for lower wake-up latency. `0` disables spinning. |
| `high_water` | `int` | `65536` | Transport write buffer high-water mark, in bytes. Above it, `transport.is_writing_paused()` becomes true and protocols get `pause_writing()`. |
| `low_water` | `int` | `16384` | Write buffer low-water mark. Writing resumes below it. Must be `<= high_water`. |
| `accept_pool` | `int` | `64` | Accept operations posted concurrently per listener. Raise it for connection-storm workloads; each slot costs a pending kernel op. |
| `rio_cq_size` | `int` | `65536` | Windows RIO completion queue size. Ignored on other backends. |
| `rio_rq_recv` | `int` | `32` | RIO per-socket receive request queue depth. |
| `rio_rq_send` | `int` | `32` | RIO per-socket send request queue depth. |
| `dns_cache` | `bool` | `False` | Cache `getaddrinfo` results. Off by default here to match the `AbstractEventLoop` contract — real `asyncio.getaddrinfo` never caches. (`Config`/`serve()` default it **on**.) |
| `dns_cache_ttl` | `float` | `5.0` | Seconds a cached DNS answer lives. RFC TTLs from the resolver are **ignored**. |
| `tfo` | `bool` | `False` | Enable TCP Fast Open on listeners. |
| `loopback_fast_path` | `bool` | `True` | Windows `SIO_LOOPBACK_FAST_PATH`. Relevant to loopback benchmarks; no effect on real network traffic. |

```python
loop = cadeloop.Loop(
    spin_us=200,          # latency over CPU
    accept_pool=256,      # heavy accept churn
    high_water=1 << 20,   # 1 MiB before backpressure
    low_water=1 << 18,
    dns_cache=True,
)
```

Beyond these, a `Loop` is an ordinary `asyncio` loop:
`run_forever`, `run_until_complete`, `call_soon`, `call_later`, `call_at`,
`call_soon_threadsafe`, `create_task`, `create_future`, `run_in_executor`,
`set_default_executor`, `add_reader`/`add_writer`, `sock_*`,
`add_signal_handler`, `create_server`, `create_connection`,
`create_datagram_endpoint`, `create_unix_server`, `subprocess_exec`,
`set_exception_handler`, `set_task_factory`, `sendfile`, `start_tls`,
`shutdown_asyncgens`, `shutdown_default_executor`, and `close`.

Two attributes worth knowing:

| attribute | meaning |
|---|---|
| `loop.slow_callback_duration` | Seconds a callback may run before debug mode logs it. Default `0.1`. Settable; the native dispatcher honours it. |
| `loop.stats()` | Live counters — see [`loop.stats()`](#loopstats--introspection). |

---

### `Config` — every tunable

`cadeloop.Config` is a validated dataclass holding every server tunable.
Validation is **eager**: bad values raise `ValueError` at construction, and
unknown keyword arguments raise `TypeError`.

```python
from cadeloop import Config

cfg = Config(latency_mode="spin", workers=4, max_body=None)
cfg = Config.from_env()                       # reads CADELOOP_* variables
cadeloop.serve(app, "0.0.0.0", 8000, **vars(cfg))
```

#### Loop and reactor

| field | type | default | meaning |
|---|---|---|---|
| `backend` | `str` | `"auto"` | `"auto"`, `"iocp"`, `"rio"`, `"epoll"`. `"rio"` additionally requires `CADELOOP_ALLOW_EXPERIMENTAL_RIO=1`. |
| `latency_mode` | `str` | `"balanced"` | Preset controlling spin and flush behaviour: `"throughput"` (spin 0µs), `"balanced"` (20µs), `"spin"` (200µs + immediate flush). |
| `spin_us` | `int \| None` | `None` | Explicit spin window in µs, overriding the preset. `None` derives it from `latency_mode`. |
| `immediate_flush` | `bool \| None` | `None` | Put each response on the wire as soon as it is ready instead of corking until the tick's flush phase. Costs syscalls, buys tail latency. `None` derives from `latency_mode` (on only for `"spin"`). |

#### Kernel I/O

| field | type | default | meaning |
|---|---|---|---|
| `accept_pool` | `int` | `64` | Concurrent accept operations per listener. Must be ≥ 1. |
| `rio_cq_size` | `int` | `65536` | Windows RIO completion queue size. Must be ≥ 1. |
| `rio_rq_recv` | `int` | `32` | RIO receive queue depth. Must be ≥ 1. |
| `rio_rq_send` | `int` | `32` | RIO send queue depth. Must be ≥ 1. |
| `loopback_fast_path` | `bool` | `True` | Windows `SIO_LOOPBACK_FAST_PATH`; benchmark-relevant only. |
| `tfo` | `bool` | `False` | TCP Fast Open on listeners. |

#### DNS

| field | type | default | meaning |
|---|---|---|---|
| `dns_cache` | `bool` | `True` | Cache resolver answers. On by default for servers (a deliberate tradeoff), unlike bare `Loop()`. |
| `dns_cache_ttl` | `float` | `5.0` | Cache lifetime in seconds. Must be finite and ≥ 0. RFC TTLs are ignored. |

#### Tasks and GC

| field | type | default | meaning |
|---|---|---|---|
| `eager_tasks` | `bool` | `True` | Start tasks eagerly (run synchronously until first suspension). Turn off if a library depends on deferred-start semantics. |
| `gc_mode` | `str` | `"freeze"` | `"default"`, `"freeze"` (call `gc.freeze()` after warmup so startup objects stop being traced), or `"disable"`. |
| `warmup` | `int` | `1000` | Requests served before `gc.freeze()` runs. Must be ≥ 0. |

#### HTTP engine

| field | type | default | meaning |
|---|---|---|---|
| `max_header_bytes` | `int` | `65536` | Maximum total header bytes per request. Over it → `431`. Must be ≥ 1. |
| `max_headers` | `int` | `100` | Maximum header count per request. Must be ≥ 1. |
| `max_url` | `int` | `8192` | Maximum request-target length. Must be ≥ 1. |
| `request_line_timeout` | `float` | `5.0` | Seconds a connection may stay silent before sending a complete request head. Guards slowloris. `0` disables. |
| `keepalive_idle` | `float` | `75.0` | Seconds an idle keep-alive connection is held open. |
| `max_body` | `int \| None` | `16777216` | Maximum request body bytes; over it → `413`. **Finite on purpose**: the engine buffers the whole body before dispatch, so `None` (unlimited) lets an unauthenticated client turn one request into unbounded memory. |
| `reuse_scope` | `bool` | `False` | Reuse the ASGI scope dict between requests on a connection. Faster, but unsafe for apps that retain the scope past the response. |

#### Transports

| field | type | default | meaning |
|---|---|---|---|
| `write_high_water` | `int` | `65536` | Backpressure high-water mark in bytes. Must be ≥ 0. |
| `write_low_water` | `int` | `16384` | Backpressure low-water mark. Must be ≥ 0 and ≤ `write_high_water`. |

#### Multi-process

| field | type | default | meaning |
|---|---|---|---|
| `workers` | `int` | `0` | Worker processes. `0` means one per physical core. Must be ≥ 0. |
| `pin` | `bool` | `True` | Pin each worker to a CPU core. |
| `grace` | `float` | `10.0` | Graceful-drain budget in seconds on shutdown. Must be finite and ≥ 0. |

#### Observability

| field | type | default | meaning |
|---|---|---|---|
| `access_log` | `bool` | `False` | Emit a per-request line on the `cadeloop.access` logger. Nothing appears until logging is configured at `INFO` — `logging.basicConfig(level=logging.INFO)` — because the root logger defaults to `WARNING`. |
| `stats_endpoint` | `int \| None` | `None` | Serve `loop.stats()` as JSON on `127.0.0.1:<port>`. Bound by one worker only; the payload names which. Must be `None` or a port in 1–65535. |

#### `Config.from_env(prefix="CADELOOP_")`

Reads `{prefix}{FIELD_NAME_UPPERCASED}` for every field, with typed parsing.

| field type | accepted values |
|---|---|
| `bool` | `1/true/yes/on` and `0/false/no/off` (case-insensitive) |
| `int`, `float` | any literal Python accepts |
| optional (`int \| None`) | `none` or the empty string → `None` |

```bash
export CADELOOP_LATENCY_MODE=spin
export CADELOOP_WORKERS=8
export CADELOOP_MAX_BODY=none
export CADELOOP_ACCESS_LOG=true
```

```python
cfg = Config.from_env()                 # or Config.from_env(prefix="MYAPP_")
```

---

### `serve()` — every argument

```python
cadeloop.serve(app, host="127.0.0.1", port=8000, *,
               workers=1, backend="auto", ssl=None,
               latency_mode="balanced", access_log=False, **cfg)
```

| argument | type | default | meaning |
|---|---|---|---|
| `app` | callable or `str` | *required* | An ASGI 3.0 application, or a `"module:attribute"` spec. Use the string form with `workers > 1` on Windows. |
| `host` | `str` | `"127.0.0.1"` | Bind address. Use `"0.0.0.0"` to accept external traffic. |
| `port` | `int` | `8000` | Bind port. |
| `workers` | `int` | `1` | Worker processes; `0` means one per CPU. |
| `backend` | `str` | `"auto"` | Reactor backend, as in `Config.backend`. |
| `ssl` | `ssl.SSLContext \| None` | `None` | Enables TLS. Must be an `SSLContext` — anything else raises `TypeError`. Its ALPN list is forced to `["http/1.1"]`. |
| `latency_mode` | `str` | `"balanced"` | `"throughput"`, `"balanced"`, or `"spin"`. |
| `access_log` | `bool` | `False` | Per-request logging. |
| `**cfg` | | | **Any other `Config` field.** Unknown names raise `TypeError`; invalid values raise `ValueError`. |

Because `**cfg` forwards to `Config`, every tunable in the tables above is
available here:

```python
cadeloop.serve(
    "myapp:app", "0.0.0.0", 8443,
    workers=0,
    ssl=ctx,
    latency_mode="spin",
    max_body=64 * 1024 * 1024,      # 64 MiB uploads
    keepalive_idle=30.0,
    request_line_timeout=3.0,
    stats_endpoint=9001,
    gc_mode="freeze",
    access_log=True,
)
```

---

### CLI flag reference

```
cadeloop APP [options]
python -m cadeloop APP [options]
```

`APP` is a required `module:attribute` spec. It is validated (imported) before
anything binds, so a typo fails immediately rather than after the port is taken.

| flag | maps to | notes |
|---|---|---|
| `--host HOST` | — | Bind address. |
| `--port`, `-p PORT` | — | Bind port. |
| `--workers`, `-w N` | `workers` | `0` = one per CPU. |
| `--backend {auto,epoll}` | `backend` | Choices are platform-dependent (`iocp` on Windows). |
| `--latency-mode {throughput,balanced,spin}` | `latency_mode` | |
| `--access-log` | `access_log` | |
| `--spin-us N` | `spin_us` | Accepts `none` to derive from `--latency-mode`. |
| `--immediate-flush` / `--no-immediate-flush` | `immediate_flush` | |
| `--accept-pool N` | `accept_pool` | |
| `--rio-cq-size N`, `--rio-rq-recv N`, `--rio-rq-send N` | RIO fields | Windows RIO only. |
| `--loopback-fast-path` / `--no-loopback-fast-path` | `loopback_fast_path` | |
| `--tfo` / `--no-tfo` | `tfo` | |
| `--dns-cache` / `--no-dns-cache` | `dns_cache` | |
| `--dns-cache-ttl S` | `dns_cache_ttl` | |
| `--eager-tasks` / `--no-eager-tasks` | `eager_tasks` | |
| `--gc-mode {default,freeze,disable}` | `gc_mode` | |
| `--warmup N` | `warmup` | |
| `--max-header-bytes N` | `max_header_bytes` | |
| `--max-headers N` | `max_headers` | |
| `--max-url N` | `max_url` | |
| `--request-line-timeout S` | `request_line_timeout` | |
| `--keepalive-idle S` | `keepalive_idle` | |
| `--max-body N` | `max_body` | Accepts `none` to remove the cap. |
| `--reuse-scope` / `--no-reuse-scope` | `reuse_scope` | |
| `--write-high-water N`, `--write-low-water N` | watermarks | |
| `--pin` / `--no-pin` | `pin` | |
| `--grace S` | `grace` | |
| `--stats-endpoint PORT` | `stats_endpoint` | Accepts `none` to disable. |

Every boolean tunable has a paired `--no-` form, and the three
`int | None` options (`--spin-us`, `--max-body`, `--stats-endpoint`) accept the
literal `none` — without it, those states would be unreachable from the shell.

---

### Environment variables

| variable | read by | effect |
|---|---|---|
| `CADELOOP_<FIELD>` | `Config.from_env()` | Sets the matching `Config` field. |
| `CADELOOP_BACKEND` | `Loop()` | Default backend when the `backend` argument is `None`. Lets a whole test or benchmark run target one backend. |
| `CADELOOP_ALLOW_EXPERIMENTAL_RIO` | `Config` | Required to set `backend="rio"` through `Config`/`serve()`. `Loop(backend="rio")` stays reachable without it, for RIO diagnosis. |
| `PYTHONASYNCIODEBUG` | `Loop()` | Enables asyncio debug mode, as with the stdlib loop. `-X dev` does the same. |

---

### `loop.stats()` — introspection

`loop.stats()` returns a plain dict of live counters. It is cheap enough to poll
and is the intended way to answer "what is this loop actually doing".

```python
loop = cadeloop.new_event_loop()
print(loop.stats())
```

| key | meaning |
|---|---|
| `backend` | Active reactor: `"iocp"`, `"rio"`, `"epoll-dev"`. |
| `ticks` | Loop iterations completed. |
| `polls` | Kernel wait calls made. |
| `completions` | I/O completions reaped. |
| `callbacks_dispatched` | Callbacks run. |
| `timers_fired` | Timers that fired. |
| `xthread_items` | Items delivered via `call_soon_threadsafe`. |
| `spin_hits` | Times the spin window found work without a blocking wait. |
| `ready_len`, `timers_len` | Current ready-queue and timer-heap depth. |
| `connections`, `listeners` | Live counts. |
| `buffers_in_use` | Buffer slots currently held by kernel ops or exported memoryviews. |
| `bytes_received`, `bytes_sent` | Cumulative byte counters. |
| `connections_accepted` | Cumulative accepts. |
| `accept_starved` | Times a listener's accept pool ran dry — raise `accept_pool` if this climbs. |
| `pipeline_pauses` | Times a pipelined read was paused for budget. |
| `sends_posted` | Send operations posted. Read against `bytes_sent`, this shows what write corking is buying you. |
| `accept_ops` | Accept operations posted. |
| `stale_buffer_ids`, `unreaped_ops` | Leak indicators; both should stay at zero. |
| `ops_by_target` | Per-target op breakdown: `recv`, `send`, `accept`, `connect`, `dgram`, `pipe`. A stuck loop looks healthy on every other counter; this is the one that says where. |

Serve it over HTTP with `--stats-endpoint`:

```bash
cadeloop myapp:app --stats-endpoint 9001 &
curl -s localhost:9001 | python -m json.tool
```

---

## Use cases and recipes

### Python services on Windows

The original reason cadeloop exists. uvloop does not support Windows, so the
stdlib Proactor loop has been the ceiling. cadeloop uses IOCP directly and gives
Windows deployments the same class of speedup Linux users get from uvloop —
plus a native ASGI server on top.

```bash
cadeloop myapp:app --host 0.0.0.0 --port 8000 --workers 0
```

### Latency-sensitive services

Spend CPU to avoid kernel wait wake-ups, and flush responses as soon as they are
ready rather than at tick end:

```python
cadeloop.serve(app, "0.0.0.0", 8000, latency_mode="spin")
```

```python
cadeloop.serve(app, "0.0.0.0", 8000, spin_us=500, immediate_flush=True)
```

This burns a core busy-waiting. Measure it: the tradeoff is real and
deployment-specific, and on a small VM the run-to-run p99 spread can exceed the
difference between modes.

### Throughput-oriented services

Let writes cork and batch, and skip the spin window entirely:

```bash
cadeloop myapp:app -w 0 --latency-mode throughput --keepalive-idle 120
```

### Large uploads

The body cap is finite by default because the engine buffers a whole request body
before dispatch. Raise it deliberately, and pair it with a longer head timeout:

```python
cadeloop.serve(app, "0.0.0.0", 8000,
               max_body=512 * 1024 * 1024,
               request_line_timeout=30.0)
```

Setting `max_body=None` removes the cap entirely — only do that behind
authentication or a proxy that enforces its own limit.

### Keeping uvicorn, gaining the loop

If you depend on uvicorn's features (lifespan flavours, `--reload`, its logging),
you can keep it and replace only the loop underneath.

**There is no `--loop cadeloop` flag**, and installing the policy is not enough
either: modern uvicorn (0.35+) picks its loop *by class* through a loop-factory
table — `--loop asyncio` hands back `SelectorEventLoop`/`ProactorEventLoop`
regardless of `asyncio.set_event_loop_policy()`. Setting a policy and passing
`--loop asyncio` silently runs the stdlib loop.

Tell uvicorn to build no loop at all, and drive it from one you made:

```python
import cadeloop, uvicorn

config = uvicorn.Config("myapp:app", host="0.0.0.0", port=8000, loop="none")
server = uvicorn.Server(config)

loop = cadeloop.new_event_loop()
try:
    loop.run_until_complete(server.serve())
finally:
    loop.close()
```

You get the loop and transport speedup while HTTP parsing stays in Python — and
as the [benchmarks](#benchmarks) show, that second half is where the time
actually goes.

### Hardening a public listener

```python
import logging
logging.basicConfig(level=logging.INFO)   # or access_log writes nowhere

cadeloop.serve(
    app, "0.0.0.0", 8000,
    max_body=2 * 1024 * 1024,        # small bodies only
    max_header_bytes=16 * 1024,
    max_headers=50,
    max_url=2048,
    request_line_timeout=3.0,        # slowloris guard
    keepalive_idle=15.0,
    access_log=True,
)
```

```
cadeloop.access 127.0.0.1:55888 "GET /" 200 0.42ms
```

### Graceful shutdown behind an orchestrator

```bash
cadeloop myapp:app -w 4 --grace 30
```

`SIGTERM` starts a drain: existing connections finish, new ones stop being
accepted, and a reserved slice of the budget guarantees `lifespan.shutdown` runs.

### Running alongside another loop

Use `cadeloop.run()` instead of `install()` so nothing else in the process
changes behaviour:

```python
import cadeloop

def run_worker(coro):
    return cadeloop.run(coro)         # this call only
```

### Watching a live server

```python
import json, cadeloop

async def stats(scope, receive, send):
    body = json.dumps(loop.stats()).encode()
    await send({"type": "http.response.start", "status": 200,
                "headers": [(b"content-type", b"application/json")]})
    await send({"type": "http.response.body", "body": body})
```

Or skip the code and use `--stats-endpoint 9001`.

---

## Compatibility

| surface | state |
|---|---|
| Scheduling: `call_soon`, `call_later`, `call_at`, timers, threadsafe, tasks | ✅ tested |
| TCP transports, `create_server` / `create_connection`, streams | ✅ tested |
| TLS: native termination (`serve(ssl=...)`, https/wss), client `ssl=` / `start_tls` | ✅ tested |
| `sock_*`, `add_reader` / `add_writer`, signals | ✅ tested |
| UDP (`create_datagram_endpoint`) | ✅ tested |
| WebSockets (RFC 6455, native engine) | ✅ tested |
| Native HTTP/1.1 + ASGI engine, lifespan, CLI | ✅ tested (Starlette, FastAPI) |
| Multi-worker (`workers > 1`) | ✅ tested (fork + `SO_REUSEPORT`; spawn + shared listener on Windows) |
| Subprocess (`create_subprocess_exec` / `shell`) | ✅ POSIX; Windows pipes in progress |
| Drop-in with uvicorn, aiohttp | ✅ interop-tested |
| CPython asyncio conformance suite | ✅ runs against the stdlib's own tests |
| RIO backend (`backend="rio"`) | 🔶 implemented; blocked on an OS-level RIO failure on the test machine — `auto` stays on IOCP |
| Native `loop.sendfile` | 🔶 `sock_sendfile` fallback works |

Full requirement-by-requirement map:
[docs/requirements-traceability.md](docs/requirements-traceability.md).

---

## Architecture

```
L4  Python user code / ASGI app
L3  python/cadeloop — Loop facade, policy, Config, CLI       [Python]
L2  crates/pyshim   — transports, listeners, bindings        [Rust]
L1  crates/core     — reactor: timers, queues, dispatch      [Rust]
L0  crates/core     — IOCP | RIO (hybrid) | epoll (Linux)    [Rust]
```

- **One completion-style op API over both kernels** — IOCP natively; epoll
  wrapped as a proactor (syscall attempted at post time, parked only on
  `EWOULDBLOCK`), so a single Rust transport layer serves both.
- **One thread, one conditional GIL release per tick.** The kernel poll drops the
  GIL only when it can actually block; completions dispatch in batches via
  vectorcall, with per-connection protocol callbacks cached as bound methods.
- **Cancel-safe by construction.** Pinned op slabs with a
  `{Free, Posted, Completed, Cancelled}` state machine (property-tested), buffer
  slots refcounted by kernel ops and memoryview exports alike, and a graveyard
  protocol so no Python object is dropped — and no `__del__` can re-enter —
  inside the loop's critical section.
- **Corked gather writes.** Writes coalesce within a tick into ≤16-slice
  `writev`/`WSASend` calls, flushing at tick end, at 64 KiB, or on drain; `bytes`
  payloads are retained zero-copy.

Details in [docs/architecture.md](docs/architecture.md); design decisions in
[docs/decisions.md](docs/decisions.md).

---

## Development

```bash
cargo test --workspace                                    # Rust core
cargo check -p cadeloop-core --target x86_64-pc-windows-msvc

cargo build -p cadeloop-pyshim --release                  # extension
cp target/release/lib_core.so python/cadeloop/_core.so    # Linux dev shortcut
pip install pytest pytest-timeout uvicorn aiohttp trustme starlette fastapi
PYTHONPATH=python pytest tests/unit tests/conformance

pip install maturin && maturin build --release            # the real wheel
python tests/conformance/run_cpython_suite.py             # CPython asyncio suite
```

Repo layout: `crates/core`, `crates/pyshim`, `python/cadeloop`, `vendor/llhttp`,
`tests/{unit,conformance,stress}`, `bench/{echo,http,sched,harness}`, `docs/`.

Release and packaging process: [docs/ops.md](docs/ops.md).

---

## License

MIT — see [LICENSE](LICENSE). No GPL dependencies, enforced by `cargo deny`.
