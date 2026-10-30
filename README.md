# cadeloop

**A fast asyncio event loop and ASGI web server for CPython 3.11, written in Rust.**
Runs on Windows and Linux.

[![Python 3.11](https://img.shields.io/badge/python-3.11-5a9996?style=flat-square&labelColor=0B0D11)](https://pypi.org/project/cadeloop/)
[![CI](https://img.shields.io/github/actions/workflow/status/cadezawacki/cadeloop/ci.yml?branch=main&style=flat-square&label=ci&labelColor=0B0D11&color=5a9996)](https://github.com/cadezawacki/cadeloop/actions/workflows/ci.yml)
[![MIT](https://img.shields.io/badge/license-MIT-5a9996?style=flat-square&labelColor=0B0D11)](LICENSE)
[![PyPI](https://img.shields.io/pypi/v/cadeloop?style=flat-square&label=pypi&labelColor=0B0D11&color=5a9996)](https://pypi.org/project/cadeloop/)
[![Platforms](https://img.shields.io/badge/platform-windows%20x64%20%7C%20linux%20x64-5a9996?style=flat-square&labelColor=0B0D11)](#install)

cadeloop does two jobs, and you can use either one on its own.

**It is an asyncio event loop.** Put it under code you already have, such as
asyncio scripts, aiohttp, websockets clients or uvicorn, and that code runs
on cadeloop instead of the standard library loop. Nothing else in your code
changes.

```python
import asyncio
import cadeloop

cadeloop.install()   # from here on, asyncio runs on cadeloop
asyncio.run(main())
```

**It is an ASGI web server.** `cadeloop.serve()`, or the `cadeloop` command,
runs FastAPI, Starlette and other ASGI apps with the HTTP work done in Rust:
parsing requests, building the `scope` your app receives, and encoding the
response. The only Python left on a request's path is your app.

```bash
cadeloop main:app --host 0.0.0.0 --port 8000
```

On a plaintext benchmark on Windows 11, `cadeloop.serve()` answered 4.3 times
as many requests per second as uvicorn on winloop, at a quarter of the CPU
per request. On Linux it answered 4.8 times as many as `uvicorn[standard]`
(httptools on uvloop) and 2.4 times as many as granian. Details are under
[benchmarks](#benchmarks).

New to event loops? [How cadeloop works](docs/learn/README.md) is a short,
illustrated tour that starts from the basics.

---

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Which part should I use?](#which-part-should-i-use)
- [Using cadeloop with FastAPI](#using-cadeloop-with-fastapi)
- [Benchmarks](#benchmarks)
- [Event loop mode: reference](#event-loop-mode-reference)
- [Server mode: reference](#server-mode-reference)
- [Settings in both modes](#settings-in-both-modes)
- [Recipes](#recipes)
- [How it works](#how-it-works)
- [Development](#development)
- [License](#license)

---

## Install

```bash
pip install cadeloop
```

| | |
|---|---|
| Python | CPython 3.11 |
| OS | Windows 10 or 11, or Linux (glibc 2.17 or newer) |
| CPU | x86-64 |

Wheels are published for Windows x64 and Linux x64 (manylinux2014), with a
source distribution for everything else. To check an install, print the
version and the backend the loop picked:

```bash
python -c "import cadeloop; l = cadeloop.new_event_loop(); print(cadeloop.__version__, l.stats()['backend']); l.close()"
```

It prints `iocp` on Windows and `epoll-dev` on Linux after the version.

---

## Quick start

### Make existing asyncio code faster

Call `cadeloop.install()` once, before anything creates an event loop:

```python
import asyncio
import cadeloop


async def main():
    reader, writer = await asyncio.open_connection("example.org", 80)
    writer.write(b"GET / HTTP/1.0\r\nHost: example.org\r\n\r\n")
    await writer.drain()
    print(await reader.read())
    writer.close()


cadeloop.install()
asyncio.run(main())
```

`asyncio.start_server`, `open_connection`, subprocesses, signal handlers,
`run_in_executor`, `add_reader` and the rest of the asyncio API work as
before, and so do libraries built on them.

### Serve a FastAPI app

```python
# main.py
from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def index():
    return {"hello": "world"}
```

```bash
pip install cadeloop fastapi
cadeloop main:app --port 8000
```

[Using cadeloop with FastAPI](#using-cadeloop-with-fastapi) covers lifespan,
workers and production settings.

### Keep uvicorn, swap only the loop

If you rely on uvicorn's own features, run uvicorn on cadeloop's loop. You
get the faster loop with no other change:

```python
import cadeloop
import uvicorn

config = uvicorn.Config("main:app", host="0.0.0.0", port=8000, loop="none")
server = uvicorn.Server(config)
try:
    cadeloop.run(server.serve())
except KeyboardInterrupt:
    pass  # uvicorn re-raises Ctrl-C once it has shut down cleanly
```

`loop="none"` matters. uvicorn creates its own loop by class, asyncio's or
uvloop's, and ignores the installed event loop policy. So
`cadeloop.install()` followed by a plain `uvicorn main:app` still runs on
asyncio's loop, or on uvloop if it is installed.

---

## Which part should I use?

| you have | use | the change |
|---|---|---|
| asyncio code that isn't a web server: clients, bots, scrapers, queue workers | the event loop | `cadeloop.install()` before `asyncio.run()` |
| an ASGI app on uvicorn, and you want the low-risk first step | the event loop, under uvicorn | the snippet [above](#keep-uvicorn-swap-only-the-loop) |
| an ASGI app, and you want the most requests per CPU | the server | `cadeloop main:app` |

The server speaks HTTP/1.1 and WebSockets. For HTTP/2 or HTTP/3 from
browsers, put a proxy such as nginx or Caddy in front, which is also the
usual way to terminate public TLS. [How cadeloop compares](docs/learn/5-how-it-compares.md)
goes through the alternatives.

---

## Using cadeloop with FastAPI

### A complete app with lifespan

FastAPI's `lifespan` sets up shared resources before the first request and
closes them after the last. cadeloop runs it like any ASGI server does. Here
it opens one HTTP client for the whole app:

```python
# main.py
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: runs once in each worker, before it accepts connections.
    client = httpx.AsyncClient(timeout=5.0)
    yield {"http": client}  # each request sees this as request.state.http
    # Shutdown: runs once in each worker, after the last request.
    await client.aclose()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def index():
    return {"hello": "world"}


@app.get("/items/{item_id}")
async def read_item(item_id: int, q: str | None = None):
    return {"item_id": item_id, "q": q}


@app.get("/status")
async def upstream_status(request: Request):
    response = await request.state.http.get("https://example.org")
    return {"upstream": response.status_code}


@app.websocket("/ws")
async def shout(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            text = await websocket.receive_text()
            await websocket.send_text(text.upper())
    except WebSocketDisconnect:
        pass


if __name__ == "__main__":
    import cadeloop

    cadeloop.serve(app, "0.0.0.0", 8000)
```

Whatever the lifespan function yields as a dict is copied into every
request's `request.state`. If you prefer the older style, assign to
`app.state` in the startup half instead and read `request.app.state` in your
routes; both work.

### Running it

From the command line:

```bash
cadeloop main:app --host 0.0.0.0 --port 8000
```

`python -m cadeloop main:app ...` is the same thing. Or from Python, as at
the bottom of `main.py`:

```bash
python main.py
```

`main:app` is a `module:attribute` string, imported from the current
directory (or from `--app-dir`). It is checked before the server binds its
port, so a typo fails straight away.

### More than one worker

One worker is one process with one thread, and it uses one CPU core. To use
more cores, start more workers:

```bash
cadeloop main:app --host 0.0.0.0 --port 8000 --workers 4
```

```python
cadeloop.serve("main:app", "0.0.0.0", 8000, workers=4)
```

`--workers 0` starts one worker per CPU the process is allowed to use. Each
worker runs your lifespan separately, so each has its own HTTP client,
database pool and in-memory state. Keep anything that must be shared, like
sessions or rate-limit counters, in Redis or your database.

Pass the app as a `"main:app"` string when you use more than one worker. On
Windows it is required: workers there are fresh processes that import your
app themselves. On Linux, workers are forked from a supervisor that imported
the app once, so a string or the app object both work.

### Settings worth knowing for production

```bash
cadeloop main:app --host 0.0.0.0 --port 8000 --workers 0 \
    --max-body 10485760 --keepalive-idle 30 --grace 20 --access-log
```

- `--max-body` caps request bodies (16 MiB by default). The server collects
  each body in memory and hands it to your app in one piece, so this limit is
  also the most memory one upload can take. Larger bodies get a `413`.
- `--keepalive-idle` closes idle keep-alive connections after this many
  seconds (75 by default).
- `--grace` is how long a shutdown waits for requests in flight (10 s by
  default). Part of it is kept for your lifespan's shutdown code.
- `--access-log` logs one line per request to the `cadeloop.access` logger.
  Configure logging at INFO level to see it.

All of these are listed under [server settings](#settings).

### Behind a proxy

The server does not read `X-Forwarded-For` itself. uvicorn's
`ProxyHeadersMiddleware` is plain ASGI middleware, so it works under
cadeloop too:

```python
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

app = ProxyHeadersMiddleware(app, trusted_hosts=["10.0.0.5"])  # your proxy's address
```

### During development

cadeloop has no auto-reload. Keep `uvicorn main:app --reload` while you
write code and run cadeloop where you deploy; the app is the same either way.

---

## Benchmarks

Measured on two machines, each with its own set of contenders: a Windows 11
desktop, and a Linux cloud VM. Both are loopback runs, with the load
generator on the same machine as the server, so they compare servers under
identical conditions; they are not capacity numbers.

In the tables, the best result in each row (or column, for per-server
tables) is **bold on a teal background**. When most entries tie, nothing is
marked. How the runs were made, and every number as text:
[Linux](docs/benchmarks-linux.md), [Windows](docs/benchmarks-windows.md),
[method](docs/benchmarks.md).

- [Windows 11 (IOCP)](#windows-11-iocp)
- [Linux (epoll)](#linux-epoll)

### Windows 11 (IOCP)

Measured in October 2026 on Windows 11 Home 10.0.26200, Intel Core Ultra 7
265K (20 cores, 32 GB), CPython 3.11.9; the raw data is in
[`bench/baselines/windows-2026-10`](bench/baselines/windows-2026-10/). The
machine's VPN client and ad blocker, which both filter network traffic, were
stopped for every run. Load comes from
[`bench/loadgen`](bench/loadgen/), a native Go load generator that checks
every response. Every server ran the same ASGI app
([`bench/suite/app.py`](bench/suite/app.py)) in one worker process. Each
number is the median of three runs, each against a fresh server, with the
order of servers rotated between runs; latency percentiles come from the
three runs' merged histograms. No run had a failed request.

#### HTTP

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-http-dark.svg">
  <img alt="Windows HTTP requests per second by workload: cadeloop serve() leads every workload, 1.6 to 4.8 times uvicorn on winloop" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-http.svg">
</picture>

<!-- table:bench-win-table-http -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-table-http-dark.svg">
  <img alt="HTTP requests per second · Windows 11. plaintext: cadeloop serve() 78.0 K best, uvicorn + cadeloop 18.4 K, uvicorn + winloop 18.0 K, uvicorn + asyncio 10.8 K, hypercorn 7.6 K; plaintext, 1 connection: cadeloop serve() 47.2 K best, uvicorn + cadeloop 13.1 K, uvicorn + winloop 10.9 K, uvicorn + asyncio 7.7 K, hypercorn 5.8 K; JSON: cadeloop serve() 57.8 K best, uvicorn + cadeloop 16.0 K, uvicorn + winloop 15.9 K, uvicorn + asyncio 10.0 K, hypercorn 7.0 K; 64 KiB body: cadeloop serve() 41.2 K best, uvicorn + cadeloop 14.5 K, uvicorn + winloop 14.9 K, uvicorn + asyncio 8.9 K, hypercorn 6.4 K; 1 MiB stream (16 conns): cadeloop serve() 3.3 K best, uvicorn + cadeloop 2.0 K, uvicorn + winloop 2.0 K, uvicorn + asyncio 1.1 K, hypercorn 858; 16 KiB upload: cadeloop serve() 61.1 K best, uvicorn + cadeloop 13.0 K, uvicorn + winloop 12.8 K, uvicorn + asyncio 9.1 K, hypercorn 6.2 K; 1 ms await: cadeloop serve() 31.6 K best, uvicorn + cadeloop 17.4 K, uvicorn + winloop 10.9 K, uvicorn + asyncio 10.1 K, hypercorn 7.0 K; conn per request (16): cadeloop serve() 24.1 K best, uvicorn + cadeloop 8.6 K, uvicorn + winloop 9.2 K, uvicorn + asyncio 4.7 K, hypercorn 2.9 K; pipelined ×16: cadeloop serve() 553 K best, uvicorn + cadeloop 17.5 K, uvicorn + winloop 17.6 K, uvicorn + asyncio 13.2 K, hypercorn 7.7 K." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-table-http.svg">
</picture>
<!-- /table -->

uvicorn ran its h11 parser (httptools was not installed on this machine)
and wsproto; hypercorn ran on asyncio. In `1 ms await` the app awaits
`asyncio.sleep(0.001)` before answering, `conn per request` opens a new
connection for every request, and `pipelined ×16` sends 16 requests per
round trip, which takes most of the kernel's per-request cost out and shows
the HTTP engine on its own.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-http-cpu-dark.svg">
  <img alt="Server CPU per request on Windows: cadeloop 12.8 µs on plaintext against 55 µs for uvicorn on winloop and 92 µs on asyncio" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-http-cpu.svg">
</picture>

<!-- table:bench-win-table-plaintext -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-table-plaintext-dark.svg">
  <img alt="Plaintext HTTP in detail · Windows 11. cadeloop serve(): req/s 78.0 K best, p50 819 µs best, p99 991 µs best, p99.9 1.75 ms best, CPU / request 12.8 µs best, peak memory 33 MiB, threads 5; uvicorn + cadeloop: req/s 18.4 K, p50 3.38 ms, p99 5.05 ms, p99.9 5.77 ms, CPU / request 54.1 µs, peak memory 37 MiB, threads 5; uvicorn + winloop: req/s 18.0 K, p50 3.44 ms, p99 6.95 ms, p99.9 7.73 ms, CPU / request 55.3 µs, peak memory 61 MiB, threads 12; uvicorn + asyncio: req/s 10.8 K, p50 5.83 ms, p99 7.01 ms, p99.9 10.09 ms, CPU / request 92.2 µs, peak memory 29 MiB, threads 5; hypercorn: req/s 7.6 K, p50 8.39 ms, p99 9.96 ms, p99.9 13.11 ms, CPU / request 131.5 µs, peak memory 28 MiB best, threads 5." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-table-plaintext.svg">
</picture>
<!-- /table -->

Most of cadeloop's 12.8 µs per request is the Windows kernel: 1.6 µs is
user time (parsing, the call into your app, encoding the response), and the
rest is the socket receive and send. Pipelined, where one receive carries
16 requests, the same engine served 553 K requests per second at 1.8 µs
each.

#### Memory

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-idle-mem-dark.svg">
  <img alt="Private memory per idle keep-alive connection on Windows at 1,000 and 5,000 connections" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-idle-mem.svg">
</picture>

<!-- table:bench-win-table-idle -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-table-idle-dark.svg">
  <img alt="Idle keep-alive connections · Windows 11. cadeloop serve(): at rest 30 MiB, holding 5,000 68 MiB best, per connection 6.6 KiB, after they close 42 MiB best, handles / conn 1; uvicorn + cadeloop: at rest 36 MiB, holding 5,000 106 MiB, per connection 11.2 KiB, after they close 67 MiB, handles / conn 1; uvicorn + winloop: at rest 61 MiB, holding 5,000 98 MiB, per connection 6.4 KiB best, after they close 79 MiB, handles / conn 1; uvicorn + asyncio: at rest 25 MiB, holding 5,000 376 MiB, per connection 57.9 KiB, after they close 376 MiB, handles / conn 1; hypercorn: at rest 23 MiB best, holding 5,000 408 MiB, per connection 63.0 KiB, after they close 408 MiB, handles / conn 1." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-table-idle.svg">
</picture>
<!-- /table -->

Each connection made one request and was then held open. Under load,
cadeloop peaked between 33 and 42 MiB of private memory across the HTTP
workloads. uvicorn on asyncio ranged from 29 to 40 MiB, hypercorn from 28 to
50 MiB (its peak was connection churn), uvicorn on cadeloop from 37 to
45 MiB, and uvicorn on winloop held 61 MiB from the start, with 12 threads.

#### WebSocket

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-ws-dark.svg">
  <img alt="WebSocket echo messages per second on Windows" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-ws.svg">
</picture>

<!-- table:bench-win-table-ws -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-table-ws-dark.svg">
  <img alt="WebSocket echo · Windows 11. 64 B, 1 connection: cadeloop 48.3 K (45 µs) best, uvicorn + winloop 22.8 K (133 µs), uvicorn + asyncio 17.7 K (137 µs), hypercorn 15.5 K (160 µs); 64 B, 64 connections: cadeloop 77.8 K (999 µs) best, uvicorn + winloop 43.3 K (3.93 ms), uvicorn + asyncio 31.3 K (4.52 ms), hypercorn 26.3 K (3.18 ms); 16 KiB, 16 connections: cadeloop 47.6 K (463 µs) best, uvicorn + winloop 21.0 K (2.52 ms), uvicorn + asyncio 18.5 K (3.24 ms), hypercorn 15.4 K (1.46 ms); 1 KiB stream, 32 in flight ×16: cadeloop 407 K (1.67 ms) best, uvicorn + winloop 92.1 K (17.56 ms), uvicorn + asyncio 66.4 K (19.14 ms), hypercorn 56.7 K (12.06 ms)." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-table-ws.svg">
</picture>
<!-- /table -->

#### TLS

<!-- table:bench-win-table-tls -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-table-tls-dark.svg">
  <img alt="TLS 1.3 · Windows 11. plaintext, 64 keep-alive conns: cadeloop 62.6 K (1.39 ms) best, uvicorn + winloop 15.7 K (7.93 ms), uvicorn + asyncio 9.2 K (8.32 ms); new connection per request (16): cadeloop 3.6 K (2.85 ms) best, uvicorn + winloop 2.3 K (4.59 ms), uvicorn + asyncio 1.5 K (3.05 ms)." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-table-tls.svg">
</picture>
<!-- /table -->

Every server used Python's `ssl` module. `new connection per request`
includes the full TLS 1.3 handshake.

#### Timers and wake-ups

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-timers-dark.svg">
  <img alt="How late asyncio.sleep returns on Windows: cadeloop 0.55 ms past a 1 ms deadline, asyncio and winloop about 14.4 ms" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-timers.svg">
</picture>

<!-- table:bench-win-table-timers -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-table-timers-dark.svg">
  <img alt="Timers and wake-ups · Windows 11. cadeloop: sleep(1 ms) +0.55 / 0.88 ms best, sleep(5 ms) +0.40 / 0.63 ms best, sleep(10 ms) +0.31 / 0.82 ms best, cross-thread wakeup 16 / 117 µs best; winloop: sleep(1 ms) +14.49 / 15.41 ms, sleep(5 ms) +10.73 / 15.17 ms, sleep(10 ms) +6.00 / 12.47 ms, cross-thread wakeup 19 / 66 µs; asyncio: sleep(1 ms) +14.33 / 15.51 ms, sleep(5 ms) +10.82 / 15.22 ms, sleep(10 ms) +5.88 / 11.31 ms, cross-thread wakeup 68 / 162 µs." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-table-timers.svg">
</picture>
<!-- /table -->

A Windows wait with a millisecond timeout ends on the next clock interrupt,
which comes every 15.6 ms by default. A loop that waits that way turns a
1 ms sleep into about 15 ms, which is also why `1 ms await` is the HTTP
workload where asyncio and winloop fall furthest behind. cadeloop ends a
wait that is bound by a timer on a high-resolution timer instead
([how](docs/learn/3-inside-cadeloop.md#timers-on-windows)); the
remaining half millisecond is the platform's own floor, and CPython 3.11's
`time.sleep(0.001)` overshoots by the same amount on this machine. A busy
loop seldom waits long enough for this to show; it matters most for lightly
loaded services and for code that sleeps on purpose.

#### TCP echo: the loop alone

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-echo-dark.svg">
  <img alt="TCP echo messages per second on Windows on cadeloop, winloop, rsloop and asyncio" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-echo.svg">
</picture>

<!-- table:bench-win-table-echo -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-table-echo-dark.svg">
  <img alt="TCP echo, the loop alone · Windows 11. 1 KiB, 1 connection: cadeloop 51.8 K (41 µs) best, winloop 40.4 K (54 µs), rsloop 40.4 K (48 µs), asyncio 31.5 K (79 µs); 1 KiB, 64 connections: cadeloop 84.4 K (909 µs), winloop 80.1 K (1.56 ms), rsloop 87.7 K (958 µs) best, asyncio 53.6 K (1.54 ms); 1 KiB, 16 in flight ×64: cadeloop 346 K (3.44 ms), winloop 375 K (5.18 ms), rsloop 457 K (2.49 ms) best, asyncio 287 K (4.46 ms); 64 KiB, 8 connections: cadeloop 29.8 K (614 µs), winloop 26.8 K (745 µs), rsloop 43.7 K (324 µs) best, asyncio 31.4 K (459 µs)." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-table-echo.svg">
</picture>
<!-- /table -->

The same `asyncio.Protocol` echo server ran on every loop. rsloop does its
socket I/O on worker threads: it used 1.5 to 2 cores (26 threads) where the
others used one, and more CPU per message than cadeloop (4.4 against 2.9 µs
with 16 messages in flight). With one connection, cadeloop spins for up to
20 µs before it sleeps, which is why it answered fastest there.

#### Overload and recovery

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-overload-dark.svg">
  <img alt="p99 latency per second through 10 seconds at 150 percent of each server's capacity on Windows: every server keeps serving and is back to normal 12 to 14 seconds after the overload ends" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-overload.svg">
</picture>

Each server was offered 50% of its own measured capacity, then 150% for
10 seconds, then 50% again, from 256 connections at a fixed rate (open
loop), with latency measured from when each request was due so the backlog
shows. Every server kept serving at 92 to 95% of its measured capacity
through the overload, without errors, and was back to normal 12 to 14 seconds
after the overload ended. cadeloop's capacity
here was 77.9 K requests per second, against 18.4 K for uvicorn on cadeloop,
18.1 K on winloop and 10.9 K on asyncio.

#### Scheduling microbenchmarks

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-sched-dark.svg">
  <img alt="Windows scheduling microbenchmarks as a share of the fastest loop per benchmark" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-sched.svg">
</picture>

<!-- table:bench-win-table-sched -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-table-sched-dark.svg">
  <img alt="Scheduling microbenchmarks · Windows 11. task_fib: cadeloop 0.194, winloop 0.186, rsloop 0.196 best, asyncio 0.125; call_soon_chain: cadeloop 11.027 best, winloop 2.347, rsloop 9.785, asyncio 0.906; call_soon_burst: cadeloop 12.652 best, winloop 1.596, rsloop hung, asyncio 1.170; timer_schedule_cancel: cadeloop 6.732 best, winloop 0.744, rsloop hung, asyncio 0.910; timer_fire: cadeloop 4.619 best, winloop 2.016, rsloop hung, asyncio 0.530; sleep0_chain: cadeloop 3.635 best, winloop 1.555, rsloop 3.265, asyncio 0.758; task_spawn: cadeloop 0.565, winloop 0.497, rsloop 0.567 best, asyncio 0.381; threadsafe_throughput: cadeloop 8.066 best, winloop 2.570, rsloop 4.581, asyncio 0.069; future_chain: cadeloop 2.725, winloop 0.945, rsloop 2.820 best, asyncio 0.405; gather_fanin: cadeloop 0.613 best, winloop 0.519, rsloop 0.572, asyncio 0.334; queue_pingpong: cadeloop 1.871 best, winloop 1.765, rsloop 1.853, asyncio 1.759; queue_pingpong_native: cadeloop 6.055 best, winloop 5.369, rsloop 5.927, asyncio 4.977." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-table-sched.svg">
</picture>
<!-- /table -->

Each benchmark times one kind of loop operation in isolation, in a fresh
process per run. rsloop 0.1.30 did not finish the timer benchmarks and was
stopped after 12 s.

### Linux (epoll)

Measured in October 2026 on a cloud VM with 4 vCPUs (Intel Xeon @ 2.10 GHz,
16 GB), Linux 6.18, CPython 3.11.17, cadeloop revision `808d72d`; the raw
data is in [`bench/baselines/linux-2026-10`](bench/baselines/linux-2026-10/).
It is the same suite, load generator and app as on Windows, with the
contenders common on Linux: uvicorn on uvloop with h11 and wsproto (set up as
on Windows), `uvicorn[standard]` (httptools and websockets on uvloop, the
usual production setup), and granian. The load generator shared the VM,
limited to two threads so the server kept the other two cores. Each number is
the median of three runs with the order of servers rotated. No run had a
failed request.

#### HTTP

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-http-dark.svg">
  <img alt="Linux HTTP requests per second by workload: cadeloop serve() leads every workload; granian comes closest with 64 KiB bodies and a 1 ms await" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-http.svg">
</picture>

<!-- table:bench-linux-table-http -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-table-http-dark.svg">
  <img alt="HTTP requests per second · Linux. plaintext: cadeloop serve() 117 K best, granian 48.7 K, uvicorn[standard] 24.5 K, uvicorn + cadeloop 9.60 K, uvicorn + uvloop 6.39 K, uvicorn + asyncio 5.30 K, hypercorn 3.57 K; plaintext, 1 connection: cadeloop serve() 28.1 K best, granian 7.95 K, uvicorn[standard] 12.8 K, uvicorn + cadeloop 5.59 K, uvicorn + uvloop 6.31 K, uvicorn + asyncio 5.34 K, hypercorn 3.33 K; JSON: cadeloop serve() 49.9 K best, granian 27.4 K, uvicorn[standard] 13.7 K, uvicorn + cadeloop 7.87 K, uvicorn + uvloop 5.86 K, uvicorn + asyncio 5.44 K, hypercorn 3.38 K; 64 KiB body: cadeloop serve() 30.9 K best, granian 28.7 K, uvicorn[standard] 18.4 K, uvicorn + cadeloop 6.94 K, uvicorn + uvloop 6.07 K, uvicorn + asyncio 5.41 K, hypercorn 3.48 K; 1 MiB stream (16 conns): cadeloop serve() 2.05 K best, granian 1.76 K, uvicorn[standard] 1.12 K, uvicorn + cadeloop 1.03 K, uvicorn + uvloop 620, uvicorn + asyncio 591, hypercorn 395; 16 KiB upload: cadeloop serve() 61.7 K best, granian 16.6 K, uvicorn[standard] 15.4 K, uvicorn + cadeloop 7.12 K, uvicorn + uvloop 5.26 K, uvicorn + asyncio 4.26 K, hypercorn 3.25 K; 1 ms await: cadeloop serve() 29.9 K best, granian 28.9 K, uvicorn[standard] 17.9 K, uvicorn + cadeloop 9.52 K, uvicorn + uvloop 6.34 K, uvicorn + asyncio 5.23 K, hypercorn 3.81 K; conn per request (16): cadeloop serve() 16.9 K best, granian 12.8 K, uvicorn[standard] 10.5 K, uvicorn + cadeloop 5.21 K, uvicorn + uvloop 4.13 K, uvicorn + asyncio 4.17 K, hypercorn 2.19 K; pipelined ×16: cadeloop serve() 320 K best, granian 50.5 K, uvicorn[standard] 21.7 K, uvicorn + cadeloop 9.30 K, uvicorn + uvloop 6.63 K, uvicorn + asyncio 5.79 K, hypercorn 4.36 K." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-table-http.svg">
</picture>
<!-- /table -->

`cadeloop.serve()` led every workload. On plaintext it handled 117 K requests
per second: 4.8 times `uvicorn[standard]` and 2.4 times granian, at 8.4 µs of
CPU per request against 40.3 and 31.5 µs. granian, which also does its HTTP
work in Rust, came closest where the handler or the response dominates:
within 4% with a 1 ms await in the handler (29.9 K against 28.9 K), within 8%
on 64 KiB responses, and 14% behind on 1 MiB streams. With one connection,
where each request waits for the previous answer, cadeloop answered in 32 µs
at the median.

Keeping uvicorn and its h11 parser fixed and changing only the loop,
cadeloop's loop made uvicorn 50% faster than on uvloop (9.6 K against 6.4 K
requests per second) and 81% faster than on asyncio (5.3 K).

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-http-cpu-dark.svg">
  <img alt="Server CPU per request on Linux: cadeloop 8.4 µs on plaintext against 31.5 µs for granian and 40.3 µs for uvicorn[standard]" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-http-cpu.svg">
</picture>

<!-- table:bench-linux-table-plaintext -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-table-plaintext-dark.svg">
  <img alt="Plaintext HTTP in detail · Linux. cadeloop serve(): req/s 117 K best, p50 479 µs best, p99 1.28 ms best, p99.9 2.33 ms best, CPU / request 8.4 µs best, peak memory 25 MiB, threads 1; granian: req/s 48.7 K, p50 1.31 ms, p99 2.92 ms, p99.9 5.37 ms, CPU / request 31.5 µs, peak memory 25 MiB, threads 6; uvicorn[standard]: req/s 24.5 K, p50 2.49 ms, p99 4.92 ms, p99.9 7.73 ms, CPU / request 40.3 µs, peak memory 24 MiB, threads 5; uvicorn + cadeloop: req/s 9.60 K, p50 6.42 ms, p99 10.49 ms, p99.9 15.99 ms, CPU / request 103.2 µs, peak memory 31 MiB, threads 1; uvicorn + uvloop: req/s 6.39 K, p50 9.31 ms, p99 15.20 ms, p99.9 22.81 ms, CPU / request 155.1 µs, peak memory 23 MiB, threads 5; uvicorn + asyncio: req/s 5.30 K, p50 11.67 ms, p99 18.09 ms, p99.9 26.74 ms, CPU / request 187.1 µs, peak memory 21 MiB best, threads 1; hypercorn: req/s 3.57 K, p50 16.52 ms, p99 26.48 ms, p99.9 33.29 ms, CPU / request 278.2 µs, peak memory 21 MiB best, threads 1." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-table-plaintext.svg">
</picture>
<!-- /table -->

cadeloop ran as one process with one thread and kept one core busy, a bit
more than half of it in the kernel's socket calls. granian ran two processes
(a supervisor and its worker) with six threads and used about 1.5 cores.
`pipelined ×16`, where one read carries 16 requests, shows the HTTP engine on
its own: 320 K requests per second at 3.1 µs of CPU each.

#### Memory

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-idle-mem-dark.svg">
  <img alt="Unique memory per idle keep-alive connection on Linux at 1,000 and 5,000 connections" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-idle-mem.svg">
</picture>

<!-- table:bench-linux-table-idle -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-table-idle-dark.svg">
  <img alt="Idle keep-alive connections · Linux. cadeloop serve(): at rest 23 MiB, holding 5,000 64 MiB, per connection 7.2 KiB, after they close 33 MiB best, fds / conn 1; granian: at rest –, holding 5,000 –, per connection –, after they close –, fds / conn –; uvicorn[standard]: at rest 24 MiB, holding 5,000 61 MiB, per connection 6.2 KiB, after they close 48 MiB, fds / conn 1; uvicorn + cadeloop: at rest 28 MiB, holding 5,000 99 MiB, per connection 11.7 KiB, after they close 70 MiB, fds / conn 1; uvicorn + uvloop: at rest 23 MiB, holding 5,000 57 MiB, per connection 5.6 KiB, after they close 45 MiB, fds / conn 1; uvicorn + asyncio: at rest 20 MiB best, holding 5,000 54 MiB best, per connection 5.5 KiB best, after they close 53 MiB, fds / conn 1; hypercorn: at rest 20 MiB best, holding 5,000 88 MiB, per connection 11.2 KiB, after they close 88 MiB, fds / conn 0.94." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-table-idle.svg">
</picture>
<!-- /table -->

Memory on Linux is unique memory (USS): pages that belong to the server's
processes alone. At rest, every server used 20 to 28 MiB. Holding 5,000 idle
connections, uvicorn on asyncio and on uvloop used the least per connection
(5.5 and 5.6 KiB), `uvicorn[standard]` 6.2 KiB and cadeloop 7.2 KiB. Once the
connections closed, cadeloop handed back three quarters of its growth (it
went from 64 MiB to 33 MiB), while uvicorn on asyncio and hypercorn kept
nearly all of theirs. With its default settings, granian accepted 648 of the
5,000 connections and stopped there: 4,096 more waited unaccepted in the
kernel's queue and the last 256 timed out while connecting. So it has no
comparable figure at 5,000; at 1,000 connections it used 21.4 KiB each.

#### WebSocket

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-ws-dark.svg">
  <img alt="WebSocket echo messages per second on Linux: cadeloop leads every workload" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-ws.svg">
</picture>

<!-- table:bench-linux-table-ws -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-table-ws-dark.svg">
  <img alt="WebSocket echo · Linux. 64 B, 1 connection: cadeloop 27.7 K (91 µs) best, granian 6.80 K (328 µs), uvicorn[standard] 12.1 K (291 µs), uvicorn + uvloop 10.3 K (260 µs), uvicorn + asyncio 5.78 K (463 µs), hypercorn 6.49 K (467 µs); 64 B, 64 connections: cadeloop 114 K (1.28 ms) best, granian 50.6 K (2.95 ms), uvicorn[standard] 19.2 K (5.70 ms), uvicorn + uvloop 22.6 K (5.31 ms), uvicorn + asyncio 10.3 K (10.88 ms), hypercorn 11.6 K (8.78 ms); 16 KiB, 16 connections: cadeloop 42.1 K (967 µs) best, granian 27.5 K (1.47 ms), uvicorn[standard] 16.4 K (1.87 ms), uvicorn + uvloop 12.8 K (2.88 ms), uvicorn + asyncio 7.57 K (3.74 ms), hypercorn 7.38 K (3.70 ms); 1 KiB stream, 32 in flight ×16: cadeloop 380 K (4.59 ms) best, granian 35.4 K (22.02 ms), uvicorn[standard] 39.7 K (21.76 ms), uvicorn + uvloop 28.8 K (39.32 ms), uvicorn + asyncio 29.4 K (33.03 ms), hypercorn 23.0 K (43.52 ms)." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-table-ws.svg">
</picture>
<!-- /table -->

cadeloop led every WebSocket workload: 2.3 times `uvicorn[standard]` with one
connection (27.7 K against 12.1 K messages per second), 2.3 times granian with
64 connections (114 K against 50.6 K), and 9.6 times `uvicorn[standard]` when
each connection kept 32 messages in flight (380 K against 39.7 K).

#### TLS

<!-- table:bench-linux-table-tls -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-table-tls-dark.svg">
  <img alt="TLS · Linux. plaintext, 64 keep-alive conns: cadeloop 65.0 K (2.13 ms) best, granian 45.4 K (3.24 ms), uvicorn[standard] 15.4 K (7.86 ms), uvicorn + uvloop 5.88 K (16.52 ms), uvicorn + asyncio 5.21 K (19.92 ms); new connection per request (16): cadeloop 1.11 K (9.04 ms) best, granian 1.06 K (18.35 ms), uvicorn[standard] 763 (5.96 ms), uvicorn + uvloop 690 (6.16 ms), uvicorn + asyncio 753 (14.42 ms)." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-table-tls.svg">
</picture>
<!-- /table -->

granian handles TLS in Rust; every other server here uses Python's `ssl`
module. Over keep-alive connections, cadeloop served 65.0 K requests per
second against granian's 45.4 K. With a new connection and a full TLS 1.3
handshake for every request, the two were within 5% of each other (1.11 K
against 1.06 K), each spending about 0.9 ms of CPU per request, most of it on
the handshake.

#### Timers and wake-ups

<!-- table:bench-linux-table-timers -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-table-timers-dark.svg">
  <img alt="Timers and wake-ups · Linux. cadeloop: sleep(1 ms) +0.13 / 0.37 ms, sleep(5 ms) +0.29 / 0.49 ms, sleep(10 ms) +0.32 / 1.89 ms, cross-thread wake-up 93 / 642 µs; uvloop: sleep(1 ms) +0.16 / 0.98 ms, sleep(5 ms) +0.20 / 1.57 ms, sleep(10 ms) +0.25 / 0.46 ms, cross-thread wake-up 95 / 617 µs; rloop: sleep(1 ms) +0.12 / 0.32 ms, sleep(5 ms) +0.17 / 33.26 ms, sleep(10 ms) +0.18 / 0.28 ms, cross-thread wake-up 78 / 1034 µs best; rsloop: sleep(1 ms) +0.07 / 0.27 ms best, sleep(5 ms) +0.13 / 0.36 ms best, sleep(10 ms) +0.14 / 0.31 ms best, cross-thread wake-up 94 / 320 µs; asyncio: sleep(1 ms) +0.16 / 0.28 ms, sleep(5 ms) +0.22 / 0.65 ms, sleep(10 ms) +0.24 / 0.46 ms, cross-thread wake-up 123 / 599 µs." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-table-timers.svg">
</picture>
<!-- /table -->

Linux wakes a sleeping thread on a fine-grained timer, so the Windows problem
above doesn't arise: on every loop, a 1 ms sleep overshot by 0.07 to 0.16 ms
at the median. Longer sleeps overshot a little more, cadeloop's the most:
0.29 ms at 5 ms and 0.32 ms at 10 ms, against 0.13 to 0.25 ms on the other
loops. A `call_soon_threadsafe()` from another thread reached a
sleeping loop in 78 to 123 µs at the median, with rloop fastest and cadeloop
at 93 µs.

#### TCP echo: the loop alone

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-echo-dark.svg">
  <img alt="TCP echo messages per second on Linux on cadeloop, uvloop, rloop, rsloop and asyncio" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-echo.svg">
</picture>

<!-- table:bench-linux-table-echo -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-table-echo-dark.svg">
  <img alt="TCP echo, the loop alone · Linux. 1 KiB, 1 connection: cadeloop 33.7 K (76 µs) best, uvloop 24.2 K (85 µs), rloop 23.6 K (88 µs), rsloop 24.6 K (85 µs), asyncio 20.4 K (109 µs); 1 KiB, 64 connections: cadeloop 170 K (975 µs), uvloop 149 K (1.05 ms), rloop 171 K (827 µs) best, rsloop 150 K (975 µs), asyncio 115 K (1.46 ms); 1 KiB, 16 in flight ×64: cadeloop 352 K (14.94 ms), uvloop 366 K (18.09 ms), rloop 366 K (18.87 ms), rsloop 381 K (13.63 ms) best, asyncio 379 K (14.68 ms); 64 KiB, 8 connections: cadeloop 31.9 K (803 µs), uvloop 39.1 K (483 µs), rloop 42.0 K (401 µs) best, rsloop 36.1 K (406 µs), asyncio 37.7 K (541 µs)." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-table-echo.svg">
</picture>
<!-- /table -->

The same `asyncio.Protocol` echo server ran on each loop, so this compares
the loops alone. With one connection, cadeloop answered fastest (33.7 K
messages per second, 26 µs at the median), using more CPU per message than
uvloop, rloop and rsloop because it keeps spinning while messages arrive back
to back. With 64 connections, cadeloop and rloop were level at 170 K. With 16
messages in flight, all five loops landed within 9% of each other, which
suggests the two-thread load generator set the pace there rather than the
loops. With 64 KiB messages cadeloop was the slowest of the five, at 31.9 K
messages per second against rloop's 42.0 K: large messages are the weakest
spot of cadeloop's Linux backend today.

#### Overload and recovery

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-overload-dark.svg">
  <img alt="p99 latency per second on Linux through 10 seconds at 150 percent of each server's capacity: no server fails a request; granian, uvicorn on asyncio and cadeloop are back to normal 7, 8 and 14 seconds after the overload, the other uvicorn setups are still working off their backlog when the test ends" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-overload.svg">
</picture>

The same test as on Windows: 50% of each server's own measured capacity, then
150% for 10 seconds, then 50% again, from 256 connections at a fixed rate.
No server failed a request or stopped serving. How quickly each one recovered
came down to how fast it worked through its backlog compared with the
capacity measured beforehand with 64 connections. granian served 9% above
that capacity while it had a backlog (45.5 K against 41.9 K requests per
second) and was back to normal 7 seconds after the overload ended; uvicorn on
asyncio served 5% above and took 8 seconds; cadeloop served 8% below (110 K
against 120 K) and took 14 seconds. The other three uvicorn setups served 10
to 15% below and were still working through their backlogs when the
30 seconds ended.

#### Scheduling microbenchmarks

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-sched-dark.svg">
  <img alt="Linux scheduling microbenchmarks as a share of the fastest loop per benchmark" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-sched.svg">
</picture>

<!-- table:bench-linux-table-sched -->
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-table-sched-dark.svg">
  <img alt="Scheduling microbenchmarks · Linux. task_fib: cadeloop 0.096 best, uvloop 0.081, rloop 0.088, rsloop 0.090, asyncio 0.056; call_soon_chain: cadeloop 3.412, uvloop 1.345, rloop 3.456 best, rsloop 1.939, asyncio 0.499; call_soon_burst: cadeloop 4.117 best, uvloop 1.102, rloop 2.536, rsloop 1.010, asyncio 0.710; timer_schedule_cancel: cadeloop 2.693 best, uvloop 0.427, rloop 1.683, rsloop 1.127, asyncio 0.492; timer_fire: cadeloop 2.219 best, uvloop 1.136, rloop 1.344, rsloop 0.988, asyncio 0.258; sleep0_chain: cadeloop 1.653 best, uvloop 0.846, rloop 1.496, rsloop 0.961, asyncio 0.368; task_spawn: cadeloop 0.262 best, uvloop 0.252, rloop 0.255, rsloop 0.254, asyncio 0.180; threadsafe_throughput: cadeloop 3.528, uvloop 1.734, rloop 3.674 best, rsloop 3.439, asyncio 0.060; future_chain: cadeloop 1.202 best, uvloop 0.514, rloop 0.869, rsloop 0.610, asyncio 0.210; gather_fanin: cadeloop 0.269 best, uvloop 0.231, rloop 0.243, rsloop 0.242, asyncio 0.153; queue_pingpong: cadeloop 1.134, uvloop 1.051, rloop 1.150 best, rsloop 1.129, asyncio 1.001; queue_pingpong_native: cadeloop 2.764, uvloop 2.710, rloop 2.887 best, rsloop 2.358, asyncio 2.041." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-linux-table-sched.svg">
</picture>
<!-- /table -->

cadeloop was fastest on 8 of the 12 benchmarks, including `task_fib`, where
one call to `fib(21)` grows a tree of 35,421 tasks (96 K calls per second,
against 81 to 90 K for the other compiled loops and 56 K for asyncio). rloop
led the other four, by 1 to 5%. On this VM a single pass of these benchmarks
varies a lot (one pass's runs of cadeloop's `task_fib` spanned 50 K to 95 K),
so each number is the median of 15 runs over three passes.

---

## Event loop mode: reference

Everything in this section is about cadeloop as an asyncio event loop. It
applies whether you run plain asyncio code, aiohttp, or uvicorn on top. For
the HTTP server, see [server mode](#server-mode-reference).

### Turning it on

There are five ways in, depending on how much of the process you want
cadeloop to take over.

**`cadeloop.install()`** makes cadeloop the event loop for the whole
process: every later `asyncio.run()`, `asyncio.new_event_loop()` or
`asyncio.Runner()` gets a cadeloop loop. This is how uvloop and winloop are
usually used too. Call it once, at startup, before anything creates a loop;
a library that created its loop earlier keeps that one.

```python
import asyncio
import cadeloop

cadeloop.install()
asyncio.run(main())
```

**`cadeloop.run(coro, *, debug=None)`** works like `asyncio.run()` but always
uses a new cadeloop loop, without changing anything for the rest of the
process. Good for libraries, tests, and processes that share their main
thread with other code.

```python
result = cadeloop.run(main())
```

**`asyncio.Runner` with a loop factory** gives you a tuned loop with the
standard runner:

```python
with asyncio.Runner(loop_factory=lambda: cadeloop.Loop(spin_us=200)) as runner:
    runner.run(main())
```

**`cadeloop.new_event_loop()`** returns a loop object you manage yourself:

```python
loop = cadeloop.new_event_loop()
try:
    loop.run_until_complete(main())
finally:
    loop.close()
```

**`cadeloop.EventLoopPolicy()`** is for frameworks that take an event loop
policy:

```python
asyncio.set_event_loop_policy(cadeloop.EventLoopPolicy())
```

The policy extends the platform's default policy, so subprocess support
keeps working after the swap on Linux.

### `Loop()` arguments

`cadeloop.Loop(**kwargs)` creates a loop with non-default settings. Every
argument is keyword-only. These arguments exist only in loop mode; the
server has its own settings, some with different names or defaults (see
[settings in both modes](#settings-in-both-modes)).

| argument | default | what it does |
|---|---|---|
| `backend` | `None` | The I/O backend: `"auto"`, `"iocp"`, `"rio"` or `"epoll"`. `None` reads the `CADELOOP_BACKEND` environment variable, else uses `"auto"`, which picks IOCP on Windows and epoll on Linux. `"rio"` is experimental and warns. |
| `spin_us` | `20` | How many microseconds the loop keeps checking for new work before it sleeps. More spinning lowers wake-up latency and costs CPU. `0` turns it off. |
| `adaptive_spin` | `True` | Spin only while spins are catching work, and skip it while they aren't ([how it decides](docs/learn/3-inside-cadeloop.md#spin-then-park)). `False` spins before every sleep. |
| `high_water` | `65536` | Bytes a transport may queue before it asks the protocol to pause writing (`pause_writing()`, `drain()` waits). |
| `low_water` | `16384` | Writing resumes once the queue falls below this. Must not exceed `high_water`. |
| `accept_pool` | `64` | Accepts kept ready per listening socket. Raise it if a burst of new connections makes `stats()["accept_starved"]` climb. |
| `dns_cache` | `False` | Cache `getaddrinfo()` answers. Off by default, as in asyncio. |
| `dns_cache_ttl` | `5.0` | How long a cached DNS answer is kept, in seconds. The record's own TTL is not used. |
| `tfo` | `False` | TCP Fast Open on listening sockets. |
| `loopback_fast_path` | `True` | Windows `SIO_LOOPBACK_FAST_PATH` on loopback sockets. Only matters for traffic within one machine. |
| `rio_cq_size`, `rio_rq_recv`, `rio_rq_send` | `65536`, `32`, `32` | Queue sizes for the experimental RIO backend on Windows. Ignored elsewhere. |

```python
loop = cadeloop.Loop(
    spin_us=200,          # lower latency, more CPU
    accept_pool=256,      # many new connections at once
    high_water=1 << 20,   # let transports queue 1 MiB before backpressure
    low_water=1 << 18,
    dns_cache=True,
)
```

Apart from these, a `Loop` is an ordinary asyncio event loop. It supports
`run_forever`, `run_until_complete`, `call_soon`, `call_later`, `call_at`,
`call_soon_threadsafe`, `create_task`, `create_future`, `run_in_executor`,
`add_reader` and `add_writer`, the `sock_*` methods, `add_signal_handler`,
`create_server`, `create_connection`, `create_datagram_endpoint`,
`create_unix_server`, `subprocess_exec`, `sendfile`, `start_tls`,
`set_exception_handler`, `set_task_factory` and `close`. Debug mode
(`PYTHONASYNCIODEBUG=1` or `python -X dev`) and `loop.slow_callback_duration`
behave as in asyncio.

### `loop.stats()`

`loop.stats()` returns a dict of counters describing what the loop has done.
It is cheap enough to call often. A server started with `--stats-endpoint`
serves the same dict as JSON.

| key | meaning |
|---|---|
| `backend` | The backend in use: `"iocp"`, `"rio"` or `"epoll-dev"`. |
| `ticks` | Trips around the loop so far. |
| `polls` | Times the loop asked the OS for I/O events. |
| `polls_elided` | Polls skipped because no I/O was in flight, so the OS had nothing to report. Pure scheduling work, such as tasks passing messages through a queue, never makes the system call. |
| `completions` | I/O operations that finished. |
| `callbacks_dispatched` | Callbacks run. |
| `timers_fired` | Timers that went off. |
| `xthread_items` | Callbacks and timers that other threads handed over while the loop was asleep. |
| `spins` | Waits that began with a spin. |
| `spin_hits` | Spins that caught new work, saving a sleep and a wake-up. Work that was already waiting when the spin began doesn't count. |
| `precise_parks` | Waits ended by a high-resolution timer (Windows). |
| `ready_len`, `timers_len` | Callbacks waiting to run, and timers scheduled, right now. |
| `connections`, `listeners` | Open connections and listening sockets. |
| `connections_accepted` | Connections accepted so far. |
| `bytes_received`, `bytes_sent` | Totals so far. |
| `sends_posted` | Send operations made. Compare with `bytes_sent` to see how much writes are being batched. |
| `buffers_in_use` | Receive buffers currently holding data. |
| `buffer_bytes_in_use`, `buffer_bytes_held` | Memory in those buffers, and memory the buffer pool holds in total, used or free. Free memory beyond a small reserve goes back to the OS after a burst. |
| `accept_starved` | Times a listener ran out of ready accepts. If it keeps climbing, raise `accept_pool`. |
| `accept_ops` | Accept operations waiting in the kernel. |
| `pipeline_pauses` | Times the HTTP server stopped reading a connection because too many pipelined requests were queued. |
| `ops_by_target` | Kernel operations in flight, by kind (`recv`, `send`, `accept`, `connect`, `dgram`, `pipe`). A stuck server shows up here first. |
| `foreign_completions` | Windows: completion packets that did not belong to a live operation of this loop, for example another process's I/O on a socket handle shared with it. They are dropped and counted. Always 0 on Linux. |
| `stale_buffer_ids`, `unreaped_ops` | Leak checks. Both should always be 0. |

---

## Server mode: reference

Everything in this section is about cadeloop as an HTTP server: the
`cadeloop` command, `cadeloop.serve()` and `cadeloop.Config`. They take the
same settings.

### Three ways to start it

```bash
cadeloop main:app --port 8000 --workers 4                    # the command
```

```python
cadeloop.serve("main:app", "0.0.0.0", 8000, workers=4)       # from Python
```

```python
cfg = cadeloop.Config(workers=4, latency_mode="throughput")  # settings as an object
cadeloop.serve("main:app", "0.0.0.0", 8000, **vars(cfg))
```

`serve()` blocks until the server stops: on `SIGINT` or `SIGTERM`, or when
your code calls `loop.stop()`.

### `serve()` arguments

```python
cadeloop.serve(app, host="127.0.0.1", port=8000, *, workers=1, backend="auto",
               ssl=None, latency_mode="balanced", access_log=False, **settings)
```

| argument | default | meaning |
|---|---|---|
| `app` | required | An ASGI 3 app, or a `"module:attribute"` string. Use the string with `workers > 1` (required on Windows). |
| `host` | `"127.0.0.1"` | Address to listen on. `"0.0.0.0"` accepts connections from other machines. |
| `port` | `8000` | Port to listen on. |
| `workers` | `1` | Worker processes. `0` means one per CPU this process may use (its affinity mask, so a container's CPU limit counts). |
| `ssl` | `None` | An `ssl.SSLContext` to serve HTTPS. Its ALPN protocols are set to `http/1.1`. |
| `**settings` | | Any of the [settings](#settings) below, by name. An unknown name raises `TypeError`, a bad value `ValueError`. |

`backend`, `latency_mode` and `access_log` are settings too; they are spelled
out in the signature because they are the ones most often changed.

### Settings

Every setting can be given to `serve()` by name, to `Config(...)`, as a
command-line flag (underscores become dashes, so `max_body` is `--max-body`),
or as an environment variable read by `Config.from_env()` (`CADELOOP_MAX_BODY`).
Values are checked when the server starts, before anything binds or runs.

#### Performance

| setting | default | meaning |
|---|---|---|
| `latency_mode` | `"balanced"` | A preset for the three settings below. `"throughput"`: never spin. `"balanced"`: spin up to 20 µs while spinning catches work. `"spin"`: spin 200 µs before every sleep and send responses at once. See [choosing a latency mode](#choosing-a-latency-mode). |
| `spin_us` | from `latency_mode` | Spin window in microseconds. Set it to override the preset. |
| `adaptive_spin` | from `latency_mode` | Spin only while spinning catches work. On unless `latency_mode="spin"`. |
| `immediate_flush` | from `latency_mode` | Send each response as soon as it is ready, instead of batching a connection's writes until the end of the loop's tick. On only for `"spin"`. |
| `backend` | `"auto"` | The I/O backend, as in loop mode. `"rio"` also needs `CADELOOP_ALLOW_EXPERIMENTAL_RIO=1`. |
| `eager_tasks` | `True` | Start each request's handler immediately, inside the tick that parsed the request. Turn off if a library expects tasks to start later. |
| `gc_mode` | `"freeze"` | `"freeze"` calls `gc.freeze()` after startup and again after `warmup` requests, so the garbage collector stops re-scanning objects that live for the whole run. `"default"` leaves the collector alone; `"disable"` turns it off. |
| `warmup` | `1000` | With `gc_mode="freeze"`, the number of requests to serve before the second freeze. `0` freezes only at startup. |

#### Requests and connections

| setting | default | meaning |
|---|---|---|
| `max_body` | `16777216` | Largest request body in bytes; bigger ones get `413`. The server collects the whole body in memory and passes it to your app in one message, so this also bounds the memory one request can take. `None` removes the limit. |
| `max_header_bytes` | `65536` | Largest request head in bytes; bigger ones get `431`. |
| `max_headers` | `100` | Most header lines per request. |
| `max_url` | `8192` | Longest request target (path and query). |
| `request_line_timeout` | `5.0` | Seconds a client may take to send a complete request head. Stops clients that open connections and trickle bytes. `0` disables it. |
| `keepalive_idle` | `75.0` | Seconds an idle keep-alive connection stays open. `0` keeps it open until the client closes it. |
| `write_high_water`, `write_low_water` | `65536`, `16384` | Backpressure: when a connection's unsent output passes the high mark, `send()` waits until it drains below the low mark. |
| `accept_pool` | `64` | Accepts kept ready per listening socket. |
| `tfo` | `False` | TCP Fast Open on the listening socket. |

#### Workers and shutdown

| setting | default | meaning |
|---|---|---|
| `workers` | `0` in `Config`, `1` in `serve()` and the command | Worker processes; `0` means one per available CPU. |
| `grace` | `10.0` | Seconds a shutdown waits for requests in flight before closing them. Up to a quarter of it, at most 5 s, is kept for your lifespan's shutdown code. |
| `pin` | `False` | Pin each worker's loop thread to one CPU. It measured no gain on loopback and hurts on shared or virtual CPUs; worth trying on dedicated hardware with NIC queues aligned to the workers. |

#### Logging, monitoring and DNS

| setting | default | meaning |
|---|---|---|
| `access_log` | `False` | Log each request on the `cadeloop.access` logger, from a background thread that never blocks the loop. Nothing shows until logging is configured at INFO, for example with `logging.basicConfig(level=logging.INFO)`. |
| `stats_endpoint` | `None` | A port on which to serve `loop.stats()` as JSON, on `127.0.0.1` only. In a multi-worker server, one worker answers, and the payload says which. |
| `dns_cache` | `True` | Cache `getaddrinfo()` answers for outgoing connections your app makes. On by default for servers, unlike loop mode. |
| `dns_cache_ttl` | `5.0` | Seconds a cached answer is kept. |

#### Windows only

| setting | default | meaning |
|---|---|---|
| `loopback_fast_path` | `True` | `SIO_LOOPBACK_FAST_PATH` on loopback sockets. |
| `rio_cq_size`, `rio_rq_recv`, `rio_rq_send` | `65536`, `32`, `32` | Queue sizes for the experimental RIO backend. |

### The `cadeloop` command

```bash
cadeloop APP [options]
python -m cadeloop APP [options]
```

| option | meaning |
|---|---|
| `APP` | The app as `module:attribute`, imported from the current directory. |
| `--app-dir DIR` | Import the app from `DIR` instead. |
| `--host HOST`, `--port`, `-p PORT` | Where to listen. Defaults: `127.0.0.1`, `8000`. |
| `--workers`, `-w N` | Worker processes. Default 1; `0` means one per CPU. |
| `--backend {auto,epoll}` | On Windows the choices are `auto`, `iocp` and `rio`. |
| `--latency-mode {throughput,balanced,spin}` | The performance preset. |
| `--SETTING VALUE` | Every other [setting](#settings), with dashes for underscores: `--max-body 1048576`, `--keepalive-idle 30`, `--gc-mode default`. |
| `--SETTING` / `--no-SETTING` | On/off settings: `--access-log`, `--no-eager-tasks`, `--no-adaptive-spin`. |
| `none` | `--spin-us`, `--max-body` and `--stats-endpoint` accept `none`: `--max-body none` removes the body limit. |

```bash
# one low-latency worker, no body limit, stats on port 9001
cadeloop main:app --latency-mode spin --max-body none --stats-endpoint 9001

# a worker per CPU, tuned for throughput, short keep-alive
cadeloop main:app -w 0 --latency-mode throughput --keepalive-idle 15
```

### Environment variables

| variable | read by | effect |
|---|---|---|
| `CADELOOP_<SETTING>` | `Config.from_env()` | Sets that setting: `CADELOOP_WORKERS=8`, `CADELOOP_MAX_BODY=none`, `CADELOOP_ACCESS_LOG=true`. On/off values accept `1/0`, `true/false`, `yes/no`, `on/off`. |
| `CADELOOP_BACKEND` | `Loop()` | The default backend when none is given. Useful for running a whole test suite against one backend. |
| `CADELOOP_ALLOW_EXPERIMENTAL_RIO` | `Config` | Required before `Config`, `serve()` or the command accept `backend="rio"`. |
| `PYTHONASYNCIODEBUG` | `Loop()` | asyncio debug mode, as with the standard loop. |

```python
cfg = cadeloop.Config.from_env()     # or Config.from_env(prefix="MYAPP_")
cadeloop.serve("main:app", "0.0.0.0", 8000, **vars(cfg))
```

### HTTPS

```python
import ssl

ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ctx.load_cert_chain("cert.pem", "key.pem")
cadeloop.serve("main:app", "0.0.0.0", 8443, ssl=ctx)
```

Encryption uses Python's `ssl` module, so any context you can build works.
The server speaks HTTP/1.1 only, so it sets the context's ALPN list to
`http/1.1`; otherwise a client could agree on HTTP/2 during the handshake
and then have every request rejected.

### WebSockets

WebSocket connections reach your app as `websocket` scopes, as in any ASGI
server; FastAPI's and Starlette's WebSocket routes work as they do
elsewhere. Without a framework:

```python
async def app(scope, receive, send):
    if scope["type"] == "websocket":
        await receive()                                   # websocket.connect
        await send({"type": "websocket.accept"})
        while True:
            message = await receive()
            if message["type"] == "websocket.disconnect":
                break
            await send({"type": "websocket.send", "text": message["text"]})
```

### Lifespan

The ASGI lifespan protocol is detected and run automatically, once per
worker: startup before the worker accepts connections, shutdown after its
last request. An app that doesn't support lifespan is served normally.
FastAPI's `lifespan=` and Starlette's `lifespan=` both use it. Without a
framework:

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

### Workers in detail

- **Linux:** the supervisor imports your app once, freezes the garbage
  collector's view of it, and forks. Workers share those memory pages
  instead of each copying them. Every worker listens on the port itself
  (`SO_REUSEPORT`) and the kernel spreads new connections between them.
  Starting a server on a port another program already holds fails with
  `EADDRINUSE` rather than quietly sharing it.
- **Windows:** there is no fork. The supervisor owns the listening socket,
  accepts connections and hands each one to a worker. Each worker imports
  your app from the `"module:attribute"` string.

The supervisor restarts workers that crash. A worker that keeps dying right
after starting is given up on after five tries. If the supervisor itself
dies, the workers shut down gracefully.

### Shutdown

`SIGTERM`, or Ctrl-C, starts a graceful drain: the server stops accepting
connections, lets requests in flight finish for up to `grace` seconds, and
runs your lifespan's shutdown. A second `SIGTERM` (systemd sends one to every
process in the unit) does not cut the drain short; a second Ctrl-C does.

---

## Settings in both modes

Some settings exist in both modes, sometimes under a different name or with
a different default. A setting given to the server configures the loop the
server creates.

| what it controls | loop mode: `Loop(...)` | server mode: settings | command-line flag |
|---|---|---|---|
| I/O backend | `backend` | `backend` | `--backend` |
| spin window | `spin_us` (20) | `spin_us`, or `latency_mode` | `--spin-us`, `--latency-mode` |
| adaptive spinning | `adaptive_spin` (on) | `adaptive_spin`, or `latency_mode` | `--adaptive-spin` |
| write backpressure | `high_water`, `low_water` | `write_high_water`, `write_low_water` | `--write-high-water`, `--write-low-water` |
| ready accepts per listener | `accept_pool` | `accept_pool` | `--accept-pool` |
| DNS cache | `dns_cache` (off) | `dns_cache` (on) | `--dns-cache` |
| TCP Fast Open | `tfo` | `tfo` | `--tfo` |
| everything HTTP: limits, timeouts, workers, logging | not applicable | see [settings](#settings) | |

---

## Recipes

### Choosing a latency mode

`latency_mode` decides how the loop behaves when it runs out of work. Before
it goes to sleep, the loop can keep checking for new work for a short while
(spinning). Work that arrives during the spin is handled without a sleep and
a wake-up, which saves tens of microseconds; a spin that catches nothing is
CPU spent for nothing.

| mode | spin | adaptive | responses | use it for |
|---|---:|---|---|---|
| `throughput` | 0 | — | batched per tick | the most requests per CPU; idle CPU near zero |
| `balanced` (default) | up to 20 µs | yes | batched per tick | most services: spins while traffic is steady, stops when it isn't |
| `spin` | 200 µs, always | no | sent at once | latency-critical services on dedicated cores |

With adaptive spinning, the loop keeps track of how often spinning pays off
and stops when it mostly doesn't, trying again every eighth wait.
`stats()["spin_hits"]` against `stats()["spins"]` shows how often spinning
pays off on your traffic.

On a 4-core Linux VM, adaptive spinning matched spinning before every sleep
on back-to-back and bursty traffic, and cut server CPU per message by 19 to
38% at gaps of 50 µs to 5 ms between messages. At 200 µs gaps, spinning
before every sleep still had the lower median latency, and
`--no-adaptive-spin` keeps that behavior
([measurements](docs/ops.md#latency-modes-r-060)).

The parts of a mode can be set one by one:

```bash
# spin before every sleep, but keep batching responses
cadeloop main:app --spin-us 200 --no-adaptive-spin --no-immediate-flush
```

Measure on your own hardware before choosing `spin`. On Linux loopback,
sending responses immediately cost throughput and CPU (numbers in
[docs/ops.md](docs/ops.md#write-policy)).

### Accepting large uploads

The server collects a whole request body in memory before your app sees
it, so `max_body` (16 MiB by default) bounds memory per request. For 200 MB
video uploads, 8 at a time:

```python
cadeloop.serve(
    "main:app", "0.0.0.0", 8000,
    max_body=200 * 1024 * 1024,   # a 201 MiB upload gets a 413
    request_line_timeout=60.0,    # slow mobile clients need longer to send the head
    keepalive_idle=120.0,
)
```

Eight uploads in flight can then hold about 1.6 GiB before your app has
allocated anything. Size the machine and the worker count against that.
`max_body=None` removes the limit, which leaves memory up to whatever clients
send; only do that behind authentication or a proxy with its own limit.

### Hardening a public listener

```python
import logging

logging.basicConfig(level=logging.INFO)   # otherwise the access log goes nowhere

cadeloop.serve(
    "main:app", "0.0.0.0", 8000,
    max_body=2 * 1024 * 1024,      # small bodies only
    max_header_bytes=16 * 1024,
    max_headers=50,
    max_url=2048,
    request_line_timeout=3.0,      # drop clients that trickle a request head
    keepalive_idle=15.0,
    access_log=True,
)
```

With that `basicConfig`, each request logs a line like this; the part after
`cadeloop.access:` is the client, the request line, the status and the time
taken:

```
INFO:cadeloop.access:127.0.0.1:55888 "GET /" 200 0.42ms
```

### Graceful shutdown behind an orchestrator

```bash
cadeloop main:app -w 4 --grace 30
```

Give the orchestrator's stop timeout (Kubernetes'
`terminationGracePeriodSeconds`, systemd's `TimeoutStopSec`) a few seconds
more than `--grace`, so the drain can finish before the process is killed.

### Watching a live server

```bash
cadeloop main:app --stats-endpoint 9001 &
curl -s localhost:9001 | python -m json.tool
```

[Reading `loop.stats()`](docs/learn/6-measuring.md#reading-loopstats)
explains which counters to look at.

### Using cadeloop in a library or a test

`cadeloop.run()` runs one coroutine on a fresh cadeloop loop without changing
anything else in the process, so it is safe in libraries and tests:

```python
import cadeloop


def fetch_all(urls):
    return cadeloop.run(_fetch_all(urls))
```

---

## How it works

[How cadeloop works](docs/learn/README.md) is a six-part illustrated tour:
what an event loop does, how Windows and Linux report I/O, cadeloop's layers
and one tick step by step, where an HTTP request's time goes, how cadeloop
compares to asyncio, uvloop, winloop, rloop, rsloop, uvicorn, hypercorn and
granian, and how to measure performance without fooling yourself.

The short version: everything below your code is Rust, except a thin Python
layer that provides the asyncio API.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/learn/img/layers-dark.svg">
  <img alt="cadeloop's layers from top to bottom: your code; cadeloop's Python layer; the Rust bindings and HTTP engine; the Rust reactor; the Rust backends (IOCP, epoll, RIO); the operating system. The GIL is held everywhere except while the backend waits for I/O." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/learn/img/layers.svg">
</picture>

The loop holds the GIL except while it waits for I/O, batches a connection's
writes into one system call per tick, spins briefly before sleeping when
that pays off, and on Windows uses I/O completion ports directly, with
precise timers. As a server, it keeps HTTP out of Python entirely:

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/learn/img/request-path-dark.svg">
  <img alt="Six steps per request. uvicorn: read bytes, parse HTTP, build scope, your app, encode reply, write, with four of the six in Python. cadeloop serve(): only your app runs in Python; receiving, parsing, building the scope, encoding and writing are Rust." src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/learn/img/request-path.svg">
</picture>

[docs/architecture.md](docs/architecture.md) and the
[engineering record](docs/README.md) have the details for contributors.

---

## Development

```bash
cargo test --workspace                                    # Rust tests
cargo check -p cadeloop-core --target x86_64-pc-windows-msvc

cargo build -p cadeloop-pyshim --release                  # the extension
cp target/release/lib_core.so python/cadeloop/_core.so    # Linux: use it in place
pip install pytest pytest-timeout uvicorn aiohttp trustme starlette fastapi
PYTHONPATH=python pytest tests/unit tests/conformance

pip install maturin && maturin build --release            # a wheel
python tests/conformance/run_cpython_suite.py             # CPython's asyncio tests
```

Layout: `crates/core` (reactor and backends), `crates/pyshim` (bindings and
the HTTP engine), `python/cadeloop` (the Python package), `vendor/llhttp`,
`tests/`, `bench/` (load generator and benchmark suites), `docs/`.

Releases and packaging: [docs/ops.md](docs/ops.md).

---

## License

MIT, see [LICENSE](LICENSE). No GPL dependencies, checked by `cargo deny`.
