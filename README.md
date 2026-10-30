# cadeloop

**Rust Powered drop-in asyncio event loop +<br>
Native HTTP/1.1 + ASGI server**

[![Python 3.11](https://img.shields.io/badge/python-3.11-5a9996?style=flat-square&labelColor=0B0D11)](https://pypi.org/project/cadeloop/)
[![CI](https://img.shields.io/github/actions/workflow/status/cadezawacki/cadeloop/ci.yml?branch=main&style=flat-square&label=ci&labelColor=0B0D11&color=5a9996)](https://github.com/cadezawacki/cadeloop/actions/workflows/ci.yml)
[![MIT](https://img.shields.io/badge/license-MIT-5a9996?style=flat-square&labelColor=0B0D11)](LICENSE)

[![PyPI](https://img.shields.io/pypi/v/cadeloop?style=flat-square&label=pypi&labelColor=0B0D11&color=5a9996)](https://pypi.org/project/cadeloop/)
[![Platforms](https://img.shields.io/badge/platform-windows%20x64%20%7C%20linux%20x64-5a9996?style=flat-square&labelColor=0B0D11)](#installation)

```python
import asyncio, cadeloop

cadeloop.install()          # every asyncio API below now runs on cadeloop
asyncio.run(main())
```

Two lines to boost the performance of your existing asyncio code. Nothing else
changes: `asyncio.start_server`, `open_connection`, `add_reader`, `sock_*`,
subprocesses, signals, and third-party libraries like **uvicorn** and **aiohttp**
work unmodified.

For an even faster performance boost, skip the Python HTTP stack entirely.
`cadeloop.serve()` parses HTTP, builds the ASGI scope, and serializes responses
**in Rust**. Your `async def app(scope, receive, send)` is the only Python left
on the request path — which is why, on Windows, it serves **4.3× uvicorn on
winloop** and **7.2× uvicorn on stdlib asyncio** at a quarter or less of their
CPU per request, and on Linux **3.8× a tuned uvicorn** (httptools + uvloop) and
**2.4× granian** ([benchmarks](#benchmarks)).

---

## Table of contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Benchmarks](#benchmarks)
  - [Windows 11 (IOCP)](#windows-11-iocp)
  - [Linux (epoll dev backend)](#linux-epoll-dev-backend)
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
- [Development](#development)
- [License](#license)

---

## Installation

```bash
pip install cadeloop
```

### Requirements

| | |
|---|---|
| Python | CPython 3.11.x |
| OS | Windows 10/11, or Linux |
| CPU | x86-64 |

Wheels are published for Windows x64 and Linux x64 (manylinux2014,
glibc ≥ 2.17), plus a source distribution.

### Verifying an install

```bash
python -c "import cadeloop; l = cadeloop.new_event_loop(); print(cadeloop.__version__, l.stats()['backend']); l.close()"
# 0.0.2 iocp      (Windows)
# 0.0.2 epoll-dev  (Linux)
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

Two platforms, measured separately. **Windows** is the production target
(IOCP backend); **Linux** runs the `epoll` development backend. Both are
loopback runs with the load generator on the same machine: they compare
servers under identical conditions, they are not capacity numbers.

Methodology, harnesses, raw data and the measurement traps we hit along the
way: [docs/benchmarks.md](docs/benchmarks.md) and
[bench/windows/](bench/windows/README.md). Every Windows number below, with
all columns: [docs/benchmarks-windows.md](docs/benchmarks-windows.md).

### Windows 11 (IOCP)

Windows 11 Home 10.0.26200, Intel Core Ultra 7 265K (20 cores, 32 GB),
CPython 3.11.9; the host's VPN client and ad blocker (both filter network
traffic) stopped for every run, and its antivirus protection paused for the
comparison session. Load comes from
[`bench/loadgen`](bench/loadgen/), a native Go generator that validates
every response. Every server runs the same ASGI app
([`bench/windows/app.py`](bench/windows/app.py)) as one worker; each cell
is three runs, each against a fresh server, with the order of servers
rotated; the median run is shown, and percentiles come from all three runs'
merged latency histograms. **No cell below had a failed request.**

#### HTTP / ASGI

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-http-dark.svg">
  <img alt="Windows HTTP requests per second by workload: cadeloop serve() leads every workload, 1.6 to 4.8 times uvicorn on winloop" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-http.svg">
</picture>

Requests per second, 64 keep-alive connections unless noted:

<table>
<thead><tr><th>workload</th><th align="right">cadeloop <code>serve()</code></th><th align="right">uvicorn + cadeloop</th><th align="right">uvicorn + winloop</th><th align="right">uvicorn + asyncio</th><th align="right">hypercorn</th></tr></thead>
<tbody>
<tr><td>plaintext</td><td align="right" style="background-color: #00f0d11a"><b>78.0 K</b></td><td align="right">18.4 K</td><td align="right">18.0 K</td><td align="right">10.8 K</td><td align="right">7.6 K</td></tr>
<tr><td>plaintext, 1 connection</td><td align="right" style="background-color: #00f0d11a"><b>47.2 K</b></td><td align="right">13.1 K</td><td align="right">10.9 K</td><td align="right">7.7 K</td><td align="right">5.8 K</td></tr>
<tr><td>JSON</td><td align="right" style="background-color: #00f0d11a"><b>57.8 K</b></td><td align="right">16.0 K</td><td align="right">15.9 K</td><td align="right">10.0 K</td><td align="right">7.0 K</td></tr>
<tr><td>64 KiB body</td><td align="right" style="background-color: #00f0d11a"><b>41.2 K</b></td><td align="right">14.5 K</td><td align="right">14.9 K</td><td align="right">8.9 K</td><td align="right">6.4 K</td></tr>
<tr><td>1 MiB stream (16 conns)</td><td align="right" style="background-color: #00f0d11a"><b>3.3 K</b></td><td align="right">2.0 K</td><td align="right">2.0 K</td><td align="right">1.1 K</td><td align="right">858</td></tr>
<tr><td>16 KiB upload</td><td align="right" style="background-color: #00f0d11a"><b>61.1 K</b></td><td align="right">13.0 K</td><td align="right">12.8 K</td><td align="right">9.1 K</td><td align="right">6.2 K</td></tr>
<tr><td>1 ms await</td><td align="right" style="background-color: #00f0d11a"><b>31.6 K</b></td><td align="right">17.4 K</td><td align="right">10.9 K</td><td align="right">10.1 K</td><td align="right">7.0 K</td></tr>
<tr><td>conn per request (16)</td><td align="right" style="background-color: #00f0d11a"><b>24.1 K</b></td><td align="right">8.6 K</td><td align="right">9.2 K</td><td align="right">4.7 K</td><td align="right">2.9 K</td></tr>
<tr><td>pipelined ×16</td><td align="right" style="background-color: #00f0d11a"><b>553 K</b></td><td align="right">17.5 K</td><td align="right">17.6 K</td><td align="right">13.2 K</td><td align="right">7.7 K</td></tr>
</tbody>
</table>

uvicorn runs its h11 parser (httptools is not installed here) with
wsproto; hypercorn runs on stdlib asyncio. The `1 ms await` app awaits
`asyncio.sleep(0.001)` before answering; `conn per request` opens a new
connection for every request; `pipelined ×16` writes 16 requests per round
trip, which strips out per-round-trip kernel costs and shows the engine
itself.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-http-cpu-dark.svg">
  <img alt="Server CPU per request: cadeloop 12.8 us on plaintext vs 55 us for uvicorn on winloop and 92 us on stdlib asyncio" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-http-cpu.svg">
</picture>

Plaintext, 64 connections, in detail — server CPU is user + kernel time of
the server's whole process tree per successful request:

<table>
<thead><tr><th>server</th><th align="right">req/s</th><th align="right">p50</th><th align="right">p99</th><th align="right">p99.9</th><th align="right">CPU / request</th><th align="right">peak memory</th><th align="right">threads</th></tr></thead>
<tbody>
<tr><td><b>cadeloop</b> <code>serve()</code></td><td align="right" style="background-color: #00f0d11a"><b>78.0 K</b></td><td align="right" style="background-color: #00f0d11a"><b>819 µs</b></td><td align="right" style="background-color: #00f0d11a"><b>991 µs</b></td><td align="right" style="background-color: #00f0d11a"><b>1.75 ms</b></td><td align="right" style="background-color: #00f0d11a"><b>12.8 µs</b></td><td align="right" style="background-color: #00f0d11a"><b>33 MiB</b></td><td align="right">5</td></tr>
<tr><td>cadeloop 1.0.1</td><td align="right">73.7 K</td><td align="right">860 µs</td><td align="right">1.06 ms</td><td align="right">1.57 ms</td><td align="right">13.5 µs</td><td align="right">35 MiB</td><td align="right">5</td></tr>
<tr><td>uvicorn + cadeloop</td><td align="right">18.4 K</td><td align="right">3.38 ms</td><td align="right">5.05 ms</td><td align="right">5.77 ms</td><td align="right">54.1 µs</td><td align="right">37 MiB</td><td align="right">5</td></tr>
<tr><td>uvicorn + winloop</td><td align="right">18.0 K</td><td align="right">3.44 ms</td><td align="right">6.95 ms</td><td align="right">7.73 ms</td><td align="right">55.3 µs</td><td align="right">61 MiB</td><td align="right">12</td></tr>
<tr><td>uvicorn + asyncio</td><td align="right">10.8 K</td><td align="right">5.83 ms</td><td align="right">7.01 ms</td><td align="right">10.09 ms</td><td align="right">92.2 µs</td><td align="right">29 MiB</td><td align="right">5</td></tr>
<tr><td>hypercorn</td><td align="right">7.6 K</td><td align="right">8.39 ms</td><td align="right">9.96 ms</td><td align="right">13.11 ms</td><td align="right">131.5 µs</td><td align="right">28 MiB</td><td align="right">5</td></tr>
</tbody>
</table>

Most of cadeloop's 12.8 µs per request is the Windows kernel: 1.6 µs is
user time (parsing, the ASGI call, serialization), the rest is the receive
and send on the socket. Pipelined, where one receive carries 16 requests,
the same engine serves 553 K req/s at 1.8 µs each.

#### Memory

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-idle-mem-dark.svg">
  <img alt="Private memory per idle keep-alive connection at 1000 and 5000 connections" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-idle-mem.svg">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-idle-total-dark.svg">
  <img alt="Total server private memory while holding 1000 and 5000 idle connections" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-idle-total.svg">
</picture>

Idle keep-alive connections (each made one request, then held open), private
bytes of the server's process tree:

<table>
<thead><tr><th>server</th><th align="right">at rest</th><th align="right">holding 5,000</th><th align="right">per connection</th><th align="right">after they close</th><th align="right">handles / conn</th></tr></thead>
<tbody>
<tr><td><b>cadeloop</b> <code>serve()</code></td><td align="right">30 MiB</td><td align="right" style="background-color: #00f0d11a"><b>68 MiB</b></td><td align="right" style="background-color: #00f0d11a"><b>6.6 KiB</b></td><td align="right" style="background-color: #00f0d11a"><b>42 MiB</b></td><td align="right">1</td></tr>
<tr><td>cadeloop 1.0.1</td><td align="right">31 MiB</td><td align="right">365 MiB</td><td align="right">54.6 KiB</td><td align="right">355 MiB</td><td align="right">1</td></tr>
<tr><td>uvicorn + cadeloop</td><td align="right">36 MiB</td><td align="right">106 MiB</td><td align="right">11.2 KiB</td><td align="right">67 MiB</td><td align="right">1*</td></tr>
<tr><td>uvicorn + winloop</td><td align="right">61 MiB</td><td align="right">98 MiB</td><td align="right">6.4 KiB</td><td align="right">79 MiB</td><td align="right">1</td></tr>
<tr><td>uvicorn + asyncio</td><td align="right">25 MiB</td><td align="right">376 MiB</td><td align="right">57.9 KiB</td><td align="right">376 MiB</td><td align="right">1</td></tr>
<tr><td>hypercorn</td><td align="right">23 MiB</td><td align="right">408 MiB</td><td align="right">63.0 KiB</td><td align="right">408 MiB</td><td align="right">1</td></tr>
</tbody>
</table>

\* 2 in this run: cadeloop duplicated the socket that uvicorn asks every
connection for. It now wraps the connection's own handle. Re-measured
against this build in a later session: 1.00 handle per connection at
1,000 and 5,000, and 11.7–12.1 KiB of private memory per connection at
5,000 against this build's 11.2–11.3 KiB in the same session
([follow-up A/B](docs/benchmarks-windows.md#follow-up-ab-socket-handle-and-websocket-receive-path)).

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-http-mem-dark.svg">
  <img alt="Server private memory at rest and at its peak under each HTTP load" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-http-mem.svg">
</picture>

Under load, cadeloop peaks between 33 and 42 MiB of private memory across
the workloads (31 MiB at rest). uvicorn on stdlib asyncio ranges 29–40 MiB,
hypercorn 28–50 MiB (its peak is connection churn), uvicorn on cadeloop
37–45 MiB, and uvicorn on winloop holds 61 MiB from the start (12 threads).
In each rep the workloads run in sequence against one server process, so a
peak also reflects whatever the process kept from the workloads before it.

#### WebSocket

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-ws-dark.svg">
  <img alt="WebSocket echo messages per second" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-ws.svg">
</picture>

Echo of each message back to the client; messages per second, with p99
round-trip latency:

<table>
<thead><tr><th>workload</th><th align="right">cadeloop</th><th align="right">uvicorn + winloop</th><th align="right">uvicorn + asyncio</th><th align="right">hypercorn</th></tr></thead>
<tbody>
<tr><td>64 B, 1 connection</td><td align="right" style="background-color: #00f0d11a"><b>48.3 K <sub>45 µs</sub></b></td><td align="right">22.8 K <sub>133 µs</sub></td><td align="right">17.7 K <sub>137 µs</sub></td><td align="right">15.5 K <sub>160 µs</sub></td></tr>
<tr><td>64 B, 64 connections</td><td align="right" style="background-color: #00f0d11a"><b>77.8 K <sub>999 µs</sub></b></td><td align="right">43.3 K <sub>3.93 ms</sub></td><td align="right">31.3 K <sub>4.52 ms</sub></td><td align="right">26.3 K <sub>3.18 ms</sub></td></tr>
<tr><td>16 KiB, 16 connections</td><td align="right" style="background-color: #00f0d11a"><b>47.6 K <sub>463 µs</sub></b></td><td align="right">21.0 K <sub>2.52 ms</sub></td><td align="right">18.5 K <sub>3.24 ms</sub></td><td align="right">15.4 K <sub>1.46 ms</sub></td></tr>
<tr><td>1 KiB stream, 32 in flight ×16</td><td align="right" style="background-color: #00f0d11a"><b>407 K <sub>1.67 ms</sub></b></td><td align="right">92.1 K <sub>17.56 ms</sub></td><td align="right">66.4 K <sub>19.14 ms</sub></td><td align="right">56.7 K <sub>12.06 ms</sub></td></tr>
</tbody>
</table>

#### TLS

<table>
<thead><tr><th>workload</th><th align="right">cadeloop</th><th align="right">uvicorn + winloop</th><th align="right">uvicorn + asyncio</th></tr></thead>
<tbody>
<tr><td>plaintext, 64 keep-alive conns</td><td align="right" style="background-color: #00f0d11a"><b>62.6 K <sub>1.39 ms</sub></b></td><td align="right">15.7 K <sub>7.93 ms</sub></td><td align="right">9.2 K <sub>8.32 ms</sub></td></tr>
<tr><td>new connection per request (16)</td><td align="right" style="background-color: #00f0d11a"><b>3.6 K <sub>2.85 ms</sub></b></td><td align="right">2.3 K <sub>4.59 ms</sub></td><td align="right">1.5 K <sub>3.05 ms</sub></td></tr>
</tbody>
</table>

TLS 1.3 through Python's `ssl` module in every server; `new connection per
request` includes the full handshake.

#### Timers and wakeups

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-timers-dark.svg">
  <img alt="asyncio.sleep overshoot: cadeloop 0.55 ms past a 1 ms deadline, asyncio, winloop and cadeloop 1.0.1 about 14.4 ms" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-timers.svg">
</picture>

How late `asyncio.sleep(d)` returns on an otherwise idle loop (median /
p99), and how long a `call_soon_threadsafe()` from another thread takes to
run on a parked loop:

<table>
<thead><tr><th></th><th align="right">sleep(1 ms)</th><th align="right">sleep(5 ms)</th><th align="right">sleep(10 ms)</th><th align="right">cross-thread wakeup</th></tr></thead>
<tbody>
<tr><td><b>cadeloop</b></td><td align="right" style="background-color: #00f0d11a"><b>+0.55 / 0.88 ms</b></td><td align="right" style="background-color: #00f0d11a"><b>+0.40 / 0.63 ms</b></td><td align="right" style="background-color: #00f0d11a"><b>+0.31 / 0.82 ms</b></td><td align="right">16 / 117 µs</td></tr>
<tr><td>cadeloop 1.0.1</td><td align="right">+14.41 / 15.50 ms</td><td align="right">+10.60 / 15.97 ms</td><td align="right">+5.66 / 15.52 ms</td><td align="right">16 / 91 µs</td></tr>
<tr><td>winloop</td><td align="right">+14.49 / 15.41 ms</td><td align="right">+10.73 / 15.17 ms</td><td align="right">+6.00 / 12.47 ms</td><td align="right">19 / 66 µs</td></tr>
<tr><td>asyncio</td><td align="right">+14.33 / 15.51 ms</td><td align="right">+10.82 / 15.22 ms</td><td align="right">+5.88 / 11.31 ms</td><td align="right">68 / 162 µs</td></tr>
</tbody>
</table>

A Windows wait with a millisecond timeout ends on the next system clock
interrupt, 15.6 ms apart by default, so every loop that parks that way turns
a 1 ms sleep into ~15 ms — which is also why the `1 ms await` HTTP workload
above is the one where the stdlib and winloop rows fall furthest behind.
cadeloop ends a timer-bound wait on a high-resolution waitable timer
instead; its remaining ~0.5 ms is the platform's own floor (CPython 3.11's
`time.sleep(0.001)` overshoots by the same amount on this machine). Under
load, when no loop parks, all of them are on time. winloop fires sub-ms
timers early (0.5 ms sleeps return at once), and on a busy loop stdlib
asyncio fires timers up to its 15.6 ms clock resolution early.

#### TCP echo — the loop alone

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-echo-dark.svg">
  <img alt="TCP echo messages per second on cadeloop, winloop, rsloop and asyncio" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-echo.svg">
</picture>

The same `asyncio.Protocol` echo server on every loop; messages per second
(p99):

<table>
<thead><tr><th>workload</th><th align="right">cadeloop</th><th align="right">winloop</th><th align="right">rsloop</th><th align="right">asyncio</th></tr></thead>
<tbody>
<tr><td>1 KiB, 1 connection</td><td align="right" style="background-color: #00f0d11a"><b>51.8 K <sub>41 µs</sub></b></td><td align="right">40.4 K <sub>54 µs</sub></td><td align="right">40.4 K <sub>48 µs</sub></td><td align="right">31.5 K <sub>79 µs</sub></td></tr>
<tr><td>1 KiB, 64 connections</td><td align="right">84.4 K <sub>909 µs</sub></td><td align="right">80.1 K <sub>1.56 ms</sub></td><td align="right" style="background-color: #00f0d11a"><b>87.7 K <sub>958 µs</sub></b></td><td align="right">53.6 K <sub>1.54 ms</sub></td></tr>
<tr><td>1 KiB, 16 in flight ×64</td><td align="right">346 K <sub>3.44 ms</sub></td><td align="right">375 K <sub>5.18 ms</sub></td><td align="right" style="background-color: #00f0d11a"><b>457 K <sub>2.49 ms</sub></b></td><td align="right">287 K <sub>4.46 ms</sub></td></tr>
<tr><td>64 KiB, 8 connections</td><td align="right">29.8 K <sub>614 µs</sub></td><td align="right">26.8 K <sub>745 µs</sub></td><td align="right" style="background-color: #00f0d11a"><b>43.7 K <sub>324 µs</sub></b></td><td align="right">31.4 K <sub>459 µs</sub></td></tr>
</tbody>
</table>

rsloop runs its socket I/O on worker threads: it uses 1.5–2 cores (26
threads) where the others use one, and more CPU per message than cadeloop
(4.4 vs 2.9 µs with 16 in flight). With one connection, cadeloop spins for
up to 20 µs before parking (`spin_us`), which is why it answers fastest and
also why it shows a full core busy at that load.

#### Overload and recovery

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-overload-dark.svg">
  <img alt="p99 latency per second through 10 seconds at 150 percent of each server's capacity: every server keeps serving and recovers once the backlog drains" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-overload.svg">
</picture>

Open-loop load at 50% of each server's own measured capacity, then 150% for
10 s, then 50% again, from 256 connections; latency is measured from when
each request was due, so the backlog shows. Every server kept serving at
capacity through the overload without errors or collapse, and recovered
once the backlog drained; cadeloop's capacity here is 77.9 K req/s against
18.4 K for uvicorn on cadeloop, 18.1 K on winloop and 10.9 K on asyncio.

#### Scheduling microbenchmarks

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-sched-dark.svg">
  <img alt="Windows scheduling microbenchmarks as a share of the fastest loop per benchmark" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-win-sched.svg">
</picture>

Median of 5 runs after 3 warmups, a fresh process per run; millions of operations per second ("hung": rsloop 0.1.30 does not finish the timer benchmarks, killed at 12 s):

<table>
<thead><tr><th>benchmark</th><th align="right">cadeloop</th><th align="right">winloop</th><th align="right">rsloop</th><th align="right">asyncio</th></tr></thead>
<tbody>
<tr><td><code>task_fib</code></td><td align="right">0.194</td><td align="right">0.186</td><td align="right" style="background-color: #00f0d11a"><b>0.196</b></td><td align="right">0.125</td></tr>
<tr><td><code>call_soon_chain</code></td><td align="right" style="background-color: #00f0d11a"><b>11.027</b></td><td align="right">2.347</td><td align="right">9.785</td><td align="right">0.906</td></tr>
<tr><td><code>call_soon_burst</code></td><td align="right" style="background-color: #00f0d11a"><b>12.652</b></td><td align="right">1.596</td><td align="right">hung</td><td align="right">1.170</td></tr>
<tr><td><code>timer_schedule_cancel</code></td><td align="right" style="background-color: #00f0d11a"><b>6.732</b></td><td align="right">0.744</td><td align="right">hung</td><td align="right">0.910</td></tr>
<tr><td><code>timer_fire</code></td><td align="right" style="background-color: #00f0d11a"><b>4.619</b></td><td align="right">2.016</td><td align="right">hung</td><td align="right">0.530</td></tr>
<tr><td><code>sleep0_chain</code></td><td align="right" style="background-color: #00f0d11a"><b>3.635</b></td><td align="right">1.555</td><td align="right">3.265</td><td align="right">0.758</td></tr>
<tr><td><code>task_spawn</code></td><td align="right">0.565</td><td align="right">0.497</td><td align="right" style="background-color: #00f0d11a"><b>0.567</b></td><td align="right">0.381</td></tr>
<tr><td><code>threadsafe_throughput</code></td><td align="right" style="background-color: #00f0d11a"><b>8.066</b></td><td align="right">2.570</td><td align="right">4.581</td><td align="right">0.069</td></tr>
<tr><td><code>future_chain</code></td><td align="right">2.725</td><td align="right">0.945</td><td align="right" style="background-color: #00f0d11a"><b>2.820</b></td><td align="right">0.405</td></tr>
<tr><td><code>gather_fanin</code></td><td align="right" style="background-color: #00f0d11a"><b>0.613</b></td><td align="right">0.519</td><td align="right">0.572</td><td align="right">0.334</td></tr>
<tr><td><code>queue_pingpong</code></td><td align="right" style="background-color: #00f0d11a"><b>1.871</b></td><td align="right">1.765</td><td align="right">1.853</td><td align="right">1.759</td></tr>
<tr><td><code>queue_pingpong_native</code></td><td align="right" style="background-color: #00f0d11a"><b>6.055</b></td><td align="right">5.369</td><td align="right">5.927</td><td align="right">4.977</td></tr>
</tbody>
</table>

#### What this release changed on Windows

cadeloop 1.0.1 against the current build, same machine, same session,
three rotated runs each:

<table>
<thead><tr><th>workload</th><th align="right">1.0.1</th><th align="right">now</th><th align="right">change</th></tr></thead>
<tbody>
<tr><td>HTTP plaintext, 64 conns</td><td align="right">74.8 K</td><td align="right">78.0 K</td><td align="right">+4.3%</td></tr>
<tr><td>HTTP JSON</td><td align="right">56.1 K</td><td align="right">57.8 K</td><td align="right">+2.9%</td></tr>
<tr><td>HTTP 64 KiB body</td><td align="right">39.1 K</td><td align="right">41.2 K</td><td align="right">+5.5%</td></tr>
<tr><td>HTTP 1 MiB stream</td><td align="right">3.1 K</td><td align="right">3.3 K</td><td align="right">+5.1%</td></tr>
<tr><td>HTTP 16 KiB upload</td><td align="right">61.3 K</td><td align="right">61.1 K</td><td align="right">-0.4%</td></tr>
<tr><td>WebSocket 64 B, 64 conns</td><td align="right">74.2 K</td><td align="right">77.8 K</td><td align="right">+4.9%</td></tr>
<tr><td>TCP echo 1 KiB, 64 conns</td><td align="right">80.5 K</td><td align="right">84.4 K</td><td align="right">+4.8%</td></tr>
<tr><td>HTTP with a 1 ms await</td><td align="right">4.5 K</td><td align="right">31.6 K</td><td align="right">7.0×</td></tr>
<tr><td>memory per idle connection (5,000)</td><td align="right">54.6 KiB</td><td align="right">6.6 KiB</td><td align="right">-88%</td></tr>
<tr><td>memory kept after 5,000 close</td><td align="right">+324 MiB</td><td align="right">+12 MiB</td><td></td></tr>
<tr><td><code>asyncio.sleep(0.001)</code> overshoot</td><td align="right">14.4 ms</td><td align="right">0.55 ms</td><td align="right">26× sooner</td></tr>
</tbody>
</table>

The fixes behind these, and the correctness fixes that came with them, are
listed in the [engineering record](docs/README.md).

### Linux (epoll dev backend)

Linux 6.18.44, 4 vCPU Intel Xeon @ 2.10 GHz, CPython 3.11.17. Loopback,
single box, load generator sharing the cores. Linux runs the `epoll`
development backend, not the Windows/IOCP production target.

#### HTTP / ASGI

`wrk -t2 -c64`, 3s warmup + 3×10s measured, median. Single worker, plaintext
`Hello, World!` ASGI app. No contender returned a non-2xx response or a socket
error.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-http-ranked-dark.svg">
  <img alt="HTTP throughput: cadeloop serve() 105.4K req/s, granian 44.7K, uvicorn+httptools/uvloop 27.4K, uvicorn+h11 6.9-10.7K, hypercorn 4.2K" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-http-ranked.svg">
</picture>

<table>
<thead><tr><th>server</th><th>parsing</th><th>loop</th><th align="right">req/s</th><th align="right">p50</th><th align="right">p99</th></tr></thead>
<tbody>
<tr><td><b>cadeloop</b> <code>serve()</code></td><td>Rust</td><td>cadeloop</td><td align="right" style="background-color: #00f0d11a"><b>105.4 K</b></td><td align="right" style="background-color: #00f0d11a"><b>0.53 ms</b></td><td align="right" style="background-color: #00f0d11a"><b>2.21 ms</b></td></tr>
<tr><td>granian</td><td>Rust (hyper)</td><td>its own</td><td align="right">44.7 K</td><td align="right">1.36 ms</td><td align="right">3.66 ms</td></tr>
<tr><td>uvicorn + httptools</td><td>C</td><td>uvloop</td><td align="right">27.4 K</td><td align="right">2.17 ms</td><td align="right">4.84 ms</td></tr>
<tr><td>uvicorn + httptools</td><td>C</td><td>asyncio</td><td align="right">21.3 K</td><td align="right">2.75 ms</td><td align="right">6.03 ms</td></tr>
<tr><td>uvicorn + h11</td><td>Python</td><td>cadeloop</td><td align="right">10.7 K</td><td align="right">5.59 ms</td><td align="right">10.71 ms</td></tr>
<tr><td>uvicorn + h11</td><td>Python</td><td>rsloop</td><td align="right">7.5 K</td><td align="right">8.01 ms</td><td align="right">13.56 ms</td></tr>
<tr><td>uvicorn + h11</td><td>Python</td><td>uvloop</td><td align="right">7.1 K</td><td align="right">8.38 ms</td><td align="right">15.30 ms</td></tr>
<tr><td>uvicorn + h11</td><td>Python</td><td>rloop</td><td align="right">6.9 K</td><td align="right">8.66 ms</td><td align="right">17.68 ms</td></tr>
<tr><td>uvicorn + h11</td><td>Python</td><td>asyncio</td><td align="right">6.9 K</td><td align="right">8.67 ms</td><td align="right">14.18 ms</td></tr>
<tr><td>hypercorn</td><td>Python</td><td>asyncio</td><td align="right">4.2 K</td><td align="right">14.07 ms</td><td align="right">24.13 ms</td></tr>
</tbody>
</table>

Holding uvicorn+h11 fixed, cadeloop is 1.54× stdlib asyncio and 1.42–1.54×
the other loops. Holding the loop fixed at cadeloop, `serve()` is 9.9×
uvicorn+h11. The comparable stacks — those that also moved parsing out of
Python — are granian (2.4×) and uvicorn+httptools+uvloop (3.8×).

#### Scheduling — recursive async `fib(21)`

The composite workload: `fib(k)` spawns its two recursive calls as concurrent
tasks, so one call expands into 35,421 coroutine calls across a deep, irregular
task tree. Task creation, the ready queue, future resolution, and `gather`
bookkeeping all run together, in the interleaving real application code
produces — the event-loop analogue of naive recursive `fib()`. Both the result
and the node count are verified, so a loop cannot win by skipping work.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-sched-task-fib-dark.svg">
  <img alt="Recursive async fib(21): uvloop 79.4K calls/s, rsloop 77.4K, rloop 74.9K, cadeloop 71.8K, asyncio 50.4K" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-sched-task-fib.svg">
</picture>

<table>
<thead><tr><th>loop</th><th align="right">K coroutine calls/s</th><th align="right">vs asyncio</th></tr></thead>
<tbody>
<tr><td>uvloop</td><td align="right">79.4</td><td align="right">1.57×</td></tr>
<tr><td>rsloop</td><td align="right">77.4</td><td align="right">1.53×</td></tr>
<tr><td>rloop</td><td align="right">74.9</td><td align="right">1.49×</td></tr>
<tr><td><b>cadeloop</b></td><td align="right" style="background-color: #00f0d11a"><b>71.8</b></td><td align="right" style="background-color: #00f0d11a"><b>1.42×</b></td></tr>
<tr><td>asyncio</td><td align="right">50.4</td><td align="right">1.00×</td></tr>
</tbody>
</table>

Every native loop lands in a 1.4–1.6× band over stdlib, and the four sit within
11% of each other — about the size of this machine's run-to-run noise on this
workload. Most of the time here goes to CPython's coroutine machinery, which no
loop replaces, so the single-operation gaps below mostly wash out.

#### Scheduling microbenchmarks

Single-operation isolation, 3 warmup + 5 measured runs per pass, fresh process
per run; three passes, so each figure is the median of 15 runs. Millions of
ops/second.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-sched-dark.svg">
  <img alt="Scheduling speedup vs stdlib asyncio across twelve scheduling benchmarks" src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/bench-sched.svg">
</picture>

<table>
<thead><tr><th>benchmark</th><th align="right">cadeloop</th><th align="right">asyncio</th><th align="right">uvloop</th><th align="right">rloop</th><th align="right">rsloop</th></tr></thead>
<tbody>
<tr><td><code>call_soon_chain</code></td><td align="right" style="background-color: #00f0d11a"><b>5.821</b></td><td align="right">0.551</td><td align="right">1.773</td><td align="right">4.366</td><td align="right">2.534</td></tr>
<tr><td><code>call_soon_burst</code></td><td align="right">2.371</td><td align="right">0.751</td><td align="right">1.218</td><td align="right" style="background-color: #00f0d11a"><b>2.829</b></td><td align="right">1.240</td></tr>
<tr><td><code>timer_schedule_cancel</code></td><td align="right">1.798</td><td align="right">0.524</td><td align="right">0.447</td><td align="right" style="background-color: #00f0d11a"><b>2.018</b></td><td align="right">1.148</td></tr>
<tr><td><code>timer_fire</code></td><td align="right">1.434</td><td align="right">0.293</td><td align="right">1.223</td><td align="right" style="background-color: #00f0d11a"><b>1.498</b></td><td align="right">1.200</td></tr>
<tr><td><code>sleep0_chain</code></td><td align="right">1.778</td><td align="right">0.411</td><td align="right">1.059</td><td align="right" style="background-color: #00f0d11a"><b>1.843</b></td><td align="right">1.297</td></tr>
<tr><td><code>task_spawn</code></td><td align="right">0.274</td><td align="right">0.200</td><td align="right">0.276</td><td align="right" style="background-color: #00f0d11a"><b>0.279</b></td><td align="right">0.278</td></tr>
<tr><td><code>threadsafe_throughput</code></td><td align="right">2.646</td><td align="right">0.060</td><td align="right">1.726</td><td align="right">3.810</td><td align="right" style="background-color: #00f0d11a"><b>3.997</b></td></tr>
<tr><td><code>future_chain</code></td><td align="right" style="background-color: #00f0d11a"><b>1.231</b></td><td align="right">0.233</td><td align="right">0.654</td><td align="right">1.127</td><td align="right">0.777</td></tr>
<tr><td><code>gather_fanin</code></td><td align="right">0.256</td><td align="right">0.176</td><td align="right">0.246</td><td align="right" style="background-color: #00f0d11a"><b>0.279</b></td><td align="right">0.268</td></tr>
<tr><td><code>queue_pingpong</code></td><td align="right" style="background-color: #00f0d11a"><b>1.348</b></td><td align="right">1.211</td><td align="right">1.294</td><td align="right">1.170</td><td align="right">1.253</td></tr>
<tr><td><code>queue_pingpong_native</code></td><td align="right" style="background-color: #00f0d11a"><b>3.443</b></td><td align="right">2.672</td><td align="right">3.029</td><td align="right">2.902</td><td align="right">2.891</td></tr>
</tbody>
</table>

cadeloop beats stdlib asyncio on all eleven and uvloop on ten of eleven, and
leads outright on four: `call_soon_chain` (10.6× asyncio), `future_chain`, and
both queue workloads. rloop 0.5 leads most of the rest, by 2–19%;
`threadsafe_throughput` is the one wide gap, where rsloop and rloop are 1.4–1.5×
ahead.

`queue_pingpong_native` is the same workload on `cadeloop.Queue` instead of
`asyncio.Queue`. The queue is loop-agnostic, so it runs under every contender,
and the pair isolates what the stdlib queue's pure-Python `put`/`get` costs:
2.6× on cadeloop, and 2.2–2.5× on every other loop. That gap is the queue,
not the loop.

---

## Usage guide

cadeloop has three entry points, and they stack: a **loop**, a **server** built
on that loop, and a **CLI** wrapping that server.

<table>
<tr>
<td width="33%" align="center">
  <img src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/brand/icon-loop.svg" width="36" alt=""><br>
  <b><a href="#1-as-a-drop-in-event-loop">Event loop</a></b><br>
  <sub>Swap the loop under code you already have</sub>
</td>
<td width="33%" align="center">
  <img src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/brand/icon-modules.svg" width="36" alt=""><br>
  <b><a href="#2-as-an-asgi-server-cadeloopserve">ASGI server</a></b><br>
  <sub><code>serve()</code> — HTTP parsed and serialized in Rust</sub>
</td>
<td width="33%" align="center">
  <img src="https://raw.githubusercontent.com/cadezawacki/cadeloop/main/docs/assets/brand/icon-terminal.svg" width="36" alt=""><br>
  <b><a href="#3-from-the-command-line">Command line</a></b><br>
  <sub><code>cadeloop myapp:app</code>, every tunable a flag</sub>
</td>
</tr>
</table>

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

`workers=0` starts one worker per CPU the process may run on — its affinity
mask (`taskset`, a container's cpuset), not the machine's CPU count.

Two process models, chosen automatically:

- **POSIX** — `fork` + `SO_REUSEPORT`; each worker has its own listener and the
  kernel balances accepts. The application is imported once, in the supervisor,
  and its heap is frozen there before forking (`gc_mode="freeze"`), so workers
  share it instead of copying it. Starting a pool on a port something already
  holds fails with `EADDRINUSE` rather than silently sharing the port.
- **Windows** — `spawn`: the supervisor owns the listener and the accept loop
  and hands each accepted connection to a worker (`WSADuplicateSocketW` per
  connection); workers re-import the app (pass the `"module:attribute"`
  string) using the supervisor's `sys.path`.

Workers are supervised and restarted. A worker slot that keeps dying without
ever serving, or right after starting, gives up after five tries; a worker that
served and then crashed is simply restarted. A worker stopped by a signal sent
to it alone (recycling one bloated worker) is replaced; the application calling
`loop.stop()` stops the pool. Workers also stop, gracefully, if the supervisor
dies. `pin=True` pins each worker's event-loop thread to a CPU (opt-in: see
`Config.pin`).

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
Validation is **eager**: out-of-range values raise `ValueError` at construction;
values of the wrong type (`access_log="false"`, `workers="4"`) and unknown keyword
arguments raise `TypeError`.

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
| `latency_mode` | `str` | `"balanced"` | Preset controlling spin and flush behaviour: `"throughput"` (spin 0µs), `"balanced"` (20µs while spins catch work), `"spin"` (200µs before every park + immediate flush). |
| `spin_us` | `int \| None` | `None` | Explicit spin window in µs, overriding the preset. `None` derives it from `latency_mode`. |
| `adaptive_spin` | `bool \| None` | `None` | Spin only while spins are catching work, rather than before every park. `None` derives from `latency_mode` (on unless `"spin"`). |
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
| `warmup` | `int` | `1000` | With `gc_mode="freeze"`: requests served before a second `gc.freeze()` takes in what the first requests built lazily (route and template caches, compiled statements), so full collections stop re-walking them. `0` freezes at startup only. Must be ≥ 0. |

#### HTTP engine

| field | type | default | meaning |
|---|---|---|---|
| `max_header_bytes` | `int` | `65536` | Maximum total header bytes per request. Over it → `431`. Must be ≥ 1. |
| `max_headers` | `int` | `100` | Maximum header count per request. Must be ≥ 1. |
| `max_url` | `int` | `8192` | Maximum request-target length. Must be ≥ 1. |
| `request_line_timeout` | `float` | `5.0` | Seconds a connection may stay silent before sending a complete request head. Guards slowloris. `0` disables. |
| `keepalive_idle` | `float` | `75.0` | Seconds an idle keep-alive connection is held open. `0` disables the idle timeout: idle connections stay open until the client closes them. |
| `max_body` | `int \| None` | `16777216` | Maximum request body bytes; over it → `413`. **Finite on purpose**: the engine buffers the whole body before dispatch, so `None` (unlimited) lets an unauthenticated client turn one request into unbounded memory. |

#### Transports

| field | type | default | meaning |
|---|---|---|---|
| `write_high_water` | `int` | `65536` | Backpressure high-water mark in bytes. Must be ≥ 0. |
| `write_low_water` | `int` | `16384` | Backpressure low-water mark. Must be ≥ 0 and ≤ `write_high_water`. |

#### Multi-process

| field | type | default | meaning |
|---|---|---|---|
| `workers` | `int` | `0` | Worker processes. `0` means one per CPU the process may run on (its affinity mask). Must be ≥ 0. |
| `pin` | `bool` | `False` | Pin each worker's event-loop thread to one CPU of the process's affinity mask (threads it starts are not pinned). Off by default: on loopback it measured no gain, and it hurts on shared or virtualised CPUs. Worth it on dedicated hardware with RSS-aligned NIC queues. |
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
| `workers` | `int` | `1` | Worker processes; `0` means one per CPU the process may run on. |
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

`APP` is a required `module:attribute` spec, imported from the current directory
(or `--app-dir`). It is validated before anything binds, so a typo fails
immediately rather than after the port is taken.

| flag | maps to | notes |
|---|---|---|
| `--app-dir DIR` | — | Directory to import the app from. Default: the current one. |
| `--host HOST` | — | Bind address. |
| `--port`, `-p PORT` | — | Bind port. |
| `--workers`, `-w N` | `workers` | `0` = one per CPU. |
| `--backend {auto,epoll}` | `backend` | Choices are platform-dependent (`iocp` on Windows). |
| `--latency-mode {throughput,balanced,spin}` | `latency_mode` | |
| `--access-log` / `--no-access-log` | `access_log` | |
| `--spin-us N` | `spin_us` | Accepts `none` to derive from `--latency-mode`. |
| `--adaptive-spin` / `--no-adaptive-spin` | `adaptive_spin` | |
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
| `CADELOOP_COARSE_TIMERS` | `Loop()` (Windows) | Parks with a timer pending end on the system clock tick (15.6 ms by default), as they did before high-resolution parks. For A/B measurement and diagnosis only. |

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
| `xthread_items` | Callbacks and timers other threads handed over while the loop was parked in its poll. Calls made while the loop is reachable -- including every `call_soon_threadsafe` from its own thread -- go straight to the ready queue and are not counted. |
| `spins` | Waits preceded by a spin. With adaptive spinning, far fewer than `polls` while spins catch nothing. |
| `spin_hits` | Spins that caught work arriving during the window, saving a blocking wait. Work already waiting when the spin began is not counted. |
| `precise_parks` | Waits bounded by a timer's deadline that ended on a high-resolution timer (Windows IOCP) rather than the system clock tick. |
| `ready_len`, `timers_len` | Current ready-queue and timer-heap depth. |
| `connections`, `listeners` | Live counts. |
| `buffers_in_use` | Buffer slots currently held by kernel ops or exported memoryviews. |
| `buffer_bytes_in_use`, `buffer_bytes_held` | Bytes those slots occupy, and bytes the buffer pool keeps from the OS in all (on Windows, its commit charge). Free slots beyond a warm set of 256 per size class are handed back, so the second falls again after a burst. |
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

### Choosing a latency mode

`latency_mode` sets three things at once: how long the loop spins before making
a blocking kernel wait, whether it spins before every wait or only while spinning
is catching work, and whether responses are flushed immediately or corked until
the tick's flush phase.

| mode | `spin_us` | `adaptive_spin` | `immediate_flush` | behaviour |
|---|---:|---|---|---|
| `throughput` | 0 | — | off | Never spins. Every idle tick makes a blocking wait; writes coalesce and go out at tick end. Lowest CPU per request, highest syscall efficiency. |
| `balanced` *(default)* | 20 | on | off | Spins up to 20 µs before parking while spins are catching work, and parks straight away while they are not. |
| `spin` | 200 | off | on | Spins 200 µs before every park *and* writes each response the moment it is ready. Trades CPU and syscalls for tail latency. |

**Spinning** removes the sleep/wake round trip when work arrives just after the
loop decides to park. A spin that nothing arrives during is its whole window of
CPU spent for nothing: spinning 200 µs before every park, a loop serving light
traffic burns close to a core doing nothing.

**Adaptive spinning** (`adaptive_spin`) spins only while spins pay. A spin that
catches work earns credit and one that catches nothing spends it, so the loop
keeps spinning while more than about one spin in four catches work, however
the misses bunch together, and parks without spinning below that. Stopped, it
probes with a spin one park in eight; the first probe that catches work turns
spinning back on, so traffic that turns dense gets it back within a few parks.
Work already waiting when a spin begins does not count as a catch, since a park
would have returned just as fast. The bookkeeping is a few integer operations
per park.
`stats()["spins"]` counts the parks that spun and `stats()["spin_hits"]` the
spins that caught work. `spin` mode turns it off: picking `spin` means the
loop has the core to itself. Measured numbers are in
[`docs/ops.md`](docs/ops.md#latency-modes-r-060).

**Immediate flush** matters for tail latency specifically. With corking, a
response that became wire-ready first still waits behind however many *other*
connections' app dispatch the same tick batched ahead of it. Flushing
immediately puts it on the wire straight away. A response sent as one body
message is one send either way; streamed chunks and pipelined responses are
where corking saves sends, by coalescing what one tick produced for a
connection. Either way nothing corked is lost: `close()` and `abort()` hand any
corked output to the kernel first.

The settings are independent, so a mixed setting is legitimate:

```python
# spin before every park for wake-up latency, but keep corking
cadeloop.serve(app, "0.0.0.0", 8000, spin_us=200, adaptive_spin=False, immediate_flush=False)
```

Measure before committing. On Linux loopback, immediate flush measured as a
net loss: the same one send per request, but less throughput and more server
CPU per request (numbers in [`docs/ops.md`](docs/ops.md#latency-modes-r-060)).
`stats()["sends_posted"]` read against `stats()["bytes_sent"]` shows what
corking is actually buying.

### Throughput-oriented services

Let writes cork and batch, and skip the spin window entirely:

```bash
cadeloop myapp:app -w 0 --latency-mode throughput --keepalive-idle 120
```

### Accepting large request bodies

`max_body` defaults to **16 MiB**. The engine buffers a whole request body in
memory before dispatching it to your app, so the cap is a memory bound, not
just a policy: peak usage is roughly `max_body x concurrent uploads`.

Say you accept 200 MB video uploads, 8 at a time:

```python
cadeloop.serve(
    app, "0.0.0.0", 8000,
    max_body=200 * 1024 * 1024,   # 200 MiB -> a 201 MiB upload gets a 413
    request_line_timeout=60.0,    # slow phones need longer to send the head
    keepalive_idle=120.0,
)
```

That configuration can hold **~1.6 GiB** of request bodies at 8 concurrent
uploads, before your app has allocated anything. Size the worker count and the
box against that number, not against the request rate.

Over the cap the client gets a `413`. `max_body=None` removes it entirely,
which makes memory usage a function of what clients choose to send — only
reasonable behind authentication or a proxy enforcing its own limit.

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
A repeated `SIGTERM` (systemd's default `KillMode=control-group` signals every
process of the unit, and the supervisor forwards its own) does not cut the drain
short. A second Ctrl-C does: it stops waiting for in-flight requests (and in a
worker pool, kills the workers), as uvicorn's force-quit does.

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
