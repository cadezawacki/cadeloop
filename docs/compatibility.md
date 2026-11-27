# Compatibility

What of the `asyncio` surface is implemented and tested. Install and
usage documentation is in the [README](../README.md).

| surface | state |
|---|---|
| Scheduling: `call_soon`, `call_later`, `call_at`, timers, threadsafe, tasks | ✅ tested |
| TCP transports, `create_server` / `create_connection`, streams | ✅ tested |
| TLS: native termination (`serve(ssl=...)`, https/wss), client `ssl=` / `start_tls` | ✅ tested |
| `sock_*`, `add_reader` / `add_writer`, signals | ✅ tested |
| UDP (`create_datagram_endpoint`) | ✅ tested |
| WebSockets (RFC 6455, native engine; `permessage-deflate` with `ws_compression`) | ✅ tested |
| Native HTTP/1.1 + ASGI engine, lifespan, CLI | ✅ tested (Starlette, FastAPI) |
| Multi-worker (`workers > 1`) | ✅ tested (fork + `SO_REUSEPORT`; spawn + shared listener on Windows) |
| Subprocess (`create_subprocess_exec` / `shell`) | ✅ POSIX; Windows pipes in progress |
| Drop-in with uvicorn, aiohttp | ✅ interop-tested |
| CPython asyncio conformance suite | ✅ runs against the stdlib's own tests |
| RIO backend (`backend="rio"`) | 🔶 experimental, and stopped: it has not worked on any Windows machine tested, for reasons inside Windows. `auto` never selects it; IOCP is the Windows backend |
| Native `loop.sendfile` | 🔶 `sock_sendfile` fallback works |
| ASGI extensions | ✅ `http.response.trailers`, `http.response.pathsend` (Starlette/FastAPI `FileResponse` uses it) |

## Python versions

CPython 3.11, 3.12 and 3.13, with a wheel for each. 3.11 is the primary
target: it is what cadeloop is developed and benchmarked on. Every test
suite -- unit, asyncio parity, the generated differential scenarios, and
that version's own CPython asyncio tests -- runs on all three.

asyncio itself is not the same on the three, and **cadeloop follows the
asyncio of the interpreter it runs on.** Code that moves from 3.11 to 3.13
sees the same changes on cadeloop that it would see on asyncio:

| behavior | 3.11 | 3.12 | 3.13 |
|---|---|---|---|
| `Server.wait_closed()` called after `close()` | returns at once | waits for the server's connections | same as 3.12 |
| `transport.writelines()` | `write(b"".join(...))`: its errors say `write()` | its own method: errors say `writelines()`; takes anything `memoryview()` takes | same as 3.12 |
| `transport.write()` with the wrong type | "must be a bytes-like object" | same as 3.11 | "must be a bytes, bytearray, or memoryview object" |
| a bind failure's message | `...: address already in use` | `...: [errno 98] address already in use` | same as 3.12 |
| a custom exception handler runs in | the context current where the failure was reported | the context of the task or callback that failed | same as 3.12 |
| `Handle.get_context()` | — | ✅ | ✅ |
| a subprocess's exit reaches its transport | one loop iteration later | at once | at once |
| `create_datagram_endpoint(reuse_port=True)` | set for any family | for IPv4 and IPv6 only (Linux 6.12.9 refuses it elsewhere) | same as 3.12 |
| a `Handle`'s repr and "Exception in callback ..." | show the arguments | same as 3.11 | show them in debug mode only |
| `sendto(b"")` on a datagram transport | sends nothing | sends nothing | sends an empty datagram |
| `Server.serve_forever()` cancelled | closes the server | closes the server | closes the server and its connections |
| `create_unix_server()` closes | the socket file stays | the socket file stays | the file is removed (`cleanup_socket=True`) |
| `start_tls()` server-side with bytes already in a `StreamReader` | bytes are lost to the handshake | same as 3.11 | handed to the TLS layer |

What newer versions added as an **argument or a method** works on every
version, since taking it changes nothing for code that does not use it:
`shutdown_default_executor(timeout)`, `create_connection(all_errors=True)`,
`create_server(keep_alive=True)`, `create_unix_server(cleanup_socket=...)`,
`Server.close_clients()` and `Server.abort_clients()`.
`asyncio.run(main(), loop_factory=cadeloop.new_event_loop)` and
`asyncio.eager_task_factory` (both 3.12) work as with any loop.

Three things are deliberately not copied:

- On 3.12, asyncio's `writelines()` on a connection that is already lost
  raises `TypeError: 'NoneType' object is not callable` (and leaves the
  data in a buffer that is never sent). 3.11 and 3.13 drop the data
  silently; cadeloop does that on all three.
- On 3.13, a task made through a task factory without a `name` is named
  `"None"` by asyncio. cadeloop leaves the factory's name alone.
- Within a version cadeloop follows its latest patch release. 3.12.3, the
  Python that Ubuntu 24.04 ships, still words bind failures the 3.11 way;
  cadeloop on any 3.12 words them the newer way.

3.14 is not supported yet: it removes the child watchers that subprocess
support is built on. Free-threaded builds (3.13t) are not supported.

The same cadeloop revision on the three interpreters (Linux loopback, one
worker, 64 connections; requests or messages per second):

| workload | 3.11 | 3.12 | 3.13 |
|---|---:|---:|---:|
| Echo, 1 KiB, `asyncio.Protocol` | 208 K | 217 K | 206 K |
| Echo, 1 KiB, asyncio streams | 139 K | 142 K | 133 K |
| `cadeloop.serve()`, plain-text response | 158 K | 153 K | 149 K |
| `cadeloop.serve()`, FastAPI JSON through a pydantic model | 15.3 K | 18.2 K | 16.2 K |

Where the loop and the server do the work, the interpreter makes little
difference (within 6%); where application code does, a newer interpreter
helps. Treat the columns as indicative: the three interpreters on the
test machine were not built the same way (3.12 was a python-build-standalone
build, the other two the distribution's).

## Known gaps

- **Datagram transports have no write flow control**
  (`set_write_buffer_limits()`, `pause_writing()`), on any version. Output
  is bounded instead: past 1 MiB queued behind the socket, a datagram is
  dropped and `error_received(ENOBUFS)` is called.
- **`cadeloop.Queue` has no `shutdown()`** (`asyncio.Queue.shutdown()`,
  3.13). `asyncio.Queue` works on a cadeloop loop as on any other.
- A cadeloop transport is not an instance of `asyncio.Transport` (a native
  type cannot inherit from it), as with uvloop.

Full requirement-by-requirement map:
[requirements-traceability.md](requirements-traceability.md).
