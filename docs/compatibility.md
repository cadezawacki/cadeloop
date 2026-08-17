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
| WebSockets (RFC 6455, native engine) | ✅ tested |
| Native HTTP/1.1 + ASGI engine, lifespan, CLI | ✅ tested (Starlette, FastAPI) |
| Multi-worker (`workers > 1`) | ✅ tested (fork + `SO_REUSEPORT`; spawn + shared listener on Windows) |
| Subprocess (`create_subprocess_exec` / `shell`) | ✅ POSIX; Windows pipes in progress |
| Drop-in with uvicorn, aiohttp | ✅ interop-tested |
| CPython asyncio conformance suite | ✅ runs against the stdlib's own tests |
| RIO backend (`backend="rio"`) | 🔶 implemented; blocked on an OS-level RIO failure on the test machine — `auto` stays on IOCP |
| Native `loop.sendfile` | 🔶 `sock_sendfile` fallback works |

Full requirement-by-requirement map:
[requirements-traceability.md](requirements-traceability.md).
