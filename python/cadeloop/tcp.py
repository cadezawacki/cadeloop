"""TCP surface of the facade: create_connection / create_server / Server,
sock_* readiness helpers, and TLS via the stdlib ``asyncio.sslproto``
compatibility path (R-059: "fall back to a compatibility path built on
ssl.MemoryBIO in Python (correctness first)"; the native OpenSSL engine
replaces it in M4). Mixed into ``cadeloop.Loop``.
"""

from __future__ import annotations

import asyncio
import os
import socket
import sys
from asyncio import sslproto

try:
    import ssl as ssl_module
except ImportError:  # pragma: no cover
    ssl_module = None

__all__ = ["TcpSurface", "Server"]


def _fileno(fd):
    if isinstance(fd, int):
        return fd
    return fd.fileno()


class Server:
    """asyncio.AbstractServer implementation over native listeners."""

    def __init__(self, loop, entries, factory, accept_pool, serving):
        # entries: list of (lid, sockname, rawfd)
        self._loop = loop
        self._entries = entries
        self._factory = factory
        self._accept_pool = accept_pool
        self._serving = serving
        self._closed = False
        self._close_waiters = []
        self._sockets = None

    # -- introspection ----------------------------------------------------

    @property
    def sockets(self):
        if self._sockets is None:
            socks = []
            for _lid, _name, fd in self._entries:
                # Duplicate so the reporting socket's lifetime is
                # independent of the native listener. On Windows a SOCKET
                # is a kernel handle, not a CRT fd — os.dup() raises EBADF
                # there (found by the first Windows run); WSADuplicateSocket
                # via share/fromshare is the documented dup.
                if sys.platform == "win32":
                    tmp = socket.socket(fileno=fd)
                    try:
                        socks.append(socket.fromshare(tmp.share(os.getpid())))
                    finally:
                        tmp.detach()  # never close the native listener
                else:
                    socks.append(socket.socket(fileno=os.dup(fd)))
            self._sockets = socks
        return tuple(self._sockets)

    def get_loop(self):
        return self._loop

    def is_serving(self):
        return self._serving and not self._closed

    # -- lifecycle --------------------------------------------------------

    async def start_serving(self):
        if self._closed:
            raise RuntimeError("server is closed")
        if not self._serving:
            self._serving = True
            for lid, _name, _fd in self._entries:
                self._loop._core.listener_start(lid)

    async def serve_forever(self):
        await self.start_serving()
        try:
            await self._loop.create_future()  # until cancelled
        finally:
            self.close()

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._serving = False
        for lid, _name, _fd in self._entries:
            try:
                self._loop._core.listener_close(lid)
            except RuntimeError:
                pass  # loop already closed
        if self._sockets:
            for s in self._sockets:
                s.close()
        for waiter in self._close_waiters:
            if not waiter.done():
                waiter.set_result(None)
        self._close_waiters.clear()

    async def wait_closed(self):
        if self._closed:
            return
        waiter = self._loop.create_future()
        self._close_waiters.append(waiter)
        await waiter

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.close()
        await self.wait_closed()

    def __repr__(self):
        names = [name for _lid, name, _fd in self._entries]
        return f"<cadeloop.Server sockets={names} serving={self.is_serving()}>"


class TcpSurface:
    """create_connection / create_server / sock_* mixin for Loop."""

    # -- outgoing ---------------------------------------------------------

    async def create_connection(
        self,
        protocol_factory,
        host=None,
        port=None,
        *,
        ssl=None,
        family=0,
        proto=0,
        flags=0,
        sock=None,
        local_addr=None,
        server_hostname=None,
        ssl_handshake_timeout=None,
        ssl_shutdown_timeout=None,
        happy_eyeballs_delay=None,
        interleave=None,
    ):
        if server_hostname is not None and not ssl:
            raise ValueError("server_hostname is only meaningful with ssl")
        if ssl:
            if server_hostname is None:
                if not host:
                    raise ValueError("You must set server_hostname when using ssl without a host")
                server_hostname = host
        if ssl_handshake_timeout is not None and not ssl:
            raise ValueError("ssl_handshake_timeout is only meaningful with ssl")

        if sock is not None:
            if host is not None or port is not None:
                raise ValueError("host/port and sock can not be specified at the same time")
            fd = sock.detach()
            return await self._wrap_outgoing(
                fd, protocol_factory, ssl, server_hostname, ssl_handshake_timeout, ssl_shutdown_timeout
            )

        if host is None or port is None:
            raise ValueError("host and port was not specified and no sock specified")

        infos = await self.getaddrinfo(
            host, port, family=family, type=socket.SOCK_STREAM, proto=proto, flags=flags
        )
        if not infos:
            raise OSError(f"getaddrinfo({host!r}) returned empty list")
        local_ip, local_port = None, 0
        if local_addr is not None:
            local_ip, local_port = local_addr[0], local_addr[1]

        errors = []
        for _af, _st, _pr, _cname, address in infos:
            fut = self.create_future()
            try:
                self._core.tcp_connect(address[0], address[1], fut, local_ip, local_port)
                fd = await fut
            except OSError as exc:
                errors.append(exc)
                continue
            return await self._wrap_outgoing(
                fd, protocol_factory, ssl, server_hostname, ssl_handshake_timeout, ssl_shutdown_timeout
            )
        if len(errors) == 1:
            raise errors[0]
        raise OSError(
            f"Multiple exceptions: {', '.join(str(e) for e in errors)}"
        ) from (errors[0] if errors else None)

    async def _wrap_outgoing(
        self, fd, protocol_factory, ssl, server_hostname, ssl_handshake_timeout, ssl_shutdown_timeout
    ):
        if not ssl:
            protocol = protocol_factory()
            transport = self._core.attach_stream(fd, protocol)
            return transport, protocol
        sslcontext = self._make_ssl_context(ssl, server_side=False)
        app_protocol = protocol_factory()
        waiter = self.create_future()
        protocol = self._make_ssl_protocol(
            app_protocol,
            sslcontext,
            waiter,
            server_side=False,
            server_hostname=server_hostname,
            ssl_handshake_timeout=ssl_handshake_timeout,
            ssl_shutdown_timeout=ssl_shutdown_timeout,
        )
        self._core.attach_stream(fd, protocol)
        await waiter  # handshake
        return protocol._app_transport, app_protocol

    async def start_tls(
        self,
        transport,
        protocol,
        sslcontext,
        *,
        server_side=False,
        server_hostname=None,
        ssl_handshake_timeout=None,
        ssl_shutdown_timeout=None,
    ):
        """Upgrade an established transport to TLS (sslproto path)."""
        waiter = self.create_future()
        ssl_protocol = self._make_ssl_protocol(
            protocol,
            sslcontext,
            waiter,
            server_side=server_side,
            server_hostname=server_hostname,
            ssl_handshake_timeout=ssl_handshake_timeout,
            ssl_shutdown_timeout=ssl_shutdown_timeout,
            call_connection_made=False,
        )
        transport.pause_reading()
        transport.set_protocol(ssl_protocol)
        conmade = self.call_soon(ssl_protocol.connection_made, transport)
        resume = self.call_soon(transport.resume_reading)
        try:
            await waiter
        except BaseException:
            transport.close()
            conmade.cancel()
            resume.cancel()
            raise
        return ssl_protocol._app_transport

    def _make_ssl_context(self, ssl, *, server_side):
        if ssl_module is None:
            raise RuntimeError("stdlib ssl module is not available")
        if isinstance(ssl, bool):
            if server_side:
                raise ValueError("ssl=True is not supported server-side; pass an SSLContext")
            return ssl_module.create_default_context()
        return ssl

    def _make_ssl_protocol(self, app_protocol, sslcontext, waiter, **kwargs):
        # R-059 compatibility path: stdlib sslproto (MemoryBIO) over native
        # transports, exactly as uvloop does. Native engine: M4.
        timeout = kwargs.pop("ssl_handshake_timeout", None)
        shutdown = kwargs.pop("ssl_shutdown_timeout", None)
        extra = {}
        if timeout is not None:
            extra["ssl_handshake_timeout"] = timeout
        if shutdown is not None and sys.version_info >= (3, 11):
            extra["ssl_shutdown_timeout"] = shutdown
        return sslproto.SSLProtocol(self, app_protocol, sslcontext, waiter, **kwargs, **extra)

    # -- serving ----------------------------------------------------------

    async def create_server(
        self,
        protocol_factory,
        host=None,
        port=None,
        *,
        family=socket.AF_UNSPEC,
        flags=socket.AI_PASSIVE,
        sock=None,
        backlog=100,
        ssl=None,
        reuse_address=None,
        reuse_port=None,
        ssl_handshake_timeout=None,
        ssl_shutdown_timeout=None,
        start_serving=True,
    ):
        if ssl is not None and isinstance(ssl, bool):
            raise TypeError("ssl argument must be an SSLContext or None")
        if ssl_handshake_timeout is not None and ssl is None:
            raise ValueError("ssl_handshake_timeout is only meaningful with ssl")

        factory = protocol_factory
        if ssl is not None:
            sslcontext = ssl

            def factory():
                return self._make_ssl_protocol(
                    protocol_factory(),
                    sslcontext,
                    None,
                    server_side=True,
                    ssl_handshake_timeout=ssl_handshake_timeout,
                    ssl_shutdown_timeout=ssl_shutdown_timeout,
                )

        accept_pool = getattr(self, "_accept_pool", 64)  # R-032 default
        entries = []

        if sock is not None:
            if host is not None or port is not None:
                raise ValueError("host/port and sock can not be specified at the same time")
            sock.setblocking(False)
            fd = sock.detach()
            lid, name, rawfd = self._core.listen_fd(fd, factory, accept_pool, start_serving)
            entries.append((lid, name, rawfd))
            return Server(self, entries, factory, accept_pool, start_serving)

        if port is None:
            port = 0
        if reuse_address is None:
            reuse_address = os.name == "posix" and sys.platform != "cygwin"

        hosts = [host] if (host is None or isinstance(host, str)) else list(host)
        resolved = []
        for h in hosts:
            if h == "" or h is None:
                h = None  # wildcard
            infos = await self.getaddrinfo(
                h, port, family=family, type=socket.SOCK_STREAM, flags=flags
            )
            for af, _st, _pr, _cname, address in infos:
                if af not in (socket.AF_INET, socket.AF_INET6):
                    continue
                key = (address[0], address[1])
                if key not in resolved:
                    resolved.append(key)
        if not resolved:
            raise OSError(f"getaddrinfo({host!r}) returned empty list")

        try:
            for ip, bind_port in resolved:
                lid, name, rawfd = self._core.tcp_listen(
                    ip,
                    bind_port,
                    factory,
                    backlog,
                    reuse_address,
                    bool(reuse_port),
                    accept_pool,
                    start_serving,
                )
                entries.append((lid, name, rawfd))
        except BaseException:
            for lid, _name, _fd in entries:
                self._core.listener_close(lid)
            raise
        return Server(self, entries, factory, accept_pool, start_serving)

    # -- readiness callbacks (R-057) ---------------------------------------

    def add_reader(self, fd, callback, *args):
        self._core.add_reader(_fileno(fd), callback, *args)

    def remove_reader(self, fd):
        return self._core.remove_reader(_fileno(fd))

    def add_writer(self, fd, callback, *args):
        self._core.add_writer(_fileno(fd), callback, *args)

    def remove_writer(self, fd):
        return self._core.remove_writer(_fileno(fd))

    # -- sock_* (readiness-based; cold path by design) ---------------------

    async def sock_recv(self, sock, n):
        try:
            return sock.recv(n)
        except (BlockingIOError, InterruptedError):
            pass
        fut = self.create_future()
        fd = sock.fileno()

        def on_readable():
            if fut.done():
                return
            try:
                data = sock.recv(n)
            except (BlockingIOError, InterruptedError):
                return
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                fut.set_exception(exc)
            else:
                fut.set_result(data)

        self._core.add_reader(fd, on_readable)
        try:
            return await fut
        finally:
            self._core.remove_reader(fd)

    async def sock_recv_into(self, sock, buf):
        try:
            return sock.recv_into(buf)
        except (BlockingIOError, InterruptedError):
            pass
        fut = self.create_future()
        fd = sock.fileno()

        def on_readable():
            if fut.done():
                return
            try:
                nbytes = sock.recv_into(buf)
            except (BlockingIOError, InterruptedError):
                return
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                fut.set_exception(exc)
            else:
                fut.set_result(nbytes)

        self._core.add_reader(fd, on_readable)
        try:
            return await fut
        finally:
            self._core.remove_reader(fd)

    async def sock_sendall(self, sock, data):
        try:
            n = sock.send(data)
        except (BlockingIOError, InterruptedError):
            n = 0
        if n == len(data):
            return
        view = memoryview(data)[n:]
        fut = self.create_future()
        fd = sock.fileno()
        pos = [0]

        def on_writable():
            if fut.done():
                return
            try:
                sent = sock.send(view[pos[0] :])
            except (BlockingIOError, InterruptedError):
                return
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                fut.set_exception(exc)
                return
            pos[0] += sent
            if pos[0] >= len(view):
                fut.set_result(None)

        self._core.add_writer(fd, on_writable)
        try:
            await fut
        finally:
            self._core.remove_writer(fd)

    async def sock_connect(self, sock, address):
        # Resolve if needed (numeric fast path first).
        try:
            socket.inet_pton(sock.family, address[0])
        except (OSError, ValueError, IndexError):
            infos = await self.getaddrinfo(
                address[0], address[1], family=sock.family, type=sock.type, proto=sock.proto
            )
            if not infos:
                raise OSError(f"getaddrinfo({address!r}) returned empty list")
            address = infos[0][4]
        err = sock.connect_ex(address)
        if err == 0:
            return
        if err not in (
            getattr(os, "EINPROGRESS", 115),
            115,  # EINPROGRESS
            10035,  # WSAEWOULDBLOCK
            36,  # EINPROGRESS (BSD)
        ):
            raise OSError(err, os.strerror(err))
        fut = self.create_future()
        fd = sock.fileno()

        def on_writable():
            if fut.done():
                return
            e = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if e == 0:
                if sys.platform == "win32":
                    # IOCP writable watches are zero-byte-send probes, which
                    # can fire while the connect is still in flight (SO_ERROR
                    # is 0 until the handshake resolves). Only resolve once
                    # actually connected; the level-triggered watch re-fires.
                    try:
                        sock.getpeername()
                    except OSError:
                        return
                fut.set_result(None)
            else:
                fut.set_exception(OSError(e, f"Connect call failed {address}"))

        self._core.add_writer(fd, on_writable)
        try:
            await fut
        finally:
            self._core.remove_writer(fd)

    async def sock_accept(self, sock):
        try:
            return sock.accept()
        except (BlockingIOError, InterruptedError):
            pass
        fut = self.create_future()
        fd = sock.fileno()

        def on_readable():
            if fut.done():
                return
            try:
                conn, addr = sock.accept()
            except (BlockingIOError, InterruptedError):
                return
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                fut.set_exception(exc)
            else:
                conn.setblocking(False)
                fut.set_result((conn, addr))

        self._core.add_reader(fd, on_readable)
        try:
            return await fut
        finally:
            self._core.remove_reader(fd)

    async def sock_sendfile(self, sock, file, offset=0, count=None, *, fallback=True):
        # Portable fallback: chunked read + sock_sendall (native
        # TransmitFile arrives with R-036 on Windows).
        if offset:
            file.seek(offset)
        blocksize = min(count, 16384) if count else 16384
        total = 0
        while True:
            if count:
                blocksize = min(count - total, blocksize)
                if blocksize <= 0:
                    break
            data = file.read(blocksize)
            if not data:
                break
            await self.sock_sendall(sock, data)
            total += len(data)
        return total


class _DatagramTransport(asyncio.DatagramTransport):
    """R-058 datagram transport over the native endpoint (core `udp_*`).

    The socket is detached into the engine at `_open`; sends are copied
    into the core (fire-and-forget, serialized per endpoint), receives
    arrive via protocol.datagram_received from the event dispatch.
    """

    def __init__(self, loop, sock, protocol, remote_addr):
        self._loop = loop
        self._sock_obj = sock
        self._protocol = protocol
        self._remote_addr = remote_addr
        self._did = None
        self._closing = False
        self._extra = {}

    def _open(self):
        self._extra["sockname"] = self._sock_obj.getsockname()
        try:
            self._extra["peername"] = self._sock_obj.getpeername()
        except OSError:
            self._extra["peername"] = None
        fd = self._sock_obj.detach()
        self._did = self._loop._core.udp_open(
            fd,
            self._protocol.datagram_received,
            self._protocol.error_received,
            self._connection_lost,
        )
        # Synchronous: guarantees connection_made precedes any
        # datagram_received (those dispatch only from future loop ticks).
        self._protocol.connection_made(self)

    def _connection_lost(self, exc):
        self._closing = True
        self._protocol.connection_lost(exc)

    # ---- asyncio.DatagramTransport surface ----------------------------

    def sendto(self, data, addr=None):
        if self._closing:
            return
        if self._remote_addr is not None and addr not in (None, self._remote_addr):
            raise ValueError(f"Invalid address: must be None or {self._remote_addr}")
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"data argument must be a bytes-like object, not {type(data).__name__}")
        self._loop._core.udp_sendto(self._did, bytes(data), addr)

    def close(self):
        if self._closing:
            return
        self._closing = True
        self._loop._core.udp_close(self._did, abort=False)

    def abort(self):
        self._closing = True
        self._loop._core.udp_close(self._did, abort=True)

    def is_closing(self):
        return self._closing

    def get_extra_info(self, name, default=None):
        return self._extra.get(name, default)

    def get_protocol(self):
        return self._protocol

    def set_protocol(self, protocol):
        self._protocol = protocol

    def get_write_buffer_size(self):
        return 0  # sends are copied into the engine immediately
