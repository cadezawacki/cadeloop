"""TCP surface of the facade: create_connection / create_server / Server,
sock_* readiness helpers, and TLS via the stdlib ``asyncio.sslproto``
compatibility path (R-059: "fall back to a compatibility path built on
ssl.MemoryBIO in Python (correctness first)"; the native OpenSSL engine
replaces it in M4). Mixed into ``cadeloop.Loop``.
"""

from __future__ import annotations

import asyncio
import errno
import functools
import os
import socket
import stat
import sys
from asyncio import sslproto, staggered
from asyncio.base_events import _interleave_addrinfos

try:
    import ssl as ssl_module
except ImportError:  # pragma: no cover
    ssl_module = None

__all__ = ["TcpSurface", "Server"]

logger = __import__("logging").getLogger("cadeloop")


def _fileno(fd):
    if isinstance(fd, int):
        return fd
    return fd.fileno()


def _check_ssl_socket(sock):
    """base_events._check_ssl_socket: a sock= passed to
    create_connection/create_server/connect_accepted_socket must be a
    plain socket cadeloop can wrap in its own transport — an
    already-wrapped ssl.SSLSocket has its own I/O semantics that would
    silently double-wrap or misbehave."""
    if ssl_module is not None and isinstance(sock, ssl_module.SSLSocket):
        raise TypeError("Socket cannot be of type SSLSocket")


class Server(asyncio.AbstractServer):
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
        # The future serve_forever() parks on, so close() can wake it.
        self._serving_forever = None

    # -- introspection ----------------------------------------------------

    @property
    def sockets(self):
        if self._closed:
            # asyncio reports an empty tuple after close. Rebuilding here
            # would duplicate descriptors the native listener has already
            # closed -- raising EBADF, or worse, handing back a duplicate
            # of whatever unrelated socket has since reused the number.
            return ()
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
        if self._closed:
            raise RuntimeError("server is closed")
        if self._serving_forever is not None:
            raise RuntimeError("server is already being awaited on serve_forever()")
        await self.start_serving()
        # Held, not anonymous. An anonymous future is unreachable from
        # close(), so a server closed by another task left this coroutine
        # parked for good -- even after wait_closed() returned -- and the
        # caller had to separately cancel a server it had already closed.
        self._serving_forever = self._loop.create_future()
        try:
            await self._serving_forever
        finally:
            self._serving_forever = None
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
        self._sockets = ()
        self._entries = []  # the raw descriptors are gone; do not reuse them
        # Cancelled rather than resolved, matching the stdlib: serve_forever
        # raises CancelledError when its server is closed underneath it.
        fut, self._serving_forever = self._serving_forever, None
        if fut is not None and not fut.done():
            fut.cancel()
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
        if ssl_shutdown_timeout is not None and not ssl:
            raise ValueError("ssl_shutdown_timeout is only meaningful with ssl")

        if sock is not None:
            if host is not None or port is not None:
                raise ValueError("host/port and sock can not be specified at the same time")
            _check_ssl_socket(sock)
            # Checked before ownership transfers, as create_server() and
            # connect_accepted_socket() already do. A datagram socket here
            # got the native STREAM transport, which applies byte-stream
            # flow control and EOF semantics to packet I/O -- and an
            # unconnected UDP socket then fails on the first write with no
            # destination.
            if sock.type != socket.SOCK_STREAM:
                raise ValueError(f"a stream socket was expected, got {sock!r}")
            fd = sock.detach()
            return await self._wrap_outgoing(
                fd, protocol_factory, ssl, server_hostname, ssl_handshake_timeout, ssl_shutdown_timeout
            )

        if host is None or port is None:
            raise ValueError("host and port was not specified and no sock specified")

        if happy_eyeballs_delay is not None and interleave is None:
            # RFC 6555 default: interleave by address family once racing
            # is on (matches base_events.create_connection).
            interleave = 1

        infos = await self.getaddrinfo(
            host, port, family=family, type=socket.SOCK_STREAM, proto=proto, flags=flags
        )
        if not infos:
            raise OSError(f"getaddrinfo({host!r}) returned empty list")
        laddr_infos = None
        if local_addr is not None:
            laddr_infos = await self.getaddrinfo(
                local_addr[0], local_addr[1], family=family, type=socket.SOCK_STREAM,
                proto=proto, flags=flags,
            )
            if not laddr_infos:
                raise OSError("getaddrinfo() returned empty list for local_addr")
        if interleave:
            infos = _interleave_addrinfos(infos, interleave)

        errors = []

        async def attempt(address, af):
            # The whole sockaddr, not host/port: an IPv6 one carries the
            # flow info and interface scope in elements 2 and 3, and a
            # link-local peer (fe80::...%eth0) is unreachable without the
            # scope however cleanly it resolved.
            local_sockaddr = None
            if laddr_infos is not None:
                for lfamily, _lst, _lpr, _lcname, laddr in laddr_infos:
                    if lfamily == af:
                        local_sockaddr = laddr
                        break
                else:
                    exc = OSError(f"no matching local address with family={af!r} found")
                    errors.append(exc)
                    raise exc
            fut = self.create_future()
            try:
                op = self._core.tcp_connect(address, fut, local_sockaddr)
            except OSError as exc:
                errors.append(exc)
                raise
            try:
                return await fut
            except asyncio.CancelledError:
                # Cancel the native op too. Every losing Happy Eyeballs
                # attempt lands here, so without this each connection
                # leaves N-1 sockets in a half-open connect until the OS
                # times them out -- which can be minutes. The completion
                # still arrives and closes the socket; this only brings
                # that forward.
                self._core.cancel_connect(op)
                raise
            except OSError as exc:
                errors.append(exc)
                raise

        if happy_eyeballs_delay is None:
            fd = None
            for af, _st, _pr, _cname, address in infos:
                try:
                    fd = await attempt(address, af)
                    break
                except OSError:
                    continue
        else:
            # Staggered concurrent attempts (RFC 6555): a slow/broken
            # address family no longer stalls every later candidate
            # behind it — reuses stdlib's own racing logic, just racing
            # the native tcp_connect fast path instead of a plain socket.
            fd, _, _ = await staggered.staggered_race(
                (
                    functools.partial(attempt, address, af)
                    for af, _st, _pr, _cname, address in infos
                ),
                happy_eyeballs_delay,
                loop=self,
            )
        if fd is None:
            if len(errors) == 1:
                raise errors[0]
            raise OSError(
                f"Multiple exceptions: {', '.join(str(e) for e in errors)}"
            ) from (errors[0] if errors else None)
        return await self._wrap_outgoing(
            fd, protocol_factory, ssl, server_hostname, ssl_handshake_timeout, ssl_shutdown_timeout
        )

    async def _wrap_outgoing(
        self,
        fd,
        protocol_factory,
        ssl,
        server_hostname,
        ssl_handshake_timeout,
        ssl_shutdown_timeout,
        *,
        server_side=False,
    ):
        # Until attach_stream() takes it, this descriptor has no owner:
        # it came back from a native connect, or from a `sock=` the caller
        # already detached. A protocol_factory that raises (or an
        # SSLContext that will not build) therefore leaked one connected
        # socket per failure -- invisible until the process ran out.
        if not ssl:
            try:
                protocol = protocol_factory()
            except BaseException:
                self._core.discard_socket(fd)
                raise
            transport = self._core.attach_stream(fd, protocol)
            return transport, protocol
        # Everything up to attach_stream() belongs in the rollback, not
        # just the factory calls: `_make_ssl_context` hands back anything
        # that is not a bool unchanged, so `ssl="bad"` reaches
        # `_make_ssl_protocol` and raises inside wrap_bio -- and with the
        # protocol built outside this try, that raised past the only code
        # that would have closed the descriptor.
        try:
            sslcontext = self._make_ssl_context(ssl, server_side=server_side)
            app_protocol = protocol_factory()
            waiter = self.create_future()
            protocol = self._make_ssl_protocol(
                app_protocol,
                sslcontext,
                waiter,
                server_side=server_side,
                server_hostname=server_hostname,
                ssl_handshake_timeout=ssl_handshake_timeout,
                ssl_shutdown_timeout=ssl_shutdown_timeout,
            )
        except BaseException:
            self._core.discard_socket(fd)
            raise
        transport = self._core.attach_stream(fd, protocol)
        try:
            await waiter  # handshake
        except BaseException:
            # Cancellation (an application wrapping this in wait_for) or a
            # handshake failure both land here. The transport is already
            # attached and the SSL protocol would otherwise keep the socket
            # until its own handshake timeout -- and could still complete
            # and call into the application protocol after the caller
            # believes the connection attempt was abandoned.
            transport.close()
            raise
        return protocol._app_transport, app_protocol

    async def connect_accepted_socket(
        self,
        protocol_factory,
        sock,
        *,
        ssl=None,
        ssl_handshake_timeout=None,
        ssl_shutdown_timeout=None,
    ):
        """Wrap an already-connected, externally-accepted socket (e.g.
        handed off between processes) in a transport/protocol pair —
        the generic counterpart to this project's own multi-worker
        socket.share/fromshare handoff (server.py), at the public
        AbstractEventLoop level."""
        if sock.type != socket.SOCK_STREAM:
            raise ValueError(f"A Stream Socket was expected, got {sock!r}")
        if ssl_handshake_timeout is not None and not ssl:
            raise ValueError("ssl_handshake_timeout is only meaningful with ssl")
        if ssl_shutdown_timeout is not None and not ssl:
            raise ValueError("ssl_shutdown_timeout is only meaningful with ssl")
        _check_ssl_socket(sock)
        fd = sock.detach()
        return await self._wrap_outgoing(
            fd, protocol_factory, ssl, None, ssl_handshake_timeout, ssl_shutdown_timeout, server_side=True
        )

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
        if ssl_shutdown_timeout is not None and ssl is None:
            raise ValueError("ssl_shutdown_timeout is only meaningful with ssl")
        if sock is not None:
            _check_ssl_socket(sock)
            # Checked before ownership transfers. A SOCK_DGRAM socket got
            # detached and registered as a listener, and then accept() on
            # it failed forever -- the listener rearms after each failure,
            # so the caller was handed an apparently serving Server that
            # only logged accept errors, instead of a ValueError here.
            if sock.type != socket.SOCK_STREAM:
                raise ValueError(f"a stream socket was expected, got {sock!r}")
        if reuse_port and not hasattr(socket, "SO_REUSEPORT"):
            raise ValueError("reuse_port not supported by socket module")

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
            # stdlib create_server() accepts a bound-but-not-listening
            # socket and calls listen() itself. Without this the native
            # accepts were posted against a socket in no state to accept
            # them: every post failed, the listener rearmed after each
            # failure, and the caller got a Server that looked like it was
            # serving and could never take a connection.
            try:
                sock.listen(backlog)
            except OSError as exc:
                # Already listening is fine and is the common case for a
                # socket handed in ready to go.
                if exc.errno not in (errno.EINVAL, errno.EISCONN):
                    raise
            fd = sock.detach()
            try:
                lid, name, rawfd = self._core.listen_fd(fd, factory, accept_pool, start_serving)
            except BaseException:
                # detach() already ran, so nothing else owns this
                # descriptor; without the close it leaks.
                self._core.discard_socket(fd)
                raise
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
                # Keep the flow info and interface scope an IPv6 sockaddr
                # carries: a scoped bind address (fe80::1%eth0) resolves
                # with a scope_id naming the interface, and dropping it
                # bound with scope zero -- the wrong interface, or nothing
                # at all, despite a clean resolution.
                key = (address[0], address[1]) + tuple(address[2:4])
                if key not in resolved:
                    resolved.append(key)
        if not resolved:
            raise OSError(f"getaddrinfo({host!r}) returned empty list")

        try:
            for entry in resolved:
                ip, bind_port = entry[0], entry[1]
                flowinfo, scope_id = (entry[2], entry[3]) if len(entry) == 4 else (0, 0)
                lid, name, rawfd = self._core.tcp_listen(
                    ip,
                    bind_port,
                    factory,
                    backlog,
                    reuse_address,
                    bool(reuse_port),
                    accept_pool,
                    start_serving,
                    flowinfo,
                    scope_id,
                )
                entries.append((lid, name, rawfd))
        except BaseException:
            for lid, _name, _fd in entries:
                self._core.listener_close(lid)
            raise
        return Server(self, entries, factory, accept_pool, start_serving)

    # -- AF_UNIX (delegates to create_connection/create_server's sock= ----
    # path — the native transport/listener machinery operates on a raw
    # socket fd via recv/send-style ops, family-agnostic, so an AF_UNIX
    # stream socket works through it exactly like AF_INET does).

    async def create_unix_connection(
        self,
        protocol_factory,
        path=None,
        *,
        ssl=None,
        sock=None,
        server_hostname=None,
        ssl_handshake_timeout=None,
        ssl_shutdown_timeout=None,
    ):
        if not hasattr(socket, "AF_UNIX"):
            raise NotImplementedError("Unix sockets are not supported on this platform")
        assert server_hostname is None or isinstance(server_hostname, str)
        if ssl:
            if server_hostname is None:
                raise ValueError("you have to pass server_hostname when using ssl")
        else:
            if server_hostname is not None:
                raise ValueError("server_hostname is only meaningful with ssl")
            if ssl_handshake_timeout is not None:
                raise ValueError("ssl_handshake_timeout is only meaningful with ssl")
            if ssl_shutdown_timeout is not None:
                raise ValueError("ssl_shutdown_timeout is only meaningful with ssl")

        if path is not None:
            if sock is not None:
                raise ValueError("path and sock can not be specified at the same time")
            path = os.fspath(path)
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
            try:
                sock.setblocking(False)
                await self.sock_connect(sock, path)
            except BaseException:
                sock.close()
                raise
        else:
            if sock is None:
                raise ValueError("no path and sock were specified")
            if sock.family != socket.AF_UNIX or sock.type != socket.SOCK_STREAM:
                raise ValueError(f"A UNIX Domain Stream Socket was expected, got {sock!r}")
            sock.setblocking(False)

        return await self.create_connection(
            protocol_factory,
            sock=sock,
            ssl=ssl,
            server_hostname=server_hostname,
            ssl_handshake_timeout=ssl_handshake_timeout,
            ssl_shutdown_timeout=ssl_shutdown_timeout,
        )

    async def create_unix_server(
        self,
        protocol_factory,
        path=None,
        *,
        sock=None,
        backlog=100,
        ssl=None,
        ssl_handshake_timeout=None,
        ssl_shutdown_timeout=None,
        start_serving=True,
    ):
        if not hasattr(socket, "AF_UNIX"):
            raise NotImplementedError("Unix sockets are not supported on this platform")
        if isinstance(ssl, bool):
            raise TypeError("ssl argument must be an SSLContext or None")
        if ssl_handshake_timeout is not None and not ssl:
            raise ValueError("ssl_handshake_timeout is only meaningful with ssl")
        if ssl_shutdown_timeout is not None and not ssl:
            raise ValueError("ssl_shutdown_timeout is only meaningful with ssl")

        if path is not None:
            if sock is not None:
                raise ValueError("path and sock can not be specified at the same time")
            path = os.fspath(path)
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            # Clear a stale socket file left by a crashed prior instance
            # (matches stdlib: abstract-namespace paths start with a NUL
            # and have no filesystem entry to check).
            if path[0] not in (0, "\x00"):
                try:
                    if stat.S_ISSOCK(os.stat(path).st_mode):
                        os.remove(path)
                except FileNotFoundError:
                    pass
                except OSError as err:
                    logger.error("Unable to check or remove stale UNIX socket %r: %r", path, err)
            try:
                sock.bind(path)
            except OSError as exc:
                sock.close()
                if exc.errno == errno.EADDRINUSE:
                    raise OSError(errno.EADDRINUSE, f"Address {path!r} is already in use") from None
                raise
            except BaseException:
                sock.close()
                raise
        else:
            if sock is None:
                raise ValueError("path was not specified, and no sock specified")
            if sock.family != socket.AF_UNIX or sock.type != socket.SOCK_STREAM:
                raise ValueError(f"A UNIX Domain Stream Socket was expected, got {sock!r}")

        sock.listen(backlog)  # create_server's sock= path expects this done already
        sock.setblocking(False)
        return await self.create_server(
            protocol_factory,
            sock=sock,
            ssl=ssl,
            ssl_handshake_timeout=ssl_handshake_timeout,
            ssl_shutdown_timeout=ssl_shutdown_timeout,
            start_serving=start_serving,
        )

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
        # Resolution only applies to AF_INET/AF_INET6 (matches stdlib):
        # AF_UNIX addresses are filesystem paths, not (host, port) pairs,
        # and any other family's address is used exactly as given.
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            # Numeric fast path first.
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
            conn, addr = sock.accept()
        except (BlockingIOError, InterruptedError):
            pass
        else:
            # A connection already queued takes this fast path, which must
            # normalise the socket exactly as the deferred path does: an
            # accepted socket does NOT inherit the listener's nonblocking
            # flag on Linux, so a later `await loop.sock_recv(conn, ...)`
            # would run a blocking recv() and freeze the whole loop.
            conn.setblocking(False)
            return conn, addr
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

    async def sock_recvfrom(self, sock, bufsize):
        try:
            return sock.recvfrom(bufsize)
        except (BlockingIOError, InterruptedError):
            pass
        fut = self.create_future()
        fd = sock.fileno()

        def on_readable():
            if fut.done():
                return
            try:
                result = sock.recvfrom(bufsize)
            except (BlockingIOError, InterruptedError):
                return
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                fut.set_exception(exc)
            else:
                fut.set_result(result)

        self._core.add_reader(fd, on_readable)
        try:
            return await fut
        finally:
            self._core.remove_reader(fd)

    async def sock_recvfrom_into(self, sock, buf, nbytes=0):
        if not nbytes:
            nbytes = len(buf)
        try:
            return sock.recvfrom_into(buf, nbytes)
        except (BlockingIOError, InterruptedError):
            pass
        fut = self.create_future()
        fd = sock.fileno()

        def on_readable():
            if fut.done():
                return
            try:
                result = sock.recvfrom_into(buf, nbytes)
            except (BlockingIOError, InterruptedError):
                return
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                fut.set_exception(exc)
            else:
                fut.set_result(result)

        self._core.add_reader(fd, on_readable)
        try:
            return await fut
        finally:
            self._core.remove_reader(fd)

    async def sock_sendto(self, sock, data, address):
        try:
            return sock.sendto(data, address)
        except (BlockingIOError, InterruptedError):
            pass
        fut = self.create_future()
        fd = sock.fileno()

        def on_writable():
            if fut.done():
                return
            try:
                sent = sock.sendto(data, address)
            except (BlockingIOError, InterruptedError):
                return
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                fut.set_exception(exc)
            else:
                # The immediate path returns the byte count, so this one
                # must too: discarding it made sock_sendto's return type
                # depend on transient socket readiness.
                fut.set_result(sent)

        self._core.add_writer(fd, on_writable)
        try:
            return await fut
        finally:
            self._core.remove_writer(fd)

    async def sock_sendfile(self, sock, file, offset=0, count=None, *, fallback=True):
        """Native os.sendfile first (TransmitFile is the remaining R-036
        refinement on Windows, same caveat as loop.py's sendfile()), the
        chunked read + sock_sendall path as the fallback — mirrors
        sendfile()'s own native-first/fallback/validation shape exactly;
        this previously always took the chunked path regardless of
        fallback= and skipped every one of these checks."""
        if "b" not in getattr(file, "mode", "b"):
            raise ValueError("file should be opened in binary mode")
        if sock.type != socket.SOCK_STREAM:
            raise ValueError("only SOCK_STREAM type sockets are supported")
        if count is not None:
            if not isinstance(count, int):
                raise TypeError(f"count must be a positive integer (got {count!r})")
            if count <= 0:
                raise ValueError(f"count must be a positive integer (got {count!r})")
        if not isinstance(offset, int):
            raise TypeError(f"offset must be a non-negative integer (got {offset!r})")
        if offset < 0:
            raise ValueError(f"offset must be a non-negative integer (got {offset!r})")

        can_native = hasattr(os, "sendfile") and hasattr(file, "fileno")
        if can_native:
            try:
                file.fileno()
            except (OSError, AttributeError, ValueError):
                can_native = False
        if not can_native:
            if not fallback:
                raise asyncio.SendfileNotAvailableError(
                    "syscall sendfile is not available for this socket/file combination"
                )
            return await self._sock_sendfile_fallback(sock, file, offset, count)
        return await self._sock_sendfile_native(sock, file, offset, count)

    async def _sock_sendfile_native(self, sock, file, offset, count):
        fd = sock.fileno()
        in_fd = file.fileno()
        total = 0
        # Position restored in a finally, as in Loop._sendfile_native_fd:
        # an interrupted transfer whose bytes already went out must not
        # leave tell() at the original offset, or a caller retrying from
        # the reported position sends them twice.
        try:
            while True:
                blocksize = 256 * 1024 if count is None else min(count - total, 256 * 1024)
                if blocksize <= 0:
                    break
                try:
                    sent = os.sendfile(fd, in_fd, offset + total, blocksize)
                except BlockingIOError:
                    sent = None
                except InterruptedError:
                    continue
                if sent == 0:
                    break  # end of file
                if sent is not None:
                    total += sent
                    continue
                # Socket buffer full: wait for writability.
                fut = self.create_future()

                def on_writable():
                    if not fut.done():
                        fut.set_result(None)

                self._core.add_writer(fd, on_writable)
                try:
                    await fut
                finally:
                    self._core.remove_writer(fd)
            return total
        finally:
            file.seek(offset + total)  # stdlib convention

    async def _sock_sendfile_fallback(self, sock, file, offset, count):
        # Unconditional, matching Loop._sendfile_fallback: the old guard
        # skipped the seek for the default offset=0, so this path sent from
        # the file's current position while the native path sent from byte
        # zero.
        #
        # Reads go through the executor and the position is restored in a
        # finally, for the same reasons as Loop._sendfile_fallback: a
        # blocking read here stalls every connection on the loop, and a
        # transfer interrupted part-way must still report how far it got.
        blocksize = min(count, 16384) if count else 16384
        total = 0
        try:
            await self.run_in_executor(None, file.seek, offset)
            while True:
                if count:
                    blocksize = min(count - total, blocksize)
                    if blocksize <= 0:
                        break
                data = await self.run_in_executor(None, file.read, blocksize)
                if not data:
                    break
                await self.sock_sendall(sock, data)
                total += len(data)
            return total
        finally:
            # Synchronous for the same reason as Loop._sendfile_fallback:
            # an await here is skipped when unwinding a cancellation.
            file.seek(offset + total)


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
        try:
            self._did = self._loop._core.udp_open(
                fd,
                self._protocol.datagram_received,
                self._protocol.error_received,
                self._connection_lost,
            )
        except BaseException:
            # detach() has already run, so neither the caller nor
            # create_datagram_endpoint's cleanup holds a socket object
            # that owns this descriptor any more.
            self._loop._core.discard_socket(fd)
            raise
        # Synchronous: guarantees connection_made precedes any
        # datagram_received (those dispatch only from future loop ticks).
        try:
            self._protocol.connection_made(self)
        except BaseException:
            # udp_open already detached the socket and installed the native
            # endpoint, so the caller's cleanup (which only closes the now-
            # detached Python socket) would leave the descriptor, its
            # outstanding receive and the protocol callbacks alive until the
            # whole loop closed. Tear the endpoint down before propagating.
            did, self._did = self._did, None
            if did is not None:
                try:
                    self._loop._core.udp_close(did, abort=True)
                except (OSError, RuntimeError):
                    pass
            raise

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
        # _open() cached BOUND methods of the previous protocol in the
        # native endpoint, so without this every datagram and send error
        # kept going to the old object while get_protocol() reported the
        # new one. connection_lost already routes through a transport
        # method, which is why only these two need swapping.
        if self._did is not None:
            self._loop._core.udp_set_callbacks(
                self._did, protocol.datagram_received, protocol.error_received
            )

    def get_write_buffer_size(self):
        # Payloads are copied into the engine, but they still sit in its
        # send queue behind the one in-flight datagram — reporting zero
        # made monitoring and flow control claim there was never anything
        # queued, no matter how far behind the socket fell.
        if self._did is None:
            return 0
        return self._loop._core.udp_queued_bytes(self._did)
