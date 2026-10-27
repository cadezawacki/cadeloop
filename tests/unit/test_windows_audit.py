"""Regressions for the Windows IOCP/RIO audit (W1-W19, C15).

Most of these drive the IOCP backend and run only on win32. The others
run everywhere: readiness semantics every backend must share, code the
same fixes touched that is not Windows-specific, and Windows Python logic
driven through a stand-in for the native core. The backend halves of
these fixes have Rust tests in crates/core/src/backend/iocp.rs and
iocp_util.rs.
"""

import asyncio
import contextlib
import gc
import itertools
import logging
import os
import socket
import sys
import threading
import time

import cadeloop
import pytest
from cadeloop import _winpipes

windows_only = pytest.mark.skipif(sys.platform != "win32", reason="IOCP backend (win32 only)")

_pipe_ids = itertools.count()


@pytest.fixture()
def loop():
    lp = cadeloop.new_event_loop()
    asyncio.set_event_loop(lp)
    yield lp
    asyncio.set_event_loop(None)
    if not lp.is_closed():
        lp.close()


def _closed_port():
    """A loopback port with nothing listening on it."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def _echo_server(loop):
    async def handler(reader, writer):
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
        writer.close()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[:2]


# --------------------------------------------------------------------- #
# W2/W5: readiness watches (add_reader/add_writer, sock_*)              #
# --------------------------------------------------------------------- #


def test_a_parked_sock_recvfrom_gets_a_datagram_sent_later(loop):
    """W2. The IOCP read watch was a zero-byte WSARecv, which on a datagram
    socket receives -- and discards -- the datagram it reports. The waiter
    woke, found nothing, and parked again for a datagram already lost."""
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        rx.bind(("127.0.0.1", 0))
        rx.setblocking(False)
        addr = rx.getsockname()

        async def main():
            for payload in (b"one", b"two"):
                task = loop.create_task(loop.sock_recvfrom(rx, 100))
                await asyncio.sleep(0.1)  # parked on the watch, nothing queued
                assert not task.done()
                tx.sendto(payload, addr)
                data, _ = await asyncio.wait_for(task, 5)
                assert data == payload

        loop.run_until_complete(main())
    finally:
        rx.close()
        tx.close()


def test_a_write_watch_on_connected_udp_sends_nothing(loop):
    """W2. The IOCP write watch was a zero-byte WSASend, which on a
    connected datagram socket transmits an empty datagram to the peer every
    time the watch is armed."""
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        rx.bind(("127.0.0.1", 0))
        rx.setblocking(False)
        tx.connect(rx.getsockname())
        tx.setblocking(False)
        fired = []

        async def main():
            loop.add_writer(tx, fired.append, 1)
            for _ in range(200):
                if len(fired) >= 5:
                    break
                await asyncio.sleep(0.01)
            loop.remove_writer(tx)
            await asyncio.sleep(0.1)

        loop.run_until_complete(main())
        assert len(fired) >= 5, "a connected UDP socket is writable"
        with pytest.raises(BlockingIOError):
            rx.recv(100)
    finally:
        rx.close()
        tx.close()


def _cpu_while_waiting(loop, start, seconds):
    async def main():
        task = loop.create_task(start())
        await asyncio.sleep(0.05)
        cpu = time.process_time()
        await asyncio.sleep(seconds)
        cpu = time.process_time() - cpu
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        return cpu

    return loop.run_until_complete(main())


def test_an_idle_sock_accept_does_not_spin(loop):
    """W5. On IOCP the read watch of a listening socket failed at once
    (WSAENOTCONN), reported readiness, and re-armed with a zero timeout on
    every tick: a core spun at 100% for as long as sock_accept waited."""
    lsock = socket.socket()
    try:
        lsock.bind(("127.0.0.1", 0))
        lsock.listen()
        lsock.setblocking(False)
        cpu = _cpu_while_waiting(loop, lambda: loop.sock_accept(lsock), 0.5)
        assert cpu < 0.25, f"{cpu:.2f}s of CPU in 0.5s of waiting"
    finally:
        lsock.close()


@windows_only
def test_a_pending_sock_connect_does_not_spin(loop):
    """W5. The same for a connecting socket's write watch. A refused
    loopback connect takes Windows about two seconds of SYN retries."""
    s = socket.socket()
    s.setblocking(False)
    port = _closed_port()
    try:

        async def main():
            cpu = time.process_time()
            wall = time.monotonic()
            with pytest.raises(OSError):
                await asyncio.wait_for(loop.sock_connect(s, ("127.0.0.1", port)), 10)
            return time.process_time() - cpu, time.monotonic() - wall

        cpu, wall = loop.run_until_complete(main())
        assert cpu < max(0.25, wall / 2), f"{cpu:.2f}s of CPU in {wall:.2f}s of waiting"
    finally:
        s.close()


# --------------------------------------------------------------------- #
# W4: message-mode pipes                                                #
# --------------------------------------------------------------------- #


@windows_only
def test_a_message_longer_than_the_read_buffer_arrives_whole(loop):
    """W4. A read from a message-mode pipe whose message is longer than the
    buffer fails with ERROR_MORE_DATA: the buffer is full and the rest of
    the message follows on the next read. It was treated as a failure (or
    its slot freed while its completion packet was still queued), so the
    transport died and the data was lost."""
    import _winapi
    from asyncio import windows_utils

    name = r"\\.\pipe\cadeloop-audit-{}-{}".format(os.getpid(), next(_pipe_ids))
    server = _winapi.CreateNamedPipe(
        name,
        _winapi.PIPE_ACCESS_INBOUND | _winapi.FILE_FLAG_OVERLAPPED | _winapi.FILE_FLAG_FIRST_PIPE_INSTANCE,
        _winapi.PIPE_TYPE_MESSAGE | _winapi.PIPE_READMODE_MESSAGE | _winapi.PIPE_WAIT,
        1,
        0,
        1 << 20,
        _winapi.NMPWAIT_WAIT_FOREVER,
        _winapi.NULL,
    )
    handle = windows_utils.PipeHandle(server)  # the transport closes it; so do we, idempotently
    client = _winapi.CreateFile(
        name, _winapi.GENERIC_WRITE, 0, _winapi.NULL, _winapi.OPEN_EXISTING, 0, _winapi.NULL
    )
    try:
        _winapi.ConnectNamedPipe(server, overlapped=True).GetOverlappedResult(True)
        message = bytes(range(256)) * 400  # 100 KiB, one message
        got = bytearray()

        class Reader(asyncio.Protocol):
            def __init__(self):
                self.done = loop.create_future()

            def data_received(self, data):
                got.extend(data)
                if len(got) >= len(message) and not self.done.done():
                    self.done.set_result(None)

            def connection_lost(self, exc):
                if not self.done.done():
                    self.done.set_exception(exc or EOFError("pipe closed early"))

        async def main():
            transport, proto = await loop.connect_read_pipe(Reader, handle)
            writer = threading.Thread(target=_winapi.WriteFile, args=(client, message))
            writer.start()
            try:
                await asyncio.wait_for(proto.done, 10)
            finally:
                transport.close()
                writer.join(10)
                await asyncio.sleep(0.05)  # let connection_lost run

        loop.run_until_complete(main())
        assert bytes(got) == message
    finally:
        _winapi.CloseHandle(client)
        handle.close()


# --------------------------------------------------------------------- #
# W19: loop.close() and the sockets held by undispatched events         #
# --------------------------------------------------------------------- #


def test_close_closes_sockets_held_by_undispatched_events():
    """W19. A fatal error out of one callback unwinds the dispatch batch and
    puts its tail back on the event queue; loop.close() then dropped those
    events. An Accepted event's socket was the only reference to an open
    connection, so its peer was never told the server had gone: it waited
    forever for data or EOF."""
    lp = cadeloop.new_event_loop()
    clients = []
    try:

        def factory():
            raise KeyboardInterrupt

        async def listen():
            return [await lp.create_server(factory, "127.0.0.1", 0) for _ in range(2)]

        servers = lp.run_until_complete(listen())
        addrs = [s.sockets[0].getsockname()[:2] for s in servers]
        # The loop is parked: both handshakes complete in the kernel, so the
        # two accepts are reaped into one dispatch batch. The first factory
        # call raises; the other Accepted event is left undispatched.
        clients = [socket.create_connection(a, timeout=5) for a in addrs]
        lp.call_later(2, lp.stop)
        with pytest.raises(KeyboardInterrupt):
            lp.run_forever()
        for s in servers:
            s.close()
    finally:
        lp.close()
    try:
        for c in clients:
            c.settimeout(2)
            try:
                data = c.recv(1)
            except ConnectionResetError:
                data = b""
            assert data == b"", "the server side of an accepted connection was never closed"
    finally:
        for c in clients:
            c.close()


# --------------------------------------------------------------------- #
# W1: a refused connect must not poison the next socket                 #
# --------------------------------------------------------------------- #


@windows_only
def test_a_refused_connect_does_not_poison_the_next_socket(loop):
    """W1. The socket of a refused connect was closed without being
    detached, leaving its handle value marked "skip mode set". Windows
    reissues freed handle values at once, so the next socket inherited the
    mark without the mode: its synchronous sends completed inline AND
    queued a packet, which then completed whichever op had reused the slot
    (data from freed buffers on the wire, misattributed completions, a
    debug-build panic)."""

    async def main():
        server, addr = await _echo_server(loop)
        with pytest.raises(OSError):
            await loop.create_connection(asyncio.Protocol, "127.0.0.1", _closed_port())
        for _ in range(20):
            reader, writer = await asyncio.open_connection(*addr)
            for i in range(10):
                msg = bytes([i]) * 100
                writer.write(msg)
                await writer.drain()
                assert await reader.readexactly(100) == msg
            writer.close()
            await writer.wait_closed()
        server.close()
        await server.wait_closed()

    loop.run_until_complete(asyncio.wait_for(main(), 60))
    assert loop.stats()["foreign_completions"] == 0


# --------------------------------------------------------------------- #
# W3/W6: what a closed loop leaves behind                               #
# --------------------------------------------------------------------- #


@windows_only
def test_a_socket_a_closed_loop_watched_works_with_a_new_loop():
    """W3. A watch associated the socket with the loop's completion port
    for good (a Windows file object binds to one port for life) and left a
    probe posted that nothing ever cancelled: once the loop closed, the
    socket could not be used by any other loop, and data arriving later
    completed the probe into freed memory."""
    a, b = socket.socketpair()
    a.setblocking(False)
    try:
        lp1 = cadeloop.new_event_loop()
        try:
            with pytest.raises(asyncio.TimeoutError):
                lp1.run_until_complete(asyncio.wait_for(lp1.sock_recv(a, 10), 0.1))
        finally:
            lp1.close()
        del lp1
        gc.collect()
        lp2 = cadeloop.new_event_loop()
        try:

            async def main():
                # Parked before the data exists, so it goes through a watch
                # (sock_recv's first, direct recv() would otherwise win).
                task = lp2.create_task(lp2.sock_recv(a, 10))
                await asyncio.sleep(0.1)
                b.sendall(b"ping")
                return await asyncio.wait_for(task, 5)

            data = lp2.run_until_complete(main())
        finally:
            lp2.close()
        assert data == b"ping"
    finally:
        a.close()
        b.close()


def _process_handle_count():
    import ctypes
    from ctypes import wintypes

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    k32.GetCurrentProcess.restype = wintypes.HANDLE
    k32.GetProcessHandleCount.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
    n = wintypes.DWORD()
    assert k32.GetProcessHandleCount(k32.GetCurrentProcess(), ctypes.byref(n))
    return n.value


@windows_only
def test_loop_close_releases_the_accept_pool():
    """W6. Each listener keeps a pool of AcceptEx requests (64 by default),
    each owning a pre-created accept socket. Closing the loop cancelled
    them but never reaped the cancellations, so every one of those sockets
    stayed open for the life of the process."""

    def cycle(close_server_first):
        lp = cadeloop.new_event_loop()
        try:
            server = lp.run_until_complete(lp.create_server(asyncio.Protocol, "127.0.0.1", 0))
            if close_server_first:
                server.close()
                lp.run_until_complete(server.wait_closed())
        finally:
            lp.close()
        server.close()

    def settle():
        # Each loop's resolver threads exit after close(), not during it.
        for t in threading.enumerate():
            if t.name.startswith("cadeloop-dns"):
                t.join(5)
        gc.collect()

    for first in (False, True):
        cycle(first)  # one-time costs: Winsock, extension functions, ...
    settle()
    before = _process_handle_count()
    for _ in range(3):
        for first in (False, True):
            cycle(first)
    settle()
    leaked = _process_handle_count() - before
    assert leaked < 32, f"{leaked} handles leaked over 6 loops"


# --------------------------------------------------------------------- #
# W7: pipe short writes                                                 #
# --------------------------------------------------------------------- #


class _CappedPipeCore:
    """Stands in for CoreLoop's pipe calls: each write completes on a later
    loop iteration having written at most `cap` bytes, as a WriteFile does
    once the backend caps one write (MAX_PIPE_WRITE in iocp.rs)."""

    def __init__(self, loop, cap):
        self._loop = loop
        self.cap = cap
        self.written = bytearray()
        self.writes = 0

    def pipe_register(self, handle):
        pass

    def pipe_write(self, handle, data, fut):
        n = min(len(data), self.cap)
        self.written += data[:n]
        self.writes += 1
        self._loop.call_soon(fut.set_result, n)


class _Handle:
    def fileno(self):
        return 1234

    def close(self):
        pass


def test_pipe_short_writes_are_finished_not_dropped():
    """W7. The write pipe transport ignored the byte count its WriteFile
    completed with. WriteFile takes a DWORD, so a write of 4 GiB or more
    went out truncated to len mod 2**32 and the rest was silently lost; the
    backend now caps each write at 1 GiB, so the transport must send what
    is left. Driven on a stdlib loop with a capped stand-in core, since the
    transport logic is plain Python."""
    lp = asyncio.new_event_loop()
    try:
        core = lp._core = _CappedPipeCore(lp, cap=1000)
        lost = lp.create_future()

        class Proto(asyncio.Protocol):
            def connection_lost(self, exc):
                lost.set_result(exc)

        payload = bytes(range(256)) * 20

        async def main():
            t = _winpipes._BaseWritePipeTransport(lp, _Handle(), Proto())
            await asyncio.sleep(0)
            t.write(payload[:3000])
            t.write(payload[3000:])  # buffered behind the in-flight write
            assert t.get_write_buffer_size() == len(payload)
            t.close()
            assert await asyncio.wait_for(lost, 5) is None
            assert t.get_write_buffer_size() == 0

        lp.run_until_complete(main())
        assert bytes(core.written) == payload, "bytes were dropped or reordered"
        assert core.writes == 6  # 3000 in three parts, then 2120 in three
    finally:
        lp.close()


# --------------------------------------------------------------------- #
# W17: a subprocess that outlives its loop                              #
# --------------------------------------------------------------------- #


def test_exit_status_for_a_closed_loop_is_logged_not_raised(caplog):
    """W17. The exit-waiter thread called call_soon_threadsafe on a loop
    that had closed while the child ran, and the RuntimeError escaped the
    daemon thread as a traceback per child. stdlib's ThreadedChildWatcher
    logs a warning in the same situation."""
    lp = cadeloop.new_event_loop()
    got = []
    try:
        _winpipes._deliver_exit(lp, 1, got.append, 7)
        lp.run_until_complete(asyncio.sleep(0))
        assert got == [7], "an open loop gets the status"
    finally:
        lp.close()
    with caplog.at_level(logging.WARNING, logger="asyncio"):
        _winpipes._deliver_exit(lp, 4321, got.append, 0)
    assert got == [7]
    assert "4321" in caplog.text and "is closed" in caplog.text


@windows_only
def test_a_subprocess_outliving_its_loop_does_not_raise_in_the_waiter(monkeypatch):
    """W17, end to end: the child is killed only after its loop closed."""
    errors = []
    monkeypatch.setattr(threading, "excepthook", errors.append)
    lp = cadeloop.new_event_loop()
    try:

        async def start():
            return await asyncio.create_subprocess_exec(sys.executable, "-c", "import time; time.sleep(30)")

        proc = lp.run_until_complete(start())
    finally:
        lp.close()
    proc._transport.close()  # kills the child; its waiter wakes on a closed loop
    for t in threading.enumerate():
        if t.name == "cadeloop-subprocess-wait":
            t.join(10)
    assert not errors, errors
