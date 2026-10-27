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
import itertools
import os
import socket
import sys
import threading
import time

import cadeloop
import pytest

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
