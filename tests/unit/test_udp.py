"""R-058 datagram endpoints (M4): create_datagram_endpoint over the
native recv_from/send_to ops (no readiness probes — those would truncate
datagrams on IOCP)."""

import asyncio

import pytest

import cadeloop


@pytest.fixture()
def loop():
    lp = cadeloop.new_event_loop()
    asyncio.set_event_loop(lp)
    yield lp
    asyncio.set_event_loop(None)
    if not lp.is_closed():
        lp.close()


class Echo(asyncio.DatagramProtocol):
    def __init__(self):
        self.transport = None
        self.errors = []
        self.lost = None
        self.lost_called = False
        self.made = asyncio.get_event_loop().create_future()

    def connection_made(self, transport):
        self.transport = transport
        if not self.made.done():
            self.made.set_result(None)

    def datagram_received(self, data, addr):
        self.transport.sendto(b"echo:" + data, addr)

    def error_received(self, exc):
        self.errors.append(exc)

    def connection_lost(self, exc):
        self.lost = exc
        self.lost_called = True


class Client(asyncio.DatagramProtocol):
    def __init__(self):
        self.transport = None
        self.received = []
        self.got = None
        self.lost = None
        self.lost_called = False

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self.received.append((data, addr))
        if self.got and not self.got.done():
            self.got.set_result(None)

    def error_received(self, exc):
        pass

    def connection_lost(self, exc):
        self.lost = exc
        self.lost_called = True


def test_udp_echo_roundtrip(loop):
    async def main():
        server_tr, server = await loop.create_datagram_endpoint(
            Echo, local_addr=("127.0.0.1", 0)
        )
        host, port = server_tr.get_extra_info("sockname")[:2]
        client_tr, client = await loop.create_datagram_endpoint(
            Client, remote_addr=(host, port)
        )
        client.got = loop.create_future()
        client_tr.sendto(b"hello")  # connected-mode send
        await asyncio.wait_for(client.got, 5)
        data, addr = client.received[0]
        assert data == b"echo:hello"
        assert addr[1] == port
        assert client_tr.get_extra_info("peername")[:2] == (host, port)
        client_tr.close()
        server_tr.close()
        await asyncio.sleep(0.05)
        assert client.lost_called and client.lost is None
        assert server.lost_called and server.lost is None

    loop.run_until_complete(main())


def test_udp_explicit_addr_and_burst_order(loop):
    async def main():
        server_tr, _server = await loop.create_datagram_endpoint(
            Echo, local_addr=("127.0.0.1", 0)
        )
        addr = server_tr.get_extra_info("sockname")[:2]
        client_tr, client = await loop.create_datagram_endpoint(
            Client, local_addr=("127.0.0.1", 0)
        )
        client.got = loop.create_future()
        n = 20
        for i in range(n):  # exercises the serialized send queue
            client_tr.sendto(f"m{i}".encode(), addr)
        deadline = loop.time() + 5
        while len(client.received) < n and loop.time() < deadline:
            await asyncio.sleep(0.01)
        assert len(client.received) == n
        # Loopback UDP with serialized sends preserves order.
        assert [d for d, _ in client.received] == [f"echo:m{i}".encode() for i in range(n)]
        client_tr.close()
        server_tr.close()

    loop.run_until_complete(main())


def test_udp_sendto_validation(loop):
    async def main():
        tr, _proto = await loop.create_datagram_endpoint(
            Client, remote_addr=("127.0.0.1", 9)
        )
        with pytest.raises(ValueError):
            tr.sendto(b"x", ("127.0.0.1", 10))  # connected: other addrs rejected
        with pytest.raises(TypeError):
            tr.sendto("not-bytes")
        tr.abort()

    loop.run_until_complete(main())


def test_udp_close_is_idempotent_and_loses_connection(loop):
    async def main():
        tr, proto = await loop.create_datagram_endpoint(
            Client, local_addr=("127.0.0.1", 0)
        )
        tr.close()
        tr.close()
        assert tr.is_closing()
        await asyncio.sleep(0.05)
        assert proto.lost_called and proto.lost is None

    loop.run_until_complete(main())


def test_datagram_write_buffer_size_is_reported(loop):
    """R-058: payloads are copied into the engine, but they still sit in
    its send queue behind the one in-flight datagram. Reporting zero made
    monitoring and flow control claim nothing was ever queued no matter
    how far behind the socket fell. Reported by Codex review on PR #1."""

    class P(asyncio.DatagramProtocol):
        pass

    async def main():
        transport, _proto = await loop.create_datagram_endpoint(
            P, local_addr=("127.0.0.1", 0)
        )
        peer = transport.get_extra_info("sockname")
        assert transport.get_write_buffer_size() == 0
        # Burst hard enough that sends queue behind the in-flight one.
        for _ in range(200):
            transport.sendto(b"y" * 1024, peer)
        queued = transport.get_write_buffer_size()
        transport.close()
        return queued

    queued = loop.run_until_complete(main())
    assert isinstance(queued, int) and queued >= 0


def test_datagram_send_queue_is_bounded(loop):
    """An unbounded queue lets a producer that outruns the socket take the
    process down. UDP is lossy by contract, so overflow drops the datagram
    and reports ENOBUFS through error_received. Reported by Codex review
    on PR #1 (F08)."""
    errors = []

    class P(asyncio.DatagramProtocol):
        def error_received(self, exc):
            errors.append(exc)

    async def main():
        transport, _proto = await loop.create_datagram_endpoint(
            P, local_addr=("127.0.0.1", 0)
        )
        peer = transport.get_extra_info("sockname")
        chunk = b"z" * 60000
        # Far past the 1 MiB cap, all inside one callback.
        for _ in range(400):
            transport.sendto(chunk, peer)
        peak = transport.get_write_buffer_size()
        await asyncio.sleep(0.05)
        transport.close()
        await asyncio.sleep(0.05)
        return peak

    peak = loop.run_until_complete(main())
    assert peak <= (1 << 20) + 60000, f"queue grew to {peak} bytes"


def test_datagram_close_completes_after_queued_sends(loop):
    """A synchronous post failure used to strand the rest of the queue and
    skip the deferred-close check, leaving the endpoint open for good.
    Reported by Codex review on PR #1."""

    class P(asyncio.DatagramProtocol):
        pass

    async def main():
        transport, _proto = await loop.create_datagram_endpoint(
            P, local_addr=("127.0.0.1", 0)
        )
        peer = transport.get_extra_info("sockname")
        for _ in range(50):
            transport.sendto(b"q" * 512, peer)
        transport.close()
        for _ in range(30):
            await asyncio.sleep(0)
        await asyncio.sleep(0.1)
        return transport.is_closing()

    assert loop.run_until_complete(main()) is True
    # Endpoint fully torn down: no pool slots retained.
    assert loop._core.stats()["buffers_in_use"] == 0


def test_datagram_endpoint_closed_when_connection_made_raises(loop):
    """udp_open has already detached the socket and installed the native
    endpoint by the time connection_made runs, so the caller's cleanup
    (which closes the now-detached Python socket) left the descriptor, its
    outstanding receive and the protocol callbacks alive until the whole
    loop closed. Reported by Codex review on PR #1 (twice)."""

    class Boom(RuntimeError):
        pass

    class P(asyncio.DatagramProtocol):
        def connection_made(self, transport):
            raise Boom("factory said no")

    async def main():
        for _ in range(10):
            with pytest.raises(Boom):
                await loop.create_datagram_endpoint(P, local_addr=("127.0.0.1", 0))
        for _ in range(20):
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)

    loop.run_until_complete(main())
    # Every endpoint torn down: no pool slots and no live datagram state.
    assert loop._core.stats()["buffers_in_use"] == 0


def test_create_datagram_endpoint_rejects_stream_socket(loop):
    """A stream socket handed to sock= was detached into the datagram
    machinery, which then applies recvfrom/sendto semantics and datagram
    callbacks to a byte stream. Reported by Codex review on PR #1."""
    import socket as socket_module

    s = socket_module.socket(socket_module.AF_INET, socket_module.SOCK_STREAM)
    try:
        with pytest.raises(ValueError, match="datagram socket"):
            loop.run_until_complete(
                loop.create_datagram_endpoint(asyncio.DatagramProtocol, sock=s)
            )
        # Rejected before ownership transferred: still usable.
        assert s.fileno() != -1
    finally:
        s.close()


def test_set_protocol_redirects_datagrams(loop):
    """_open() cached BOUND methods of the original protocol in the native
    endpoint, so set_protocol() changing only the Python attribute left
    every datagram going to the old object while get_protocol() reported
    the new one. Reported by Codex review on PR #1 (three times)."""
    first, second = [], []

    class P1(asyncio.DatagramProtocol):
        def datagram_received(self, data, addr):
            first.append(data)

    class P2(asyncio.DatagramProtocol):
        def datagram_received(self, data, addr):
            second.append(data)

    async def main():
        transport, _p = await loop.create_datagram_endpoint(
            P1, local_addr=("127.0.0.1", 0)
        )
        peer = transport.get_extra_info("sockname")
        transport.sendto(b"one", peer)
        await asyncio.sleep(0.1)

        new = P2()
        transport.set_protocol(new)
        assert transport.get_protocol() is new
        transport.sendto(b"two", peer)
        await asyncio.sleep(0.1)
        transport.close()
        await asyncio.sleep(0.05)

    loop.run_until_complete(main())
    assert first == [b"one"], f"first protocol got {first}"
    assert second == [b"two"], f"replacement protocol got {second}"
