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
