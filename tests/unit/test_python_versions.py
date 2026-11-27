"""asyncio is not the same on 3.11, 3.12 and 3.13, and cadeloop follows
the interpreter it runs on (ADR-45).

Each test here runs on cadeloop and, where the running version's asyncio
has the feature, on asyncio's own selector loop too -- the same contract
as test_asyncio_parity.py. What 3.12 or 3.13 *added as an argument or a
method* cadeloop takes on every version, so those are tested on cadeloop
everywhere and against asyncio where asyncio has them.
"""

from __future__ import annotations

import array
import asyncio
import contextvars
import inspect
import os
import socket
import sys
import threading
import time
import warnings

import cadeloop
import pytest

PY312 = sys.version_info >= (3, 12)
PY313 = sys.version_info >= (3, 13)
_STAGGERED_IS_CYCLE_FREE = "del exceptions" in inspect.getsource(asyncio.staggered)
needs_unix = pytest.mark.skipif(not hasattr(socket, "AF_UNIX") or sys.platform == "win32", reason="AF_UNIX")


def _loops(*, asyncio_from=None):
    """Loop kinds for a test: cadeloop always; asyncio from the version
    that has the feature (None: every version)."""
    kinds = ["cadeloop"]
    if asyncio_from is None or sys.version_info >= asyncio_from:
        kinds.append("asyncio")
    return kinds


def _make(kind):
    lp = cadeloop.new_event_loop() if kind == "cadeloop" else asyncio.SelectorEventLoop()
    asyncio.set_event_loop(lp)
    return lp


def _done(lp):
    asyncio.set_event_loop(None)
    if not lp.is_closed():
        lp.close()


def _fixture(**kwargs):
    @pytest.fixture(params=_loops(**kwargs))
    def loop(request):
        lp = _make(request.param)
        yield lp
        _done(lp)

    return loop


loop = _fixture()
loop312 = _fixture(asyncio_from=(3, 12))
loop313 = _fixture(asyncio_from=(3, 13))


async def _server_with_clients(lp, n=2, **kwargs):
    accepted = []

    class Accepted(asyncio.Protocol):
        def connection_made(self, transport):
            self.transport = transport
            self.lost = lp.create_future()
            accepted.append(self)

        def connection_lost(self, exc):
            self.lost.set_result(exc)

    server = await lp.create_server(Accepted, "127.0.0.1", 0, **kwargs)
    port = server.sockets[0].getsockname()[1]
    clients = [await lp.create_connection(asyncio.Protocol, "127.0.0.1", port) for _ in range(n)]
    while len(accepted) < n:
        await asyncio.sleep(0.005)
    return server, accepted, [t for t, _ in clients]


# --------------------------------------------------------------------- #
# 3.12                                                                   #
# --------------------------------------------------------------------- #


def test_shutdown_default_executor_takes_a_timeout(loop312):
    """Runner.close() passes one from 3.12 -- which is how `asyncio.run()`
    on 3.12 found the argument missing."""
    lp = loop312
    release = threading.Event()

    async def main():
        job = lp.run_in_executor(None, release.wait, 20)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            started = time.monotonic()
            await lp.shutdown_default_executor(0.2)
            waited = time.monotonic() - started
        release.set()
        await job
        return waited, [str(w.message) for w in caught if w.category is RuntimeWarning]

    waited, messages = lp.run_until_complete(main())
    assert 0.15 < waited < 5
    assert len(messages) == 1 and "0.2 seconds" in messages[0], messages


def test_shutdown_default_executor_without_a_timeout_waits(loop):
    lp = loop
    done = []

    async def main():
        lp.run_in_executor(None, lambda: (time.sleep(0.2), done.append(1)))
        await lp.shutdown_default_executor()
        return list(done)

    assert lp.run_until_complete(main()) == [1]
    with pytest.raises(RuntimeError, match="shutdown"):
        lp.run_in_executor(None, time.sleep, 0)


@pytest.mark.skipif(not PY312, reason="asyncio.run(loop_factory=) is 3.12")
def test_asyncio_run_with_a_loop_factory():
    async def main():
        await asyncio.to_thread(time.sleep, 0.01)  # the executor Runner.close() shuts down
        return type(asyncio.get_running_loop())

    assert asyncio.run(main(), loop_factory=cadeloop.new_event_loop) is cadeloop.Loop


def test_asyncio_runner_closes_a_cadeloop_loop():
    async def main():
        await asyncio.to_thread(time.sleep, 0.01)
        return asyncio.get_running_loop()

    with asyncio.Runner(loop_factory=cadeloop.new_event_loop) as runner:
        lp = runner.run(main())
        assert isinstance(lp, cadeloop.Loop)
    assert lp.is_closed()


def test_create_connection_all_errors(loop312):
    """all_errors=True: every attempt's error, as an ExceptionGroup,
    instead of one of them or a summary."""
    lp = loop312
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]  # closed again: nothing listens there

    async def main():
        with pytest.raises(ExceptionGroup) as info:
            await lp.create_connection(asyncio.Protocol, "127.0.0.1", port, all_errors=True)
        with pytest.raises(OSError) as plain:
            await lp.create_connection(asyncio.Protocol, "127.0.0.1", port)
        return info.value, plain.value

    group, plain = lp.run_until_complete(main())
    assert str(group).startswith("create_connection failed")
    assert len(group.exceptions) == 1 and isinstance(group.exceptions[0], ConnectionRefusedError)
    assert isinstance(plain, ConnectionRefusedError)


def test_a_failed_connection_leaves_no_reference_cycle(loop):
    """The error a failed create_connection raises is referenced by its
    caller and nothing else: no list in a frame its traceback keeps alive
    (gh-128552's family). Otherwise every refused connection waits for the
    cyclic collector, with its frames."""
    import gc

    lp = loop
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    async def main(**kwargs):
        try:
            await lp.create_connection(asyncio.Protocol, "127.0.0.1", port, **kwargs)
        except OSError as exc:
            caught = exc
        return [r for r in gc.get_referrers(caught) if isinstance(r, (list, dict))]

    assert lp.run_until_complete(main()) == []
    if _STAGGERED_IS_CYCLE_FREE:
        # The racing path goes through asyncio.staggered on either loop,
        # which kept such a list itself until later 3.12 patch releases.
        assert lp.run_until_complete(main(happy_eyeballs_delay=0.25)) == []


def test_wait_closed_waits_for_connections_from_312(loop):
    lp = loop

    async def main():
        server, accepted, clients = await _server_with_clients(lp, 1)
        waiting = asyncio.ensure_future(server.wait_closed())
        await asyncio.sleep(0.02)
        assert not waiting.done()  # every version: not closed yet
        server.close()
        late = asyncio.ensure_future(server.wait_closed())
        await asyncio.sleep(0.05)
        still_waiting = (waiting.done(), late.done())
        accepted[0].transport.close()
        await asyncio.wait_for(asyncio.gather(waiting, late), 5)
        clients[0].close()
        return still_waiting

    early_done, late_done = lp.run_until_complete(main())
    assert early_done is False  # asked before close(): waits for the connection, on every version
    assert late_done is (not PY312)  # asked after close(): 3.11 returns at once


@pytest.mark.skipif(not PY312, reason="Handle.get_context() is 3.12")
def test_handle_get_context(loop):
    lp = loop
    var = contextvars.ContextVar("var", default="unset")

    def callback():
        pass

    var.set("at scheduling time")
    implicit = lp.call_soon(callback)
    explicit_ctx = contextvars.copy_context()
    explicit = lp.call_later(60, callback, context=explicit_ctx)
    var.set("later")
    assert implicit.get_context().get(var) == "at scheduling time"
    assert explicit.get_context() is explicit_ctx
    explicit.cancel()
    lp.run_until_complete(asyncio.sleep(0))


def test_exception_handler_runs_in_the_context_of_what_failed(loop):
    """3.12: a custom exception handler sees the context variables of the
    callback or task that failed. Before, those of whoever ran the loop."""
    lp = loop
    var = contextvars.ContextVar("request", default="none")
    seen = []
    lp.set_exception_handler(lambda loop, context: seen.append((context["message"][:22], var.get())))

    def boom():
        raise ValueError("boom")

    async def main():
        var.set("request-1")
        lp.call_soon(boom)
        await asyncio.sleep(0.01)
        var.set("request-2")
        task = lp.create_task(asyncio.sleep(0))
        lp.call_exception_handler({"message": "reported by hand for a task", "task": task})
        await task

    var.set("outside")
    lp.run_until_complete(main())
    assert [m for m, _ in seen] == ["Exception in callback ", "reported by hand for a"]
    if PY312:
        assert [v for _, v in seen] == ["request-1", "request-2"]
    else:
        # 3.11 runs the handler where the report was made: the first from
        # the loop's dispatch (the main task's context is not current
        # there), the second from inside the task.
        assert seen[1][1] == "request-2"


@pytest.mark.skipif(not PY312, reason="asyncio.eager_task_factory is 3.12")
def test_eager_task_factory(loop):
    lp = loop
    order = []

    async def child(n):
        order.append(f"child {n} started")
        await asyncio.sleep(0)
        order.append(f"child {n} finished")
        return n

    async def main():
        lp.set_task_factory(asyncio.eager_task_factory)
        task = lp.create_task(child(1), name="first")
        order.append("create_task returned")
        assert task.get_name() == "first"
        done = lp.create_task(asyncio.sleep(0, result="sync"))  # completes without the loop
        return await task, done.done() or await done

    assert lp.run_until_complete(main())[0] == 1
    assert order[:2] == ["child 1 started", "create_task returned"]


def test_writelines_is_its_own_method_from_312(loop):
    lp = loop

    async def main():
        got = bytearray()

        class Sink(asyncio.Protocol):
            def connection_made(self, transport):
                self.transport = transport

            def data_received(self, data):
                got.extend(data)

        sinks = []

        def factory():
            sinks.append(Sink())
            return sinks[-1]

        server = await lp.create_server(factory, "127.0.0.1", 0)
        transport, _ = await lp.create_connection(
            asyncio.Protocol, "127.0.0.1", server.sockets[0].getsockname()[1]
        )
        transport.writelines([])  # nothing, on every version
        transport.writelines([b"a", bytearray(b"b"), memoryview(b"c")])
        if PY312:
            # memoryview(item) is the conversion now: anything with a buffer.
            transport.writelines([array.array("B", b"d")])
            with pytest.raises(TypeError, match="memoryview"):
                transport.writelines([b"never sent", "text"])
        transport.writelines(iter([b"e"]))
        transport.write_eof()
        name = "writelines" if PY312 else "write"
        with pytest.raises(RuntimeError, match=rf"Cannot call {name}\(\) after write_eof\(\)"):
            transport.writelines([b"x"])
        while len(got) < (5 if PY312 else 4):
            await asyncio.sleep(0.005)
        await asyncio.sleep(0.02)
        transport.close()
        sinks[0].transport.close()
        server.close()
        await server.wait_closed()
        return bytes(got)

    assert lp.run_until_complete(main()) == (b"abcde" if PY312 else b"abce")


# --------------------------------------------------------------------- #
# 3.13                                                                   #
# --------------------------------------------------------------------- #


def test_server_close_clients(loop313):
    lp = loop313

    async def main():
        server, accepted, clients = await _server_with_clients(lp, 3)
        server.close()
        server.close_clients()
        await asyncio.wait_for(server.wait_closed(), 5) if PY312 else None
        results = await asyncio.wait_for(asyncio.gather(*(a.lost for a in accepted)), 5)
        for c in clients:
            c.close()
        return results

    assert lp.run_until_complete(main()) == [None, None, None]


def test_server_abort_clients_drops_what_was_buffered(loop313):
    lp = loop313

    async def main():
        server, accepted, clients = await _server_with_clients(lp, 2)
        for c in clients:
            c.pause_reading()
        for a in accepted:
            while a.transport.get_write_buffer_size() == 0:
                a.transport.write(b"x" * (1 << 20))
        server.close()
        server.close_clients()  # has output to flush first: not lost yet
        await asyncio.sleep(0.05)
        assert not any(a.lost.done() for a in accepted)
        server.abort_clients()
        await asyncio.wait_for(asyncio.gather(*(a.lost for a in accepted)), 5)
        for c in clients:
            c.abort()

    lp.run_until_complete(main())


def test_close_clients_is_for_this_servers_connections_only(loop313):
    lp = loop313

    async def main():
        one, accepted_one, clients_one = await _server_with_clients(lp, 1)
        two, accepted_two, clients_two = await _server_with_clients(lp, 1)
        outgoing, _ = await lp.create_connection(
            asyncio.Protocol, "127.0.0.1", two.sockets[0].getsockname()[1]
        )
        one.close()
        one.close_clients()
        await asyncio.wait_for(accepted_one[0].lost, 5)
        await asyncio.sleep(0.02)
        untouched = not accepted_two[0].lost.done() and not outgoing.is_closing()
        for t in (*clients_one, *clients_two, outgoing):
            t.abort()
        two.close()
        two.abort_clients()
        return untouched

    assert lp.run_until_complete(main()) is True


def test_serve_forever_cancelled_closes_the_clients_from_313(loop):
    lp = loop

    async def main():
        server, accepted, clients = await _server_with_clients(lp, 1)
        serving = asyncio.ensure_future(server.serve_forever())
        await asyncio.sleep(0.02)
        serving.cancel()
        if PY313:
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(serving, 5)
            closed_by_server = accepted[0].lost.done()
        else:
            await asyncio.sleep(0.05)
            closed_by_server = accepted[0].lost.done()
            accepted[0].transport.close()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(serving, 5)
        clients[0].close()
        return closed_by_server

    assert lp.run_until_complete(main()) is PY313


def test_create_server_keep_alive(loop313):
    lp = loop313

    async def main():
        results = {}
        for keep_alive in (None, True):
            server, accepted, clients = await _server_with_clients(lp, 1, keep_alive=keep_alive)
            sock = accepted[0].transport.get_extra_info("socket")
            results[keep_alive] = (
                server.sockets[0].getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE),
                sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE),
            )
            clients[0].close()
            accepted[0].transport.close()
            server.close()
        return results

    results = lp.run_until_complete(main())
    assert results[None] == (0, 0)
    assert all(results[True]), results


@needs_unix
@pytest.mark.parametrize("cleanup", [None, True, False])
def test_create_unix_server_cleanup_socket(loop313, tmp_path, cleanup):
    """3.13 removes the socket file when the server closes, unless told
    not to; before 3.13 asyncio left it, and so does cadeloop there unless
    asked."""
    lp = loop313
    path = str(tmp_path / "s.sock")
    kwargs = {} if cleanup is None else {"cleanup_socket": cleanup}

    async def main():
        server = await lp.create_unix_server(asyncio.Protocol, path, **kwargs)
        assert os.path.exists(path)
        server.close()
        await server.wait_closed()
        return os.path.exists(path)

    removed = not lp.run_until_complete(main())
    assert removed is (PY313 if cleanup is None else cleanup)


@needs_unix
def test_cleanup_socket_leaves_a_path_that_is_no_longer_its_own(loop313, tmp_path):
    lp = loop313
    path = str(tmp_path / "s.sock")

    async def main():
        server = await lp.create_unix_server(asyncio.Protocol, path, cleanup_socket=True)
        os.unlink(path)
        with open(path, "w") as f:  # somebody else's file under the same name
            f.write("not a socket")
        server.close()
        await server.wait_closed()

    lp.run_until_complete(main())
    assert open(path).read() == "not a socket"


def test_an_empty_datagram_is_sent_from_313(loop):
    lp = loop

    async def main():
        got = asyncio.Queue()

        class Rx(asyncio.DatagramProtocol):
            def datagram_received(self, data, addr):
                got.put_nowait(data)

        rx, _ = await lp.create_datagram_endpoint(Rx, local_addr=("127.0.0.1", 0))
        addr = rx.get_extra_info("sockname")
        tx, _ = await lp.create_datagram_endpoint(asyncio.DatagramProtocol, remote_addr=addr)
        tx.sendto(b"")
        tx.sendto(b"end")
        received = [await asyncio.wait_for(got.get(), 5)]
        while received[-1] != b"end":
            received.append(await asyncio.wait_for(got.get(), 5))
        tx.close()
        rx.close()
        return received

    assert lp.run_until_complete(main()) == ([b"", b"end"] if PY313 else [b"end"])


# --------------------------------------------------------------------- #
# every version: found by 3.13's tests                                   #
# --------------------------------------------------------------------- #


def _oversized():
    return b"\0" * 70_000  # more than a UDP datagram holds: the send fails


@pytest.mark.skipif(sys.platform == "win32", reason="Windows reports an oversized datagram differently")
def test_a_datagram_send_that_fails_during_close_is_still_reported():
    """close() with sends still queued: each failure reaches
    error_received, also the last one -- after which nothing is left to
    wait for and the endpoint goes -- and connection_lost follows."""
    lp = _make("cadeloop")

    async def main():
        events = []

        class P(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                self.lost = lp.create_future()

            def error_received(self, exc):
                events.append(type(exc).__name__)

            def connection_lost(self, exc):
                events.append("lost")
                self.lost.set_result(exc)

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("127.0.0.1", 0))
        transport, protocol = await lp.create_datagram_endpoint(P, sock=sock)
        addr = sock.getsockname()
        transport.sendto(b"ok", addr)
        transport.sendto(_oversized(), addr)
        transport.close()
        await asyncio.wait_for(protocol.lost, 5)
        return events

    try:
        assert lp.run_until_complete(main()) == ["OSError", "lost"]
    finally:
        _done(lp)


@pytest.mark.skipif(sys.platform == "win32", reason="Windows reports an oversized datagram differently")
def test_closing_from_error_received_still_reports_the_failures_already_queued():
    lp = _make("cadeloop")

    async def main(closer):
        events = []

        class P(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                self.transport = transport
                self.lost = lp.create_future()

            def error_received(self, exc):
                events.append("error")
                getattr(self.transport, closer)()

            def connection_lost(self, exc):
                events.append("lost")
                self.lost.set_result(exc)

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("127.0.0.1", 0))
        transport, protocol = await lp.create_datagram_endpoint(P, sock=sock)
        addr = sock.getsockname()
        transport.sendto(_oversized(), addr)
        transport.sendto(_oversized(), addr)
        await asyncio.wait_for(protocol.lost, 5)
        await asyncio.sleep(0.02)
        return events

    try:
        assert lp.run_until_complete(main("close")) == ["error", "error", "lost"]
        # abort() is the one that reports nothing more.
        assert lp.run_until_complete(main("abort")) == ["error", "lost"]
    finally:
        _done(lp)
