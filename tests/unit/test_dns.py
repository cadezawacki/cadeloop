"""The resolver (ADR-44): lookups on threads of their own, and one lookup
per name at a time.

`Loop()` keeps asyncio's arrangement (lookups in the default executor);
`resolver_threads=N` -- serve()'s default -- gives them their own, so a
lookup never waits behind the application's blocking calls, and shares one
lookup among every coroutine asking for the same name at once.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import gc
import logging
import socket
import threading

import cadeloop
import pytest
from cadeloop.loop import Loop

REAL_GETADDRINFO = socket.getaddrinfo


class _Resolver:
    """Stands in for socket.getaddrinfo: counts calls, and can hold each
    one until released."""

    def __init__(self, *, hold=False, fail=None):
        self.calls = []
        self.threads = []
        self.entered = threading.Event()
        self.release = threading.Event()
        self.fail = fail
        if not hold:
            self.release.set()

    def __call__(self, *args, **kwargs):
        self.calls.append(args)
        self.threads.append(threading.current_thread().name)
        self.entered.set()
        assert self.release.wait(20), "the test never released the lookup"
        if self.fail is not None:
            raise self.fail
        return REAL_GETADDRINFO("127.0.0.1", args[1], *args[2:], **kwargs)


@pytest.fixture
def resolver(monkeypatch):
    def install(**kwargs):
        r = _Resolver(**kwargs)
        monkeypatch.setattr(socket, "getaddrinfo", r)
        return r

    return install


@pytest.fixture
def loop_with():
    made = []

    def make(**kwargs):
        lp = Loop(**kwargs)
        made.append(lp)
        return lp

    yield make
    for lp in made:
        if not lp.is_closed():
            lp.close()


async def _until(predicate, timeout=10.0):
    end = asyncio.get_running_loop().time() + timeout
    while not predicate():
        assert asyncio.get_running_loop().time() < end, "condition never became true"
        await asyncio.sleep(0.002)


# --------------------------------------------------------------------- #
# where a lookup runs                                                    #
# --------------------------------------------------------------------- #


def test_a_plain_loop_resolves_in_the_default_executor(loop_with, resolver):
    """asyncio's arrangement, and what set_default_executor() promises."""
    r = resolver()
    lp = loop_with()
    assert lp._resolver_threads == 0
    pool = concurrent.futures.ThreadPoolExecutor(1, thread_name_prefix="the-apps-own")
    lp.set_default_executor(pool)
    try:
        lp.run_until_complete(lp.getaddrinfo("example.test", 80))
    finally:
        pool.shutdown()
    assert r.threads[0].startswith("the-apps-own"), r.threads
    assert lp._resolver is None


def test_lookups_get_threads_of_their_own(loop_with, resolver):
    r = resolver()
    lp = loop_with(resolver_threads=2)
    pool = concurrent.futures.ThreadPoolExecutor(1, thread_name_prefix="the-apps-own")
    lp.set_default_executor(pool)
    try:
        infos = lp.run_until_complete(lp.getaddrinfo("example.test", 80, type=socket.SOCK_STREAM))
        name = lp.run_until_complete(lp.getnameinfo(("127.0.0.1", 80), socket.NI_NUMERICHOST))
    finally:
        pool.shutdown()
    assert infos[0][4] == ("127.0.0.1", 80)
    assert name[0] == "127.0.0.1"
    assert r.threads[0].startswith("cadeloop-dns"), r.threads
    assert not any(t.name.startswith("the-apps-own") for t in threading.enumerate())


@pytest.mark.parametrize("threads", [0, 2])
def test_a_lookup_and_a_busy_default_executor(loop_with, resolver, threads):
    """Every thread of the default executor is inside a blocking call --
    database queries, say. With asyncio's arrangement a lookup waits for
    one of them to return; with its own threads it does not."""
    resolver()
    lp = loop_with(resolver_threads=threads)
    pool = concurrent.futures.ThreadPoolExecutor(2)
    lp.set_default_executor(pool)
    blocked = threading.Event()

    async def main():
        jobs = [lp.run_in_executor(None, blocked.wait, 20) for _ in range(2)]
        try:
            return await asyncio.wait_for(lp.getaddrinfo("example.test", 80), 0.5)
        except asyncio.TimeoutError:
            return None
        finally:
            blocked.set()
            await asyncio.gather(*jobs)

    try:
        answer = lp.run_until_complete(main())
    finally:
        pool.shutdown()
    assert (answer is not None) == (threads > 0)


# --------------------------------------------------------------------- #
# one lookup per name at a time                                          #
# --------------------------------------------------------------------- #


def test_concurrent_lookups_of_one_name_are_one_lookup(loop_with, resolver):
    r = resolver(hold=True)
    lp = loop_with(resolver_threads=4)

    async def main():
        tasks = [
            asyncio.ensure_future(lp.getaddrinfo("example.test", 443, type=socket.SOCK_STREAM))
            for _ in range(50)
        ]
        await _until(lambda: r.entered.is_set())
        await asyncio.sleep(0.02)
        r.release.set()
        return await asyncio.gather(*tasks)

    answers = lp.run_until_complete(main())
    assert len(r.calls) == 1
    assert all(a == answers[0] for a in answers)
    assert len({id(a) for a in answers}) == 50, "each caller gets a list of its own"
    stats = lp.stats()
    assert (stats["dns_lookups"], stats["dns_coalesced"]) == (1, 49)
    assert lp._dns_inflight == {}


def test_a_different_question_is_a_different_lookup(loop_with, resolver):
    r = resolver(hold=True)
    lp = loop_with(resolver_threads=8)

    async def main():
        tasks = [
            asyncio.ensure_future(lp.getaddrinfo("a.test", 80)),
            asyncio.ensure_future(lp.getaddrinfo("b.test", 80)),
            asyncio.ensure_future(lp.getaddrinfo("a.test", 81)),
            asyncio.ensure_future(lp.getaddrinfo("a.test", 80, family=socket.AF_INET)),
            asyncio.ensure_future(lp.getaddrinfo("a.test", 80)),  # the first one again
        ]
        await _until(lambda: len(r.calls) == 4)
        r.release.set()
        await asyncio.gather(*tasks)

    lp.run_until_complete(main())
    assert len(r.calls) == 4
    assert lp.stats()["dns_coalesced"] == 1


def test_without_resolver_threads_every_call_is_its_own_lookup(loop_with, resolver):
    r = resolver()
    lp = loop_with()

    async def main():
        await asyncio.gather(*(lp.getaddrinfo("example.test", 80) for _ in range(5)))

    lp.run_until_complete(main())
    assert len(r.calls) == 5
    assert lp.stats()["dns_coalesced"] == 0 and lp.stats()["dns_lookups"] == 5


def test_a_cancelled_caller_leaves_the_lookup_to_the_others(loop_with, resolver):
    r = resolver(hold=True)
    lp = loop_with(resolver_threads=2)

    async def main():
        first = asyncio.ensure_future(lp.getaddrinfo("example.test", 80))
        await _until(lambda: r.entered.is_set())
        others = [asyncio.ensure_future(lp.getaddrinfo("example.test", 80)) for _ in range(3)]
        await asyncio.sleep(0)
        first.cancel()  # the one that started it
        others[0].cancel()
        await asyncio.sleep(0.01)
        r.release.set()
        results = await asyncio.gather(first, *others, return_exceptions=True)
        return results

    results = lp.run_until_complete(main())
    assert isinstance(results[0], asyncio.CancelledError)
    assert isinstance(results[1], asyncio.CancelledError)
    assert results[2] == results[3] and results[2][0][4] == ("127.0.0.1", 80)
    assert len(r.calls) == 1


def test_a_failed_lookup_fails_every_caller_and_is_not_remembered(loop_with, resolver):
    r = resolver(hold=True, fail=socket.gaierror(socket.EAI_NONAME, "Name or service not known"))
    lp = loop_with(resolver_threads=2, dns_cache=True)
    unhandled = []
    lp.set_exception_handler(lambda loop, context: unhandled.append(context))

    async def main():
        tasks = [asyncio.ensure_future(lp.getaddrinfo("nowhere.test", 80)) for _ in range(4)]
        await _until(lambda: r.entered.is_set())
        tasks[1].cancel()
        r.release.set()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        r.fail = None
        again = await lp.getaddrinfo("nowhere.test", 80)
        return results, again

    results, again = lp.run_until_complete(main())
    gaierror, cancelled = socket.gaierror, asyncio.CancelledError
    assert [type(x) for x in results] == [gaierror, cancelled, gaierror, gaierror]
    assert again[0][4] == ("127.0.0.1", 80)
    assert len(r.calls) == 2
    gc.collect()
    lp.run_until_complete(asyncio.sleep(0))
    assert unhandled == [], unhandled


def test_a_lookup_nobody_waits_for_any_more_makes_no_noise(loop_with, resolver, caplog):
    """Every caller was cancelled, then the lookup failed: nobody is left
    to raise it to, and that is not an error to report."""
    r = resolver(hold=True, fail=socket.gaierror(socket.EAI_AGAIN, "Temporary failure in name resolution"))
    lp = loop_with(resolver_threads=1)
    unhandled = []
    lp.set_exception_handler(lambda loop, context: unhandled.append(context))

    async def main():
        tasks = [asyncio.ensure_future(lp.getaddrinfo("nowhere.test", 80)) for _ in range(3)]
        await _until(lambda: r.entered.is_set())
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        r.release.set()
        await _until(lambda: not lp._dns_inflight)

    with caplog.at_level(logging.ERROR):
        lp.run_until_complete(main())
        gc.collect()
        lp.run_until_complete(asyncio.sleep(0))
    assert unhandled == [] and not caplog.records, (unhandled, caplog.records)


def test_a_shared_lookup_fills_the_cache(loop_with, resolver):
    r = resolver(hold=True)
    lp = loop_with(resolver_threads=2, dns_cache=True, dns_cache_ttl=60.0)

    async def main():
        tasks = [asyncio.ensure_future(lp.getaddrinfo("example.test", 80)) for _ in range(3)]
        await _until(lambda: r.entered.is_set())
        r.release.set()
        await asyncio.gather(*tasks)
        return await lp.getaddrinfo("example.test", 80)

    lp.run_until_complete(main())
    stats = lp.stats()
    assert len(r.calls) == 1
    assert (stats["dns_lookups"], stats["dns_coalesced"], stats["dns_cache_hits"]) == (1, 2, 1)


def test_debug_mode_logs_the_lookup_once(loop_with, resolver, caplog):
    resolver()
    lp = loop_with(resolver_threads=2)
    lp.set_debug(True)

    async def main():
        await asyncio.gather(*(lp.getaddrinfo("example.test", 80) for _ in range(3)))

    with caplog.at_level(logging.DEBUG, logger="asyncio"):
        lp.run_until_complete(main())
    began = [rec for rec in caplog.records if rec.getMessage().startswith("Get address info example.test")]
    assert len(began) == 1


# --------------------------------------------------------------------- #
# lifetime                                                               #
# --------------------------------------------------------------------- #


def test_resolver_threads_start_when_lookups_need_them(loop_with, resolver):
    resolver()
    lp = loop_with(resolver_threads=16)

    assert lp._resolver is None
    for _ in range(5):  # one after another: one thread is enough
        lp.run_until_complete(lp.getaddrinfo("example.test", 80))
    assert len(lp._resolver._threads) == 1
    assert all(t.name.startswith("cadeloop-dns") for t in lp._resolver._threads)


def test_closing_the_loop_with_a_lookup_in_flight(loop_with, resolver, caplog):
    r = resolver(hold=True)
    lp = loop_with(resolver_threads=1)

    async def main():
        running = asyncio.ensure_future(lp.getaddrinfo("a.test", 80))
        queued = asyncio.ensure_future(lp.getaddrinfo("b.test", 80))  # behind it: one thread
        await _until(lambda: r.entered.is_set())
        return running, queued

    with caplog.at_level(logging.ERROR):
        running, queued = lp.run_until_complete(main())
        pool = lp._resolver
        lp.close()  # must not wait for the resolver
        assert lp._resolver is None
        r.release.set()
        pool.shutdown(wait=True)
    assert len(r.calls) == 1, "the queued lookup was dropped, not run for nobody"
    assert not caplog.records, [rec.getMessage() for rec in caplog.records]
    with pytest.raises(RuntimeError, match="closed"):
        asyncio.run(_closed_lookup(lp))
    del running, queued


async def _closed_lookup(lp):
    return await lp.getaddrinfo("a.test", 80)


def test_resolver_threads_is_validated():
    for bad in (-1, 1.5, "4", True, None):
        with pytest.raises((TypeError, ValueError)):
            Loop(resolver_threads=bad)
    for bad in (-1, 1025):
        with pytest.raises(ValueError, match="resolver_threads"):
            cadeloop.Config(resolver_threads=bad)
    assert cadeloop.Config().resolver_threads == 16
    assert cadeloop.Config(resolver_threads=0).resolver_threads == 0


# --------------------------------------------------------------------- #
# through the things that look names up                                  #
# --------------------------------------------------------------------- #


def test_create_connection_by_name_while_the_executor_is_busy(loop_with):
    """An outgoing connection to a host *name* -- what an HTTP client or a
    database driver makes -- while the application's own blocking calls
    hold every executor thread."""
    lp = loop_with(resolver_threads=2)
    pool = concurrent.futures.ThreadPoolExecutor(1)
    lp.set_default_executor(pool)
    blocked = threading.Event()

    async def main():
        server = await lp.create_server(asyncio.Protocol, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        job = lp.run_in_executor(None, blocked.wait, 20)
        try:
            transport, _ = await asyncio.wait_for(
                lp.create_connection(asyncio.Protocol, "localhost", port, family=socket.AF_INET), 5
            )
            transport.close()
        finally:
            blocked.set()
            await job
            server.close()
            await server.wait_closed()
        return lp.stats()["dns_lookups"]

    try:
        assert lp.run_until_complete(main()) == 1
    finally:
        pool.shutdown()


def test_aiohttp_client_resolves_through_the_loop(loop_with, resolver):
    """aiohttp's default resolver asks the loop; 20 requests to one host
    name started together are one lookup."""
    aiohttp = pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.resolver import ThreadedResolver

    r = resolver()
    lp = loop_with(resolver_threads=4)

    async def main():
        async def hi(request):
            return web.Response(text="hi")

        app = web.Application()
        app.router.add_get("/", hi)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        try:
            # use_dns_cache=False: aiohttp's own cache and its own sharing
            # of lookups out of the way, so that every request asks.
            connector = aiohttp.TCPConnector(resolver=ThreadedResolver(), use_dns_cache=False, limit=0)
            async with aiohttp.ClientSession(connector=connector) as session:

                async def get():
                    async with session.get(f"http://upstream.test:{port}/") as resp:
                        return await resp.text()

                return await asyncio.gather(*(get() for _ in range(20)))
        finally:
            await runner.cleanup()

    asyncio.set_event_loop(lp)
    try:
        bodies = lp.run_until_complete(main())
    finally:
        asyncio.set_event_loop(None)
    assert bodies == ["hi"] * 20
    stats = lp.stats()
    assert stats["dns_lookups"] + stats["dns_coalesced"] == 20
    assert len(r.calls) == stats["dns_lookups"] < 20
