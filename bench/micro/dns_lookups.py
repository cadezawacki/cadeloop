"""What dedicated resolver threads and shared lookups are worth (ADR-44).

Two situations, each with lookups in the default executor (asyncio's
arrangement, `resolver_threads=0`) and on the resolver's own threads:

1. The application's blocking calls fill the default executor -- here 64
   calls of 200 ms, the shape of database queries through
   `run_in_executor` or `asyncio.to_thread` -- and one lookup is made.
   How long does it wait?
2. 200 coroutines look the same name up at once, with a resolver that
   takes 20 ms to answer (`socket.getaddrinfo` wrapped with a sleep: a
   round trip to a DNS server, which this machine does not have). How
   long until the last one has its answer, and how many lookups reached
   the resolver?

    python bench/micro/dns_lookups.py
"""

import asyncio
import socket
import statistics
import time

import cadeloop
from cadeloop.loop import Loop

REAL = socket.getaddrinfo
CALLS = [0]


def slow_getaddrinfo(*args, **kwargs):
    CALLS[0] += 1
    time.sleep(0.020)
    return REAL(*args, **kwargs)


async def behind_blocking_work(loop):
    jobs = [loop.run_in_executor(None, time.sleep, 0.2) for _ in range(64)]
    await asyncio.sleep(0.01)  # the executor's threads are all busy now
    started = time.perf_counter()
    await loop.getaddrinfo("localhost", 80, type=socket.SOCK_STREAM)
    waited = time.perf_counter() - started
    await asyncio.gather(*jobs)
    return waited


async def same_name_at_once(loop):
    CALLS[0] = 0
    socket.getaddrinfo = slow_getaddrinfo
    try:
        started = time.perf_counter()
        await asyncio.gather(
            *(loop.getaddrinfo("localhost", 80, type=socket.SOCK_STREAM) for _ in range(200))
        )
        return time.perf_counter() - started, CALLS[0]
    finally:
        socket.getaddrinfo = REAL


def run(resolver_threads):
    waits, totals, calls = [], [], 0
    for _ in range(5):
        loop = Loop(resolver_threads=resolver_threads)
        try:
            waits.append(loop.run_until_complete(behind_blocking_work(loop)))
            total, calls = loop.run_until_complete(same_name_at_once(loop))
            totals.append(total)
        finally:
            loop.run_until_complete(loop.shutdown_default_executor())
            loop.close()
    return statistics.median(waits), statistics.median(totals), calls


def main():
    print(f"cadeloop {cadeloop.__version__}")
    for threads in (0, 16):
        wait, total, calls = run(threads)
        print(
            f"resolver_threads={threads:<2}  one lookup behind 64 blocking calls: {wait * 1e3:8.2f} ms   "
            f"200 lookups of one name: {total * 1e3:7.1f} ms, {calls} reached the resolver"
        )


if __name__ == "__main__":
    main()
