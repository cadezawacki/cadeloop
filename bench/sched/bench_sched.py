#!/usr/bin/env python3
"""Scheduling microbenchmarks: one (loop, bench) pair per process.

Prints a single JSON line: {"loop", "bench", "ops", "seconds", "ops_per_sec"}.

These benchmark the L1 scheduling core only (call_soon, timer heap, task
stepping, cross-thread wakeup). They are NOT the R-003 acceptance
benchmarks — those are Windows two-machine TCP/HTTP runs (R-131) and land
with M1/M2.
"""

import argparse
import asyncio
import gc
import json
import sys
import threading
import time


def make_loop(kind: str):
    if kind == "cadeloop":
        import cadeloop

        return cadeloop.new_event_loop()
    if kind == "asyncio":
        return asyncio.new_event_loop()
    if kind == "uvloop":
        import uvloop

        return uvloop.new_event_loop()
    if kind == "rloop":
        import rloop

        return rloop.new_event_loop()
    raise SystemExit(f"unknown loop kind: {kind}")


# --------------------------------------------------------------------- #
# benchmarks: each returns (ops, seconds)                               #
# --------------------------------------------------------------------- #


def bench_call_soon_chain(loop, n):
    """Per-callback dispatch cost: each callback schedules the next."""

    def cb(i):
        if i == 0:
            loop.stop()
        else:
            loop.call_soon(cb, i - 1)

    loop.call_soon(cb, n)
    t0 = time.perf_counter()
    loop.run_forever()
    return n, time.perf_counter() - t0


def bench_call_soon_burst(loop, n):
    """Bulk schedule + drain of independent callbacks."""
    sink = (lambda: None)
    t0 = time.perf_counter()
    for _ in range(n):
        loop.call_soon(sink)
    loop.call_soon(loop.stop)
    loop.run_forever()
    return n, time.perf_counter() - t0


def bench_timer_schedule_cancel(loop, n):
    """Timer heap churn: schedule far-future timers, cancel them all."""
    noop = (lambda: None)
    t0 = time.perf_counter()
    handles = [loop.call_later(60.0, noop) for _ in range(n)]
    for h in handles:
        h.cancel()
    loop.call_soon(loop.stop)
    loop.run_forever()
    return n, time.perf_counter() - t0


def bench_timer_fire(loop, n):
    """Timer heap throughput: n zero-delay timers scheduled then fired."""
    remaining = n

    def cb():
        nonlocal remaining
        remaining -= 1
        if remaining == 0:
            loop.stop()

    t0 = time.perf_counter()
    for _ in range(n):
        loop.call_later(0, cb)
    loop.run_forever()
    return n, time.perf_counter() - t0


def bench_sleep0_chain(loop, n):
    """Coroutine stepping: await asyncio.sleep(0) n times in one task."""

    async def main():
        for _ in range(n):
            await asyncio.sleep(0)

    t0 = time.perf_counter()
    loop.run_until_complete(main())
    return n, time.perf_counter() - t0


def bench_task_spawn(loop, n):
    """Task creation/finalization: spawn n trivial tasks, gather."""

    async def child():
        pass

    async def main():
        await asyncio.gather(*[loop.create_task(child()) for _ in range(n)])

    t0 = time.perf_counter()
    loop.run_until_complete(main())
    return n, time.perf_counter() - t0


def bench_threadsafe_throughput(loop, n):
    """call_soon_threadsafe flood from one producer thread (R-022 path)."""
    received = 0

    def cb():
        nonlocal received
        received += 1
        if received == n:
            loop.stop()

    def producer():
        for _ in range(n):
            loop.call_soon_threadsafe(cb)

    t = threading.Thread(target=producer)
    t0 = time.perf_counter()
    loop.call_soon(t.start)
    loop.run_forever()
    dt = time.perf_counter() - t0
    t.join()
    assert received == n
    return n, dt


BENCHES = {
    "call_soon_chain": (bench_call_soon_chain, 200_000),
    "call_soon_burst": (bench_call_soon_burst, 200_000),
    "timer_schedule_cancel": (bench_timer_schedule_cancel, 100_000),
    "timer_fire": (bench_timer_fire, 100_000),
    "sleep0_chain": (bench_sleep0_chain, 100_000),
    "task_spawn": (bench_task_spawn, 20_000),
    "threadsafe_throughput": (bench_threadsafe_throughput, 50_000),
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--loop", required=True)
    parser.add_argument("--bench", required=True, choices=sorted(BENCHES))
    parser.add_argument("--scale", type=int, default=0, help="override op count")
    args = parser.parse_args()

    fn, default_n = BENCHES[args.bench]
    n = args.scale or default_n
    loop = make_loop(args.loop)
    asyncio.set_event_loop(loop)
    try:
        gc.collect()
        ops, seconds = fn(loop, n)
    finally:
        asyncio.set_event_loop(None)
        loop.close()
    print(
        json.dumps(
            {
                "loop": args.loop,
                "bench": args.bench,
                "ops": ops,
                "seconds": seconds,
                "ops_per_sec": ops / seconds if seconds > 0 else 0.0,
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
