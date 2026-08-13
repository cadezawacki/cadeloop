#!/usr/bin/env python3
"""Scheduling soak (R-113 stress job, M0 scope).

Churns callbacks, timers (scheduled + cancelled), and cross-thread wakeups
for --seconds, then asserts RSS growth stayed under --max-growth (default
5%, mirroring the R-122 10k-conn soak criterion applied to the M0 surface).
"""

import argparse
import gc
import random
import sys
import threading
import time


def rss_bytes() -> int:
    if sys.platform == "win32":
        import ctypes
        import ctypes.wintypes as wt

        class PMC(ctypes.Structure):
            _fields_ = [
                ("cb", wt.DWORD),
                ("PageFaultCount", wt.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        pmc = PMC()
        pmc.cb = ctypes.sizeof(PMC)
        # Modern Windows exports this from kernel32 (K32 prefix); psapi.dll
        # is a forwarding shim that returned 0 silently on the first
        # hardware run — check the BOOL and prefer kernel32.
        k32 = ctypes.windll.kernel32
        fn = getattr(k32, "K32GetProcessMemoryInfo", None)
        if fn is None:
            fn = ctypes.windll.psapi.GetProcessMemoryInfo
        ok = fn(k32.GetCurrentProcess(), ctypes.byref(pmc), pmc.cb)
        if not ok or pmc.WorkingSetSize == 0:
            raise OSError("GetProcessMemoryInfo failed")
        return pmc.WorkingSetSize
    # Current (not peak) RSS: ru_maxrss is a high-water mark and would
    # count any transient burst as permanent "growth".
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    import resource

    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=600)
    parser.add_argument("--max-growth", type=float, default=0.05)
    parser.add_argument(
        "--slack-bytes",
        type=int,
        default=4 * 1024 * 1024,
        help="absolute allocator-noise floor: fail only if growth exceeds "
        "max(max-growth * baseline, this). This micro-soak's baseline RSS "
        "(~25 MB) is small enough that arena/fragmentation creep (~2.5 MB "
        "plateau, measured flat across 30s vs 75s runs) breaches a bare 5%%; "
        "a real per-op leak at soak scale exceeds this floor within seconds. "
        "The R-122 10k-connection soak (M1) applies the strict 5%% on its "
        "much larger baseline.",
    )
    args = parser.parse_args()

    import cadeloop

    loop = cadeloop.new_event_loop()
    deadline = time.monotonic() + args.seconds
    rng = random.Random(1337)
    stats = {"cb": 0, "timers": 0, "cancelled": 0, "xthread": 0}
    stop_producer = threading.Event()

    def producer():
        while not stop_producer.is_set():
            for _ in range(100):
                loop.call_soon_threadsafe(count, "xthread")
            time.sleep(0.001)

    def count(kind):
        stats[kind] += 1

    def churn():
        if time.monotonic() >= deadline:
            loop.stop()
            return
        for _ in range(500):
            loop.call_soon(count, "cb")
        handles = [
            loop.call_later(rng.uniform(0.0, 0.05), count, "timers")
            for _ in range(200)
        ]
        for h in handles[: len(handles) // 2]:
            h.cancel()
            stats["cancelled"] += 1
        loop.call_later(0.001, churn)

    # Warm up fully before baselining RSS: queues, heap capacity, and
    # allocator arenas must reach steady state or their one-time growth
    # reads as a leak.
    warm_deadline = time.monotonic() + max(3.0, min(60.0, args.seconds / 3))
    loop.call_soon(churn)
    t = threading.Thread(target=producer, daemon=True)
    t.start()
    while time.monotonic() < warm_deadline:
        loop.call_later(0.05, loop.stop)
        loop.run_forever()
    gc.collect()
    base_rss = rss_bytes()
    print(f"warmed up; baseline RSS {base_rss / 1e6:.1f} MB", flush=True)

    loop.call_soon(churn)
    loop.run_forever()
    stop_producer.set()
    t.join()
    gc.collect()
    final_rss = rss_bytes()
    loop.close()

    growth_bytes = final_rss - base_rss
    growth = growth_bytes / base_rss
    allowance = max(args.max_growth * base_rss, args.slack_bytes)
    print(
        f"done: {stats} | RSS {base_rss / 1e6:.1f} -> {final_rss / 1e6:.1f} MB "
        f"({growth * 100:+.2f}%, allowance {allowance / 1e6:.1f} MB)",
        flush=True,
    )
    if growth_bytes > allowance:
        print(
            f"FAIL: RSS growth {growth_bytes / 1e6:.1f} MB exceeds "
            f"max({args.max_growth * 100:.0f}%, {args.slack_bytes / 1e6:.0f} MB)"
        )
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
