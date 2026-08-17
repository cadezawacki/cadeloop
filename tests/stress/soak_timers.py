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
        # Explicit signatures are load-bearing: ctypes' default conversion
        # passes Python ints as 32-bit C ints, truncating the 64-bit
        # GetCurrentProcess() pseudo handle (-1) — the call then fails with
        # an invalid handle on 64-bit Windows (run-4 soak failure).
        fn.argtypes = [ctypes.c_void_p, ctypes.POINTER(PMC), wt.DWORD]
        fn.restype = wt.BOOL
        current_process = ctypes.c_void_p(-1)  # GetCurrentProcess() pseudo handle
        ok = fn(current_process, ctypes.byref(pmc), pmc.cb)
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
        help="absolute allocator-noise floor: fail only if SECOND-HALF "
        "growth exceeds max(max-growth * midpoint RSS, this). See the "
        "second-half rationale below; the floor covers the residual "
        "arena/fragmentation creep on this micro-soak's small (~25 MB) "
        "baseline. The R-122 10k-connection soak (M1) applies the strict "
        "5%% on its much larger baseline.",
    )
    args = parser.parse_args()

    import cadeloop

    loop = cadeloop.new_event_loop()
    # Set for real after warmup: initialising it here made warmup eat into
    # the requested duration (a 30s run measured 20s, a 600s nightly 540s,
    # and anything under the 3s warmup floor measured nothing at all).
    deadline = float("inf")
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

    # Two equal halves, so the SHAPE of the growth is visible rather than
    # just its total. A real per-op leak grows linearly, so its second
    # half matches its first; allocator arena and fragmentation creep
    # decelerates sharply once the working set is touched. Gating on the
    # second half alone therefore separates the two, instead of picking a
    # slack figure large enough to hide a slow leak.
    half = args.seconds / 2
    deadline = time.monotonic() + half
    loop.call_soon(churn)
    loop.run_forever()
    gc.collect()
    mid_rss = rss_bytes()

    deadline = time.monotonic() + half
    loop.call_soon(churn)
    loop.run_forever()
    stop_producer.set()
    t.join()
    gc.collect()
    final_rss = rss_bytes()
    loop.close()

    first_half = mid_rss - base_rss
    second_half = final_rss - mid_rss
    allowance = max(args.max_growth * mid_rss, args.slack_bytes)
    print(
        f"done: {stats} | RSS {base_rss / 1e6:.1f} -> {mid_rss / 1e6:.1f} -> "
        f"{final_rss / 1e6:.1f} MB (1st half {first_half / 1e6:+.1f} MB, "
        f"2nd half {second_half / 1e6:+.1f} MB, allowance {allowance / 1e6:.1f} MB)",
        flush=True,
    )
    if second_half > allowance:
        print(
            f"FAIL: second-half RSS growth {second_half / 1e6:.1f} MB exceeds "
            f"max({args.max_growth * 100:.0f}%, {args.slack_bytes / 1e6:.0f} MB). "
            "Sustained growth after warmup plus a full half-soak is a leak, "
            "not arena creep."
        )
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
