"""Timer and cross-thread wakeup latency per event loop (Windows or Linux).

  python bench/suite/latency.py --pkg candidate=PATH --out timers.json

Per loop, in a fresh process each:
  * overshoot of asyncio.sleep(d) past d on an idle loop, d = 0.5..50 ms
  * the same while another task keeps the loop busy with short callbacks
  * call_soon_threadsafe(): another thread's call until the callback runs,
    with the loop parked
Times are perf_counter deltas in milliseconds (QueryPerformanceCounter on
Windows, CLOCK_MONOTONIC on Linux).
"""

import argparse
import json
import os
import subprocess
import sys

PROBE = r"""
import asyncio, json, random, sys, threading, time

name = sys.argv[1]
if name == "asyncio":
    loop = asyncio.new_event_loop()
else:
    loop = __import__(name).new_event_loop()
asyncio.set_event_loop(loop)

def stats(xs):
    xs = sorted(xs)
    return {"p50": xs[len(xs) // 2], "p99": xs[min(len(xs) - 1, int(len(xs) * 0.99))], "max": xs[-1], "n": len(xs)}

async def overshoot(d, n):
    out = []
    for _ in range(n):
        t0 = time.perf_counter()
        await asyncio.sleep(d)
        out.append((time.perf_counter() - t0 - d) * 1e3)
    return stats(out)

async def busy():
    # A steady stream of short callbacks: the loop never parks.
    while True:
        await asyncio.sleep(0)

async def main():
    res = {}
    for d in (0.0005, 0.001, 0.002, 0.005, 0.01, 0.05):
        res[repr(d)] = await overshoot(d, 200 if d <= 0.005 else 40)
    t = asyncio.ensure_future(busy())
    res["busy-0.001"] = await overshoot(0.001, 200)
    t.cancel()
    try:
        await t
    except asyncio.CancelledError:
        pass
    lat = []
    done = asyncio.Event()

    def cb(t0):
        lat.append((time.perf_counter() - t0) * 1e3)
        if len(lat) == 300:
            done.set()

    def producer():
        for _ in range(300):
            time.sleep(random.uniform(0.002, 0.004))
            loop.call_soon_threadsafe(cb, time.perf_counter())

    threading.Thread(target=producer, daemon=True).start()
    await done.wait()
    res["xthread"] = stats(lat)
    return res

print("RESULT " + json.dumps(loop.run_until_complete(main())))
loop.close()
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pkg", action="append", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    pkgs = dict(p.split("=", 1) for p in a.pkg)
    runs = [(f"cadeloop[{n}]", "cadeloop", p) for n, p in pkgs.items()]
    anyp = next(iter(pkgs.values()))
    others = ("winloop", "asyncio") if os.name == "nt" else ("uvloop", "rloop", "rsloop", "asyncio")
    runs += [(name, name, anyp) for name in others]
    out = {}
    for label, kind, pkg in runs:
        env = dict(os.environ, PYTHONPATH=pkg)
        env.pop("CADELOOP_COARSE_TIMERS", None)
        p = subprocess.run([sys.executable, "-c", PROBE, kind], env=env, capture_output=True, text=True, timeout=600)
        if "RESULT " not in p.stdout:
            print(label, "FAILED", p.stderr[-1000:])
            continue
        out[label] = json.loads(p.stdout.split("RESULT ", 1)[1])
        r = out[label]
        print(f"{label:<22} sleep(1ms) +{r['0.001']['p50']:.2f}/{r['0.001']['p99']:.2f} ms  "
              f"sleep(5ms) +{r['0.005']['p50']:.2f} ms  busy +{r['busy-0.001']['p50']:.2f} ms  "
              f"xthread {r['xthread']['p50'] * 1e3:.0f}/{r['xthread']['p99'] * 1e3:.0f} us", flush=True)
    with open(a.out, "w") as f:
        json.dump(out, f, indent=1)


if __name__ == "__main__":
    main()
