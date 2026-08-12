#!/usr/bin/env python3
"""Benchmark harness (R-130): orchestrates runs, 3 warmup + 5 measured,
reports median (+ min/max), writes a JSON baseline.

Suites:
  sched  — L1 scheduling microbenchmarks (bench/sched/bench_sched.py),
           runnable on any platform. Fresh subprocess per run for
           isolation.
  echo / http — arrive with M1/M2 (two-machine authoritative per R-131).

Example:
  python bench/harness/harness.py --suite sched \
      --loops cadeloop,asyncio,uvloop --out results.json --markdown
"""

import argparse
import json
import pathlib
import statistics
import subprocess
import sys

WARMUP = 3  # R-130
MEASURED = 5  # R-130

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCHED = ROOT / "bench" / "sched" / "bench_sched.py"

SCHED_BENCHES = [
    "call_soon_chain",
    "call_soon_burst",
    "timer_schedule_cancel",
    "timer_fire",
    "sleep0_chain",
    "task_spawn",
    "threadsafe_throughput",
]


def run_one(loop: str, bench: str, scale: int) -> float | None:
    cmd = [sys.executable, str(SCHED), "--loop", loop, "--bench", bench]
    if scale:
        cmd += ["--scale", str(scale)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr[-2000:] + "\n")
        return None
    return json.loads(proc.stdout.strip().splitlines()[-1])["ops_per_sec"]


def bench_suite(loops: list[str], scale: int) -> dict:
    results: dict = {}
    for bench in SCHED_BENCHES:
        results[bench] = {}
        for loop in loops:
            runs = []
            failed = False
            for i in range(WARMUP + MEASURED):
                ops = run_one(loop, bench, scale)
                if ops is None:
                    failed = True
                    break
                if i >= WARMUP:
                    runs.append(ops)
            if failed:
                print(f"  {bench:24s} {loop:10s} FAILED", flush=True)
                results[bench][loop] = None
                continue
            entry = {
                "median_ops_per_sec": statistics.median(runs),
                "min": min(runs),
                "max": max(runs),
                "runs": runs,
            }
            results[bench][loop] = entry
            print(
                f"  {bench:24s} {loop:10s} "
                f"{entry['median_ops_per_sec'] / 1e6:8.3f} M ops/s "
                f"[{entry['min'] / 1e6:.3f}..{entry['max'] / 1e6:.3f}]",
                flush=True,
            )
    return results


def to_markdown(results: dict, loops: list[str], baseline: str) -> str:
    lines = [
        "| benchmark | " + " | ".join(loops) + " | best vs " + baseline + " |",
        "|---|" + "---|" * (len(loops) + 1),
    ]
    for bench, per_loop in results.items():
        cells = []
        base = per_loop.get(baseline)
        base_v = base["median_ops_per_sec"] if base else None
        best_loop, best_v = None, -1.0
        for lp in loops:
            e = per_loop.get(lp)
            if e is None:
                cells.append("—")
                continue
            v = e["median_ops_per_sec"]
            if v > best_v:
                best_loop, best_v = lp, v
            rel = f" ({v / base_v:.2f}x)" if base_v and lp != baseline else ""
            cells.append(f"{v / 1e6:.2f}M{rel}")
        ratio = f"{best_v / base_v:.2f}x ({best_loop})" if base_v else "—"
        lines.append(f"| {bench} | " + " | ".join(cells) + f" | {ratio} |")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suite", choices=["sched"], default="sched")
    parser.add_argument("--loops", default="cadeloop,asyncio,uvloop")
    parser.add_argument("--scale", type=int, default=0)
    parser.add_argument("--out", default="bench-results.json")
    parser.add_argument("--markdown", action="store_true")
    parser.add_argument("--baseline-loop", default="asyncio")
    args = parser.parse_args()

    loops = [l.strip() for l in args.loops.split(",") if l.strip()]
    print(f"suite={args.suite} loops={loops} warmup={WARMUP} measured={MEASURED}")
    results = bench_suite(loops, args.scale)

    payload = {
        "suite": args.suite,
        "python": sys.version,
        "platform": sys.platform,
        "warmup": WARMUP,
        "measured": MEASURED,
        "results": results,
    }
    pathlib.Path(args.out).write_text(json.dumps(payload, indent=2))
    print(f"wrote {args.out}")
    if args.markdown:
        print()
        print(to_markdown(results, loops, args.baseline_loop))
    return 0


if __name__ == "__main__":
    sys.exit(main())
