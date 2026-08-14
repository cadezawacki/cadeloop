#!/usr/bin/env python3
"""Benchmark regression gate (R-113): fail when any benchmark's median
regresses more than --threshold vs the stored baseline JSON.

If the baseline file does not exist yet, the check passes with a notice
(the harness output is uploaded as an artifact to seed it).
"""

import argparse
import json
import pathlib
import sys


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("current")
    parser.add_argument("baseline")
    parser.add_argument("--threshold", type=float, default=0.05)
    args = parser.parse_args()

    baseline_path = pathlib.Path(args.baseline)
    if not baseline_path.exists():
        print(f"NOTICE: no baseline at {args.baseline}; seed it from this run's artifact.")
        return 0
    current = json.loads(pathlib.Path(args.current).read_text())["results"]
    baseline = json.loads(baseline_path.read_text())["results"]

    failures = []
    skipped = []
    for bench, per_loop in baseline.items():
        for loop, base_entry in per_loop.items():
            if base_entry is None:
                continue
            cur_bench = current.get(bench, {})
            if loop in cur_bench and cur_bench[loop] is None:
                # Present but null: the harness ran this benchmark and it
                # crashed or timed out. That is a hard failure — skipping it
                # would let every benchmark die and still report PASS.
                failures.append(f"{bench}/{loop}: failed to produce a result")
                continue
            cur_entry = cur_bench.get(loop)
            if cur_entry is None:
                # A loop the baseline measured but this run did not is NOT a
                # regression — the run simply covered a narrower --loops set
                # (CI gates on `--loops cadeloop` against a baseline recorded
                # with the full contender field). Reported, never fatal:
                # silently dropping it would read as "compared everything".
                skipped.append(f"{bench}/{loop}")
                continue
            base = base_entry["median_ops_per_sec"]
            cur = cur_entry["median_ops_per_sec"]
            delta = (cur - base) / base
            marker = "REGRESSION" if delta < -args.threshold else "ok"
            print(f"{bench:24s} {loop:10s} {delta * 100:+7.2f}%  {marker}")
            if delta < -args.threshold:
                failures.append(f"{bench}/{loop}: {delta * 100:.2f}%")
    if skipped:
        print(f"\nNOTICE: {len(skipped)} baseline entr(ies) not measured in this run (not compared):")
        for s in skipped:
            print(f"  {s}")
    if failures:
        print(f"\nFAIL: {len(failures)} regression(s) beyond {args.threshold * 100:.0f}%:")
        for f in failures:
            print(f"  {f}")
        return 1
    print("\nPASS: no regressions beyond threshold")
    return 0


if __name__ == "__main__":
    sys.exit(main())
