#!/usr/bin/env python3
"""HTTP/ASGI benchmark driven by `wrk`, not by a Python client.

Why this exists alongside harness.py's `--suite http`: that suite's load
generator is `client.py`, 64 Python threads in one process. On a small box
it becomes the bottleneck rather than the server -- splitting the same
offered load across two client *processes* nearly doubles the measured
total (23.3K -> 43.9K req/s against the same unchanged server), which is
the signature of a GIL ceiling, not of a server limit. Every contender
fast enough to reach that ceiling then reports the same number and the
comparison silently stops meaning anything.

`wrk` is C with its own event loop, so the ceiling moves back to the
server under test. Results stay loopback and single-box -- useful for
relative comparison, not as absolute capacity numbers.

Usage:
    python bench/http/run_wrk.py                       # full matrix
    python bench/http/run_wrk.py --contenders cadeloop-native,granian
"""

from __future__ import annotations

import argparse
import json
import re
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
SERVER = HERE / "server.py"

WARMUP_SECONDS = 3
MEASURED_RUNS = 3
MEASURED_SECONDS = 10

DEFAULT_CONTENDERS = [
    "cadeloop-native",
    "granian",
    "uvicorn-httptools+uvloop",
    "uvicorn-httptools+asyncio",
    "uvicorn+cadeloop",
    "uvicorn+uvloop",
    "uvicorn+rloop",
    "uvicorn+rsloop",
    "uvicorn+asyncio",
    "hypercorn",
]

_RPS = re.compile(r"^Requests/sec:\s+([\d.]+)", re.M)
_PCTL = re.compile(r"^\s+(\d+)%\s+([\d.]+)(us|ms|s)\s*$", re.M)
_ERRORS = re.compile(r"^\s+Socket errors:.*$", re.M)
_NON_2XX = re.compile(r"^\s+Non-2xx or 3xx responses:\s+(\d+)", re.M)


def _to_us(value: str, unit: str) -> float:
    return float(value) * {"us": 1.0, "ms": 1000.0, "s": 1_000_000.0}[unit]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_ready(port: int, proc: subprocess.Popen, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early with code {proc.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("server did not become ready")


def run_wrk(port: int, seconds: int, threads: int, conns: int) -> dict:
    proc = subprocess.run(
        [
            "wrk",
            "-t", str(threads),
            "-c", str(conns),
            "-d", f"{seconds}s",
            "--latency",
            f"http://127.0.0.1:{port}/",
        ],
        capture_output=True,
        text=True,
        timeout=seconds + 60,
    )
    out = proc.stdout
    rps = _RPS.search(out)
    if not rps:
        # wrk reports a refused connection or a mid-run server death on
        # stderr and leaves stdout empty, so reporting stdout alone said
        # only "could not parse" with nothing after the colon.
        raise RuntimeError(
            f"wrk exited {proc.returncode} with no parsable result\n"
            f"stdout: {out.strip() or '(empty)'}\n"
            f"stderr: {proc.stderr.strip() or '(empty)'}"
        )
    pctl = {int(p): _to_us(v, u) for p, v, u in _PCTL.findall(out)}
    bad = _NON_2XX.search(out)
    return {
        "rps": float(rps.group(1)),
        "p50_us": pctl.get(50),
        "p99_us": pctl.get(99),
        # A contender that answers fast because it is answering wrongly is
        # not a faster contender; surface it rather than averaging it in.
        "non_2xx": int(bad.group(1)) if bad else 0,
        "socket_errors": bool(_ERRORS.search(out)),
    }


def bench(contender: str, threads: int, conns: int) -> dict | None:
    server_kind, _, loop_kind = contender.partition("+")
    loop_kind = loop_kind or "asyncio"
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, str(SERVER), "--server", server_kind,
         "--loop", loop_kind, "--port", str(port)],
        cwd=str(HERE),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        wait_ready(port, proc)
        run_wrk(port, WARMUP_SECONDS, threads, conns)
        runs = [run_wrk(port, MEASURED_SECONDS, threads, conns) for _ in range(MEASURED_RUNS)]
    except Exception as exc:  # noqa: BLE001 - one contender failing is data, not a crash
        print(f"  {contender:28s} FAILED: {exc}", flush=True)
        return None
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

    result = {
        "median_rps": statistics.median(r["rps"] for r in runs),
        "median_p50_us": statistics.median(r["p50_us"] for r in runs),
        "median_p99_us": statistics.median(r["p99_us"] for r in runs),
        "non_2xx": sum(r["non_2xx"] for r in runs),
        "socket_errors": any(r["socket_errors"] for r in runs),
        "runs": runs,
    }
    flag = ""
    if result["non_2xx"]:
        flag = f"  !! {result['non_2xx']} non-2xx"
    print(
        f"  {contender:28s} {result['median_rps'] / 1e3:8.2f} K req/s   "
        f"p50 {result['median_p50_us']:8.0f}us   p99 {result['median_p99_us']:8.0f}us{flag}",
        flush=True,
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contenders", default=",".join(DEFAULT_CONTENDERS))
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--conns", type=int, default=64)
    parser.add_argument("--out", default="bench-http-wrk.json")
    args = parser.parse_args()

    contenders = [c.strip() for c in args.contenders.split(",") if c.strip()]
    print(
        f"wrk -t{args.threads} -c{args.conns}  "
        f"warmup={WARMUP_SECONDS}s  measured={MEASURED_RUNS}x{MEASURED_SECONDS}s"
    )
    results = {c: bench(c, args.threads, args.conns) for c in contenders}

    payload = {
        "suite": "http-wrk",
        "python": sys.version,
        "platform": sys.platform,
        "params": {
            "threads": args.threads,
            "conns": args.conns,
            "warmup_seconds": WARMUP_SECONDS,
            "measured_runs": MEASURED_RUNS,
            "measured_seconds": MEASURED_SECONDS,
        },
        "results": results,
    }
    Path(args.out).write_text(json.dumps(payload, indent=2))
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
