#!/usr/bin/env python3
"""Benchmark harness (R-130): orchestrates runs, 3 warmup + 5 measured,
reports medians, writes JSON baselines.

Suites:
  sched — L1 scheduling microbenchmarks (fresh subprocess per run)
  echo  — TCP echo throughput/latency: 1 KiB messages, 64 connections
          (the R-003 shape), raw-Protocol server per loop, threaded
          blocking-socket client
  http  — HTTP/1.1 keep-alive plaintext RPS/latency: uvicorn on each loop,
          hypercorn, socketify (reference ceiling), threaded keep-alive
          client

Two-machine numbers remain the authoritative acceptance data (R-131);
loopback results are labeled as such wherever they are published.

Examples:
  python bench/harness/harness.py --suite sched --loops cadeloop,asyncio,uvloop,rloop,rsloop
  python bench/harness/harness.py --suite echo --loops cadeloop,asyncio,uvloop
  python bench/harness/harness.py --suite http --contenders uvicorn+cadeloop,uvicorn+uvloop,socketify
"""

import argparse
import json
import pathlib
import signal
import socket
import statistics
import subprocess
import sys
import time

WARMUP = 3  # R-130
MEASURED = 5  # R-130

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCHED = ROOT / "bench" / "sched" / "bench_sched.py"
ECHO_SERVER = ROOT / "bench" / "echo" / "server.py"
ECHO_CLIENT = ROOT / "bench" / "echo" / "client.py"
HTTP_SERVER = ROOT / "bench" / "http" / "server.py"
HTTP_CLIENT = ROOT / "bench" / "http" / "client.py"

SCHED_BENCHES = [
    "call_soon_chain",
    "call_soon_burst",
    "timer_schedule_cancel",
    "timer_fire",
    "sleep0_chain",
    "task_spawn",
    "threadsafe_throughput",
    "future_chain",
    "gather_fanin",
    "queue_pingpong",
]


# Hard cap per run: a hung contender records as FAILED. The slowest
# legitimate run observed anywhere (asyncio threadsafe_throughput, 50k ops
# at ~36k ops/s on Windows) finishes in under 3s including startup, so 12s
# is 4x headroom while a hang costs seconds, not minutes.
RUN_TIMEOUT = 12


def run_json(cmd: list[str]) -> dict | None:
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=RUN_TIMEOUT)
    except subprocess.TimeoutExpired:
        sys.stderr.write("TIMEOUT (>" + str(RUN_TIMEOUT) + "s): " + " ".join(cmd) + "\n")
        return None
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr[-2000:] + "\n")
        return None
    try:
        return json.loads(proc.stdout.strip().splitlines()[-1])
    except (json.JSONDecodeError, IndexError):
        sys.stderr.write(f"unparseable output from {cmd}: {proc.stdout[-500:]}\n")
        return None


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class ServerProc:
    """Benchmark server subprocess with READY-line + connect handshake."""

    def __init__(self, cmd: list[str], port: int, cwd=None):
        self.proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, cwd=cwd
        )
        self.port = port
        deadline = time.monotonic() + 10
        ready = False
        while time.monotonic() < deadline:
            line = self.proc.stdout.readline()
            if not line:
                break
            if line.startswith("READY"):
                ready = True
                break
        if not ready:
            self.stop()
            raise RuntimeError(f"server failed to start: {cmd}")
        while time.monotonic() < deadline:
            # A server that printed READY and then died (e.g. backend
            # construction failed inside serve()) must fail instantly, not
            # after the full connect deadline.
            if self.proc.poll() is not None:
                raise RuntimeError(f"server exited with {self.proc.returncode} before accepting")
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.05)
        self.stop()
        raise RuntimeError(f"server not accepting on :{port}")

    def stop(self):
        if self.proc.poll() is None:
            try:
                self.proc.send_signal(signal.SIGINT)
            except (ValueError, OSError):
                # Windows: SIGINT is not deliverable to a plain subprocess.
                self.proc.terminate()
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()


def summarize(runs: list[dict], key: str) -> dict:
    vals = [r[key] for r in runs]
    return {
        "median_" + key: statistics.median(vals),
        "min": min(vals),
        "max": max(vals),
        "median_p50_us": statistics.median(r.get("p50_us", 0) for r in runs),
        "median_p99_us": statistics.median(r.get("p99_us", 0) for r in runs),
        "runs": vals,
    }


def bench_sched(loops: list[str], scale: int) -> dict:
    results: dict = {}
    for bench in SCHED_BENCHES:
        results[bench] = {}
        for loop in loops:
            runs = []
            failed = False
            for i in range(WARMUP + MEASURED):
                cmd = [sys.executable, str(SCHED), "--loop", loop, "--bench", bench]
                if scale:
                    cmd += ["--scale", str(scale)]
                r = run_json(cmd)
                if r is None:
                    failed = True
                    break
                if i >= WARMUP:
                    runs.append(r["ops_per_sec"])
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
                f"  {bench:24s} {loop:10s} {entry['median_ops_per_sec'] / 1e6:8.3f} M ops/s",
                flush=True,
            )
    return results


def bench_echo(loops: list[str], conns: int, size: int, msgs: int) -> dict:
    results: dict = {}
    for loop in loops:
        port = free_port()
        try:
            server = ServerProc(
                [sys.executable, str(ECHO_SERVER), "--loop", loop, "--port", str(port)], port
            )
        except RuntimeError as e:
            print(f"  echo {loop:10s} SERVER FAILED: {e}", flush=True)
            results[loop] = None
            continue
        try:
            runs = []
            failed = False
            for i in range(WARMUP + MEASURED):
                r = run_json(
                    [
                        sys.executable,
                        str(ECHO_CLIENT),
                        "--port",
                        str(port),
                        "--conns",
                        str(conns),
                        "--size",
                        str(size),
                        "--msgs",
                        str(msgs),
                    ]
                )
                if r is None:
                    failed = True
                    break
                if i >= WARMUP:
                    runs.append(r)
            results[loop] = None if failed else summarize(runs, "msgs_per_sec")
            if results[loop]:
                e = results[loop]
                print(
                    f"  echo {loop:10s} {e['median_msgs_per_sec'] / 1e3:8.1f} K msg/s  "
                    f"p50 {e['median_p50_us']:7.1f}us  p99 {e['median_p99_us']:7.1f}us",
                    flush=True,
                )
        finally:
            server.stop()
    return results


def bench_http(contenders: list[str], conns: int, seconds: float) -> dict:
    results: dict = {}
    for contender in contenders:
        if "+" in contender:
            server_kind, loop_kind = contender.split("+", 1)
        else:
            server_kind, loop_kind = contender, "asyncio"
        port = free_port()
        try:
            server = ServerProc(
                [
                    sys.executable,
                    str(HTTP_SERVER),
                    "--server",
                    server_kind,
                    "--loop",
                    loop_kind,
                    "--port",
                    str(port),
                ],
                port,
                cwd=str(ROOT / "bench" / "http"),
            )
        except RuntimeError as e:
            print(f"  http {contender:18s} SERVER FAILED: {e}", flush=True)
            results[contender] = None
            continue
        try:
            runs = []
            failed = False
            for i in range(WARMUP + MEASURED):
                r = run_json(
                    [
                        sys.executable,
                        str(HTTP_CLIENT),
                        "--port",
                        str(port),
                        "--conns",
                        str(conns),
                        "--seconds",
                        str(seconds if i >= WARMUP else max(1.0, seconds / 2)),
                    ]
                )
                if r is None or r.get("errors", 1) > conns:  # tolerate few reconnects
                    failed = True
                    break
                if i >= WARMUP:
                    runs.append(r)
            results[contender] = None if failed else summarize(runs, "rps")
            if results[contender]:
                e = results[contender]
                print(
                    f"  http {contender:18s} {e['median_rps'] / 1e3:8.2f} K req/s  "
                    f"p50 {e['median_p50_us']:7.0f}us  p99 {e['median_p99_us']:7.0f}us",
                    flush=True,
                )
        finally:
            server.stop()
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suite", choices=["sched", "echo", "http"], default="sched")
    parser.add_argument("--loops", default="cadeloop,asyncio,uvloop")
    parser.add_argument(
        "--contenders", default="uvicorn+cadeloop,uvicorn+asyncio,uvicorn+uvloop,hypercorn,socketify"
    )
    parser.add_argument("--scale", type=int, default=0)
    parser.add_argument("--conns", type=int, default=64)
    parser.add_argument("--size", type=int, default=1024)  # R-003: 1 KiB
    parser.add_argument("--msgs", type=int, default=2000)
    parser.add_argument("--seconds", type=float, default=3.0)
    parser.add_argument("--out", default="bench-results.json")
    args = parser.parse_args()

    loops = [x.strip() for x in args.loops.split(",") if x.strip()]
    print(f"suite={args.suite} warmup={WARMUP} measured={MEASURED}")
    if args.suite == "sched":
        results = bench_sched(loops, args.scale)
    elif args.suite == "echo":
        results = bench_echo(loops, args.conns, args.size, args.msgs)
    else:
        contenders = [x.strip() for x in args.contenders.split(",") if x.strip()]
        results = bench_http(contenders, args.conns, args.seconds)

    payload = {
        "suite": args.suite,
        "python": sys.version,
        "platform": sys.platform,
        "warmup": WARMUP,
        "measured": MEASURED,
        "params": {
            "conns": args.conns,
            "size": args.size,
            "msgs": args.msgs,
            "seconds": args.seconds,
        },
        "results": results,
    }
    pathlib.Path(args.out).write_text(json.dumps(payload, indent=2))
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
