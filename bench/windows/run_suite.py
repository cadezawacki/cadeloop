"""Windows benchmark runner: native load (bench/loadgen) against real servers.

  python bench/windows/run_suite.py --suite http,echo,ws,tls,idle \
      --pkg baseline=PATH --pkg candidate=PATH --reps 3 --out DIR

Each (contender, rep) gets a FRESH server process. A server is used only
once a real request through it succeeds (no startup sleeps). While the
load runs, the server's whole process tree is sampled: a venv python.exe
is a launcher whose CHILD is the interpreter, and spawn-model workers are
grandchildren. Contender order rotates every rep. Every run is one JSON
line in DIR/runs.jsonl, kept whole (histograms included): nothing is
averaged away here; report.py aggregates.
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
import platform
import socket
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
import winproc  # noqa: E402

PY = sys.executable
SERVERS = os.path.join(HERE, "servers.py")
_ports = itertools.count(20000 + os.getpid() % 2000 * 10)


def free_port() -> int:
    while True:
        p = next(_ports)
        s = socket.socket()
        try:
            s.bind(("127.0.0.1", p))
            return p
        except OSError:
            continue
        finally:
            s.close()


# ---- contenders ------------------------------------------------------------

CADELOOP_ONLY = False


ONLY: set[str] = set()


def only(cons):
    """Drop the third-party contenders (an A/B of two cadeloop builds), or
    keep just the ones named by --only."""
    if ONLY:
        return [c for c in cons if c[0] in ONLY]
    return [c for c in cons if c[0].startswith("cadeloop[")] if CADELOOP_ONLY else cons


def http_contenders(pkgs):
    out = []
    for name, path in pkgs.items():
        out.append((f"cadeloop[{name}]", ["--impl", "cadeloop"], path))
    if CADELOOP_ONLY and not ONLY:
        return out
    any_pkg = next(iter(pkgs.values()))
    cand = pkgs.get("candidate", any_pkg)
    out += [
        ("uvicorn+asyncio", ["--impl", "uvicorn", "--loop", "asyncio"], any_pkg),
        ("uvicorn+winloop", ["--impl", "uvicorn", "--loop", "winloop"], any_pkg),
        ("uvicorn+cadeloop", ["--impl", "uvicorn", "--loop", "cadeloop"], cand),
        ("hypercorn+asyncio", ["--impl", "hypercorn", "--loop", "asyncio"], any_pkg),
    ]
    return only(out)


HTTP_WORKLOADS = [
    # name, loadgen args
    ("plaintext-c64", ["-path", "/", "-c", "64", "-expect-body", "Hello, World!"]),
    ("plaintext-c1", ["-path", "/", "-c", "1", "-expect-body", "Hello, World!"]),
    ("pipelined16-c64", ["-path", "/", "-c", "64", "-pipeline", "16", "-expect-len", "13"]),
    ("json-c64", ["-path", "/json", "-c", "64"]),
    ("large64k-c64", ["-path", "/large", "-c", "64", "-expect-len", "65536"]),
    ("stream1m-c16", ["-path", "/stream", "-c", "16", "-expect-len", "1048576"]),
    ("upload16k-c64", ["-path", "/upload", "-c", "64", "-body", "16384", "-expect-body", "16384"]),
    ("sleep1ms-c64", ["-path", "/sleep", "-c", "64", "-expect-len", "13"]),
    ("churn-c16", ["-path", "/", "-c", "16", "-churn", "-expect-len", "13"]),
]


# ---- server lifecycle ------------------------------------------------------

class Server:
    def __init__(self, label, args, pkg, kind="http", probe="http", tls=None):
        self.label = label
        self.port = free_port()
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join([pkg, HERE]) if pkg else HERE
        env.pop("CADELOOP_COARSE_TIMERS", None)
        cmd = [PY, SERVERS, kind, "--port", str(self.port), *args]
        if tls:
            cmd += ["--tls", *tls]
        self.cmd = cmd
        self.log = open(os.path.join(OUT, f"server-{label.replace('[', '_').replace(']', '')}-{self.port}.log"), "w")
        self.proc = subprocess.Popen(cmd, env=env, cwd=HERE, stdout=self.log, stderr=subprocess.STDOUT)
        self.sampler = winproc.TreeSampler(self.proc.pid)
        self.probe = probe
        self.tls = bool(tls)

    def wait_ready(self, timeout=30.0):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"{self.label} exited with {self.proc.returncode} before ready")
            try:
                if self._probe_once():
                    return
            except OSError as e:
                last = e
            time.sleep(0.05)
        raise RuntimeError(f"{self.label} not ready in {timeout}s ({last})")

    def _probe_once(self):
        with socket.create_connection(("127.0.0.1", self.port), timeout=2) as s:
            if self.probe == "echo":
                s.sendall(b"ping")
                return s.recv(16) == b"ping"
            if self.tls:
                import ssl

                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                s = ctx.wrap_socket(s)
            s.sendall(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            data = b""
            while b"Hello, World!" not in data:
                chunk = s.recv(4096)
                if not chunk:
                    break
                data += chunk
            return data.startswith(b"HTTP/1.1 200") and b"Hello, World!" in data

    def alive(self):
        return self.proc.poll() is None

    def stop(self):
        code = self.proc.poll()
        if code is None:
            winproc.kill_tree(self.proc.pid)
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                pass
        self.sampler.close()
        self.log.close()
        return code


# ---- one measured run ------------------------------------------------------

LOADGEN = None
OUT = None


def run_load(server: Server, args, duration, warmup, extra_meta):
    samples = []
    stop = threading.Event()

    def sample():
        while not stop.is_set():
            try:
                samples.append(server.sampler.sample())
            except Exception:  # a sampler failure must not kill the run
                pass
            stop.wait(0.5)

    tcp0 = winproc.tcp_stats()
    th = threading.Thread(target=sample, daemon=True)
    cmd = [LOADGEN, "-port", str(server.port), "-d", str(duration), "-warmup", str(warmup), *args]
    t0 = time.perf_counter()
    th.start()
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=duration + warmup + 60)
    stop.set()
    th.join()
    tcp1 = winproc.tcp_stats()
    res = None
    if "RESULT " in proc.stdout:
        res = json.loads(proc.stdout.split("RESULT ", 1)[1])
    rec = {
        **extra_meta,
        "server": server.label,
        "server_cmd": server.cmd,
        "loadgen_cmd": cmd,
        "loadgen_exit": proc.returncode,
        "loadgen_stderr": proc.stderr[-2000:],
        "server_alive_after": server.alive(),
        "wall_s": time.perf_counter() - t0,
        "tcp_delta": {k: tcp1[k] - tcp0[k] for k in ("InSegs", "OutSegs", "RetransSegs", "EstabResets", "AttemptFails")},
        "tcp_after": {k: tcp1[k] for k in ("CurrEstab", "NumConns")},
        "samples": samples,
        "result": res,
    }
    # Server cost over the measured window only (samples after warmup).
    if res and samples:
        t_start = samples[0]["t"] + warmup
        win = [s for s in samples if s["t"] >= t_start] or samples
        a, b = win[0], win[-1]
        dt = b["t"] - a["t"]
        if dt > 0:
            measured = duration or res.get("measured_s") or dt
            ok_in_win = res["ok"] * min(1.0, dt / measured)
            rec["server_cpu"] = {
                "user_pct": (b["cpu_user"] - a["cpu_user"]) / dt * 100,
                "kernel_pct": (b["cpu_kernel"] - a["cpu_kernel"]) / dt * 100,
                "us_per_op": ((b["cpu_user"] - a["cpu_user"]) + (b["cpu_kernel"] - a["cpu_kernel"]))
                / max(ok_in_win, 1) * 1e6,
                "ctx_switches_per_op": (b["ctx_switches"] - a["ctx_switches"]) / max(ok_in_win, 1),
            }
        rec["server_mem_peak"] = {
            "private": max(s["private"] for s in samples),
            "ws": max(s["ws"] for s in samples),
            "handles": max(s["handles"] for s in samples),
            "threads": max(s["threads"] for s in samples),
        }
        rec["server_mem_end"] = {k: samples[-1][k] for k in ("private", "ws", "handles", "threads")}
    with open(os.path.join(OUT, "runs.jsonl"), "a") as f:
        f.write(json.dumps(rec) + "\n")
    lu = (res or {}).get("latency_us", {})
    cpu = rec.get("server_cpu", {})
    print(f"  {extra_meta.get('workload', ''):<18} {server.label:<22} "
          f"{(res or {}).get('rate', 0):>10.0f}/s  p50 {lu.get('p50', 0):>8.0f}us p99 {lu.get('p99', 0):>9.0f}us "
          f"err {(res or {}).get('error_total', '?')} cpu/op {cpu.get('us_per_op', 0):.1f}us "
          f"{'' if server.alive() else 'SERVER DIED'}", flush=True)
    return rec


def with_server(label, args, pkg, kind, probe, tls, fn):
    s = Server(label, args, pkg, kind=kind, probe=probe, tls=tls)
    try:
        s.wait_ready()
        mem0 = s.sampler.sample()
        fn(s, mem0)
    except Exception as e:
        print(f"  !! {label}: {e}", flush=True)
        with open(os.path.join(OUT, "runs.jsonl"), "a") as f:
            f.write(json.dumps({"server": label, "error": str(e), "args": args}) + "\n")
    finally:
        code = s.stop()
        if code is not None:
            print(f"  !! {label} had exited with {code}", flush=True)


# ---- suites ----------------------------------------------------------------

def suite_http(pkgs, reps, dur, warm, workloads=None):
    cons = http_contenders(pkgs)
    wl = [w for w in HTTP_WORKLOADS if not workloads or w[0] in workloads]
    for rep in range(reps):
        order = cons[rep % len(cons):] + cons[:rep % len(cons)]
        print(f"[http rep {rep}]", flush=True)
        for label, args, pkg in order:
            def body(s, mem0, label=label):
                for name, la in wl:
                    if not s.alive():
                        break
                    run_load(s, ["-mode", "http", *la], dur, warm,
                             {"suite": "http", "workload": name, "rep": rep, "mem_idle": mem0})
            with_server(label, args, pkg, "http", "http", None, body)


def suite_tls(pkgs, reps, dur, warm, cert):
    cand = pkgs.get("candidate", next(iter(pkgs.values())))
    cons = [(f"cadeloop[{n}]", ["--impl", "cadeloop"], p) for n, p in pkgs.items()]
    cons += [("uvicorn+asyncio", ["--impl", "uvicorn", "--loop", "asyncio"], cand),
             ("uvicorn+winloop", ["--impl", "uvicorn", "--loop", "winloop"], cand)]
    cons = only(cons)
    wl = [("tls-plaintext-c64", ["-path", "/", "-c", "64", "-tls", "-expect-len", "13"]),
          ("tls-churn-c16", ["-path", "/", "-c", "16", "-tls", "-churn", "-expect-len", "13"])]
    for rep in range(reps):
        print(f"[tls rep {rep}]", flush=True)
        for label, args, pkg in cons[rep % len(cons):] + cons[:rep % len(cons)]:
            def body(s, mem0):
                for name, la in wl:
                    run_load(s, ["-mode", "http", *la], dur, warm,
                             {"suite": "tls", "workload": name, "rep": rep, "mem_idle": mem0})
            with_server(label, args, pkg, "http", "http", cert, body)


def suite_ws(pkgs, reps, dur, warm):
    cand = pkgs.get("candidate", next(iter(pkgs.values())))
    cons = [(f"cadeloop[{n}]", ["--impl", "cadeloop"], p) for n, p in pkgs.items()]
    cons += [("uvicorn+asyncio", ["--impl", "uvicorn", "--loop", "asyncio"], cand),
             ("uvicorn+winloop", ["--impl", "uvicorn", "--loop", "winloop"], cand),
             ("hypercorn+asyncio", ["--impl", "hypercorn", "--loop", "asyncio"], cand)]
    cons = only(cons)
    wl = [("ws-64b-c1", ["-c", "1", "-size", "64"]),
          ("ws-64b-c64", ["-c", "64", "-size", "64"]),
          ("ws-16k-c16", ["-c", "16", "-size", "16384"]),
          ("ws-stream-1k-w32-c16", ["-c", "16", "-size", "1024", "-window", "32"])]
    for rep in range(reps):
        print(f"[ws rep {rep}]", flush=True)
        for label, args, pkg in cons[rep % len(cons):] + cons[:rep % len(cons)]:
            def body(s, mem0):
                for name, la in wl:
                    run_load(s, ["-mode", "ws", "-path", "/ws", *la], dur, warm,
                             {"suite": "ws", "workload": name, "rep": rep, "mem_idle": mem0})
            with_server(label, args, pkg, "http", "http", None, body)


def suite_echo(pkgs, reps, dur, warm):
    cons = [(f"cadeloop[{n}]", ["--loop", "cadeloop"], p) for n, p in pkgs.items()]
    anyp = next(iter(pkgs.values()))
    cons += [(f"{l}", ["--loop", l], anyp) for l in ("asyncio", "winloop", "rsloop")]
    cons = only(cons)
    wl = [("echo-1k-c1", ["-c", "1", "-size", "1024"]),
          ("echo-1k-c64", ["-c", "64", "-size", "1024"]),
          ("echo-1k-w16-c64", ["-c", "64", "-size", "1024", "-window", "16"]),
          ("echo-64k-c8", ["-c", "8", "-size", "65536"])]
    for rep in range(reps):
        print(f"[echo rep {rep}]", flush=True)
        for label, args, pkg in cons[rep % len(cons):] + cons[:rep % len(cons)]:
            def body(s, mem0):
                for name, la in wl:
                    run_load(s, ["-mode", "echo", *la], dur, warm,
                             {"suite": "echo", "workload": name, "rep": rep, "mem_idle": mem0})
            with_server(label, args, pkg, "echo", "echo", None, body)


def suite_idle(pkgs, counts):
    """Memory per idle keep-alive connection, and recovery after load."""
    cons = http_contenders(pkgs)
    for label, args, pkg in cons:
        print(f"[idle {label}]", flush=True)

        def body(s, mem0, label=label):
            for n in counts:
                before = s.sampler.sample()
                p = subprocess.Popen([LOADGEN, "-mode", "hold", "-port", str(s.port), "-c", str(n), "-d", "6",
                                      "-hold-request", "-timeout", "20"], stdout=subprocess.PIPE, text=True)
                line = p.stdout.readline().strip()  # HOLDING n
                time.sleep(2)
                held = s.sampler.sample()
                out = p.communicate(timeout=60)[0]
                res = json.loads(out.split("RESULT ", 1)[1]) if "RESULT " in out else None
                time.sleep(2)
                after = s.sampler.sample()
                up = int(line.split()[1]) if line.startswith("HOLDING") else 0
                rec = {"suite": "idle", "workload": f"idle-{n}", "server": label, "requested": n, "held": up,
                       "alive_at_end": (res or {}).get("ok"), "errors": (res or {}).get("errors"),
                       "mem_idle": mem0, "before": before, "held_sample": held, "after_close": after,
                       "per_conn_private": (held["private"] - before["private"]) / max(up, 1),
                       "per_conn_ws": (held["ws"] - before["ws"]) / max(up, 1),
                       "per_conn_handles": (held["handles"] - before["handles"]) / max(up, 1)}
                with open(os.path.join(OUT, "runs.jsonl"), "a") as f:
                    f.write(json.dumps(rec) + "\n")
                print(f"  idle-{n:<6} {label:<22} held {up:>6} private +{rec['per_conn_private']/1024:7.1f} KiB/conn "
                      f"ws +{rec['per_conn_ws']/1024:7.1f} KiB/conn handles +{rec['per_conn_handles']:.2f}/conn "
                      f"| after close private {(after['private']-before['private'])/2**20:+.1f} MiB", flush=True)
        with_server(label, args, pkg, "http", "http", None, body)


def suite_overload(pkgs, contenders, conns=256):
    """Open-loop load at 50% -> 150% -> 50% of each server's own measured
    capacity: does goodput hold through overload, and how fast does
    latency come back once it ends?"""
    cons = [c for c in http_contenders(pkgs) if c[0] in contenders]
    for label, args, pkg in cons:
        def body(s, mem0, label=label):
            probe = run_load(s, ["-mode", "http", "-path", "/", "-c", "64", "-expect-len", "13"], 3, 1,
                             {"suite": "overload-probe", "workload": "capacity", "rep": 0, "mem_idle": mem0})
            cap = (probe.get("result") or {}).get("rate", 0)
            if cap <= 0:
                return
            lo, hi = int(cap * 0.5), int(cap * 1.5)
            phases = f"{lo}:5s,{hi}:10s,{lo}:15s"
            run_load(s, ["-mode", "http", "-path", "/", "-c", str(conns), "-phases", phases, "-timeout", "30",
                         "-expect-len", "13"], 0, 0,
                     {"suite": "overload", "workload": "overload 50%:5s,150%:10s,50%:15s of capacity",
                      "phases": phases, "capacity": cap, "rep": 0, "mem_idle": mem0})
        with_server(label, args, pkg, "http", "http", None, body)


def main():
    global LOADGEN, OUT
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", default="http")
    ap.add_argument("--pkg", action="append", required=True, help="name=path to a dir holding cadeloop/")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--duration", type=float, default=5)
    ap.add_argument("--warmup", type=float, default=1.5)
    ap.add_argument("--loadgen", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--workloads", default="")
    ap.add_argument("--cert", nargs=2)
    ap.add_argument("--idle-counts", default="1000,5000")
    ap.add_argument("--overload-contenders", default="")
    ap.add_argument("--cadeloop-only", action="store_true", help="skip third-party contenders")
    ap.add_argument("--only", default="", help="comma-separated contender labels to run")
    a = ap.parse_args()
    global CADELOOP_ONLY
    LOADGEN = a.loadgen
    OUT = a.out
    CADELOOP_ONLY = a.cadeloop_only
    ONLY.update(x for x in a.only.split(",") if x)
    os.makedirs(OUT, exist_ok=True)
    pkgs = dict(p.split("=", 1) for p in a.pkg)
    meta = {"started": time.strftime("%Y-%m-%dT%H:%M:%S"), "argv": sys.argv, "python": sys.version,
            "executable": sys.executable, "platform": platform.platform(), "machine": platform.machine(),
            "cpu_count": os.cpu_count(), "pkgs": {}}
    for n, p in pkgs.items():
        ident = os.path.join(os.path.dirname(p.rstrip("\\/")), n, "IDENTITY.txt")
        ident = os.path.join(p, "IDENTITY.txt")
        meta["pkgs"][n] = {"path": p, "identity": open(ident).read() if os.path.exists(ident) else None}
    with open(os.path.join(OUT, "meta.json"), "w") as f:
        json.dump(meta, f, indent=1)
    wl = [w for w in a.workloads.split(",") if w]
    for suite in a.suite.split(","):
        if suite == "http":
            suite_http(pkgs, a.reps, a.duration, a.warmup, wl)
        elif suite == "echo":
            suite_echo(pkgs, a.reps, a.duration, a.warmup)
        elif suite == "ws":
            suite_ws(pkgs, a.reps, a.duration, a.warmup)
        elif suite == "tls":
            suite_tls(pkgs, a.reps, a.duration, a.warmup, a.cert)
        elif suite == "idle":
            suite_idle(pkgs, [int(x) for x in a.idle_counts.split(",")])
        elif suite == "overload":
            suite_overload(pkgs, a.overload_contenders.split(","))


if __name__ == "__main__":
    main()
