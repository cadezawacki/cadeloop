"""Aggregate run_suite.py output into tables, a summary JSON and charts.

  python bench/windows/report.py RUNDIR [RUNDIR ...] --summary OUT.json \
      [--charts docs/assets] [--markdown OUT.md] [--timers TIMERS.json]

Percentiles are taken from the MERGED histogram of every rep's
observations, never averaged across reps. Throughput is the median rep;
its min-max spread is kept so noise is visible. Server CPU per operation
is the median rep's (user + kernel) over the measured window.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "harness"))
from charts import BAR, INK, hbar, svg_open  # noqa: E402

# Entity -> color, fixed across every Windows chart. Chosen by validating
# every chart's adjacent pairs in both modes with the dataviz palette
# checker (CVD and normal-vision separation); yellow and magenta sit under
# 3:1 on the light surface, and one dark pair is in the 6-8 CVD band, so
# every chart ships beside a table with the full data.
COLORS = {
    "light": {
        "cadeloop": "#13968e",
        "cadeloop 1.0.1": "#2a78d6",
        "uvicorn+cadeloop": "#eda100",
        "uvicorn+winloop": "#008300",
        "uvicorn+asyncio": "#6250d6",
        "hypercorn+asyncio": "#eb6834",
        "winloop": "#008300",
        "asyncio": "#6250d6",
        "rsloop": "#e87ba4",
    },
    "dark": {
        "cadeloop": "#1aa39a",
        "cadeloop 1.0.1": "#3987e5",
        "uvicorn+cadeloop": "#c98500",
        "uvicorn+winloop": "#008300",
        "uvicorn+asyncio": "#9085e9",
        "hypercorn+asyncio": "#d95926",
        "winloop": "#008300",
        "asyncio": "#9085e9",
        "rsloop": "#d55181",
    },
}

# Raw run labels -> what the published tables call them.
DISPLAY = {"cadeloop[candidate]": "cadeloop", "cadeloop[baseline]": "cadeloop 1.0.1"}


def disp(server):
    return DISPLAY.get(server, server)


def load(dirs):
    runs = []
    for d in dirs:
        with open(os.path.join(d, "runs.jsonl")) as f:
            for line in f:
                runs.append(json.loads(line))
    return runs


def quantile(hist: dict, q: float) -> float:
    n = sum(hist.values())
    if not n:
        return 0.0
    rank = max(1, math.ceil(q * n))
    seen = 0
    for upper in sorted(hist):
        seen += hist[upper]
        if seen >= rank:
            return upper / 1e3
    return max(hist) / 1e3


def aggregate(runs):
    for r in runs:
        cfg = (r.get("result") or {}).get("config", {})
        r["config_pipeline_gt1"] = cfg.get("pipeline", 1) > 1
    groups = defaultdict(list)
    idle = defaultdict(list)
    for r in runs:
        if r.get("suite") == "idle":
            idle[(r["workload"], r["server"])].append(r)
        elif r.get("result"):
            groups[(r["suite"], r["workload"], r["server"])].append(r)
    out = []
    for (suite, wl, server), rs in groups.items():
        rates = [r["result"]["rate"] for r in rs]
        hist = defaultdict(int)
        for r in rs:
            for upper, c in r["result"]["hist_ns"]:
                hist[upper] += c
        cpu = [r["server_cpu"] for r in rs if r.get("server_cpu")]
        med = sorted(rs, key=lambda r: r["result"]["rate"])[len(rs) // 2]
        errors = defaultdict(int)
        server_closes = 0
        for r in rs:
            server_closes += r["result"].get("server_closes", 0)
            for k, v in r["result"]["errors"].items():
                # Before loadgen counted them apart, a server's own
                # "Connection: close" after a valid response (hypercorn's
                # 1000-requests-per-connection limit) was tallied here.
                if k == "closed" and not r["config_pipeline_gt1"]:
                    server_closes += v
                    continue
                errors[k] += v
        out.append({
            "suite": suite, "workload": wl, "server": server, "reps": len(rs),
            "rate_median": statistics.median(rates), "rate_min": min(rates), "rate_max": max(rates),
            "p50_us": quantile(hist, 0.5), "p99_us": quantile(hist, 0.99), "p999_us": quantile(hist, 0.999),
            "max_us": max(r["result"]["latency_us"]["max"] for r in rs),
            "ok_total": sum(r["result"]["ok"] for r in rs),
            "errors": dict(errors), "error_total": sum(errors.values()), "server_closes": server_closes,
            "incomplete": sum(r["result"].get("incomplete", 0) for r in rs),
            "server_died": any(not r.get("server_alive_after", True) for r in rs),
            "server_cpu_us_per_op": statistics.median(c["us_per_op"] for c in cpu) if cpu else None,
            "server_user_pct": statistics.median(c["user_pct"] for c in cpu) if cpu else None,
            "server_kernel_pct": statistics.median(c["kernel_pct"] for c in cpu) if cpu else None,
            "ctx_switches_per_op": statistics.median(c["ctx_switches_per_op"] for c in cpu) if cpu else None,
            "client_cpu_s": med["result"]["client_cpu_s"],
            "server_peak_private_mib": statistics.median(r["server_mem_peak"]["private"] for r in rs
                                                         if r.get("server_mem_peak")) / 2**20
            if any(r.get("server_mem_peak") for r in rs) else None,
            "server_idle_private_mib": statistics.median(r["mem_idle"]["private"] for r in rs
                                                         if r.get("mem_idle")) / 2**20
            if any(r.get("mem_idle") for r in rs) else None,
            "server_peak_handles": statistics.median(r["server_mem_peak"]["handles"] for r in rs
                                                     if r.get("server_mem_peak")) if any(r.get("server_mem_peak") for r in rs) else None,
            "server_peak_threads": max((r["server_mem_peak"]["threads"] for r in rs if r.get("server_mem_peak")),
                                       default=None),
            "server_end_private_mib": statistics.median(r["server_mem_end"]["private"] for r in rs
                                                        if r.get("server_mem_end")) / 2**20
            if any(r.get("server_mem_end") for r in rs) else None,
            "retrans": sum(r["tcp_delta"]["RetransSegs"] for r in rs),
        })
    idle_out = []
    for (wl, server), rs in idle.items():
        r = rs[0]
        idle_out.append({
            "workload": wl, "server": server, "requested": r["requested"], "held": r["held"],
            "alive_at_end": r["alive_at_end"],
            "per_conn_private_kib": r["per_conn_private"] / 1024, "per_conn_ws_kib": r["per_conn_ws"] / 1024,
            "per_conn_handles": r["per_conn_handles"],
            "idle_private_mib": r["mem_idle"]["private"] / 2**20,
            "held_private_mib": r["held_sample"]["private"] / 2**20,
            "after_close_private_mib": r["after_close"]["private"] / 2**20,
            "threads_held": r["held_sample"]["threads"],
        })
    return out, idle_out


# ---- tables -----------------------------------------------------------------

def k(v):
    return f"{v / 1e3:.1f} K" if v >= 1e3 else f"{v:.0f}"


def us(v):
    return f"{v / 1e3:.2f} ms" if v >= 1000 else f"{v:.0f} µs"


def table(rows, suite, workloads, servers):
    lines = []
    for wl in workloads:
        sel = [r for r in rows if r["suite"] == suite and r["workload"] == wl]
        sel.sort(key=lambda r: servers.index(r["server"]) if r["server"] in servers else 99)
        if not sel:
            continue
        lines.append(f"\n**{wl}**\n")
        lines.append("| server | per s (median, min–max) | p50 | p99 | p99.9 | server CPU / op "
                     "| peak private | handles | threads | errors |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|")
        for r in sel:
            if r["server"] not in servers:
                continue
            cpu = r["server_cpu_us_per_op"]
            peak = r["server_peak_private_mib"]
            lines.append(
                f"| {disp(r['server'])} | {k(r['rate_median'])} ({k(r['rate_min'])}–{k(r['rate_max'])}) "
                f"| {us(r['p50_us'])} | {us(r['p99_us'])} | {us(r['p999_us'])} "
                f"| {f'{cpu:.1f} µs' if cpu is not None else '–'} "
                f"| {f'{peak:.0f} MiB' if peak else '–'} "
                f"| {r['server_peak_handles'] or '–'} | {r['server_peak_threads'] or '–'} | {r['error_total']} |")
    return "\n".join(lines)


# ---- charts -------------------------------------------------------------------

def grouped_hbar(path_base, title, subtitle, groups, servers, value_of, unit_fmt, story="cadeloop"):
    """Rows = groups (workloads), one thin bar per server; one shared axis."""
    for mode in ("light", "dark"):
        ink = INK[mode]
        colors = COLORS[mode]
        label_w, right = 150, 70
        width = 760
        bar, gap, group_air = BAR, 2, 16
        # Legend entries, wrapped onto further rows when one would overflow.
        legend, lx, ly = [], 24.0, 66
        for srv in servers:
            w_item = 17 + 6.6 * len(disp(srv)) + 18
            if lx + w_item > width - 16 and lx > 24:
                lx, ly = 24.0, ly + 18
            legend.append((srv, lx, ly))
            lx += w_item
        top = 92 + (ly - 66)
        plot_w = width - label_w - right
        vals = [v for v in (value_of(g, s) for g in groups for s in servers) if v is not None]
        if not vals:
            return
        vmax = max(vals) or 1
        h = top + len(groups) * (len(servers) * (bar + gap) + group_air) + 24
        p = svg_open(width, h, ink)
        p.append(f'<text x="24" y="34" font-size="16" font-weight="600" fill="{ink["primary"]}">{title}</text>')
        p.append(f'<text x="24" y="54" font-size="12" fill="{ink["secondary"]}">{subtitle}</text>')
        for s, x, y in legend:
            p.append(f'<rect x="{x:.1f}" y="{y}" width="12" height="12" rx="3" fill="{colors[disp(s)]}"/>')
            p.append(f'<text x="{x + 17:.1f}" y="{y + 10}" font-size="12" fill="{ink["secondary"]}">{disp(s)}</text>')
        y = top
        for g in groups:
            gh = len(servers) * (bar + gap)
            p.append(f'<text x="{label_w - 12}" y="{y + gh / 2 + 4}" font-size="12" text-anchor="end" '
                     f'fill="{ink["primary"]}">{g[1]}</text>')
            p.append(f'<line x1="{label_w}" y1="{y - 3}" x2="{label_w}" y2="{y + gh - 1}" '
                     f'stroke="{ink["axis"]}" stroke-width="1"/>')
            for s in servers:
                v = value_of(g, s)
                if v is not None:
                    w = v / vmax * plot_w
                    p.append(hbar(label_w, y, w, colors[disp(s)], ink["surface"]))
                    # Every bar when there are few enough to label without
                    # clutter; otherwise the story series (tables carry all).
                    if disp(s) == story or len(servers) <= 4:
                        p.append(f'<text x="{label_w + w + 6:.1f}" y="{y + bar - 3}" font-size="11" '
                                 f'fill="{ink["secondary"]}">{unit_fmt(v)}</text>')
                y += bar + gap
            y += group_air
        p.append("</svg>")
        suffix = "" if mode == "light" else "-dark"
        with open(f"{path_base}{suffix}.svg", "w", encoding="utf-8") as f:
            f.write("\n".join(p))


def overload_chart(path_base, runs, servers, phases_label):
    """p99 latency per second through an open-loop overload, one line per
    server (single y-axis, log scale: the overload phase spans decades)."""
    found = {}
    for r in runs:
        if r.get("suite") == "overload" and r.get("result") and r["server"] in servers:
            found[r["server"]] = [(p["t"], p["p99_us"] / 1e3) for p in r["result"]["series"] if p["ok"]]
    # Entity order, as the palette was validated in.
    series = {s: found[s] for s in servers if s in found}
    if not series:
        return
    for mode in ("light", "dark"):
        ink, colors = INK[mode], COLORS[mode]
        width, height = 760, 360
        left, right, top, bottom = 64, 24, 92, 44
        pw, ph = width - left - right, height - top - bottom
        tmax = max(t for s in series.values() for t, _ in s) or 1
        vals = [v for s in series.values() for _, v in s if v > 0]
        lo = 10 ** math.floor(math.log10(min(vals)))
        hi = 10 ** math.ceil(math.log10(max(vals)))
        ylog = lambda v: top + ph - (math.log10(max(v, lo)) - math.log10(lo)) / (math.log10(hi) - math.log10(lo)) * ph  # noqa: E731
        xs = lambda t: left + t / tmax * pw  # noqa: E731
        p = svg_open(width, height, ink)
        # The overload window (5-15 s), shaded behind the data.
        p.append(f'<rect x="{xs(5):.1f}" y="{top}" width="{xs(15) - xs(5):.1f}" height="{ph}" '
                 f'fill="{ink["grid"]}" opacity="0.6"/>')
        p.append(f'<text x="{(xs(5) + xs(15)) / 2:.1f}" y="{top + 14}" font-size="11" text-anchor="middle" '
                 f'fill="{ink["muted"]}">150% of capacity offered</text>')
        p.append(f'<text x="24" y="34" font-size="16" font-weight="600" fill="{ink["primary"]}">'
                 f'p99 latency through overload and recovery (Windows 11)</text>')
        p.append(f'<text x="24" y="54" font-size="12" fill="{ink["secondary"]}">{phases_label}</text>')
        x = 24
        for s in series:
            p.append(f'<rect x="{x}" y="66" width="12" height="12" rx="3" fill="{colors[disp(s)]}"/>')
            p.append(f'<text x="{x + 17}" y="76" font-size="12" fill="{ink["secondary"]}">{disp(s)}</text>')
            x += 17 + 6.6 * len(disp(s)) + 18
        d = lo
        while d <= hi:
            y = ylog(d)
            p.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + pw}" y2="{y:.1f}" stroke="{ink["grid"]}" stroke-width="1"/>')
            label = f"{d:g} ms" if d < 1000 else f"{d / 1000:g} s"
            p.append(f'<text x="{left - 8}" y="{y + 4:.1f}" font-size="11" text-anchor="end" fill="{ink["muted"]}">{label}</text>')
            d *= 10
        for t in range(0, int(tmax) + 1, 5):
            p.append(f'<text x="{xs(t):.1f}" y="{top + ph + 18}" font-size="11" text-anchor="middle" '
                     f'fill="{ink["muted"]}">{t}s</text>')
        for s, pts in series.items():
            path = " ".join(f"{'M' if i == 0 else 'L'}{xs(t):.1f},{ylog(v):.1f}" for i, (t, v) in enumerate(pts))
            p.append(f'<path d="{path}" fill="none" stroke="{colors[disp(s)]}" stroke-width="2" '
                     f'stroke-linejoin="round" stroke-linecap="round"/>')
        p.append("</svg>")
        suffix = "" if mode == "light" else "-dark"
        with open(f"{path_base}{suffix}.svg", "w", encoding="utf-8") as f:
            f.write("\n".join(p))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dirs", nargs="+")
    ap.add_argument("--summary", required=True)
    ap.add_argument("--markdown")
    ap.add_argument("--charts")
    ap.add_argument("--timers")
    ap.add_argument("--sched", help="bench/harness sched JSON (final build + contenders)")
    a = ap.parse_args()
    runs = load(a.dirs)
    rows, idle = aggregate(runs)
    meta = [json.load(open(os.path.join(d, "meta.json"))) for d in a.dirs]
    timers = json.load(open(a.timers)) if a.timers else None
    sched = json.load(open(a.sched))["results"] if a.sched else None
    with open(a.summary, "w") as f:
        json.dump({"meta": meta, "rows": rows, "idle": idle, "timers": timers}, f, indent=1)

    http_servers = ["cadeloop[candidate]", "cadeloop[baseline]", "uvicorn+cadeloop", "uvicorn+winloop",
                    "uvicorn+asyncio", "hypercorn+asyncio"]
    md = ["## HTTP"]
    md.append(table(rows, "http", [w for w, _ in __import__("run_suite").HTTP_WORKLOADS], http_servers))
    md.append("\n## WebSocket")
    md.append(table(rows, "ws", ["ws-64b-c1", "ws-64b-c64", "ws-16k-c16", "ws-stream-1k-w32-c16"],
                    ["cadeloop[candidate]", "cadeloop[baseline]", "uvicorn+winloop", "uvicorn+asyncio",
                     "hypercorn+asyncio"]))
    md.append("\n## TLS")
    md.append(table(rows, "tls", ["tls-plaintext-c64", "tls-churn-c16"],
                    ["cadeloop[candidate]", "cadeloop[baseline]", "uvicorn+winloop", "uvicorn+asyncio"]))
    md.append("\n## TCP echo (raw asyncio.Protocol)")
    md.append(table(rows, "echo", ["echo-1k-c1", "echo-1k-c64", "echo-1k-w16-c64", "echo-64k-c8"],
                    ["cadeloop[candidate]", "cadeloop[baseline]", "winloop", "rsloop", "asyncio"]))
    md.append("\n## Idle keep-alive connections\n")
    md.append("| server | connections | private / conn | working set / conn | handles / conn | private after close |")
    md.append("|---|---|---|---|---|---|")
    for r in sorted(idle, key=lambda r: (r["requested"], http_servers.index(r["server"])
                                           if r["server"] in http_servers else 99)):
        md.append(f"| {disp(r['server'])} | {r['held']} | {r['per_conn_private_kib']:.1f} KiB "
                  f"| {r['per_conn_ws_kib']:.1f} KiB | {r['per_conn_handles']:.2f} "
                  f"| {r['after_close_private_mib']:.0f} MiB |")
    if a.markdown:
        with open(a.markdown, "w", encoding="utf-8") as f:
            f.write("\n".join(md) + "\n")
    print("\n".join(md))

    if a.charts:
        os.makedirs(a.charts, exist_ok=True)
        idx = {(r["suite"], r["workload"], r["server"]): r for r in rows}
        cons = ["cadeloop[candidate]", "uvicorn+cadeloop", "uvicorn+winloop", "uvicorn+asyncio", "hypercorn+asyncio"]
        hw = [("plaintext-c64", "plaintext"), ("json-c64", "JSON"), ("large64k-c64", "64 KiB body"),
              ("upload16k-c64", "16 KiB upload"), ("sleep1ms-c64", "1 ms await"), ("churn-c16", "conn per request")]

        def rate(g, s, suite="http"):
            r = idx.get((suite, g[0], s))
            return r["rate_median"] if r else None

        def cpu(g, s, suite="http"):
            r = idx.get((suite, g[0], s))
            return r["server_cpu_us_per_op"] if r else None

        grouped_hbar(os.path.join(a.charts, "bench-win-http"), "HTTP/1.1 requests per second (Windows 11)",
                     "64 keep-alive connections (16 for conn-per-request); median of 3 runs; higher is better",
                     hw, cons, rate, lambda v: f"{v / 1e3:.1f}K")
        grouped_hbar(os.path.join(a.charts, "bench-win-http-cpu"), "Server CPU per request (Windows 11)",
                     "user + kernel time of the server process tree / successful requests; lower is better",
                     hw, cons, cpu, lambda v: f"{v:.1f} µs")
        ww = [("ws-64b-c1", "64 B, 1 conn"), ("ws-64b-c64", "64 B, 64 conns"), ("ws-16k-c16", "16 KiB, 16 conns"),
              ("ws-stream-1k-w32-c16", "1 KiB stream")]
        wcons = ["cadeloop[candidate]", "uvicorn+winloop", "uvicorn+asyncio", "hypercorn+asyncio"]
        grouped_hbar(os.path.join(a.charts, "bench-win-ws"), "WebSocket echo messages per second (Windows 11)",
                     "ping-pong except 1 KiB stream (32 in flight per connection); higher is better",
                     ww, wcons, lambda g, s: rate(g, s, "ws"), lambda v: f"{v / 1e3:.0f}K")
        ew = [("echo-1k-c1", "1 KiB, 1 conn"), ("echo-1k-c64", "1 KiB, 64 conns"),
              ("echo-1k-w16-c64", "1 KiB, 16 in flight"), ("echo-64k-c8", "64 KiB, 8 conns")]
        econs = ["cadeloop[candidate]", "winloop", "rsloop", "asyncio"]
        grouped_hbar(os.path.join(a.charts, "bench-win-echo"), "TCP echo messages per second (Windows 11)",
                     "same asyncio.Protocol on every loop; higher is better",
                     ew, econs, lambda g, s: rate(g, s, "echo"), lambda v: f"{v / 1e3:.0f}K")
        icons = ["cadeloop[candidate]", "cadeloop[baseline]", "uvicorn+cadeloop", "uvicorn+winloop",
                 "uvicorn+asyncio", "hypercorn+asyncio"]
        iw = sorted({(r["workload"], f"{r['requested']} idle") for r in idle}, key=lambda g: int(g[1].split()[0]))
        iidx = {(r["workload"], r["server"]): r for r in idle}
        grouped_hbar(os.path.join(a.charts, "bench-win-idle-mem"), "Memory per idle keep-alive connection (Windows 11)",
                     "private bytes added per held connection, KiB; lower is better",
                     iw, icons, lambda g, s: iidx[(g[0], s)]["per_conn_private_kib"] if (g[0], s) in iidx else None,
                     lambda v: f"{v:.0f} KiB")
        # Memory: the server tree's private bytes at rest (after startup and
        # one request) and at its peak under each load.
        mw = [("plaintext-c64", "at rest"), ("plaintext-c64", "plaintext, 64 conns"),
              ("large64k-c64", "64 KiB bodies"), ("stream1m-c16", "1 MiB streams"),
              ("upload16k-c64", "16 KiB uploads"), ("churn-c16", "conn per request")]

        def mem(g, s):
            r = idx.get(("http", g[0], s))
            if not r:
                return None
            return r["server_idle_private_mib"] if g[1] == "at rest" else r["server_peak_private_mib"]

        grouped_hbar(os.path.join(a.charts, "bench-win-http-mem"), "Server memory, at rest and under load (Windows 11)",
                     "private bytes of the server process tree, MiB, peak over the run; lower is better",
                     mw, cons, mem, lambda v: f"{v:.0f} MiB")
        wsm = [("ws-64b-c64", "64 B, 64 conns"), ("ws-16k-c16", "16 KiB, 16 conns"),
               ("ws-stream-1k-w32-c16", "1 KiB stream")]
        grouped_hbar(os.path.join(a.charts, "bench-win-ws-mem"), "WebSocket server memory under load (Windows 11)",
                     "peak private bytes of the server process tree, MiB; lower is better",
                     wsm, wcons, lambda g, s: (idx.get(("ws", g[0], s)) or {}).get("server_peak_private_mib"),
                     lambda v: f"{v:.0f} MiB")
        rec = sorted({(r["workload"], f"{r['held']} held") for r in idle}, key=lambda g: int(g[1].split()[0]))
        grouped_hbar(os.path.join(a.charts, "bench-win-idle-total"), "Server memory holding idle connections (Windows 11)",
                     "private bytes of the server process tree while the connections are held, MiB; lower is better",
                     rec, icons, lambda g, s: iidx[(g[0], s)]["held_private_mib"] if (g[0], s) in iidx else None,
                     lambda v: f"{v:.0f} MiB")
        ov = [r for r in runs if r.get("suite") == "overload"]
        if ov:
            overload_chart(os.path.join(a.charts, "bench-win-overload"), runs,
                           ["cadeloop[candidate]", "uvicorn+cadeloop", "uvicorn+winloop", "uvicorn+asyncio"],
                           "open loop, 256 connections: 50% of each server's own capacity for 5 s, "
                           "150% for 10 s, 50% for 15 s")
        if sched:
            # Per benchmark, each loop as a share of the fastest: raw rates
            # span 0.07 to 12.6 M ops/s, too wide for one linear axis.
            groups = [(b, b) for b in sched]
            sl = ["cadeloop", "winloop", "rsloop", "asyncio"]

            def share(g, s):
                res = sched[g[0]]
                v = (res.get(s) or {}).get("median_ops_per_sec")
                top = max((x or {}).get("median_ops_per_sec") or 0 for x in res.values())
                return v / top * 100 if v and top else None

            grouped_hbar(os.path.join(a.charts, "bench-win-sched"), "Scheduling microbenchmarks (Windows 11)",
                         "throughput as a share of the fastest loop per benchmark (missing = hung); higher is better",
                         groups, sl, share, lambda v: f"{v:.0f}%")
        if timers:
            tl = [(f"{d}", f"sleep({float(d) * 1e3:g} ms)") for d in ("0.001", "0.005", "0.01")]
            tcons = [c for c in ("cadeloop[candidate]", "cadeloop[baseline]", "winloop", "asyncio") if c in timers]
            grouped_hbar(os.path.join(a.charts, "bench-win-timers"), "Timer overshoot past the deadline (Windows 11)",
                         "median of 200 asyncio.sleep() calls (40 at 10 ms) on an idle loop; lower is better",
                         tl, tcons, lambda g, s: timers[s][g[0]]["p50"], lambda v: f"{v:.2f} ms")


if __name__ == "__main__":
    main()
