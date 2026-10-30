"""Aggregate run_suite.py output into tables, a summary JSON and charts.

  python bench/suite/report.py RUNDIR [RUNDIR ...] --summary OUT.json \
      [--charts docs/assets] [--markdown OUT.md] [--tables README-TABLES.json] \
      [--timers TIMERS.json] [--sched SCHED.json] [--platform windows|linux]

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

# Entity -> color, fixed across every chart. Chosen by validating
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
        # Linux: uvloop takes winloop's slot, the retired 1.0.1 blue goes
        # to uvicorn[standard], and rloop shares amber with uvicorn+cadeloop
        # (they never appear in the same chart).
        "uvicorn+uvloop": "#008300",
        "uvloop": "#008300",
        "uvicorn[standard]": "#2a78d6",
        "granian": "#b8338a",
        "rloop": "#eda100",
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
        "uvicorn+uvloop": "#008300",
        "uvloop": "#008300",
        "uvicorn[standard]": "#3987e5",
        "granian": "#d661ad",
        "rloop": "#c98500",
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
            # Did the server really hold them? A connection the kernel
            # completed but the server never accepted costs it nothing, so
            # per-connection memory is only meaningful when (nearly) all
            # were held and each took a descriptor.
            "valid": r["held"] >= 0.98 * r["requested"] and r["per_conn_handles"] >= 0.9,
            "errors": r.get("errors") or {},
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


def overload_chart(path_base, runs, servers, phases_label, plat_name="Windows 11"):
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
                 f'p99 latency through overload and recovery ({plat_name})</text>')
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


# Which servers each published chart and table shows, per platform.
PLATFORMS = {
    "windows": {
        "tag": "win", "name": "Windows 11",
        "http": ["cadeloop[candidate]", "uvicorn+cadeloop", "uvicorn+winloop", "uvicorn+asyncio",
                 "hypercorn+asyncio"],
        "ws": ["cadeloop[candidate]", "uvicorn+winloop", "uvicorn+asyncio", "hypercorn+asyncio"],
        "tls": ["cadeloop[candidate]", "uvicorn+winloop", "uvicorn+asyncio"],
        "echo": ["cadeloop[candidate]", "winloop", "rsloop", "asyncio"],
        "overload": ["cadeloop[candidate]", "uvicorn+cadeloop", "uvicorn+winloop", "uvicorn+asyncio"],
        "loops": ["cadeloop[candidate]", "winloop", "asyncio"],
        "sched": ["cadeloop", "winloop", "rsloop", "asyncio"],
    },
    "linux": {
        "tag": "linux", "name": "Linux",
        "http": ["cadeloop[candidate]", "granian", "uvicorn[standard]", "uvicorn+cadeloop", "uvicorn+uvloop",
                 "uvicorn+asyncio", "hypercorn+asyncio"],
        "ws": ["cadeloop[candidate]", "granian", "uvicorn[standard]", "uvicorn+uvloop", "uvicorn+asyncio",
               "hypercorn+asyncio"],
        "tls": ["cadeloop[candidate]", "granian", "uvicorn[standard]", "uvicorn+uvloop", "uvicorn+asyncio"],
        "echo": ["cadeloop[candidate]", "uvloop", "rloop", "rsloop", "asyncio"],
        "overload": ["cadeloop[candidate]", "granian", "uvicorn[standard]", "uvicorn+cadeloop", "uvicorn+uvloop",
                     "uvicorn+asyncio"],
        "loops": ["cadeloop[candidate]", "uvloop", "rloop", "rsloop", "asyncio"],
        "sched": ["cadeloop", "uvloop", "rloop", "rsloop", "asyncio"],
    },
}

HTTP_LABELS = [("plaintext-c64", "plaintext"), ("plaintext-c1", "plaintext, 1 connection"), ("json-c64", "JSON"),
               ("large64k-c64", "64 KiB body"), ("stream1m-c16", "1 MiB stream (16 conns)"),
               ("upload16k-c64", "16 KiB upload"), ("sleep1ms-c64", "1 ms await"),
               ("churn-c16", "conn per request (16)"), ("pipelined16-c64", "pipelined ×16")]
WS_LABELS = [("ws-64b-c1", "64 B, 1 connection"), ("ws-64b-c64", "64 B, 64 connections"),
             ("ws-16k-c16", "16 KiB, 16 connections"), ("ws-stream-1k-w32-c16", "1 KiB stream, 32 in flight ×16")]
TLS_LABELS = [("tls-plaintext-c64", "plaintext, 64 keep-alive conns"),
              ("tls-churn-c16", "new connection per request (16)")]
ECHO_LABELS = [("echo-1k-c1", "1 KiB, 1 connection"), ("echo-1k-c64", "1 KiB, 64 connections"),
               ("echo-1k-w16-c64", "1 KiB, 16 in flight ×64"), ("echo-64k-c8", "64 KiB, 8 connections")]


def tname(server, as_server=True):
    """A server's name in the README tables."""
    if server == "cadeloop[candidate]":
        return "cadeloop serve()" if as_server else "cadeloop"
    return {"cadeloop[baseline]": "cadeloop (baseline)", "hypercorn+asyncio": "hypercorn"}.get(
        server, server.replace("+", " + "))


def shown(text):
    """The number a reader sees in a displayed cell ("117 K", "1.28 ms",
    "8.4 µs", "21 MiB"), in one unit per kind, so ranking uses exactly the
    precision shown: two cells that look equal are equal."""
    import re

    m = re.match(r"\+?([0-9.]+)\s*(K|ms|µs|MiB|KiB)?", text.strip())
    if not m:
        return None
    v = float(m.group(1))
    return v * {"K": 1e3, "ms": 1e3, "µs": 1.0, "MiB": 1.0, "KiB": 1.0, None: 1.0}[m.group(2)]


def kfmt(v):
    if v >= 100e3:
        return f"{v / 1e3:.0f} K"
    if v >= 10e3:
        return f"{v / 1e3:.1f} K"
    return f"{v / 1e3:.2f} K" if v >= 1e3 else f"{v:.0f}"


def readme_specs(rows, idle, timers, sched, P):
    """README table specs (bench/harness/tables.py), best cells marked."""
    from tables import mark_best

    idx = {(r["suite"], r["workload"], r["server"]): r for r in rows}
    plat, tag = P["name"], P["tag"]
    specs = {}

    def ok(r):
        return r is not None and not r["error_total"] and not r["server_died"]

    def by_workload(suite, labels, cons, with_p99, as_server):
        body, vals = [], []
        for wl, label in labels:
            cells, vs = [], []
            for srv in cons:
                r = idx.get((suite, wl, srv))
                if r is None:
                    cells.append({"text": "–"})
                    vs.append(None)
                    continue
                cell = {"text": kfmt(r["rate_median"]) + ("" if ok(r) else " *")}
                if with_p99:
                    cell["sub"] = us(r["p99_us"])
                cells.append(cell)
                vs.append(shown(cell["text"]) if ok(r) else None)
            body.append({"label": label, "cells": cells})
            vals.append(vs)
        mark_best(body, True, vals, axis="row")
        return {"columns": ["workload"] + [tname(c, as_server) for c in cons], "rows": body}

    specs[f"bench-{tag}-table-http"] = {
        "title": f"HTTP requests per second · {plat}",
        "subtitle": "64 keep-alive connections unless noted · one worker per server · median of 3 runs",
        **by_workload("http", HTTP_LABELS, P["http"], False, True)}
    specs[f"bench-{tag}-table-ws"] = {
        "title": f"WebSocket echo · {plat}", "subtitle": "messages per second, p99 round trip in small type",
        **by_workload("ws", WS_LABELS, P["ws"], True, False)}
    specs[f"bench-{tag}-table-tls"] = {
        "title": f"TLS · {plat}",
        "subtitle": "requests per second, p99 in small type · every server uses Python's ssl module, "
                    "except granian (Rust)" if "granian" in P["tls"] else
                    "requests per second, p99 in small type · Python's ssl module in every server",
        **by_workload("tls", TLS_LABELS, P["tls"], True, False)}
    specs[f"bench-{tag}-table-echo"] = {
        "title": f"TCP echo, the loop alone · {plat}",
        "subtitle": "the same asyncio.Protocol on every loop · messages per second, p99 in small type",
        **by_workload("echo", ECHO_LABELS, P["echo"], True, False)}

    # Plaintext in detail: one row per server, best per column.
    body, vals = [], []
    for srv in P["http"]:
        r = idx.get(("http", "plaintext-c64", srv))
        if not r:
            continue
        cpu, peak = r["server_cpu_us_per_op"], r["server_peak_private_mib"]
        body.append({"label": tname(srv), "cells": [
            {"text": kfmt(r["rate_median"])}, {"text": us(r["p50_us"])}, {"text": us(r["p99_us"])},
            {"text": us(r["p999_us"])}, {"text": f"{cpu:.1f} µs" if cpu is not None else "–"},
            {"text": f"{peak:.0f} MiB" if peak else "–"}, {"text": str(r["server_peak_threads"] or "–")}]})
        vals.append([shown(c["text"]) for c in body[-1]["cells"]])
    mark_best(body, [True, False, False, False, False, False, False], vals, axis="col")
    specs[f"bench-{tag}-table-plaintext"] = {
        "title": f"Plaintext HTTP in detail · {plat}",
        "subtitle": "64 connections · CPU is user + kernel time of the server's processes per request",
        "columns": ["server", "req/s", "p50", "p99", "p99.9", "CPU / request", "peak memory", "threads"],
        "rows": body}

    # Idle connections, at the largest count measured.
    if idle:
        top = max(r["requested"] for r in idle)
        iidx = {r["server"]: r for r in idle if r["requested"] == top}
        body, vals, notes = [], [], []
        for srv in P["http"]:
            r = iidx.get(srv)
            if not r:
                continue
            if not r.get("valid", True):
                fds = round(r["per_conn_handles"] * r["held"])
                timeouts = sum(r.get("errors", {}).values())
                notes.append(f"{tname(srv)} accepted about {fds:,} of the {top:,} connections with its default "
                             f"settings" + (f" ({timeouts} timed out)" if timeouts else "")
                             + ", so it has no comparable figure here.")
                body.append({"label": tname(srv), "cells": [{"text": "–"}] * 5})
                vals.append([None] * 5)
                continue
            body.append({"label": tname(srv), "cells": [
                {"text": f"{r['idle_private_mib']:.0f} MiB"}, {"text": f"{r['held_private_mib']:.0f} MiB"},
                {"text": f"{r['per_conn_private_kib']:.1f} KiB"}, {"text": f"{r['after_close_private_mib']:.0f} MiB"},
                {"text": f"{r['per_conn_handles']:.2f}".rstrip("0").rstrip(".")}]})
            # Descriptors per connection is a sanity check (about 1 means
            # each connection was held), not a score: never ranked.
            vals.append([shown(c["text"]) for c in body[-1]["cells"][:4]] + [None])
        mark_best(body, False, vals, axis="col")
        specs[f"bench-{tag}-table-idle"] = {
            "title": f"Idle keep-alive connections · {plat}",
            "subtitle": f"each of {top:,} connections made one request and was held open · "
                        + ("private bytes" if tag == "win" else "unique memory (USS)") + " of the server",
            "columns": ["server", "at rest", f"holding {top:,}", "per connection", "after they close",
                        "handles / conn" if tag == "win" else "fds / conn"],
            "rows": body, **({"notes": notes} if notes else {})}

    if timers:
        body, vals = [], []
        for srv in P["loops"]:
            t = timers.get(srv)
            if not t:
                continue
            cells, vs = [], []
            for d in ("0.001", "0.005", "0.01"):
                cells.append({"text": f"+{t[d]['p50']:.2f} / {t[d]['p99']:.2f} ms"})
                vs.append(shown(cells[-1]["text"]))
            x = t["xthread"]
            cells.append({"text": f"{x['p50'] * 1e3:.0f} / {x['p99'] * 1e3:.0f} µs"})
            vs.append(shown(cells[-1]["text"]))
            body.append({"label": tname(srv, False), "cells": cells})
            vals.append(vs)
        mark_best(body, False, vals, axis="col")
        specs[f"bench-{tag}-table-timers"] = {
            "title": f"Timers and wake-ups · {plat}",
            "subtitle": "how late asyncio.sleep(d) returns on an idle loop (median / p99) · "
                        "call_soon_threadsafe() into a sleeping loop",
            "columns": ["loop", "sleep(1 ms)", "sleep(5 ms)", "sleep(10 ms)", "cross-thread wake-up"],
            "rows": body, "notes": ["Highlighted: the lowest median in each column."]}

    if sched:
        body, vals = [], []
        for bench, res in sched.items():
            cells, vs = [], []
            for lp in P["sched"]:
                v = (res.get(lp) or {}).get("median_ops_per_sec")
                cells.append({"text": f"{v / 1e6:.3f}" if v else "hung"})
                vs.append(shown(cells[-1]["text"]))
            body.append({"label": bench, "label_code": True, "cells": cells})
            vals.append(vs)
        mark_best(body, True, vals, axis="row")
        nruns = max(len((x or {}).get("runs", [])) for res in sched.values() for x in res.values())
        specs[f"bench-{tag}-table-sched"] = {
            "title": f"Scheduling microbenchmarks · {plat}",
            "subtitle": f"millions of operations per second · median of {nruns} runs, a fresh process each",
            "columns": ["benchmark"] + P["sched"], "rows": body}
    return specs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dirs", nargs="+")
    ap.add_argument("--summary", required=True)
    ap.add_argument("--markdown")
    ap.add_argument("--charts")
    ap.add_argument("--tables", help="write the README table specs (JSON) here")
    ap.add_argument("--timers")
    ap.add_argument("--sched", help="bench/harness sched JSON (final build + contenders)")
    ap.add_argument("--platform", choices=sorted(PLATFORMS), help="default: from the runs' meta.json")
    ap.add_argument("--with-baseline", action="store_true",
                    help="show cadeloop[baseline] next to the candidate (an A/B of two builds)")
    ap.add_argument("--only-charts", default="",
                    help="comma-separated chart names to write (default: all)")
    a = ap.parse_args()
    runs = load(a.dirs)
    rows, idle = aggregate(runs)
    meta = [json.load(open(os.path.join(d, "meta.json"))) for d in a.dirs]
    timers = json.load(open(a.timers)) if a.timers else None
    sched = json.load(open(a.sched))["results"] if a.sched else None
    with open(a.summary, "w") as f:
        json.dump({"meta": meta, "rows": rows, "idle": idle, "timers": timers}, f, indent=1)

    plat = a.platform or ("windows" if meta[0].get("platform", "").startswith("Windows") else "linux")
    P = {k: list(v) if isinstance(v, list) else v for k, v in PLATFORMS[plat].items()}
    if a.with_baseline:
        for key in ("http", "ws", "tls", "echo", "loops"):
            P[key].insert(1, "cadeloop[baseline]")
    tag, plat_name = P["tag"], P["name"]
    only = {x for x in a.only_charts.split(",") if x}

    md = ["## HTTP"]
    md.append(table(rows, "http", [w for w, _ in __import__("run_suite").HTTP_WORKLOADS], P["http"]))
    md.append("\n## WebSocket")
    md.append(table(rows, "ws", [w for w, _ in WS_LABELS], P["ws"]))
    md.append("\n## TLS")
    md.append(table(rows, "tls", [w for w, _ in TLS_LABELS], P["tls"]))
    md.append("\n## TCP echo (raw asyncio.Protocol)")
    md.append(table(rows, "echo", [w for w, _ in ECHO_LABELS], P["echo"]))
    md.append("\n## Idle keep-alive connections\n")
    md.append("| server | connections | private / conn | working set / conn | handles / conn | private after close |")
    md.append("|---|---|---|---|---|---|")
    for r in sorted(idle, key=lambda r: (r["requested"], P["http"].index(r["server"])
                                           if r["server"] in P["http"] else 99)):
        md.append(f"| {disp(r['server'])} | {r['held']} | {r['per_conn_private_kib']:.1f} KiB "
                  f"| {r['per_conn_ws_kib']:.1f} KiB | {r['per_conn_handles']:.2f} "
                  f"| {r['after_close_private_mib']:.0f} MiB |")
    if a.markdown:
        with open(a.markdown, "w", encoding="utf-8") as f:
            f.write("\n".join(md) + "\n")
    print("\n".join(md))

    if a.tables:
        with open(a.tables, "w", encoding="utf-8") as f:
            json.dump(readme_specs(rows, idle, timers, sched, P), f, indent=1, ensure_ascii=False)

    if a.charts:
        os.makedirs(a.charts, exist_ok=True)
        idx = {(r["suite"], r["workload"], r["server"]): r for r in rows}

        def chart(name, *args, **kw):
            if not only or name in only:
                grouped_hbar(os.path.join(a.charts, f"bench-{tag}-{name}"), *args, **kw)

        hw = [("plaintext-c64", "plaintext"), ("json-c64", "JSON"), ("large64k-c64", "64 KiB body"),
              ("upload16k-c64", "16 KiB upload"), ("sleep1ms-c64", "1 ms await"), ("churn-c16", "conn per request")]

        def rate(g, s, suite="http"):
            r = idx.get((suite, g[0], s))
            return r["rate_median"] if r else None

        def cpu(g, s, suite="http"):
            r = idx.get((suite, g[0], s))
            return r["server_cpu_us_per_op"] if r else None

        chart("http", f"HTTP/1.1 requests per second ({plat_name})",
              "64 keep-alive connections (16 for conn-per-request); median of 3 runs; higher is better",
              hw, P["http"], rate, lambda v: f"{v / 1e3:.1f}K")
        chart("http-cpu", f"Server CPU per request ({plat_name})",
              "user + kernel time of the server's processes / successful requests; lower is better",
              hw, P["http"], cpu, lambda v: f"{v:.1f} µs")
        ww = [("ws-64b-c1", "64 B, 1 conn"), ("ws-64b-c64", "64 B, 64 conns"), ("ws-16k-c16", "16 KiB, 16 conns"),
              ("ws-stream-1k-w32-c16", "1 KiB stream")]
        chart("ws", f"WebSocket echo messages per second ({plat_name})",
              "ping-pong except 1 KiB stream (32 in flight per connection); higher is better",
              ww, P["ws"], lambda g, s: rate(g, s, "ws"), lambda v: f"{v / 1e3:.0f}K")
        ew = [("echo-1k-c1", "1 KiB, 1 conn"), ("echo-1k-c64", "1 KiB, 64 conns"),
              ("echo-1k-w16-c64", "1 KiB, 16 in flight"), ("echo-64k-c8", "64 KiB, 8 conns")]
        chart("echo", f"TCP echo messages per second ({plat_name})",
              "same asyncio.Protocol on every loop; higher is better",
              ew, P["echo"], lambda g, s: rate(g, s, "echo"), lambda v: f"{v / 1e3:.0f}K")
        iw = sorted({(r["workload"], f"{r['requested']} idle") for r in idle}, key=lambda g: int(g[1].split()[0]))
        iidx = {(r["workload"], r["server"]): r for r in idle}
        mem_word = "private bytes" if tag == "win" else "unique memory (USS)"
        chart("idle-mem", f"Memory per idle keep-alive connection ({plat_name})",
              f"{mem_word} added per held connection, KiB; lower is better",
              iw, P["http"], lambda g, s: iidx[(g[0], s)]["per_conn_private_kib"]
              if (g[0], s) in iidx and iidx[(g[0], s)].get("valid", True) else None,
              lambda v: f"{v:.0f} KiB")
        # Memory: the server's private memory at rest (after startup and
        # one request) and at its peak under each load.
        mw = [("plaintext-c64", "at rest"), ("plaintext-c64", "plaintext, 64 conns"),
              ("large64k-c64", "64 KiB bodies"), ("stream1m-c16", "1 MiB streams"),
              ("upload16k-c64", "16 KiB uploads"), ("churn-c16", "conn per request")]

        def mem(g, s):
            r = idx.get(("http", g[0], s))
            if not r:
                return None
            return r["server_idle_private_mib"] if g[1] == "at rest" else r["server_peak_private_mib"]

        chart("http-mem", f"Server memory, at rest and under load ({plat_name})",
              f"{mem_word} of the server's processes, MiB, peak over the run; lower is better",
              mw, P["http"], mem, lambda v: f"{v:.0f} MiB")
        wsm = [("ws-64b-c64", "64 B, 64 conns"), ("ws-16k-c16", "16 KiB, 16 conns"),
               ("ws-stream-1k-w32-c16", "1 KiB stream")]
        chart("ws-mem", f"WebSocket server memory under load ({plat_name})",
              f"peak {mem_word} of the server's processes, MiB; lower is better",
              wsm, P["ws"], lambda g, s: (idx.get(("ws", g[0], s)) or {}).get("server_peak_private_mib"),
              lambda v: f"{v:.0f} MiB")
        rec = sorted({(r["workload"], f"{r['held']} held") for r in idle}, key=lambda g: int(g[1].split()[0]))
        chart("idle-total", f"Server memory holding idle connections ({plat_name})",
              f"{mem_word} of the server's processes while the connections are held, MiB; lower is better",
              rec, P["http"], lambda g, s: iidx[(g[0], s)]["held_private_mib"]
              if (g[0], s) in iidx and iidx[(g[0], s)].get("valid", True) else None,
              lambda v: f"{v:.0f} MiB")
        ov = [r for r in runs if r.get("suite") == "overload"]
        if ov and (not only or "overload" in only):
            overload_chart(os.path.join(a.charts, f"bench-{tag}-overload"), runs, P["overload"],
                           "open loop, 256 connections: 50% of each server's own capacity for 5 s, "
                           "150% for 10 s, 50% for 15 s", plat_name)
        if sched:
            # Per benchmark, each loop as a share of the fastest: raw rates
            # span two orders of magnitude, too wide for one linear axis.
            groups = [(b, b) for b in sched]

            def share(g, s):
                res = sched[g[0]]
                v = (res.get(s) or {}).get("median_ops_per_sec")
                top = max((x or {}).get("median_ops_per_sec") or 0 for x in res.values())
                return v / top * 100 if v and top else None

            hung = any(not (res.get(s) or {}).get("median_ops_per_sec") for res in sched.values() for s in P["sched"])
            chart("sched", f"Scheduling microbenchmarks ({plat_name})",
                  "throughput as a share of the fastest loop per benchmark"
                  + (" (missing = hung)" if hung else "") + "; higher is better",
                  groups, P["sched"], share, lambda v: f"{v:.0f}%")
        if timers:
            tl = [(f"{d}", f"sleep({float(d) * 1e3:g} ms)") for d in ("0.001", "0.005", "0.01")]
            tcons = [c for c in P["loops"] if c in timers]
            chart("timers", f"Timer overshoot past the deadline ({plat_name})",
                  "median of 200 asyncio.sleep() calls (40 at 10 ms) on an idle loop; lower is better",
                  tl, tcons, lambda g, s: timers[s][g[0]]["p50"], lambda v: f"{v:.2f} ms")


if __name__ == "__main__":
    main()
