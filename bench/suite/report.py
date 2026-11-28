"""Aggregate run_suite.py output into tables, a summary JSON and charts.

  python bench/suite/report.py RUNDIR [RUNDIR ...] --summary OUT.json \
      [--charts docs/assets] [--markdown OUT.md] [--tables README-TABLES.json] \
      [--timers TIMERS.json] [--sched SCHED.json] [--platform windows|linux]

Percentiles are taken from the MERGED histogram of every rep's
observations, never averaged across reps. Throughput is the median rep.
Server CPU per operation is the median rep's (user + kernel) over the
measured window.

How far a number can be trusted is reported with it:

* Every median comes with a confidence interval (`median_ci`): the
  distribution-free one from the order statistics of the reps, at 95% or
  better when there are enough reps to reach it (six), and the whole
  range with its true, lower coverage when there are not.
* A table's places are shared when two intervals overlap: the runs have
  not told those two apart (`tables.mark_ranks`).
* When the runs include the same build under two names
  (`cadeloop[control]` and `cadeloop[candidate]`), the differences between
  the two are reported as the suite's resolution: no difference between
  two servers smaller than that means anything.
* A cell is flagged when the load generator used 90% or more of the CPU
  it was allowed, since the number is then the load generator's limit,
  not the server's; and each cell carries the most CPU time the
  hypervisor withheld during any of its runs.
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
        # TLS at the loop level: cadeloop's native TLS takes the blue
        # (uvicorn[standard] is never in a loop chart).
        "cadeloop, native TLS": "#2a78d6",
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
        "cadeloop, native TLS": "#3987e5",
    },
}

# Raw run labels -> what the published tables call them.
DISPLAY = {"cadeloop[candidate]": "cadeloop", "cadeloop[baseline]": "cadeloop 1.0.1",
           "cadeloop[control]": "cadeloop (control)", "cadeloop+native-tls": "cadeloop, native TLS",
           "cadeloop+logging": "cadeloop, logging module"}

# A load generator at or above this share of the CPU it was allowed is the
# limit of the run, not the server.
CLIENT_LIMITED = 0.90


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


def median_ci(values, level=0.95):
    """A confidence interval for the median of the population `values`
    were drawn from: (low, high, coverage).

    Distribution-free. Between the k-th smallest and the k-th largest of n
    independent runs the true median lies with probability
    1 - 2 * P(Binomial(n, 1/2) <= k - 1), whatever the runs' distribution
    -- no normality, which throughput on a shared machine does not have.
    The narrowest such pair whose coverage is at least `level` is returned:
    [x2, x8] of nine runs (96.1%), the whole range of six (96.9%). Fewer
    than six runs cannot reach 95%; then it is the whole range, with the
    coverage it really has (75% for three), so nothing is overstated.
    """
    xs = sorted(values)
    n = len(xs)
    if n < 2:
        return (xs[0], xs[0], 0.0) if xs else (None, None, 0.0)
    best = (xs[0], xs[-1], 1 - 2 * 0.5 ** n)
    below = 0.0  # P(Binomial(n, 1/2) <= k - 1)
    for k in range(1, n // 2 + 1):
        below += math.comb(n, k - 1) * 0.5 ** n
        cover = 1 - 2 * below
        if cover < level:
            break
        best = (xs[k - 1], xs[n - k], cover)
    return best


def _ci(values):
    lo, hi, _cover = median_ci(values)
    return [lo, hi]


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
        lo, hi, cover = median_ci(rates)
        # The load generator's CPU over the measured window, as a share of
        # what it was allowed (its thread cap, else the machine).
        client = []
        for r in rs:
            res = r["result"]
            spent = sum((res.get("client_cpu_s") or {}).values())
            procs = r.get("loadgen_procs") or res.get("config", {}).get("procs") or 0
            if procs and res.get("measured_s"):
                client.append(spent / (procs * res["measured_s"]))
        steal = [r["steal_pct"] for r in rs if r.get("steal_pct") is not None]
        peaks = [r["server_mem_peak"]["private"] / 2**20 for r in rs if r.get("server_mem_peak")]
        out.append({
            "suite": suite, "workload": wl, "server": server, "reps": len(rs),
            "rate_median": statistics.median(rates), "rate_min": min(rates), "rate_max": max(rates),
            "rates": sorted(rates), "rate_ci_lo": lo, "rate_ci_hi": hi, "rate_ci_level": cover,
            "rate_cv_pct": statistics.stdev(rates) / statistics.mean(rates) * 100 if len(rates) > 1 else None,
            "p50_ci_us": _ci([r["result"]["latency_us"]["p50"] for r in rs]),
            "p99_ci_us": _ci([r["result"]["latency_us"]["p99"] for r in rs]),
            "p999_ci_us": _ci([r["result"]["latency_us"]["p999"] for r in rs]),
            "cpu_ci_us_per_op": _ci([c["us_per_op"] for c in cpu]) if cpu else None,
            "peak_private_ci_mib": _ci(peaks) if peaks else None,
            "client_util": statistics.median(client) if client else None,
            "client_limited": bool(client) and statistics.median(client) >= CLIENT_LIMITED,
            "steal_pct_max": max(steal) if steal else None,
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


def ci_label(rows):
    """What the interval column is: "96% CI" when every cell reached it,
    "range" (with the coverage it has) when the runs were too few."""
    covers = sorted({round(r["rate_ci_level"] * 100) for r in rows if r.get("reps", 0) > 1})
    if not covers:
        return "range"
    if covers[0] >= 95:
        return f"{covers[0]}% CI" if len(covers) == 1 else f"≥{covers[0]}% CI"
    return f"range ({covers[0]}% CI)" if len(covers) == 1 else "range"


def table(rows, suite, workloads, servers):
    lines = []
    for wl in workloads:
        sel = [r for r in rows if r["suite"] == suite and r["workload"] == wl and r["server"] in servers]
        sel.sort(key=lambda r: servers.index(r["server"]))
        if not sel:
            continue
        lines.append(f"\n**{wl}**\n")
        lines.append(f"| server | per s (median) | {ci_label(sel)} | runs | p50 | p99 | p99.9 | server CPU / op "
                     "| peak private | handles | threads | errors | notes |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
        for r in sel:
            cpu = r["server_cpu_us_per_op"]
            peak = r["server_peak_private_mib"]
            notes = []
            if r.get("client_limited"):
                notes.append(f"load generator at {r['client_util'] * 100:.0f}% of its CPU: a lower bound")
            if r.get("server_died"):
                notes.append("server died")
            lines.append(
                f"| {disp(r['server'])} | {k(r['rate_median'])} | {k(r['rate_ci_lo'])}–{k(r['rate_ci_hi'])} "
                f"| {r['reps']} | {us(r['p50_us'])} | {us(r['p99_us'])} | {us(r['p999_us'])} "
                f"| {f'{cpu:.1f} µs' if cpu is not None else '–'} "
                f"| {f'{peak:.0f} MiB' if peak else '–'} "
                f"| {r['server_peak_handles'] or '–'} | {r['server_peak_threads'] or '–'} | {r['error_total']} "
                f"| {'; '.join(notes)} |")
    return "\n".join(lines)


def resolution(rows):
    """The same build measured twice (`cadeloop[control]` against
    `cadeloop[candidate]`): what separates the two is noise, so it is the
    smallest difference between two servers that these runs can show."""
    idx = {(r["suite"], r["workload"], r["server"]): r for r in rows}
    cells = []
    for (suite, wl, server), cand in idx.items():
        ctrl = idx.get((suite, wl, "cadeloop[control]"))
        if server != "cadeloop[candidate]" or ctrl is None:
            continue
        mean = (cand["rate_median"] + ctrl["rate_median"]) / 2
        cells.append({
            "suite": suite, "workload": wl,
            "candidate": cand["rate_median"], "control": ctrl["rate_median"],
            "diff_pct": (cand["rate_median"] - ctrl["rate_median"]) / mean * 100,
            "intervals_overlap": cand["rate_ci_lo"] <= ctrl["rate_ci_hi"] and ctrl["rate_ci_lo"] <= cand["rate_ci_hi"],
        })
    if not cells:
        return None
    diffs = sorted(abs(c["diff_pct"]) for c in cells)
    return {
        "cells": cells, "n": len(cells),
        "abs_diff_median_pct": statistics.median(diffs),
        "abs_diff_p90_pct": diffs[min(len(diffs) - 1, math.ceil(0.9 * len(diffs)) - 1)],
        "abs_diff_max_pct": diffs[-1],
        "told_apart": sum(1 for c in cells if not c["intervals_overlap"]),
    }


def spread(rows, servers=None):
    """How much one run differs from the next, over the cells of a session:
    the coefficient of variation of the per-run rates, and the half-width
    of the median's interval, both as percentages."""
    sel = [r for r in rows if r.get("rate_cv_pct") is not None and (servers is None or r["server"] in servers)]
    if not sel:
        return None
    cv = sorted(r["rate_cv_pct"] for r in sel)
    half = sorted((r["rate_ci_hi"] - r["rate_ci_lo"]) / 2 / r["rate_median"] * 100 for r in sel if r["rate_median"])
    return {"cells": len(sel), "cv_median_pct": statistics.median(cv), "cv_p90_pct": cv[math.ceil(0.9 * len(cv)) - 1],
            "ci_half_width_median_pct": statistics.median(half),
            "ci_half_width_p90_pct": half[math.ceil(0.9 * len(half)) - 1]}


# ---- charts -------------------------------------------------------------------

def grouped_hbar(path_base, title, subtitle, groups, servers, value_of, unit_fmt, story="cadeloop",
                 interval_of=None, limited_of=None):
    """Rows = groups (workloads), one thin bar per server; one shared axis.

    `interval_of(group, server)`, when given, returns the (low, high)
    confidence interval of a bar's value, drawn as a whisker across the
    bar's end: two bars whose whiskers overlap have not been told apart.
    `limited_of(group, server)` says a bar is a lower bound (the load
    generator was the limit): its value is always printed, behind a "≥",
    and a second subtitle line says what that means. `story` names the
    series whose bars are always labeled (one name, or several)."""
    def capped(g, s):
        return bool(limited_of and limited_of(g, s))

    any_capped = any(capped(g, s) for g in groups for s in servers if value_of(g, s) is not None)
    stories = {story} if isinstance(story, str) else set(story)
    # The subtitle in as many lines as it needs at this width: its parts
    # (separated by " · ") are never split, and never run off the edge.
    sub_lines = [subtitle]
    parts = subtitle.split(" · ")
    if len(subtitle) > 120 and len(parts) > 1:
        # Two lines, cut where they come out most alike in length.
        cuts = [(" · ".join(parts[:i]), " · ".join(parts[i:])) for i in range(1, len(parts))]
        sub_lines = list(min(cuts, key=lambda c: max(len(c[0]), len(c[1]))))
    if any_capped:
        sub_lines.append("≥ marks a bar the load generator capped: the server can do at least this")
    for mode in ("light", "dark"):
        ink = INK[mode]
        colors = COLORS[mode]
        label_w, right = 150, 70
        width = 760
        bar, gap, group_air = BAR, 2, 16
        # Legend entries, wrapped onto further rows when one would overflow.
        legend, lx, ly = [], 24.0, 66 + 18 * (len(sub_lines) - 1)
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

        def whisker(g, s):
            iv = interval_of(g, s) if interval_of else None
            return iv if iv and iv[0] is not None and iv[1] > iv[0] else None

        highs = [iv[1] for iv in (whisker(g, s) for g in groups for s in servers) if iv]
        vmax = max(vals + highs) or 1
        h = top + len(groups) * (len(servers) * (bar + gap) + group_air) + 24
        p = svg_open(width, h, ink)
        p.append(f'<text x="24" y="34" font-size="16" font-weight="600" fill="{ink["primary"]}">{title}</text>')
        for i, line in enumerate(sub_lines):
            p.append(f'<text x="24" y="{54 + 18 * i}" font-size="12" fill="{ink["secondary"]}">{line}</text>')
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
                    end = w
                    iv = whisker(g, s)
                    if iv:
                        # The interval of the median: a hairline with end
                        # caps in the text ink, on a surface-colored halo
                        # so it reads across the bar's own fill.
                        x0, x1 = label_w + iv[0] / vmax * plot_w, label_w + iv[1] / vmax * plot_w
                        cy = y + bar / 2
                        d = (f"M{x0:.1f},{cy:.1f} H{x1:.1f} M{x0:.1f},{cy - 3:.1f} V{cy + 3:.1f} "
                             f"M{x1:.1f},{cy - 3:.1f} V{cy + 3:.1f}")
                        p.append(f'<path d="{d}" fill="none" stroke="{ink["surface"]}" stroke-width="3.5" '
                                 f'stroke-linecap="round"/>')
                        p.append(f'<path d="{d}" fill="none" stroke="{ink["primary"]}" stroke-width="1.25" '
                                 f'stroke-linecap="round"/>')
                        end = max(w, x1 - label_w)
                    # Every bar when there are few enough to label without
                    # clutter; otherwise the story series (tables carry all).
                    if disp(s) in stories or len(servers) <= 4 or capped(g, s):
                        text = ("≥ " if capped(g, s) else "") + unit_fmt(v)
                        p.append(f'<text x="{label_w + end + 6:.1f}" y="{y + bar - 3}" font-size="11" '
                                 f'fill="{ink["secondary"]}">{text}</text>')
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
        "tlsecho": ["cadeloop[candidate]", "cadeloop+native-tls", "winloop", "rsloop", "asyncio"],
        "loops": ["cadeloop[candidate]", "winloop", "asyncio"],
        "sched": ["cadeloop", "winloop", "rsloop", "asyncio"],
    },
    # Linux series orders: cadeloop, the uvicorn setups, then the other
    # servers. With granian next to cadeloop, as these lists once had it,
    # the two bars are one color to a deuteranope (teal and magenta, CVD
    # ΔE 5.2 light and 2.4 dark); in this order every adjacent pair of
    # every chart clears ΔE 9.7 under protan and deutan simulation and 15
    # for normal vision, in both themes (the dataviz palette checker).
    "linux": {
        "tag": "linux", "name": "Linux",
        "http": ["cadeloop[candidate]", "uvicorn[standard]", "uvicorn+uvloop", "uvicorn+asyncio",
                 "uvicorn+cadeloop", "granian", "hypercorn+asyncio"],
        "ws": ["cadeloop[candidate]", "uvicorn[standard]", "uvicorn+uvloop", "uvicorn+asyncio", "granian",
               "hypercorn+asyncio"],
        "tls": ["cadeloop[candidate]", "uvicorn[standard]", "uvicorn+uvloop", "uvicorn+asyncio", "granian"],
        "echo": ["cadeloop[candidate]", "uvloop", "rloop", "rsloop", "asyncio"],
        "tlsecho": ["cadeloop[candidate]", "cadeloop+native-tls", "uvloop", "rsloop", "asyncio"],
        "overload": ["cadeloop[candidate]", "uvicorn[standard]", "uvicorn+uvloop", "uvicorn+asyncio",
                     "uvicorn+cadeloop", "granian"],
        "loops": ["cadeloop[candidate]", "uvloop", "rloop", "rsloop", "asyncio"],
        "sched": ["cadeloop", "uvloop", "rloop", "rsloop", "asyncio"],
    },
}

HTTP_LABELS = [("plaintext-c64", "plaintext"), ("plaintext-c1", "plaintext, 1 connection"), ("json-c64", "JSON"),
               ("large64k-c64", "64 KiB body"), ("stream1m-c16", "1 MiB stream (16 conns)"),
               ("upload16k-c64", "16 KiB upload"), ("sleep1ms-c64", "1 ms await"),
               ("churn-c16", "conn per request (16)"), ("pipelined16-c64", "pipelined ×16")]
WS_LABELS = [("ws-64b-c1", "64 B, 1 connection"), ("ws-64b-c64", "64 B, 64 connections"),
             ("ws-16k-c16", "16 KiB, 16 connections"), ("ws-256k-c8", "256 KiB, 8 connections"),
             ("ws-stream-1k-w32-c16", "1 KiB stream, 32 in flight ×16")]
APP_LABELS = [("fastapi-json-c64", "JSON, async def"), ("fastapi-sync-c64", "JSON, def (worker thread)"),
              ("fastapi-file64k-c64", "FileResponse, 64 KiB"), ("fastapi-file1m-c16", "FileResponse, 1 MiB (16 conns)")]
TLSECHO_LABELS = [("tlsecho-1k-c64", "1 KiB, 64 connections"), ("tlsecho-16k-c16", "16 KiB, 16 connections")]
TLS_LABELS = [("tls-plaintext-c64", "plaintext, 64 keep-alive conns"),
              ("tls-churn-c16", "new connection per request (16)")]
# A session that ran the handshake workload under two certificates
# (run_suite.py --suite tlsrsa) names the key in both rows.
TLS_LABELS_BY_KEY = [("tls-plaintext-c64", "plaintext, 64 keep-alive conns"),
                     ("tls-churn-c16", "new connection per request, ECDSA key"),
                     ("tls-churn-rsa-c16", "new connection per request, RSA key")]
ECHO_LABELS = [("echo-1k-c1", "1 KiB, 1 connection"), ("echo-1k-c64", "1 KiB, 64 connections"),
               ("echo-1k-w16-c64", "1 KiB, 16 in flight ×64"), ("echo-64k-c8", "64 KiB, 8 connections")]


def tname(server, as_server=True):
    """A server's name in the README tables."""
    if server == "cadeloop[candidate]":
        return "cadeloop serve()" if as_server else "cadeloop"
    return {"cadeloop[baseline]": "cadeloop (baseline)", "hypercorn+asyncio": "hypercorn",
            "cadeloop+native-tls": "cadeloop, native TLS",
            "cadeloop+logging": "cadeloop serve(), logging module"}.get(server, server.replace("+", " + "))


def runs_phrase(rows, suite):
    """"median of 9 runs", from the runs a suite's cells actually have."""
    reps = sorted({r["reps"] for r in rows if r["suite"] == suite})
    if not reps:
        return "median of the runs"
    return f"median of {reps[0]} runs" if len(reps) == 1 else f"median of {reps[0]}–{reps[-1]} runs"


def ties_note(rows, suite):
    """The footnote that says what a shared place means."""
    sel = [r for r in rows if r["suite"] == suite and r.get("reps", 0) > 1]
    if not sel:
        return None
    cover = min(r["rate_ci_level"] for r in sel) * 100
    interval = f"{cover:.0f}% confidence intervals" if cover >= 95 else "ranges over the runs"
    return f"A place is shared when the {interval} of two medians overlap: the runs did not tell those two apart."


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
    """README table specs (bench/harness/tables.py), the top three of each
    row or column ranked."""
    from tables import mark_ranks

    idx = {(r["suite"], r["workload"], r["server"]): r for r in rows}
    plat, tag = P["name"], P["tag"]
    specs = {}

    def ok(r):
        return r is not None and not r["error_total"] and not r["server_died"]

    def interval(r, key="rate"):
        """A cell's confidence interval, or None when one run made it."""
        if r is None or r.get("reps", 0) < 2:
            return None
        if key == "rate":
            # A rate the load generator capped is a lower bound: the server
            # can do at least this, and the runs do not say how much more.
            # Open above, the interval can put the cell ahead of a server
            # it clearly beat and never behind one.
            return (r["rate_ci_lo"], math.inf if r.get("client_limited") else r["rate_ci_hi"])
        pair = r.get(key)
        return tuple(pair) if pair and pair[0] is not None else None

    def by_workload(suite, labels, cons, with_p99, as_server, names=None):
        body, vals, ivs, limited = [], [], [], False
        for wl, label in labels:
            cells, vs, iv = [], [], []
            if not any((suite, wl, srv) in idx for srv in cons):
                continue  # a workload these runs do not have (an older session)
            for srv in cons:
                r = idx.get((suite, wl, srv))
                if r is None:
                    cells.append({"text": "–"})
                    vs.append(None)
                    iv.append(None)
                    continue
                mark = "" if ok(r) else " *"
                if ok(r) and r.get("client_limited"):
                    mark, limited = " ≥", True
                cell = {"text": kfmt(r["rate_median"]) + mark}
                if with_p99:
                    cell["sub"] = us(r["p99_us"])
                cells.append(cell)
                vs.append(shown(cell["text"]) if ok(r) else None)
                iv.append(interval(r) if ok(r) else None)
            body.append({"label": label, "cells": cells})
            vals.append(vs)
            ivs.append(iv)
        mark_ranks(body, True, vals, axis="row", intervals=ivs)
        notes = [n for n in [ties_note(rows, suite)] if n]
        if limited:
            notes.append("≥: the load generator was at its own CPU limit, so the server can do at least this.")
        names = names or {}
        return {"columns": ["workload"] + [names.get(c, tname(c, as_server)) for c in cons], "rows": body,
                **({"notes": notes} if notes else {})}

    specs[f"bench-{tag}-table-http"] = {
        "title": f"HTTP requests per second · {plat}",
        "subtitle": f"64 keep-alive connections unless noted · one worker per server · {runs_phrase(rows, 'http')}",
        **by_workload("http", HTTP_LABELS, P["http"], False, True)}
    specs[f"bench-{tag}-table-ws"] = {
        "title": f"WebSocket echo · {plat}",
        "subtitle": f"messages per second, p99 round trip in small type · {runs_phrase(rows, 'ws')}",
        **by_workload("ws", WS_LABELS, P["ws"], True, False)}
    two_keys = ("tls", "tls-churn-rsa-c16") in {(r["suite"], r["workload"]) for r in rows}
    tls_labels = TLS_LABELS_BY_KEY if two_keys else TLS_LABELS
    specs[f"bench-{tag}-table-tls"] = {
        "title": f"TLS · {plat}",
        "subtitle": ("requests per second, p99 in small type · every server uses Python's ssl module, "
                     "except granian (Rust)" if "granian" in P["tls"] else
                     "requests per second, p99 in small type · Python's ssl module in every server")
                    + f" · {runs_phrase(rows, 'tls')}",
        **by_workload("tls", tls_labels, P["tls"], True, False)}
    if two_keys:
        specs[f"bench-{tag}-table-tls"].setdefault("notes", []).append(
            "New connections: 16 at a time, a full TLS 1.3 handshake for each request, under an ECDSA P-256 "
            "certificate and under an RSA 2048 one.")
    specs[f"bench-{tag}-table-echo"] = {
        "title": f"TCP echo, the loop alone · {plat}",
        "subtitle": "the same asyncio.Protocol on every loop · messages per second, p99 in small type · "
                    + runs_phrase(rows, "echo"),
        **by_workload("echo", ECHO_LABELS, P["echo"], True, False)}
    if any(r["suite"] == "app" for r in rows):
        specs[f"bench-{tag}-table-app"] = {
            "title": f"A FastAPI app · {plat}",
            "subtitle": "requests per second, p99 in small type · 64 connections unless noted · "
                        + runs_phrase(rows, "app"),
            **by_workload("app", APP_LABELS, P["http"], True, True)}
    if any(r["suite"] == "tlsecho" for r in rows):
        specs[f"bench-{tag}-table-tlsecho"] = {
            "title": f"TLS echo, the loop alone · {plat}",
            "subtitle": "the same asyncio.Protocol behind create_server(ssl=...) · messages per second, "
                        f"p99 in small type · {runs_phrase(rows, 'tlsecho')}",
            **by_workload("tlsecho", TLSECHO_LABELS, [c for c in P["tlsecho"] if any(
                (r["suite"], r["server"]) == ("tlsecho", c) for r in rows)], True, False,
                names={"cadeloop[candidate]": "cadeloop, stdlib TLS"})}

    # The access log: what turning it on costs each server.
    if any(r["suite"] == "log" for r in rows):
        body, vals, ivs = [], [], []
        cons = [P["http"][0], "cadeloop+logging", *P["http"][1:]]
        for srv in cons:
            on = idx.get(("log", "log-plaintext-c64", srv))
            # The logging-module variant is the same server as cadeloop's
            # first row with the log off.
            off = idx.get(("http", "plaintext-c64", P["http"][0] if srv == "cadeloop+logging" else srv))
            if not on or not off:
                continue
            cpu = on["server_cpu_us_per_op"]
            label = "cadeloop serve(), native log" if srv == P["http"][0] else tname(srv)
            body.append({"label": label, "cells": [
                {"text": kfmt(off["rate_median"])}, {"text": kfmt(on["rate_median"]), "sub": us(on["p99_us"])},
                {"text": f"{on['rate_median'] / off['rate_median'] * 100:.0f}%"},
                {"text": f"{cpu:.1f} µs" if cpu is not None else "–"}]})
            vals.append([None, shown(kfmt(on["rate_median"])), shown(body[-1]["cells"][2]["text"]),
                         shown(body[-1]["cells"][3]["text"])])
            ivs.append([None, interval(on), None, interval(on, "cpu_ci_us_per_op")])
        mark_ranks(body, [None, True, True, False], vals, axis="col", intervals=ivs)
        specs[f"bench-{tag}-table-log"] = {
            "title": f"With the access log on · {plat}",
            "subtitle": "plaintext, 64 connections · each server's own access log, written to a file · "
                        + runs_phrase(rows, "log"),
            "columns": ["server", "log off", "log on", "kept", "CPU / request, log on"],
            "rows": body,
            "notes": [n for n in [ties_note(rows, "log"),
                                  "kept: requests per second with the log on, as a share of the same server "
                                  "with it off."] if n]}

    # Plaintext in detail: one row per server, ranked per column.
    body, vals, ivs = [], [], []
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
        # The percentiles shown come from the merged histogram; whether
        # two servers' are apart is judged on the per-run percentiles.
        ivs.append([interval(r), interval(r, "p50_ci_us"), interval(r, "p99_ci_us"), interval(r, "p999_ci_us"),
                    interval(r, "cpu_ci_us_per_op"), interval(r, "peak_private_ci_mib"), None])
    # Threads describe a design rather than score one: never ranked.
    mark_ranks(body, [True, False, False, False, False, False, None], vals, axis="col", intervals=ivs)
    note = ties_note(rows, "http")
    specs[f"bench-{tag}-table-plaintext"] = {
        "title": f"Plaintext HTTP in detail · {plat}",
        "subtitle": "64 connections · CPU is user + kernel time of the server's processes per request · "
                    + runs_phrase(rows, "http"),
        "columns": ["server", "req/s", "p50", "p99", "p99.9", "CPU / request", "peak memory", "threads"],
        "rows": body, **({"notes": [note]} if note else {})}

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
        mark_ranks(body, False, vals, axis="col")
        specs[f"bench-{tag}-table-idle"] = {
            "title": f"Idle keep-alive connections · {plat}",
            "subtitle": f"each of {top:,} connections made one request and was held open · "
                        + ("private bytes" if tag == "win" else "unique memory (USS)") + " of the server",
            "columns": ["server", "at rest", f"holding {top:,}", "per connection", "after they close",
                        "handles / conn" if tag == "win" else "fds / conn"],
            "rows": body, **({"notes": notes} if notes else {})}

    if timers:
        body, vals, ivs = [], [], []

        def runs_interval(entry, scale=1.0):
            """The interval of a loop's median over its processes (in the
            unit the cell shows), when it was measured in several."""
            runs = entry.get("p50_runs")
            if not runs or len(runs) < 2:
                return None
            lo, hi, _cover = median_ci(runs)
            return (lo * scale, hi * scale)

        for srv in P["loops"]:
            t = timers.get(srv)
            if not t:
                continue
            cells, vs, iv = [], [], []
            for d in ("0.001", "0.005", "0.01"):
                cells.append({"text": f"+{t[d]['p50']:.2f} / {t[d]['p99']:.2f} ms"})
                vs.append(shown(cells[-1]["text"]))
                iv.append(runs_interval(t[d]))  # milliseconds, as the cell's first number
            x = t["xthread"]
            cells.append({"text": f"{x['p50'] * 1e3:.0f} / {x['p99'] * 1e3:.0f} µs"})
            vs.append(shown(cells[-1]["text"]))
            iv.append(runs_interval(x, 1e3))  # the cell shows microseconds
            body.append({"label": tname(srv, False), "cells": cells})
            vals.append(vs)
            ivs.append(iv)
        mark_ranks(body, False, vals, axis="col", intervals=ivs)
        nproc = max((len(t[d].get("p50_runs", [])) for t in timers.values() for d in ("0.001",)), default=0)
        notes = ["Places go by the median in each column."]
        if nproc > 1:
            notes = [f"Each figure is the median over {nproc} fresh processes per loop. Places go by the median in "
                     "each column, and are shared when its confidence intervals overlap."]
        specs[f"bench-{tag}-table-timers"] = {
            "title": f"Timers and wake-ups · {plat}",
            "subtitle": "how late asyncio.sleep(d) returns on an idle loop (median / p99) · "
                        "call_soon_threadsafe() into a sleeping loop",
            "columns": ["loop", "sleep(1 ms)", "sleep(5 ms)", "sleep(10 ms)", "cross-thread wake-up"],
            "rows": body, "notes": notes}

    if sched:
        body, vals, ivs = [], [], []
        covers = []
        for bench, res in sched.items():
            cells, vs, iv = [], [], []
            for lp in P["sched"]:
                entry = res.get(lp) or {}
                v = entry.get("median_ops_per_sec")
                cells.append({"text": f"{v / 1e6:.3f}" if v else "hung"})
                vs.append(shown(cells[-1]["text"]))
                runs = entry.get("runs") or []
                if v and len(runs) > 1:
                    lo, hi, cover = median_ci(runs)
                    iv.append((lo / 1e6, hi / 1e6))  # the unit the cell shows
                    covers.append(cover)
                else:
                    iv.append(None)
            body.append({"label": bench, "label_code": True, "cells": cells})
            vals.append(vs)
            ivs.append(iv)
        mark_ranks(body, True, vals, axis="row", intervals=ivs)
        nruns = max(len((x or {}).get("runs", [])) for res in sched.values() for x in res.values())
        note = None
        if covers:
            cover = min(covers) * 100
            interval = f"{cover:.0f}% confidence intervals" if cover >= 95 else "ranges over the runs"
            note = f"A place is shared when the {interval} of two medians overlap: the runs did not tell those two apart."
        specs[f"bench-{tag}-table-sched"] = {
            "title": f"Scheduling microbenchmarks · {plat}",
            "subtitle": f"millions of operations per second · median of {nruns} runs, a fresh process each",
            "columns": ["benchmark"] + P["sched"], "rows": body, **({"notes": [note]} if note else {})}
    return specs


def timers_markdown(timers, loops):
    """bench/suite/latency.py's results as a table: overshoot of
    asyncio.sleep(d), median / p99, with the median's interval when the
    loops were measured in several processes."""
    cols = [("0.0005", "sleep(0.5 ms)"), ("0.001", "sleep(1 ms)"), ("0.005", "sleep(5 ms)"),
            ("0.01", "sleep(10 ms)"), ("0.05", "sleep(50 ms)"), ("busy-0.001", "busy, 1 ms")]
    nproc = max((len(t["0.001"].get("p50_runs", [])) for t in timers.values()), default=0)

    def cell(entry, fmt, unit, scale=1.0):
        def num(v):
            return fmt.format(v * scale).replace("-0.00", "+0.00")  # no negative zero

        text = num(entry["p50"])
        runs = entry.get("p50_runs")
        if runs and len(runs) > 1:
            lo, hi, _cover = median_ci(runs)
            text += f" [{num(lo)}, {num(hi)}]"
        return f"{text} / {num(entry['p99']).lstrip('+')} {unit}"

    lines = ["\n## Timers and wake-ups\n"]
    how = (f"the median over {nproc} fresh processes per loop, the loops taking turns, with the "
           f"{median_ci(list(range(nproc)))[2] * 100:.0f}% confidence interval of that median in brackets"
           if nproc > 1 else "one process per loop")
    lines.append("How late `asyncio.sleep(d)` returned on an idle loop, median / p99 over 200 sleeps (40 at 10 ms "
                 f"and 50 ms): {how}. A negative figure is a sleep that returned early. `busy` is a 1 ms sleep "
                 "while another task keeps the loop running short callbacks; `cross-thread` is a "
                 "`call_soon_threadsafe()` from another thread into a sleeping loop.\n")
    lines.append("| loop | " + " | ".join(label for _, label in cols) + " | cross-thread |")
    lines.append("|---|" + "---|" * (len(cols) + 1))
    for srv in loops:
        t = timers.get(srv)
        if not t:
            continue
        cells = [cell(t[key], "{:+.2f}", "ms") for key, _ in cols]
        cells.append(cell(t["xthread"], "{:.0f}", "µs", 1e3))
        lines.append(f"| {tname(srv, False)} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def sched_markdown(sched, loops):
    """bench/harness's scheduling results as a table: the median of the
    runs with its interval."""
    nruns = max(len((x or {}).get("runs", [])) for res in sched.values() for x in res.values())
    cover = median_ci(list(range(nruns)))[2] * 100 if nruns > 1 else 0
    what = f"{cover:.0f}% confidence interval" if cover >= 95 else "slowest and fastest run"
    lines = ["\n## Scheduling microbenchmarks\n"]
    lines.append(f"Millions of operations per second: the median of {nruns} runs (a fresh process per run, "
                 f"the loops taking turns), with its {what}.\n")
    lines.append("| benchmark | " + " | ".join(loops) + " |")
    lines.append("|---|" + "---|" * len(loops))
    for bench, res in sched.items():
        cells = []
        for lp in loops:
            entry = res.get(lp) or {}
            v = entry.get("median_ops_per_sec")
            if not v:
                cells.append("hung")
                continue
            runs = entry.get("runs") or []
            lo, hi, _cover = median_ci(runs) if len(runs) > 1 else (entry.get("min", v), entry.get("max", v), 0)
            cells.append(f"{v / 1e6:.3f} ({lo / 1e6:.3f}–{hi / 1e6:.3f})")
        lines.append(f"| `{bench}` | " + " | ".join(cells) + " |")
    return "\n".join(lines)


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
    res = resolution(rows)
    noise = spread(rows)
    with open(a.summary, "w") as f:
        json.dump({"meta": meta, "rows": rows, "idle": idle, "timers": timers, "resolution": res,
                   "spread": noise}, f, indent=1)

    plat = a.platform or ("windows" if meta[0].get("platform", "").startswith("Windows") else "linux")
    P = {k: list(v) if isinstance(v, list) else v for k, v in PLATFORMS[plat].items()}
    if a.with_baseline:
        for key in ("http", "ws", "tls", "echo", "loops"):
            P[key].insert(1, "cadeloop[baseline]")
    tag, plat_name = P["tag"], P["name"]
    only = {x for x in a.only_charts.split(",") if x}

    md = []
    if noise:
        md.append("## How far to trust these numbers\n")
        md.append(f"Across {noise['cells']} cells, one run differs from the next by "
                  f"{noise['cv_median_pct']:.1f}% (coefficient of variation; {noise['cv_p90_pct']:.1f}% at the "
                  f"90th percentile of cells). The interval of a median is ±{noise['ci_half_width_median_pct']:.1f}% "
                  f"of it at the median cell and ±{noise['ci_half_width_p90_pct']:.1f}% at the 90th percentile.")
    if res:
        md.append(f"\nThe same cadeloop build was measured twice, under two names, in all {res['n']} workloads. "
                  f"The two medians differ by {res['abs_diff_median_pct']:.1f}% at the median workload, "
                  f"{res['abs_diff_p90_pct']:.1f}% at the 90th percentile and {res['abs_diff_max_pct']:.1f}% at "
                  f"most; their intervals fail to overlap in {res['told_apart']} of {res['n']}. A difference "
                  "between two servers smaller than that is not one these runs can show.\n")
        md.append("| suite | workload | candidate | control | difference | intervals overlap |")
        md.append("|---|---|---|---|---|---|")
        for c in sorted(res["cells"], key=lambda c: (c["suite"], c["workload"])):
            md.append(f"| {c['suite']} | {c['workload']} | {k(c['candidate'])} | {k(c['control'])} "
                      f"| {c['diff_pct']:+.1f}% | {'yes' if c['intervals_overlap'] else 'no'} |")
    md.append("\n## HTTP")
    md.append(table(rows, "http", [w for w, _ in __import__("run_suite").HTTP_WORKLOADS], P["http"]))
    md.append("\n## A FastAPI app")
    md.append(table(rows, "app", [w for w, _ in APP_LABELS], P["http"]))
    md.append("\n## Plaintext with the access log on")
    md.append(table(rows, "log", ["log-plaintext-c64"], [P["http"][0], "cadeloop+logging", *P["http"][1:]]))
    md.append("\n## WebSocket")
    md.append(table(rows, "ws", [w for w, _ in WS_LABELS], P["ws"]))
    md.append("\n## TLS")
    md.append(table(rows, "tls", [w for w, _ in TLS_LABELS_BY_KEY], P["tls"]))
    md.append("\n## TCP echo (raw asyncio.Protocol)")
    md.append(table(rows, "echo", [w for w, _ in ECHO_LABELS], P["echo"]))
    md.append("\n## TLS echo (raw asyncio.Protocol behind create_server(ssl=...))")
    md.append(table(rows, "tlsecho", [w for w, _ in TLSECHO_LABELS], P["tlsecho"]))
    md.append("\n## Idle keep-alive connections\n")
    md.append("| server | connections | private / conn | working set / conn | handles / conn | private after close |")
    md.append("|---|---|---|---|---|---|")
    for r in sorted(idle, key=lambda r: (r["requested"], P["http"].index(r["server"])
                                           if r["server"] in P["http"] else 99)):
        md.append(f"| {disp(r['server'])} | {r['held']} | {r['per_conn_private_kib']:.1f} KiB "
                  f"| {r['per_conn_ws_kib']:.1f} KiB | {r['per_conn_handles']:.2f} "
                  f"| {r['after_close_private_mib']:.0f} MiB |")
    if timers:
        md.append(timers_markdown(timers, P["loops"]))
    if sched:
        md.append(sched_markdown(sched, P["sched"]))
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

        def rate_ci(g, s, suite="http"):
            r = idx.get((suite, g[0], s))
            return (r["rate_ci_lo"], r["rate_ci_hi"]) if r and r["reps"] > 1 else None

        def capped(g, s, suite="http"):
            r = idx.get((suite, g[0], s))
            return bool(r and r.get("client_limited"))

        def cpu(g, s, suite="http"):
            r = idx.get((suite, g[0], s))
            return r["server_cpu_us_per_op"] if r else None

        def cpu_ci(g, s, suite="http"):
            r = idx.get((suite, g[0], s))
            return tuple(r["cpu_ci_us_per_op"]) if r and r["reps"] > 1 and r.get("cpu_ci_us_per_op") else None

        def runs_and_interval(suite):
            """"median of 9 runs with its 96% confidence interval"."""
            sel = [r for r in rows if r["suite"] == suite and r["reps"] > 1]
            if not sel:
                return runs_phrase(rows, suite)
            cover = min(r["rate_ci_level"] for r in sel) * 100
            what = f"its {cover:.0f}% confidence interval" if cover >= 95 else "the range of the runs"
            return f"{runs_phrase(rows, suite)} with {what}"

        chart("http", f"HTTP/1.1 requests per second ({plat_name})",
              f"64 keep-alive connections (16 for conn per request) · {runs_and_interval('http')} · higher is better",
              hw, P["http"], rate, lambda v: f"{v / 1e3:.1f}K", interval_of=rate_ci, limited_of=capped)
        chart("http-cpu", f"Server CPU per request ({plat_name})",
              "user + kernel time of the server's processes / successful requests · "
              f"{runs_and_interval('http')} · lower is better",
              hw, P["http"], cpu, lambda v: f"{v:.1f} µs", interval_of=cpu_ci)
        ww = [("ws-64b-c1", "64 B, 1 conn"), ("ws-64b-c64", "64 B, 64 conns"), ("ws-16k-c16", "16 KiB, 16 conns"),
              ("ws-256k-c8", "256 KiB, 8 conns"), ("ws-stream-1k-w32-c16", "1 KiB stream")]
        ww = [g for g in ww if any(("ws", g[0], s) in idx for s in P["ws"])]
        chart("ws", f"WebSocket echo messages per second ({plat_name})",
              f"ping-pong except 1 KiB stream (32 in flight per connection) · {runs_and_interval('ws')} · "
              "higher is better",
              ww, P["ws"], lambda g, s: rate(g, s, "ws"), lambda v: f"{v / 1e3:.0f}K",
              interval_of=lambda g, s: rate_ci(g, s, "ws"), limited_of=lambda g, s: capped(g, s, "ws"))
        ew = [("echo-1k-c1", "1 KiB, 1 conn"), ("echo-1k-c64", "1 KiB, 64 conns"),
              ("echo-1k-w16-c64", "1 KiB, 16 in flight"), ("echo-64k-c8", "64 KiB, 8 conns")]
        chart("echo", f"TCP echo messages per second ({plat_name})",
              f"same asyncio.Protocol on every loop · {runs_and_interval('echo')} · higher is better",
              ew, P["echo"], lambda g, s: rate(g, s, "echo"), lambda v: f"{v / 1e3:.0f}K",
              interval_of=lambda g, s: rate_ci(g, s, "echo"), limited_of=lambda g, s: capped(g, s, "echo"))
        if any(r["suite"] == "app" for r in rows):
            aw = [("fastapi-json-c64", "JSON, async def"), ("fastapi-sync-c64", "JSON, def"),
                  ("fastapi-file64k-c64", "file, 64 KiB"), ("fastapi-file1m-c16", "file, 1 MiB")]
            chart("app", f"A FastAPI app: requests per second ({plat_name})",
                  f"pydantic response model; FileResponse · {runs_and_interval('app')} · higher is better",
                  aw, P["http"], lambda g, s: rate(g, s, "app"), lambda v: f"{v / 1e3:.1f}K",
                  interval_of=lambda g, s: rate_ci(g, s, "app"), limited_of=lambda g, s: capped(g, s, "app"))
        if any(r["suite"] == "tlsecho" for r in rows):
            tw = [("tlsecho-1k-c64", "1 KiB, 64 conns"), ("tlsecho-16k-c16", "16 KiB, 16 conns")]
            tcons = [c for c in P["tlsecho"] if any(("tlsecho", g[0], c) in idx for g in tw)]
            chart("tlsecho", f"TLS echo messages per second ({plat_name})",
                  f"create_server(ssl=...); cadeloop with the stdlib's TLS (default) and its own · "
                  f"{runs_and_interval('tlsecho')}",
                  tw, tcons, lambda g, s: rate(g, s, "tlsecho"), lambda v: f"{v / 1e3:.0f}K",
                  story=("cadeloop", "cadeloop, native TLS"),
                  interval_of=lambda g, s: rate_ci(g, s, "tlsecho"),
                  limited_of=lambda g, s: capped(g, s, "tlsecho"))
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
