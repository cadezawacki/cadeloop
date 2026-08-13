#!/usr/bin/env python3
"""Generate the README benchmark charts (light + dark SVG pairs).

Follows the dataviz method: entity-fixed categorical colors (validated
palette, adjacent-pair CVD >= 8), thin bars (16px) with 4px rounded data
ends and square baselines, 2px surface gaps, hairline gridlines, text in
ink tokens (never series colors), value labels selective (the story
series), full data in the README tables (contrast-relief channel).

Usage:
  python bench/harness/charts.py --sched S.json --echo E.json --http H.json --outdir docs/assets
"""

import argparse
import json
import pathlib

# Entity -> categorical slot (fixed across every chart; color follows the
# entity, never the rank).
LIGHT = {
    "cadeloop": "#2a78d6",
    "cadeloop-native": "#164a9e",
    "asyncio": "#eb6834",
    "uvloop": "#1baf7a",
    "rloop": "#eda100",
    "rsloop": "#e87ba4",
    "aiofastnet": "#00879e",
    "aiofastnet-cadeloop": "#79aede",
    "hypercorn": "#4a3aa7",
}
DARK = {
    "cadeloop": "#3987e5",
    "cadeloop-native": "#6fa9ef",
    "asyncio": "#d95926",
    "uvloop": "#199e70",
    "rloop": "#c98500",
    "rsloop": "#d55181",
    "aiofastnet": "#1fa2b8",
    "aiofastnet-cadeloop": "#5e93c9",
    "hypercorn": "#9085e9",
}

INK = {
    "light": {
        "surface": "#fcfcfb",
        "primary": "#0b0b0b",
        "secondary": "#52514e",
        "muted": "#898781",
        "grid": "#e1e0d9",
        "axis": "#c3c2b7",
    },
    "dark": {
        "surface": "#1a1a19",
        "primary": "#ffffff",
        "secondary": "#c3c2b7",
        "muted": "#898781",
        "grid": "#2c2c2a",
        "axis": "#383835",
    },
}

FONT = "system-ui, -apple-system, 'Segoe UI', sans-serif"
BAR = 16  # bar thickness (<= 24)
GAP = 2  # surface gap between touching bars
GROUP_AIR = 14


def series_color(mode, entity):
    key = entity.split("+")[-1] if "+" in entity else entity
    table = LIGHT if mode == "light" else DARK
    return table.get(key, table["hypercorn"])


def hbar(x0, y, width, color, surface):
    """Horizontal bar: square at baseline (left), 4px rounded data end."""
    w = max(width, 0.5)
    r = min(4.0, w)
    return (
        f'<path d="M{x0:.1f},{y:.1f} h{w - r:.1f} q{r},0 {r},{r:.1f} '
        f'v{BAR - 2 * r:.1f} q0,{r} -{r},{r} h-{w - r:.1f} z" fill="{color}"/>'
        f'<rect x="{x0:.1f}" y="{y - GAP / 2:.1f}" width="0" height="0" fill="{surface}"/>'
    )


def svg_open(w, h, ink):
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" '
        f'width="{w}" height="{h}" font-family="{FONT}">',
        f'<rect width="{w}" height="{h}" fill="{ink["surface"]}" rx="8"/>',
    ]


def legend_row(x, y, entries, mode, ink):
    parts = []
    for name in entries:
        c = series_color(mode, name)
        parts.append(f'<rect x="{x}" y="{y - 9}" width="12" height="12" rx="3" fill="{c}"/>')
        parts.append(
            f'<text x="{x + 17}" y="{y + 1}" font-size="12" fill="{ink["secondary"]}">{name}</text>'
        )
        x += 17 + 7.2 * len(name) + 18
    return parts


def fmt(v):
    if v >= 100:
        return f"{v:.0f}"
    if v >= 10:
        return f"{v:.1f}"
    return f"{v:.2f}"


def fmt_tick(v):
    return f"{v:g}"


def ticks_for(vmax, n=5):
    import math

    raw = vmax / n
    mag = 10 ** math.floor(math.log10(raw)) if raw > 0 else 1
    for mult in (1, 2, 2.5, 5, 10):
        step = mag * mult
        if vmax / step <= n:
            break
    return [step * i for i in range(0, int(vmax / step) + 2)]


def chart_speedup(sched, mode, story="cadeloop"):
    """Horizontal grouped bars: speedup vs stdlib asyncio per benchmark."""
    ink = INK[mode]
    results = sched["results"]
    benches = [b for b in results if results[b].get("asyncio")]
    loops = [l for l in ("cadeloop", "uvloop", "rloop", "rsloop") if any(results[b].get(l) for b in benches)]

    left, right, top = 190, 70, 66
    plot_w = 560
    group_h = len(loops) * (BAR + GAP) + GROUP_AIR
    h = top + len(benches) * group_h + 44
    w = left + plot_w + right

    data = {}
    vmax = 1.0
    for b in benches:
        base = results[b]["asyncio"]["median_ops_per_sec"]
        data[b] = {}
        for l in loops:
            e = results[b].get(l)
            if e:
                s = e["median_ops_per_sec"] / base
                data[b][l] = s
                vmax = max(vmax, s)

    # Cap the axis so one outlier group cannot squash every other group;
    # clamped bars carry their true value as a label.
    axis_cap = 8.0
    capped = min(vmax * 1.06, axis_cap)
    tks = [i * 2.0 for i in range(int(capped / 2) + 1)] if capped > 4 else ticks_for(capped)
    scale = plot_w / max(tks[-1], 1e-9)
    out = svg_open(w, h, ink)
    out.append(
        f'<text x="20" y="28" font-size="15" font-weight="600" fill="{ink["primary"]}">'
        "Scheduling speedup vs stdlib asyncio (higher is better)</text>"
    )
    out += legend_row(20, 48, loops, mode, ink)

    for t in tks:
        x = left + t * scale
        out.append(
            f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{h - 36}" stroke="{ink["grid"]}" stroke-width="1"/>'
        )
        out.append(
            f'<text x="{x:.1f}" y="{h - 20}" font-size="11" fill="{ink["muted"]}" '
            f'text-anchor="middle">{fmt_tick(t)}x</text>'
        )
    # 1x reference (the stdlib baseline).
    x1 = left + 1.0 * scale
    out.append(
        f'<line x1="{x1:.1f}" y1="{top}" x2="{x1:.1f}" y2="{h - 36}" stroke="{ink["axis"]}" stroke-width="1.5"/>'
    )
    out.append(
        f'<text x="{x1:.1f}" y="{h - 6}" font-size="10.5" fill="{ink["muted"]}" '
        f'text-anchor="middle">asyncio = 1x</text>'
    )

    y = top + 6
    for b in benches:
        label_y = y + (len(loops) * (BAR + GAP)) / 2 + 4
        out.append(
            f'<text x="{left - 10}" y="{label_y:.1f}" font-size="12" fill="{ink["secondary"]}" '
            f'text-anchor="end">{b}</text>'
        )
        for l in loops:
            s = data[b].get(l)
            if s is None:
                y += BAR + GAP
                continue
            clamped = s > tks[-1]
            drawn = min(s, tks[-1])
            out.append(hbar(left, y, drawn * scale, series_color(mode, l), ink["surface"]))
            if clamped:
                # Off-scale bar: chevron + true value (never silently clip).
                tip = left + drawn * scale
                out.append(
                    f'<text x="{tip + 4:.1f}" y="{y + BAR - 4}" font-size="11" '
                    f'font-weight="600" fill="{ink["primary"]}">&#187; {fmt(s)}x</text>'
                )
            elif l == story:
                out.append(
                    f'<text x="{left + drawn * scale + 6:.1f}" y="{y + BAR - 4}" font-size="11" '
                    f'font-weight="600" fill="{ink["primary"]}">{fmt(s)}x</text>'
                )
            y += BAR + GAP
        y += GROUP_AIR
    out.append(
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{h - 36}" stroke="{ink["axis"]}" stroke-width="1"/>'
    )
    out.append("</svg>")
    return "\n".join(out), w, h


def chart_two_panel(title, entries, left_metric, right_metric, mode):
    """Two panels: throughput (left) and p99 latency (right), one bar per
    entity, value labels on every bar (few bars)."""
    ink = INK[mode]
    names = [n for n, _ in entries]
    panel_w, lab_w, val_w = 300, 150, 96
    top = 64
    h = top + len(names) * (BAR + 10) + 46
    w = 2 * (lab_w + panel_w + val_w) + 40

    out = svg_open(w, h, ink)
    out.append(
        f'<text x="20" y="28" font-size="15" font-weight="600" fill="{ink["primary"]}">{title}</text>'
    )

    def panel(x0, metric_key, header, unit, better):
        vals = {n: m[metric_key] for n, m in entries if m}
        vmax = max(vals.values())
        tks = ticks_for(vmax * 1.08, 4)
        scale = panel_w / tks[-1]
        parts = [
            f'<text x="{x0 + lab_w}" y="{top - 14}" font-size="12" fill="{ink["secondary"]}">'
            f"{header} <tspan fill=\"{ink['muted']}\" font-size=\"10.5\">({better})</tspan></text>"
        ]
        for t in tks:
            x = x0 + lab_w + t * scale
            parts.append(
                f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{h - 38}" stroke="{ink["grid"]}" stroke-width="1"/>'
            )
            parts.append(
                f'<text x="{x:.1f}" y="{h - 22}" font-size="10.5" fill="{ink["muted"]}" '
                f'text-anchor="middle">{fmt_tick(t)}</text>'
            )
        y = top + 6
        for n in names:
            v = vals.get(n)
            if v is None:
                y += BAR + 10
                continue
            parts.append(
                f'<text x="{x0 + lab_w - 10}" y="{y + BAR - 4}" font-size="12" fill="{ink["secondary"]}" '
                f'text-anchor="end">{n}</text>'
            )
            parts.append(hbar(x0 + lab_w, y, v * scale, series_color(mode, n), ink["surface"]))
            parts.append(
                f'<text x="{x0 + lab_w + v * scale + 6:.1f}" y="{y + BAR - 4}" font-size="11" '
                f'font-weight="600" fill="{ink["primary"]}">{fmt(v)} {unit}</text>'
            )
            y += BAR + 10
        parts.append(
            f'<line x1="{x0 + lab_w}" y1="{top}" x2="{x0 + lab_w}" y2="{h - 38}" '
            f'stroke="{ink["axis"]}" stroke-width="1"/>'
        )
        return parts

    out += panel(0, "thr", *left_metric)
    out += panel(lab_w + panel_w + val_w + 40, "p99", *right_metric)
    out.append("</svg>")
    return "\n".join(out), w, h


def build_two_panel(results_json, thr_key, thr_scale, title, thr_header, thr_unit, mode):
    raw = [(n, e) for n, e in results_json["results"].items() if e]
    use_ms = any(e["median_p99_us"] >= 1000 for _n, e in raw)
    div, unit = (1000.0, "ms") if use_ms else (1.0, "us")
    entries = [
        (n, {"thr": e[thr_key] / thr_scale, "p99": e["median_p99_us"] / div}) for n, e in raw
    ]
    entries.sort(key=lambda x: -x[1]["thr"])
    return chart_two_panel(
        title,
        entries,
        (thr_header, thr_unit, "higher is better"),
        ("p99 latency", unit, "lower is better"),
        mode,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sched")
    parser.add_argument("--echo")
    parser.add_argument("--echo-title", default="TCP echo — 1 KiB messages, 64 connections (loopback)")
    parser.add_argument("--http")
    parser.add_argument("--outdir", default="docs/assets")
    args = parser.parse_args()
    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    def emit(name, builder):
        for mode in ("light", "dark"):
            svg, _w, _h = builder(mode)
            suffix = "" if mode == "light" else "-dark"
            path = outdir / f"{name}{suffix}.svg"
            path.write_text(svg)
            print(f"wrote {path}")

    if args.sched:
        sched = json.loads(pathlib.Path(args.sched).read_text())
        emit("bench-sched", lambda m: chart_speedup(sched, m))
    if args.echo:
        echo = json.loads(pathlib.Path(args.echo).read_text())
        emit(
            "bench-echo",
            lambda m: build_two_panel(
                echo,
                "median_msgs_per_sec",
                1e3,
                args.echo_title,
                "throughput, K msgs/s",
                "K/s",
                m,
            ),
        )
    if args.http:
        http = json.loads(pathlib.Path(args.http).read_text())
        emit(
            "bench-http",
            lambda m: build_two_panel(
                http,
                "median_rps",
                1e3,
                "HTTP/1.1 plaintext — 64 keep-alive connections (loopback)",
                "requests/s, K",
                "K/s",
                m,
            ),
        )


if __name__ == "__main__":
    main()
