"""Comparison tables as light/dark SVG pairs, the top three marked.

GitHub and PyPI strip `style` and `bgcolor` from README HTML, so a text
table cannot carry a cell fill on either. These tables are images instead,
drawn in the same ink as the charts (charts.py), with the three best values
of each row (or column) on gold, silver and bronze badges. Each badge also
carries its place as a number, so the ranking never rests on color alone.

A table spec is plain data, so the same JSON drives the README image and
can be checked by eye:

    {
      "title": "...", "subtitle": "...",
      "columns": ["workload", "cadeloop", "uvicorn + uvloop", ...],
      "rows": [
        {"label": "plaintext", "cells": [{"text": "78.0 K", "sub": "819 µs", "rank": 1}, ...]},
        ...
      ],
      "notes": ["optional footnote lines"]
    }

`rank` (1, 2 or 3) puts a cell on its medal; mark_ranks() sets it.
`label_code: true` on a row sets its label in monospace (benchmark names).

  python bench/harness/tables.py SPEC.json [SPEC.json ...] --outdir docs/assets
writes <outdir>/<spec file stem>.svg and <stem>-dark.svg.
"""

from __future__ import annotations

import argparse
import html
import json
import os

from charts import FONT, INK

MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"

# Medal badges for 1st, 2nd and 3rd: muted gold, silver and bronze tints,
# each with a stronger disc that carries the place number. Chosen in OKLCH;
# the three discs stay apart (OKLab ΔE ≥ 15) for normal vision and under
# simulated protan/deutan vision, and every text clears 5:1 on its fill.
# The pale fills alone are close by design; the numbered disc is the cue.
MEDALS = {
    "light": {
        1: {"fill": "#fbf0cf", "stroke": "#e3cfa1", "disc": "#eab53b", "digit": "#311d03",
            "text": "#3f2903", "sub": "#72552b"},
        2: {"fill": "#eceff2", "stroke": "#c7cbd0", "disc": "#afb5bc", "digit": "#171b1f",
            "text": "#25292e", "sub": "#575b60"},
        3: {"fill": "#fce7d6", "stroke": "#e0bca2", "disc": "#c6764d", "digit": "#2a140c",
            "text": "#4f2611", "sub": "#834e36"},
    },
    "dark": {
        1: {"fill": "#3a2f16", "stroke": "#6c582d", "disc": "#ecbd4a", "digit": "#2f1e02",
            "text": "#f6e7bb", "sub": "#cdbd92"},
        2: {"fill": "#2c2e31", "stroke": "#54595e", "disc": "#b2b8bf", "digit": "#171b1f",
            "text": "#e5e8ec", "sub": "#bbbec1"},
        3: {"fill": "#3a2619", "stroke": "#6f4c34", "disc": "#d2865b", "digit": "#2a140c",
            "text": "#f7d7c2", "sub": "#d5b39d"},
    },
}
PLACES = {1: "1st", 2: "2nd", 3: "3rd"}
DISC_R, DISC_GAP = 6.5, 5  # the numbered disc at a badge's left, and the space after it

TITLE_PX, SUB_PX, HEAD_PX, CELL_PX, NOTE_PX, SMALL_PX = 16, 12, 12, 13, 11, 11
ROW_H, HEAD_H, PAD_X, CELL_PAD = 30, 30, 24, 14


def text_width(s: str, px: float, bold: bool = False, mono: bool = False) -> float:
    """A conservative width estimate for system-ui / monospace text."""
    per = 0.60 if mono else 0.56
    w = 0.0
    for ch in s:
        if ch in "il.,:;|!'()[] ":
            w += px * 0.30
        elif ch in "mwMW%@":
            w += px * 0.85
        elif ch.isupper() or ch in "µ×–—":
            w += px * 0.66
        else:
            w += px * per
    return w * (1.06 if bold else 1.0)


def esc(s: str) -> str:
    return html.escape(s, quote=False)


def cell_width(cell: dict) -> float:
    """The cell's text, and on a badge the numbered disc beside it."""
    w = text_width(cell.get("text", ""), CELL_PX, bold=True)
    if cell.get("sub"):
        w += 5 + text_width(cell["sub"], SMALL_PX)
    if cell.get("rank"):
        w += 2 * DISC_R + DISC_GAP
    return w


def render(spec: dict, mode: str) -> str:
    ink = INK[mode]
    medals = MEDALS[mode]
    cols = spec["columns"]
    rows = spec["rows"]
    ncol = len(cols)
    widths = []
    for i, name in enumerate(cols):
        w = text_width(name, HEAD_PX, bold=True)
        for r in rows:
            if i == 0:
                w = max(w, text_width(r["label"], CELL_PX, bold=r.get("label_bold", False),
                                      mono=r.get("label_code", False)))
            else:
                w = max(w, cell_width(r["cells"][i - 1]))
        widths.append(w + 2 * CELL_PAD)
    table_w = sum(widths)
    title_w = max(text_width(spec.get("title", ""), TITLE_PX, bold=True),
                  text_width(spec.get("subtitle", ""), SUB_PX),
                  max((text_width(n, NOTE_PX) for n in spec.get("notes", [])), default=0))
    width = max(table_w, title_w) + 2 * PAD_X
    # Spread spare width over the value columns so the table fills the card.
    spare = width - 2 * PAD_X - table_w
    if spare > 0 and ncol > 1:
        for i in range(1, ncol):
            widths[i] += spare / (ncol - 1)
    top = 24
    y = top
    out = []
    if spec.get("title"):
        y += 18
        out.append(f'<text x="{PAD_X}" y="{y}" font-size="{TITLE_PX}" font-weight="600" '
                   f'fill="{ink["primary"]}">{esc(spec["title"])}</text>')
    if spec.get("subtitle"):
        y += 20
        out.append(f'<text x="{PAD_X}" y="{y}" font-size="{SUB_PX}" fill="{ink["secondary"]}">'
                   f'{esc(spec["subtitle"])}</text>')
    y += 14
    # Header row.
    x = PAD_X
    head_base = y + HEAD_H / 2 + 4
    for i, name in enumerate(cols):
        if i == 0:
            out.append(f'<text x="{x + CELL_PAD:.1f}" y="{head_base:.1f}" font-size="{HEAD_PX}" '
                       f'font-weight="600" '
                       f'fill="{ink["secondary"]}">{esc(name)}</text>')
        else:
            out.append(f'<text x="{x + widths[i] - CELL_PAD:.1f}" y="{head_base:.1f}" font-size="{HEAD_PX}" '
                       f'font-weight="600" text-anchor="end" fill="{ink["secondary"]}">{esc(name)}</text>')
        x += widths[i]
    y += HEAD_H
    out.append(f'<line x1="{PAD_X}" y1="{y:.1f}" x2="{PAD_X + sum(widths):.1f}" y2="{y:.1f}" '
               f'stroke="{ink["axis"]}" stroke-width="1"/>')
    for ri, r in enumerate(rows):
        base = y + ROW_H / 2 + 4.5
        x = PAD_X
        label_font = f' font-family="{MONO}"' if r.get("label_code") else ""
        weight = ' font-weight="600"' if r.get("label_bold") else ""
        out.append(f'<text x="{x + CELL_PAD:.1f}" y="{base:.1f}" font-size="{CELL_PX}"{label_font}{weight} '
                   f'fill="{ink["primary"]}">{esc(r["label"])}</text>')
        x += widths[0]
        for ci, cell in enumerate(r["cells"]):
            w = widths[ci + 1]
            right = x + w - CELL_PAD
            text = cell.get("text", "")
            sub = cell.get("sub")
            rank = cell.get("rank")
            if rank:
                m = medals[rank]
                cw = cell_width(cell) + 16
                left = right - cw + 8
                out.append(f'<rect x="{left:.1f}" y="{y + 4:.1f}" width="{cw:.1f}" height="{ROW_H - 8}" '
                           f'rx="6" fill="{m["fill"]}" stroke="{m["stroke"]}" stroke-width="1"/>')
                cx, cy = left + 6 + DISC_R, y + ROW_H / 2
                out.append(f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{DISC_R}" fill="{m["disc"]}"/>')
                out.append(f'<text x="{cx:.1f}" y="{cy + 3.4:.1f}" font-size="9.5" font-weight="700" '
                           f'text-anchor="middle" fill="{m["digit"]}">{rank}</text>')
                main_fill, sub_fill = m["text"], m["sub"]
                main_weight = ' font-weight="700"' if rank == 1 else ' font-weight="600"'
            else:
                main_fill, sub_fill, main_weight = ink["primary"], ink["muted"], ""
            if sub:
                out.append(f'<text x="{right:.1f}" y="{base:.1f}" font-size="{SMALL_PX}" text-anchor="end" '
                           f'fill="{sub_fill}">{esc(sub)}</text>')
                right -= text_width(sub, SMALL_PX) + 5
            out.append(f'<text x="{right:.1f}" y="{base:.1f}" font-size="{CELL_PX}" '
                       f'text-anchor="end"{main_weight} '
                       f'fill="{main_fill}">{esc(text)}</text>')
            x += w
        y += ROW_H
        if ri < len(rows) - 1:
            out.append(f'<line x1="{PAD_X}" y1="{y:.1f}" x2="{PAD_X + sum(widths):.1f}" y2="{y:.1f}" '
                       f'stroke="{ink["grid"]}" stroke-width="1"/>')
    for note in spec.get("notes", []):
        y += 18
        out.append(f'<text x="{PAD_X}" y="{y + 4:.1f}" font-size="{NOTE_PX}" fill="{ink["muted"]}">'
                   f'{esc(note)}</text>')
    height = y + 22
    head = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width:.0f} {height:.0f}" '
        f'width="{width:.0f}" height="{height:.0f}" font-family="{FONT}">',
        f'<title>{esc(spec.get("title", ""))}</title>',
        f'<rect width="{width:.0f}" height="{height:.0f}" fill="{ink["surface"]}" rx="8"/>',
    ]
    return "\n".join(head + out + ["</svg>"]) + "\n"


def alt_text(spec: dict) -> str:
    """The table as text, for the image's alt attribute: one sentence per
    row, cells separated by semicolons, places spelled out."""
    cols = spec["columns"][1:]
    rows = []
    for r in spec["rows"]:
        cells = []
        for name, c in zip(cols, r["cells"], strict=True):
            v = c.get("text", "")
            if c.get("sub"):
                v += f" ({c['sub']})"
            if c.get("rank"):
                v += f", {PLACES[c['rank']]}"
            cells.append(f"{name} {v}")
        rows.append(f"{r['label']}: " + "; ".join(cells))
    return f"{spec.get('title', '')}. " + ". ".join(rows) + "."


def write(spec: dict, path_base: str) -> None:
    for mode in ("light", "dark"):
        suffix = "" if mode == "light" else "-dark"
        with open(f"{path_base}{suffix}.svg", "w", encoding="utf-8") as f:
            f.write(render(spec, mode))


def mark_ranks(rows: list[dict], higher_is_better: bool | None | list[bool | None],
               values: list[list[float | None]], axis: str = "row") -> None:
    """Set `rank` 1, 2 or 3 on the three best cells of every row (axis="row")
    or every column (axis="col"). `values` parallels the cells; a None value
    is never ranked. `higher_is_better` is one flag, or one per row/column,
    and None there leaves that row/column unranked.

    Places work as medals do: equal values share a place and the next place
    is skipped (two firsts, then a third). The last place never gets a
    badge, so a row of two or three marks only its leaders, and when most
    of a row or column ties for first, nothing in it is marked."""
    lines = range(len(rows)) if axis == "row" else range(len(values[0]))
    for i in lines:
        hib = higher_is_better[i] if isinstance(higher_is_better, list) else higher_is_better
        if hib is None:
            continue
        if axis == "row":
            cells, vals = rows[i]["cells"], values[i]
        else:
            cells, vals = [r["cells"][i] for r in rows], [v[i] for v in values]
        present = [v for v in vals if v is not None]
        if len(set(present)) < 2:
            continue  # nothing to pick between
        best, worst = (max(present), min(present)) if hib else (min(present), max(present))
        if present.count(best) * 2 > len(present):
            continue  # most of them tie: no one stands out
        for cell, v in zip(cells, vals, strict=True):
            if v is None or v == worst:
                continue
            place = 1 + sum(1 for o in present if (o > v if hib else o < v))
            if place <= 3:
                cell["rank"] = place


RAW = "https://raw.githubusercontent.com/cadezawacki/cadeloop/main/"


def picture(name: str, spec: dict, asset_dir: str) -> str:
    """The README markup for one table: GitHub swaps the dark file in for
    dark-theme readers. Absolute URLs, so PyPI shows the images too."""
    base = RAW + asset_dir.strip("/") + "/" + name
    alt = html.escape(alt_text(spec), quote=True)
    return (f'<picture>\n  <source media="(prefers-color-scheme: dark)" srcset="{base}-dark.svg">\n'
            f'  <img alt="{alt}" src="{base}.svg">\n</picture>')


def fill_readme(path: str, specs: dict, asset_dir: str) -> None:
    """Replace each `<!-- table:NAME -->` marker, or the block between it
    and `<!-- /table -->`, with that table's picture."""
    import re

    with open(path, encoding="utf-8") as f:
        text = f.read()
    for name, spec in specs.items():
        block = f"<!-- table:{name} -->\n{picture(name, spec, asset_dir)}\n<!-- /table -->"
        # The block that follows a marker, if it was filled before: one
        # <picture>, then the end marker, never reaching another marker
        # (a bare marker must not swallow the text up to a later block).
        pattern = re.compile(rf"<!-- table:{re.escape(name)} -->"
                             r"(?:\n<picture>(?:(?!<!-- table:).)*?</picture>\n<!-- /table -->)?", re.S)
        text, n = pattern.subn(lambda _m, b=block: b, text)
        if n:
            print("filled", name, "in", path)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("specs", nargs="+")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--readme", help="also fill this README's <!-- table:NAME --> markers")
    a = ap.parse_args()
    os.makedirs(a.outdir, exist_ok=True)
    every = {}
    for path in a.specs:
        with open(path, encoding="utf-8") as f:
            specs = json.load(f)
        # One file may hold one spec, or a {name: spec} map of several.
        if "rows" in specs:
            specs = {os.path.splitext(os.path.basename(path))[0]: specs}
        for name, spec in specs.items():
            write(spec, os.path.join(a.outdir, name))
            print("wrote", os.path.join(a.outdir, name) + "{,-dark}.svg")
        every.update(specs)
    if a.readme:
        fill_readme(a.readme, every, os.path.relpath(a.outdir, os.path.dirname(os.path.abspath(a.readme))))


if __name__ == "__main__":
    main()
