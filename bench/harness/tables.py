"""Comparison tables as light/dark SVG pairs, the best cell highlighted.

GitHub and PyPI strip `style` and `bgcolor` from README HTML, so a text
table cannot carry a cell fill on either. These tables are images instead,
drawn in the same ink as the charts (charts.py), with the best value of
each row (or column) bold on a light teal fill.

A table spec is plain data, so the same JSON drives the README image and
can be checked by eye:

    {
      "title": "...", "subtitle": "...",
      "columns": ["workload", "cadeloop", "uvicorn + uvloop", ...],
      "rows": [
        {"label": "plaintext", "cells": [{"text": "78.0 K", "sub": "819 µs", "best": true}, ...]},
        ...
      ],
      "notes": ["optional footnote lines"]
    }

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

# The highlight: a light teal fill with dark ink in both themes, so the
# best cell reads the same on GitHub's light and dark pages.
BEST = {
    "light": {"fill": "#c9efe8", "stroke": "#8fd8cb", "text": "#03332d", "sub": "#24665c"},
    "dark": {"fill": "#a5e6da", "stroke": "#c6f2ea", "text": "#02261f", "sub": "#1d5a50"},
}

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
    w = text_width(cell.get("text", ""), CELL_PX, bold=True)
    if cell.get("sub"):
        w += 5 + text_width(cell["sub"], SMALL_PX)
    return w


def render(spec: dict, mode: str) -> str:
    ink = INK[mode]
    best = BEST[mode]
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
            if cell.get("best"):
                cw = cell_width(cell) + 16
                out.append(f'<rect x="{right - cw + 8:.1f}" y="{y + 4:.1f}" width="{cw:.1f}" '
                           f'height="{ROW_H - 8}" '
                           f'rx="6" fill="{best["fill"]}" stroke="{best["stroke"]}" stroke-width="1"/>')
                main_fill, sub_fill, main_weight = best["text"], best["sub"], ' font-weight="700"'
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
    """The table as one line of text, for the image's alt attribute."""
    cols = spec["columns"][1:]
    rows = []
    for r in spec["rows"]:
        cells = []
        for name, c in zip(cols, r["cells"], strict=True):
            v = c.get("text", "")
            if c.get("sub"):
                v += f" ({c['sub']})"
            if c.get("best"):
                v += " best"
            cells.append(f"{name} {v}")
        rows.append(f"{r['label']}: " + ", ".join(cells))
    return f"{spec.get('title', '')}. " + "; ".join(rows) + "."


def write(spec: dict, path_base: str) -> None:
    for mode in ("light", "dark"):
        suffix = "" if mode == "light" else "-dark"
        with open(f"{path_base}{suffix}.svg", "w", encoding="utf-8") as f:
            f.write(render(spec, mode))


def mark_best(rows: list[dict], higher_is_better: bool | list[bool], values: list[list[float | None]],
              axis: str = "row") -> None:
    """Set `best` on the winning cell of every row (axis="row") or every
    column (axis="col"). `values` parallels the cells; None never wins.
    Ties all win, unless most of the row or column ties: then nothing is
    marked. `higher_is_better` may be one flag, or one per row/column."""
    if axis == "row":
        for ri, r in enumerate(rows):
            vals = values[ri]
            hib = higher_is_better[ri] if isinstance(higher_is_better, list) else higher_is_better
            present = [v for v in vals if v is not None]
            if len(set(present)) < 2:
                continue  # nothing to pick between
            target = max(present) if hib else min(present)
            if present.count(target) * 2 > len(present):
                continue  # most of them tie: no one stands out
            for ci, v in enumerate(vals):
                if v is not None and v == target:
                    r["cells"][ci]["best"] = True
    else:
        ncol = len(values[0])
        for ci in range(ncol):
            col = [values[ri][ci] for ri in range(len(rows))]
            hib = higher_is_better[ci] if isinstance(higher_is_better, list) else higher_is_better
            present = [v for v in col if v is not None]
            if len(set(present)) < 2:
                continue  # nothing to pick between
            target = max(present) if hib else min(present)
            if present.count(target) * 2 > len(present):
                continue  # most of them tie: no one stands out
            for ri, v in enumerate(col):
                if v is not None and v == target:
                    rows[ri]["cells"][ci]["best"] = True


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
