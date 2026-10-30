#!/usr/bin/env python3
"""Draw the diagrams for docs/learn as light/dark SVG pairs.

  python docs/learn/diagrams.py            # writes docs/learn/img/*.svg

Every diagram is a function of the color mode, so the two versions never
drift apart. The pages embed each pair with <picture>, which GitHub
switches with the reader's theme.
"""

from __future__ import annotations

import html
import math
import os

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "img")
FONT = "system-ui, -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"
MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"

PALETTE = {
    "light": {
        "surface": "#fcfcfb", "ink": "#1f2328", "ink2": "#4b535d", "muted": "#7d8590",
        "line": "#c8ced6", "grid": "#e8ebee",
        "py": "#3a6fd8", "py_soft": "#dde7fb",
        "rust": "#13968e", "rust_soft": "#d3f1ec",
        "c": "#8250df", "c_soft": "#ebe3fb",
        "os": "#6e7781", "os_soft": "#eceff2",
        "warn": "#c4620c", "warn_soft": "#fbe6d2",
        "good": "#1a7f37", "good_soft": "#d9f3e1",
        "bad": "#cf222e", "bad_soft": "#fde1e3",
    },
    "dark": {
        "surface": "#0B0D11", "ink": "#e6edf3", "ink2": "#b1bac4", "muted": "#8b949e",
        "line": "#3b434d", "grid": "#1c2129",
        "py": "#79a5f2", "py_soft": "#172a4f",
        "rust": "#36c2b6", "rust_soft": "#0e3733",
        "c": "#b195f6", "c_soft": "#2a2148",
        "os": "#8b949e", "os_soft": "#232830",
        "warn": "#f0a050", "warn_soft": "#3d2711",
        "good": "#57d47a", "good_soft": "#13301d",
        "bad": "#ff8a82", "bad_soft": "#3d1719",
    },
}


def esc(s):
    return html.escape(str(s), quote=False)


class Canvas:
    def __init__(self, w, h, mode, title=None, subtitle=None):
        self.w, self.h, self.mode = w, h, mode
        self.p = PALETTE[mode]
        self.parts = []
        self.defs = []
        self.label = title or ""
        if title:
            self.text(24, 34, title, size=16, weight=600)
        if subtitle:
            self.text(24, 55, subtitle, size=12.5, fill=self.p["ink2"])

    def c(self, key):
        return self.p.get(key, key)

    def rect(self, x, y, w, h, fill="surface", stroke=None, rx=8, sw=1.2, dash=None, opacity=None):
        a = f' stroke="{self.c(stroke)}" stroke-width="{sw}"' if stroke else ""
        if dash:
            a += f' stroke-dasharray="{dash}"'
        if opacity is not None:
            a += f' opacity="{opacity}"'
        self.parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" rx="{rx}" '
                          f'fill="{self.c(fill)}"{a}/>')

    def hatch(self, x, y, w, h, color="line", rx=4, fill="os_soft"):
        """A box with diagonal hatching: time spent waiting."""
        pid = f"h{len(self.defs)}"
        col = self.c(color)
        self.defs.append(f'<pattern id="{pid}" width="7" height="7" patternUnits="userSpaceOnUse" '
                         f'patternTransform="rotate(45)"><rect width="7" height="7" fill="{self.c(fill)}"/>'
                         f'<line x1="0" y1="0" x2="0" y2="7" stroke="{col}" stroke-width="2.2"/></pattern>')
        self.parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" rx="{rx}" '
                          f'fill="url(#{pid})"/>')

    def text(self, x, y, s, size=13, fill="ink", anchor="start", weight=None, mono=False, italic=False):
        a = f' font-weight="{weight}"' if weight else ""
        if mono:
            a += f' font-family="{MONO}"'
        if italic:
            a += ' font-style="italic"'
        if anchor != "start":
            a += f' text-anchor="{anchor}"'
        self.parts.append(f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" fill="{self.c(fill)}"{a}>'
                          f'{esc(s)}</text>')

    def lines(self, x, y, rows, size=12, gap=15, **kw):
        for i, r in enumerate(rows):
            self.text(x, y + i * gap, r, size=size, **kw)

    def line(self, x1, y1, x2, y2, color="line", sw=1.2, dash=None):
        a = f' stroke-dasharray="{dash}"' if dash else ""
        self.parts.append(f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
                          f'stroke="{self.c(color)}" stroke-width="{sw}"{a}/>')

    def path(self, d, color="line", sw=1.6, fill="none", dash=None):
        a = f' stroke-dasharray="{dash}"' if dash else ""
        self.parts.append(f'<path d="{d}" fill="{self.c(fill) if fill != "none" else "none"}" '
                          f'stroke="{self.c(color)}" stroke-width="{sw}" stroke-linecap="round" '
                          f'stroke-linejoin="round"{a}/>')

    def head(self, x, y, angle, color="ink2", size=7):
        """Arrowhead with its tip at (x, y), pointing along `angle` (radians)."""
        a1, a2 = angle + math.radians(150), angle - math.radians(150)
        pts = [(x, y), (x + size * math.cos(a1), y + size * math.sin(a1)),
               (x + size * math.cos(a2), y + size * math.sin(a2))]
        self.parts.append('<path d="M{:.1f},{:.1f} L{:.1f},{:.1f} L{:.1f},{:.1f} z" fill="{}"/>'.format(
            *pts[0], *pts[1], *pts[2], self.c(color)))

    def arrow(self, x1, y1, x2, y2, color="ink2", sw=1.5, dash=None, size=7):
        ang = math.atan2(y2 - y1, x2 - x1)
        # Stop the shaft short of the tip so the head stays sharp.
        sx, sy = x2 - math.cos(ang) * size * 0.8, y2 - math.sin(ang) * size * 0.8
        self.line(x1, y1, sx, sy, color, sw, dash)
        self.head(x2, y2, ang, color, size)

    def curve(self, x1, y1, cx, cy, x2, y2, color="ink2", sw=1.6, size=7):
        """Quadratic curve with an arrowhead at the end."""
        ang = math.atan2(y2 - cy, x2 - cx)
        ex, ey = x2 - math.cos(ang) * size * 0.8, y2 - math.sin(ang) * size * 0.8
        self.path(f"M{x1:.1f},{y1:.1f} Q{cx:.1f},{cy:.1f} {ex:.1f},{ey:.1f}", color, sw)
        self.head(x2, y2, ang, color, size)

    def chip(self, x, y, label, fill, stroke, w=None, h=22, size=11.5, color="ink", rx=6, anchor="middle"):
        w = w or (len(label) * size * 0.58 + 18)
        self.rect(x, y, w, h, fill, stroke, rx=rx, sw=1)
        tx = x + w / 2 if anchor == "middle" else x + 9
        self.text(tx, y + h / 2 + size * 0.36, label, size=size, fill=color, anchor=anchor)
        return w

    def legend(self, x, y, items, size=11.5):
        """items: (kind, label) where kind is a color key or 'hatch'."""
        for kind, label in items:
            if kind == "hatch":
                self.hatch(x, y - 10, 14, 12)
            else:
                fill, stroke = kind if isinstance(kind, tuple) else (kind, None)
                self.rect(x, y - 10, 14, 12, fill, stroke, rx=3, sw=1)
            self.text(x + 20, y, label, size=size, fill="ink2")
            x += 20 + len(label) * size * 0.56 + 22
        return x

    def svg(self):
        body = "\n".join(self.parts)
        defs = f"<defs>{''.join(self.defs)}</defs>\n" if self.defs else ""
        return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.w} {self.h}" width="{self.w}" '
                f'height="{self.h}" font-family="{FONT}">\n<title>{esc(self.label)}</title>\n{defs}'
                f'<rect width="{self.w}" height="{self.h}" rx="10" fill="{self.p["surface"]}"/>\n'
                f'{body}\n</svg>\n')


# --------------------------------------------------------------------------- #
# 1. What an event loop does                                                  #
# --------------------------------------------------------------------------- #

def blocking_vs_loop(mode):
    c = Canvas(820, 380, mode, "Three requests, one thread",
               "Each request does a little work, waits 80 ms for a database, then does a little more.")
    x0, k = 96, 1.85  # px per ms

    def seg(t0, t1, y, kind, h=22, label=None):
        x, w = x0 + t0 * k, (t1 - t0) * k
        if kind == "work":
            c.rect(x, y, w, h, "py", rx=4)
        else:
            c.hatch(x, y, w, h)
        if label:
            c.text(x + w / 2, y + h / 2 + 4, label, size=11, anchor="middle",
                   fill="surface" if kind == "work" else "ink2", weight=600 if kind == "work" else None)

    c.text(24, 92, "Blocking code: one request at a time", size=13, weight=600)
    for i, name in enumerate("ABC"):
        t = i * 120
        seg(t, t + 20, 104, "work", label=name)
        seg(t + 20, t + 100, 104, "wait", label="waiting" if i == 0 else None)
        seg(t + 100, t + 120, 104, "work", label=name)
    c.line(x0 + 360 * k, 98, x0 + 360 * k, 132, "bad", sw=2)
    c.text(x0 + 360 * k - 6, 146, "all done at 360 ms", size=11.5, anchor="end", fill="bad", weight=600)

    c.text(24, 180, "Event loop: while one request waits, the thread runs another", size=13, weight=600)
    for i, name in enumerate("ABC"):
        y = 192 + i * 30
        c.text(78, y + 15, name, size=12, anchor="end", fill="ink2", weight=600)
        t = i * 20
        seg(t, t + 20, y, "work", label=name)
        seg(t + 20, t + 100, y, "wait")
        seg(t + 100, t + 120, y, "work", label=name)
    y = 290
    c.text(78, y + 13, "thread", size=11.5, anchor="end", fill="ink2")
    for t0, t1 in ((0, 60), (100, 160)):
        c.rect(x0 + t0 * k, y, (t1 - t0) * k, 18, "py", rx=4)
    c.rect(x0 + 60 * k, y, 40 * k, 18, "grid", rx=4)
    c.text(x0 + 80 * k, y + 13, "idle", size=10.5, anchor="middle", fill="muted")
    c.line(x0 + 160 * k, 186, x0 + 160 * k, 312, "good", sw=2)
    c.text(x0 + 160 * k + 8, 306, "all done at 160 ms", size=11.5, fill="good", weight=600)

    # Time axis.
    ya = 336
    c.line(x0, ya, x0 + 360 * k, ya, "line")
    for t in range(0, 361, 40):
        c.line(x0 + t * k, ya, x0 + t * k, ya + 4, "line")
        c.text(x0 + t * k, ya + 17, f"{t} ms", size=10.5, anchor="middle", fill="muted")
    c.legend(470, 366, [("py", "running Python code"), ("hatch", "waiting for I/O")])
    return c.svg()


def loop_cycle(mode):
    c = Canvas(820, 420, mode, "What the loop does, over and over",
               "One trip around is a tick. A busy server makes tens of thousands of ticks per second.")
    cx, cy, R = 410, 245, 135
    boxes = [
        (cx, cy - R, "1  Run the callbacks that are ready", "your coroutines resume here", "py"),
        (cx + R + 120, cy, "2  Ask the OS what happened", "and sleep, if nothing is ready", "os"),
        (cx, cy + R, "3  Turn finished I/O into callbacks", "data arrived, a send completed", "rust"),
        (cx - R - 120, cy, "4  Queue timers that came due", "asyncio.sleep(), call_later()", "rust"),
    ]
    bw, bh = 250, 58
    for x, y, t1, t2, kind in boxes:
        c.rect(x - bw / 2, y - bh / 2, bw, bh, f"{kind}_soft", kind, rx=10)
        c.text(x, y - 3, t1, size=13, anchor="middle", weight=600)
        c.text(x, y + 16, t2, size=11.5, anchor="middle", fill="ink2")
    # Curved arrows between the boxes, around the ring.
    r = 0.62
    ex = R + 120  # the side boxes' distance from the center
    c.curve(cx + bw / 2 - 20, cy - R + bh / 2, cx + ex - 10, cy - R * r - 10, cx + ex - 20, cy - bh / 2)
    c.curve(cx + ex - 20, cy + bh / 2, cx + ex - 10, cy + R * r + 10, cx + bw / 2 - 20, cy + R - bh / 2)
    c.curve(cx - bw / 2 + 20, cy + R - bh / 2, cx - ex + 10, cy + R * r + 10, cx - ex + 20, cy + bh / 2)
    c.curve(cx - ex + 20, cy - bh / 2, cx - ex + 10, cy - R * r - 10, cx - bw / 2 + 20, cy - R + bh / 2)
    c.text(cx, cy - 4, "one tick", size=14, anchor="middle", weight=600, fill="ink2")
    c.text(cx, cy + 15, "then the next", size=11.5, anchor="middle", fill="muted")
    return c.svg()


def await_sequence(mode):
    c = Canvas(820, 430, mode, "What happens at an await",
               "A coroutine never blocks the thread. It hands the waiting to the loop and steps aside.")
    cols = [(150, "your coroutine", "py"), (410, "event loop", "rust"), (670, "operating system", "os")]
    for x, name, kind in cols:
        c.rect(x - 85, 76, 170, 34, f"{kind}_soft", kind, rx=8)
        c.text(x, 98, name, size=13, anchor="middle", weight=600)
        c.line(x, 110, x, 410, "line", dash="4 4")

    def msg(y, x1, x2, label, sub=None, mono=False):
        c.arrow(x1, y, x2, y)
        mid = (x1 + x2) / 2
        c.text(mid, y - 8, label, size=12, anchor="middle", mono=mono)
        if sub:
            c.text(mid, y + 16, sub, size=11, anchor="middle", fill="muted")

    c.rect(144, 122, 12, 40, "py", rx=3)
    msg(152, 156, 404, "await reader.read()", "suspend me until data comes", mono=True)
    msg(200, 416, 664, "watch this socket for me")
    c.rect(404, 222, 12, 78, "rust", rx=3)
    c.rect(426, 230, 200, 56, "rust_soft", "rust", rx=8)
    c.lines(526, 254, ["runs other coroutines", "meanwhile"], size=11.5, anchor="middle", gap=15)
    msg(328, 664, 416, "data arrived")
    msg(372, 404, 156, "resume, here is the data")
    c.rect(144, 380, 12, 26, "py", rx=3)
    c.text(166, 398, "carries on after the await", size=11.5, fill="ink2")
    return c.svg()


# --------------------------------------------------------------------------- #
# 2. How the OS reports I/O                                                    #
# --------------------------------------------------------------------------- #

def readiness_vs_completion(mode):
    c = Canvas(860, 420, mode, "Two ways an operating system reports network I/O",
               "Linux says when you may read. Windows says when the read is done.")

    def panel(x, title, kind, steps, foot, kernel_note=None):
        c.rect(x, 74, 400, 326, "surface", "grid", rx=10)
        c.text(x + 20, 100, title, size=13.5, weight=600)
        px, kx = x + 80, x + 320
        for xx, name, k in ((px, "program", "py"), (kx, "kernel", "os")):
            c.rect(xx - 50, 114, 100, 28, f"{k}_soft", k, rx=7)
            c.text(xx, 132, name, size=12, anchor="middle", weight=600)
            c.line(xx, 142, xx, 352, "line", dash="4 4")
        for y, to_kernel, label in steps:
            if to_kernel:
                c.arrow(px + 4, y, kx - 4, y)
            else:
                c.arrow(kx - 4, y, px + 4, y, color=kind)
            c.text((px + kx) / 2, y - 7, label, size=11.5, anchor="middle")
        if kernel_note:
            y0, y1, label = kernel_note
            c.rect(kx - 8, y0, 16, y1 - y0, "os_soft", "os", rx=4)
            c.lines(kx - 16, (y0 + y1) / 2 - 4, label, size=10.5, anchor="end", fill="ink2", gap=13)
        c.text(x + 20, 382, foot, size=11.5, fill="ink2")

    panel(24, "Readiness · Linux epoll", "rust", [
        (172, True, "tell me when socket 7 is readable"),
        (218, False, "socket 7 is readable"),
        (264, True, "recv(socket 7)"),
        (310, False, "here are 512 bytes"),
    ], "Two round trips per read; the data is copied at the end.")
    panel(436, "Completion · Windows IOCP", "rust", [
        (172, True, "read socket 7 into this buffer"),
        (318, False, "done: 512 bytes in your buffer"),
    ], "One round trip, but the buffer must stay put until it's done.",
        kernel_note=(196, 296, ["fills the buffer", "when data arrives"]))
    return c.svg()


# --------------------------------------------------------------------------- #
# 3. Inside cadeloop                                                           #
# --------------------------------------------------------------------------- #

def layers(mode):
    c = Canvas(840, 470, mode, "cadeloop's layers",
               "Under your code there is a thin Python layer for the asyncio API; "
               "everything below it is Rust.")
    bands = [
        ("Your code", "asyncio programs, FastAPI, Starlette, aiohttp, uvicorn", "py"),
        ("cadeloop, Python layer", "Loop (the asyncio API) · Config · serve() · the CLI", "py"),
        ("Bindings and HTTP engine", "callbacks, transports, HTTP and WebSocket parsing, ASGI", "rust"),
        ("Reactor", "ready queue, timer heap, cross-thread queue, spin-then-park", "rust"),
        ("Backends", "IOCP on Windows · epoll on Linux · RIO (experimental)", "rust"),
        ("Operating system", "sockets, timers, the network stack", "os"),
    ]
    y, h, gap, x, w = 78, 50, 10, 24, 560
    for name, desc, kind in bands:
        c.rect(x, y, w, h, f"{kind}_soft", kind, rx=9)
        c.text(x + 18, y + 21, name, size=13.5, weight=600)
        c.text(x + 18, y + 39, desc, size=11.5, fill="ink2")
        lang = {"py": "Python", "rust": "Rust", "os": "kernel"}[kind]
        c.text(x + w - 16, y + 30, lang, size=11, anchor="end", fill=kind, weight=600)
        y += h + gap
    # GIL bracket: held for everything above the backends' wait.
    bx = x + w + 24
    top, mid = 78, 78 + 4 * (h + gap) + 6
    c.path(f"M{bx},{top} h10 V{mid - 8} h-10", "py", 2)
    c.lines(bx + 20, top + 120, ["Python's GIL is held", "while the loop runs", "callbacks and code"],
            size=11.5, fill="ink2", gap=15)
    c.rect(bx, mid + 2, 220, 52, "warn_soft", "warn", rx=8)
    c.lines(bx + 12, mid + 22, ["GIL released only here, while", "the backend waits for I/O"], size=11.5,
            gap=15)
    return c.svg()


def tick(mode):
    c = Canvas(880, 284, mode, "One tick, step by step",
               "The loop holds the GIL for the whole tick except the wait, "
               "so other Python threads run while it sleeps.")
    segs = [
        ("flush", ["send writes left", "from last tick"], 96, "rust"),
        ("prepare", ["work from other", "threads, timeout"], 104, "rust"),
        ("wait for I/O", ["spin a little,", "then sleep"], 156, "wait"),
        ("translate", ["completions", "become events"], 96, "rust"),
        ("callbacks", ["everything that", "was ready"], 104, "py"),
        ("I/O events", ["protocols and", "HTTP requests"], 104, "py"),
        ("timers", ["the ones", "that came due"], 76, "py"),
        ("flush", ["send this", "tick's writes"], 90, "rust"),
    ]
    x, y, h = 30, 130, 44
    gil = []
    for name, desc, w, kind in segs:
        if kind == "wait":
            c.hatch(x, y, w - 4, h, rx=6)
            c.text(x + (w - 4) / 2, y + 27, name, size=12.5, anchor="middle", weight=600)
        else:
            c.rect(x, y, w - 4, h, f"{kind}_soft", kind, rx=6)
            c.text(x + (w - 4) / 2, y + 27, name, size=12.5, anchor="middle", weight=600)
        c.lines(x + (w - 4) / 2, y + h + 22, desc, size=11, anchor="middle", fill="ink2", gap=14)
        gil.append((x, w - 4, kind != "wait"))
        x += w
    # GIL lane above.
    c.text(30, 104, "GIL", size=11.5, fill="ink2", weight=600)
    for gx, gw, held in gil:
        if held:
            c.rect(gx, 110, gw, 8, "py", rx=3)
        else:
            c.rect(gx, 110, gw, 8, "warn_soft", "warn", rx=3, sw=1)
    wait_x = gil[2][0]
    c.text(wait_x + gil[2][1] / 2, 101, "released", size=11, anchor="middle", fill="warn", weight=600)
    c.legend(30, 254, [("rust_soft", "cadeloop (Rust)"), ("py_soft", "your Python code runs here"),
                       ("hatch", "waiting")])
    return c.svg()


def spin_then_park(mode):
    c = Canvas(860, 400, mode, "Spin, then park",
               "Before it sleeps, the loop checks for new work for a few microseconds. "
               "Work that arrives then skips a sleep.")
    x0, k = 210, 6.0  # px per microsecond

    def row(y, title, note, items, note_color="ink2"):
        c.text(24, y + 16, title, size=12.5, weight=600)
        for kind, t0, t1, label in items:
            x, w = x0 + t0 * k, (t1 - t0) * k
            if kind == "sleep":
                c.hatch(x, y, w, 24, rx=4)
            elif kind == "spin":
                c.rect(x, y, w, 24, "rust_soft", "rust", rx=4, dash="3 3")
            elif kind == "work":
                c.rect(x, y, w, 24, "py", rx=4)
            elif kind == "wake":
                c.rect(x, y, w, 24, "warn_soft", "warn", rx=4)
            if label:
                c.text(x + w / 2, y + 16, label, size=11, anchor="middle",
                       fill="surface" if kind == "work" else "ink")
        c.text(x0 + 80 * k + 12, y + 16, note, size=11.5, fill=note_color)

    row(86, "work arrives mid-spin", "caught: no sleep",
        [("spin", 0, 12, "spin"), ("work", 12, 26, "handled")], note_color="good")
    c.arrow(x0 + 12 * k, 76, x0 + 12 * k, 86, "good", size=6)
    c.text(x0 + 12 * k, 72, "message arrives", size=10.5, anchor="middle", fill="good")
    row(140, "nothing arrives", "the spin bought nothing",
        [("spin", 0, 20, "spin 20 µs"), ("sleep", 20, 54, "asleep"), ("wake", 54, 64, "wake"),
         ("work", 64, 78, "handled")], note_color="warn")
    c.arrow(x0 + 52 * k, 130, x0 + 52 * k, 140, "ink2", size=6)
    c.text(x0 + 52 * k, 126, "message arrives", size=10.5, anchor="middle", fill="ink2")
    # Time axis for the two rows.
    c.line(x0, 182, x0 + 80 * k, 182, "grid")
    for t in range(0, 81, 10):
        c.text(x0 + t * k, 196, f"{t} µs", size=10, anchor="middle", fill="muted")

    # Adaptive spinning: credit over a run of waits.
    c.text(24, 236, "adaptive spinning", size=12.5, weight=600)
    c.lines(24, 254, ["spins only while they", "catch work"], size=11, fill="ink2", gap=14)
    events = "HHMHMMMMMMMMMMMMMMM....P.......P..HH"
    credit, xs, base = 12, x0, 344
    c.line(x0, base, x0 + len(events) * 15, base, "line")
    c.text(x0 - 10, base - 60 + 4, "credit", size=10.5, anchor="end", fill="muted")
    for i, e in enumerate(events):
        x = xs + i * 15
        if e == "H":
            credit = min(12, credit + 3)
            c.rect(x + 2, 222, 11, 11, "good", rx=5.5)
        elif e in "MP":
            credit = max(0, credit - 1)
            if e == "P":
                c.rect(x + 2, 222, 11, 11, "surface", "warn", rx=5.5, sw=1.6)
            else:
                c.rect(x + 2, 222, 11, 11, "surface", "muted", rx=5.5, sw=1.4)
        else:
            c.text(x + 7.5, 232, "·", size=14, anchor="middle", fill="muted")
        hgt = credit * 5
        if hgt:
            c.rect(x + 2, base - hgt, 11, hgt, "rust", rx=2)
    c.legend(x0, 384, [("good", "spin caught work (+3)"), (("surface", "muted"), "spin caught nothing (−1)"),
                       (("surface", "warn"), "probe while stopped"), ("rust", "credit")])
    c.text(x0 + 20 * 15, 262, "no credit left: no spinning,", size=11, fill="ink2")
    c.text(x0 + 20 * 15, 276, "except a probe every 8th wait", size=11, fill="ink2")
    return c.svg()


def corking(mode):
    c = Canvas(860, 320, mode, "Corking: one send per connection per tick",
               "Writes made during a tick wait in a queue and leave together when the tick ends.")

    def row(y, title, sends, note):
        c.text(24, y + 4, title, size=12.5, weight=600)
        for i in range(3):
            c.rect(230 + i * 70, y - 14, 60, 26, "py_soft", "py", rx=5)
            c.text(260 + i * 70, y + 4, f"write {i + 1}", size=11.5, anchor="middle")
        c.line(220, y + 36, 780, y + 36, "line")
        c.text(790, y + 40, "kernel", size=10.5, fill="muted")
        for x0, label in sends:
            c.arrow(x0, y + 14, x0, y + 34, "rust")
            c.text(x0 + 6, y + 30, label, size=10.5, fill="rust")
        c.text(460, y + 4, note, size=11.5, fill="ink2")

    row(98, "immediate flush", [(260, "send"), (330, "send"), (400, "send")], "three sends")
    c.rect(220, 176, 300, 0.1, "line")
    c.text(24, 194, "corked (the default)", size=12.5, weight=600)
    for i in range(3):
        c.rect(230 + i * 70, 176, 60, 26, "py_soft", "py", rx=5)
        c.text(260 + i * 70, 194, f"write {i + 1}", size=11.5, anchor="middle")
    c.rect(455, 172, 150, 34, "rust_soft", "rust", rx=6, dash="4 3")
    c.text(530, 194, "queued until tick ends", size=11, anchor="middle")
    c.arrow(612, 189, 650, 189, "ink2")
    c.rect(655, 176, 110, 26, "rust", rx=5)
    c.text(710, 194, "one writev", size=11.5, anchor="middle", fill="surface", weight=600)
    c.line(220, 232, 780, 232, "line")
    c.arrow(710, 204, 710, 230, "rust")
    c.text(790, 236, "kernel", size=10.5, fill="muted")
    c.lines(24, 270, ["A response sent as a single body message is one send either way. Corking pays off for",
                      "streamed chunks and pipelined responses, which it coalesces per connection."],
            size=11.5, fill="ink2", gap=16)
    return c.svg()


def timer_resolution(mode):
    c = Canvas(860, 280, mode, "Why a 1 ms sleep can take 15 ms on Windows",
               "A plain wait ends on the next clock interrupt, every 15.6 ms. "
               "cadeloop also arms a high-resolution timer.")
    x0, k = 190, 19.0  # px per ms
    for t in (0.0, 15.6, 31.2):
        x = x0 + t * k
        if x < 840:
            c.line(x, 76, x, 224, "line", dash="3 4")
            c.text(x, 242, f"clock tick ({t:g} ms)", size=10.5, anchor="middle", fill="muted")
    rows = [
        (104, "asyncio, winloop", 15.4, "wakes on the next tick: about 14 ms late", "bad"),
        (164, "cadeloop", 1.55, "wakes about 0.5 ms after the deadline", "good"),
    ]
    for y, name, end, note, color in rows:
        c.text(24, y + 17, name, size=12.5, weight=600)
        c.hatch(x0, y, end * k, 26, rx=4)
        c.line(x0 + 1 * k, y - 6, x0 + 1 * k, y + 32, "py", sw=2)
        c.rect(x0 + end * k - 3, y - 2, 6, 30, color, rx=2)
        c.text(x0 + end * k + 10, y + 17, note, size=11.5, fill=color, weight=600)
    c.text(x0 + 1 * k + 4, 96, "asleep until 1 ms", size=10.5, fill="py")
    c.text(24, 270, "Medians measured on Windows 11 with asyncio.sleep(0.001) on an idle loop.", size=11,
           fill="muted")
    return c.svg()


def buffer_pool(mode):
    c = Canvas(860, 300, mode, "Where received bytes go",
               "Reads land in slots carved from 2 MiB regions. "
               "A slot is reused, not freed, once its data has been used.")
    sizes = [("4 KiB slots", 16, "rust"), ("16 KiB slots", 8, "rust"), ("64 KiB slots", 4, "rust")]
    y = 92
    for name, n, _kind in sizes:
        c.text(24, y + 18, name, size=12.5, weight=600)
        w = 560 / n
        for i in range(n):
            state = "use" if i % 3 == 0 else ("free" if i % 3 == 1 else "empty")
            if state == "use":
                c.rect(150 + i * w, y, w - 4, 26, "rust", rx=4)
            elif state == "free":
                c.rect(150 + i * w, y, w - 4, 26, "rust_soft", "rust", rx=4)
            else:
                c.rect(150 + i * w, y, w - 4, 26, "surface", "line", rx=4, dash="3 3")
        y += 46
    c.legend(150, 248, [("rust", "holding data (a read, or a memoryview you kept)"),
                        (("rust_soft", "rust"), "free, kept warm")])
    c.legend(150, 272, [(("surface", "line"), "free beyond the warm set: handed back to the OS")])
    c.lines(730, 110, ["bigger reads move", "up a size class;", "small ones move", "back down"],
            size=11, fill="ink2", gap=15)
    return c.svg()


# --------------------------------------------------------------------------- #
# 4. The HTTP server                                                           #
# --------------------------------------------------------------------------- #

def request_path(mode):
    c = Canvas(900, 330, mode, "Where a request spends its time",
               "uvicorn does the HTTP work in Python (or C with httptools). "
               "cadeloop serve() leaves only your app in Python.")
    steps_a = [("read bytes", "loop", "c"), ("parse HTTP", "h11 or httptools", "py"),
               ("build scope", "Python", "py"), ("your app", "Python", "py"),
               ("encode reply", "Python", "py"), ("write", "loop", "c")]
    steps_b = [("receive", "pooled buffer", "rust"), ("parse HTTP", "llhttp", "rust"),
               ("build scope", "Rust", "rust"), ("your app", "Python", "py"),
               ("encode reply", "Rust", "rust"), ("write", "corked", "rust")]

    def row(y, title, steps, note):
        c.text(24, y - 14, title, size=13, weight=600)
        x = 24
        for i, (name, sub, kind) in enumerate(steps):
            c.rect(x, y, 124, 46, f"{kind}_soft", kind, rx=8)
            c.text(x + 62, y + 20, name, size=12.5, anchor="middle", weight=600)
            c.text(x + 62, y + 37, sub, size=10.5, anchor="middle", fill="ink2")
            if i < len(steps) - 1:
                c.arrow(x + 126, y + 23, x + 144, y + 23, "ink2", size=6)
            x += 146
        c.text(24, y + 68, note, size=11.5, fill="ink2")

    row(98, "uvicorn on asyncio or uvloop", steps_a,
        "With h11, Python runs four of the six steps for every request; httptools moves the parsing to C.")
    row(214, "cadeloop serve()", steps_b, "Python runs one step: your application.")
    c.legend(440, 312, [("py_soft", "Python"), ("c_soft", "C (libuv, selectors)"),
                        ("rust_soft", "cadeloop, Rust")])
    return c.svg()


def workers(mode):
    c = Canvas(900, 360, mode, "Running several worker processes",
               "One worker per CPU. How a new connection reaches a worker differs between Linux and Windows.")

    def box(x, y, w, h, lines_, kind, size=12):
        c.rect(x, y, w, h, f"{kind}_soft", kind, rx=8)
        c.lines(x + w / 2, y + 20, lines_, size=size, anchor="middle", gap=15)

    c.text(24, 92, "Linux: fork, one listener each", size=13, weight=600)
    box(120, 104, 240, 52, ["supervisor", "imports the app, then forks"], "os")
    for i in range(3):
        x = 40 + i * 140
        c.arrow(240, 158, x + 60, 196, "ink2", size=6)
        box(x, 198, 120, 52, [f"worker {i + 1}", "listens on :8000"], "rust", size=11.5)
        c.arrow(x + 60, 300, x + 60, 254, "rust", size=6)
    c.rect(40, 302, 400, 30, "os_soft", "os", rx=8)
    c.text(240, 322, "kernel spreads new connections (SO_REUSEPORT)", size=11.5, anchor="middle")

    c.text(484, 92, "Windows: one listener, connections handed over", size=13, weight=600)
    box(560, 104, 240, 52, ["supervisor", "owns :8000 and accepts"], "os")
    for i in range(3):
        x = 484 + i * 136
        c.arrow(680, 158, x + 60, 196, "rust", size=6)
        box(x, 198, 120, 52, [f"worker {i + 1}", "imports the app"], "rust", size=11.5)
    c.lines(680, 286, ["each accepted socket is duplicated into a worker",
                       "(WSADuplicateSocketW over a pipe)"], size=11.5, anchor="middle", fill="ink2", gap=15)
    return c.svg()


# --------------------------------------------------------------------------- #
# 5. How it compares                                                           #
# --------------------------------------------------------------------------- #

def stack_comparison(mode):
    cols = ["waiting for I/O", "scheduling", "HTTP parsing", "scope and reply", "your app"]
    rows = [
        ("uvicorn on asyncio", [("C", "c"), ("Python", "py"), ("Python (h11)", "py"), ("Python", "py"),
                                ("Python", "py")]),
        ("uvicorn[standard] on uvloop", [("C (libuv)", "c"), ("Cython", "c"), ("C (httptools)", "c"),
                                         ("Python", "py"), ("Python", "py")]),
        ("uvicorn on winloop", [("C (libuv)", "c"), ("Cython", "c"), ("Python (h11)", "py"), ("Python", "py"),
                                ("Python", "py")]),
        ("uvicorn on rloop / rsloop", [("Rust", "c"), ("Rust", "c"), ("Python (h11)", "py"), ("Python", "py"),
                                       ("Python", "py")]),
        ("uvicorn on cadeloop", [("Rust", "rust"), ("Rust", "rust"), ("Python (h11)", "py"), ("Python", "py"),
                                 ("Python", "py")]),
        ("granian", [("Rust (tokio)", "c"), ("Rust + uvloop", "c"), ("Rust (hyper)", "c"), ("Rust", "c"),
                     ("Python", "py")]),
        ("cadeloop serve()", [("Rust", "rust"), ("Rust", "rust"), ("C (llhttp)", "rust"), ("Rust", "rust"),
                              ("Python", "py")]),
    ]
    c = Canvas(920, 120 + len(rows) * 38 + 50, mode, "What runs in Python, and what doesn't",
               "One row per setup; the columns are jobs every request goes through.")
    x0, cw, y = 230, 134, 84
    for i, name in enumerate(cols):
        c.text(x0 + i * cw + cw / 2 - 2, y + 4, name, size=11.5, anchor="middle", fill="ink2", weight=600)
    y += 16
    for name, cells in rows:
        bold = 600 if name.startswith("cadeloop") or name.endswith("cadeloop") else None
        c.text(24, y + 20, name, size=12.5, weight=bold)
        for i, (label, kind) in enumerate(cells):
            c.rect(x0 + i * cw, y + 3, cw - 6, 28, f"{kind}_soft", kind, rx=6, sw=1)
            c.text(x0 + i * cw + (cw - 6) / 2, y + 22, label, size=11.5, anchor="middle")
        y += 38
    c.legend(x0, y + 26, [("py_soft", "Python"), ("c_soft", "compiled: C, Cython or Rust"),
                          ("rust_soft", "compiled: cadeloop")])
    return c.svg()


# --------------------------------------------------------------------------- #
# 6. Measuring                                                                  #
# --------------------------------------------------------------------------- #

def closed_vs_open(mode):
    c = Canvas(900, 380, mode, "Closed-loop and open-loop load",
               "If the load generator waits for each answer before sending the next, "
               "a server stall also stalls the measurement.")
    x0, k = 190, 0.28  # px per ms, 0..2000 ms

    def axis(y):
        c.line(x0, y, x0 + 2000 * k, y, "line")
        for t in range(0, 2001, 500):
            c.text(x0 + t * k, y + 15, f"{t} ms", size=10, anchor="middle", fill="muted")

    for y in (86, 216):
        c.rect(x0 + 500 * k, y, 1000 * k, 100, "bad_soft", rx=4, opacity=0.8)
        c.text(x0 + 1000 * k, y + 14, "server stalls for 1 s", size=10.5, anchor="middle", fill="bad")
    c.text(24, 112, "closed loop", size=12.5, weight=600)
    c.lines(24, 130, ["send, wait for the reply,", "then send the next"], size=11, fill="ink2", gap=14)
    # Closed loop: requests every 25 ms of a fast server, one stuck through the stall.
    for t in range(0, 500, 25):
        c.rect(x0 + t * k, 150, 3, 22, "py", rx=1)
    c.rect(x0 + 500 * k, 150, 1000 * k, 22, "warn", rx=3)
    c.text(x0 + 1000 * k, 166, "one request, 1000 ms", size=11, anchor="middle", fill="surface", weight=600)
    for t in range(1500, 2000, 25):
        c.rect(x0 + t * k, 150, 3, 22, "py", rx=1)
    axis(186)
    c.text(x0 + 2000 * k + 10, 166, "1 slow result", size=11.5, fill="warn", weight=600)

    c.text(24, 242, "open loop", size=12.5, weight=600)
    c.lines(24, 260, ["send on a fixed schedule,", "answer or not"], size=11, fill="ink2", gap=14)
    for t in range(0, 2000, 25):
        if 500 <= t < 1500:
            wait = 1500 - t
            c.rect(x0 + t * k, 282 - min(wait, 1000) * 0.03, 3, 2 + min(wait, 1000) * 0.03, "warn", rx=1)
        else:
            c.rect(x0 + t * k, 280, 3, 22, "py", rx=1)
    axis(316)
    c.text(x0 + 2000 * k + 10, 296, "40 slow results", size=11.5, fill="warn", weight=600)
    c.lines(24, 350, ["Latency is measured from when each request was due, so the stall shows up in p99.",
                      "bench/loadgen does this with -rate and -phases."], size=11.5, fill="ink2", gap=15)
    return c.svg()


def percentiles(mode):
    c = Canvas(860, 330, mode, "Why p99 matters more than the average",
               "Most requests are quick and a few are slow. "
               "The average sits between them and describes neither.")
    # An illustrative latency distribution: a main body plus a long tail.
    buckets = [2, 6, 18, 42, 70, 82, 68, 46, 28, 16, 10, 7, 5, 4, 3, 3, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1]
    x0, base, bw = 70, 250, 26
    peak = max(buckets)
    for i, n in enumerate(buckets):
        h = n / peak * 150
        c.rect(x0 + i * bw, base - h, bw - 4, h, "py_soft", "py", rx=3, sw=1)
    c.line(x0, base, x0 + len(buckets) * bw, base, "line")
    # The marks are computed from the drawn buckets, so the picture is
    # self-consistent (interpolated within a bucket).
    total = sum(buckets)

    def quantile(q):
        seen = 0
        for i, n in enumerate(buckets):
            if seen + n >= q * total:
                return i + (q * total - seen) / n
            seen += n
        return len(buckets)

    mean = sum((i + 0.5) * n for i, n in enumerate(buckets)) / total
    marks = [(quantile(0.5), "median", "good", 92), (mean, "mean", "ink2", 108),
             (quantile(0.99), "p99", "warn", 92), (quantile(0.999), "p99.9", "bad", 108)]
    for pos, label, color, ty in marks:
        x = x0 + pos * bw
        c.line(x, base, x, ty + 6, color, sw=2, dash="4 3" if label == "mean" else None)
        c.text(x, ty, label, size=12, anchor="middle", fill=color, weight=600)
    c.text(x0, base + 22, "fast", size=11, fill="muted")
    c.text(x0 + len(buckets) * bw, base + 22, "slow", size=11, fill="muted", anchor="end")
    c.text(x0 + len(buckets) * bw / 2, base + 22, "latency →", size=11, fill="muted", anchor="middle")
    c.lines(24, 300, ["Illustrative shape, not a measurement. p99 is the latency that 1 request in 100 "
                      "exceeds: at 10,000",
                      "requests per second, that is 100 slow answers every second."],
            size=11.5, fill="ink2", gap=15)
    return c.svg()


def noise(mode):
    c = Canvas(860, 326, mode, "Measure several times, and rotate the order",
               "Two runs of the same thing differ. Machine state drifts during a session, too.")
    runs = {"server A": [61.2, 66.4, 63.9], "server B": [64.8, 58.1, 62.5]}
    x0, k, lo_axis = 190, 26.0, 55  # px per K req/s from 55 K
    y = 100
    for name, vals in runs.items():
        c.text(24, y + 22, name, size=12.5, weight=600)
        lo, hi = min(vals), max(vals)
        c.rect(x0 + (lo - lo_axis) * k, y + 10, (hi - lo) * k, 16, "py_soft", rx=4)
        for v in vals:
            c.rect(x0 + (v - lo_axis) * k - 2, y + 6, 4, 24, "py", rx=2)
        med = sorted(vals)[1]
        c.rect(x0 + (med - lo_axis) * k - 3, y + 2, 6, 32, "ink", rx=2)
        c.text(x0 + (70 - lo_axis) * k + 16, y + 22, f"median {med:.1f} K, range {lo:.1f}–{hi:.1f} K",
               size=11.5, fill="ink2")
        y += 64
    c.line(x0, y + 8, x0 + (70 - lo_axis) * k, y + 8, "line")
    for v in range(lo_axis, 71, 5):
        c.text(x0 + (v - lo_axis) * k, y + 24, f"{v} K", size=10.5, anchor="middle", fill="muted")
    c.lines(24, y + 54, ["The ranges overlap, so with one run each, either server could come out ahead.",
                         "Report the median of several runs with the range next to it."], size=11.5,
            fill="ink2", gap=15)
    c.text(830, y + 70, "illustrative numbers", size=10.5, anchor="end", fill="muted", italic=True)
    return c.svg()


DIAGRAMS = {
    "blocking-vs-loop": blocking_vs_loop,
    "loop-cycle": loop_cycle,
    "await-sequence": await_sequence,
    "readiness-vs-completion": readiness_vs_completion,
    "layers": layers,
    "tick": tick,
    "spin-then-park": spin_then_park,
    "corking": corking,
    "timer-resolution": timer_resolution,
    "buffer-pool": buffer_pool,
    "request-path": request_path,
    "workers": workers,
    "stack-comparison": stack_comparison,
    "closed-vs-open": closed_vs_open,
    "percentiles": percentiles,
    "noise": noise,
}


def main():
    os.makedirs(OUT, exist_ok=True)
    for name, fn in DIAGRAMS.items():
        for mode in ("light", "dark"):
            suffix = "" if mode == "light" else "-dark"
            with open(os.path.join(OUT, f"{name}{suffix}.svg"), "w", encoding="utf-8") as f:
                f.write(fn(mode))
    print(f"wrote {len(DIAGRAMS) * 2} files to {OUT}")


if __name__ == "__main__":
    main()
