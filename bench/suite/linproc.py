"""Process and TCP accounting for the benchmark runner on Linux (/proc only).

The Linux twin of winproc.py, with the same interface: run_suite.py
imports whichever matches the platform. Nothing here needs root.

What each sample field means here, next to its Windows counterpart:

  cpu_user, cpu_kernel  utime and stime of every process in the tree
                        (/proc/<pid>/stat), in seconds
  private               private resident memory: Private_Clean +
                        Private_Dirty from /proc/<pid>/smaps_rollup (USS).
                        Windows reports private commit, which also counts
                        committed-but-untouched pages; USS does not.
  ws                    resident set size (VmRSS), the working set
  handles               open file descriptors (sockets included)
  threads               threads in the tree
  ctx_switches          voluntary + involuntary switches of every thread
"""

from __future__ import annotations

import os
import signal
import time

_TICK = os.sysconf("SC_CLK_TCK")


def _read(path: str) -> str:
    with open(path) as f:
        return f.read()


def _stat_fields(pid: int) -> list[str]:
    """Fields of /proc/<pid>/stat after the command name (which may itself
    contain spaces and parentheses, so split after the LAST ')')."""
    raw = _read(f"/proc/{pid}/stat")
    return raw.rsplit(")", 1)[1].split()


def tree(root: int) -> list[int]:
    """root and all its descendants."""
    kids: dict[int, list[int]] = {}
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            ppid = int(_stat_fields(int(name))[1])
        except (OSError, IndexError, ValueError):
            continue
        kids.setdefault(ppid, []).append(int(name))
    out, stack = [], [root]
    while stack:
        p = stack.pop()
        out.append(p)
        stack.extend(k for k in kids.get(p, []) if k != p)
    return out


def _status(pid: int) -> dict[str, str]:
    out = {}
    for line in _read(f"/proc/{pid}/status").splitlines():
        key, _, value = line.partition(":")
        out[key] = value.strip()
    return out


def _proc_sample(pid: int) -> dict | None:
    try:
        f = _stat_fields(pid)
        st = _status(pid)
        rollup = _read(f"/proc/{pid}/smaps_rollup")
        nfd = len(os.listdir(f"/proc/{pid}/fd"))
        switches = 0
        for tid in os.listdir(f"/proc/{pid}/task"):
            try:
                ts = _status_task(pid, tid)
            except OSError:
                continue
            switches += ts
    except (OSError, IndexError, ValueError):
        return None  # exited between the listing and the read
    private = 0
    for line in rollup.splitlines():
        if line.startswith(("Private_Clean:", "Private_Dirty:")):
            private += int(line.split()[1]) * 1024
    return {
        # stat fields after the name: state=0, ppid=1, ... utime=11, stime=12
        "cpu_user": int(f[11]) / _TICK,
        "cpu_kernel": int(f[12]) / _TICK,
        "private": private,
        "ws": int(st.get("VmRSS", "0 kB").split()[0]) * 1024,
        "handles": nfd,
        "threads": int(st.get("Threads", "0")),
        "ctx_switches": switches,
    }


def _status_task(pid: int, tid: str) -> int:
    n = 0
    for line in _read(f"/proc/{pid}/task/{tid}/status").splitlines():
        if line.startswith(("voluntary_ctxt_switches:", "nonvoluntary_ctxt_switches:")):
            n += int(line.split()[1])
    return n


def tcp_stats() -> dict:
    """The system's TCP counters (/proc/net/snmp), under winproc's names."""
    lines = [ln.split() for ln in _read("/proc/net/snmp").splitlines() if ln.startswith("Tcp:")]
    names, values = lines[0][1:], lines[1][1:]
    out = {n: int(v) for n, v in zip(names, values, strict=True)}
    # Windows' NumConns counts every TCP endpoint in the table, listeners
    # and TIME_WAIT included; count the same from /proc/net/tcp{,6}.
    num = 0
    for path in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            num += max(0, len(_read(path).splitlines()) - 1)
        except OSError:
            pass
    out["NumConns"] = num
    return out


class TreeSampler:
    """Samples a process tree; values are summed over its live members.
    CPU time of members that exited is kept, so the totals never go
    backwards when a worker process ends mid-run."""

    def __init__(self, root: int):
        self.root = root
        self.last: dict[int, dict] = {}
        self.dead_cpu = [0.0, 0.0]

    def sample(self) -> dict:
        live = {}
        for pid in tree(self.root):
            s = _proc_sample(pid)
            if s is not None:
                live[pid] = s
        for pid, prev in self.last.items():
            if pid not in live:
                self.dead_cpu[0] += prev["cpu_user"]
                self.dead_cpu[1] += prev["cpu_kernel"]
        self.last = live
        total = {k: sum(s[k] for s in live.values()) for k in
                 ("cpu_user", "cpu_kernel", "private", "ws", "handles", "threads", "ctx_switches")}
        total["cpu_user"] += self.dead_cpu[0]
        total["cpu_kernel"] += self.dead_cpu[1]
        return {"t": time.perf_counter(), **total, "nprocs": len(live)}

    def close(self):
        self.last.clear()


def kill_tree(pid: int) -> None:
    """SIGKILL a process tree WE started, leaves first."""
    for p in reversed(tree(pid)):
        try:
            os.kill(p, signal.SIGKILL)
        except ProcessLookupError:
            pass


if __name__ == "__main__":
    me = os.getpid()
    print(TreeSampler(me).sample())
    print(tcp_stats())
