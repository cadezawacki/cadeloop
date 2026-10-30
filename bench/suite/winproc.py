"""Process and TCP accounting for the Windows benchmark runner (ctypes only).

Nothing here needs elevation: per-process times and memory come from the
process's own handle, per-thread context switches from one
NtQuerySystemInformation(SystemProcessInformation) snapshot, and TCP
counters from GetTcpStatisticsEx.

A venv's python.exe on Windows is a launcher that runs the base
interpreter as a CHILD process. Measuring the Popen handle measures the
launcher, so every sampler here works on a process TREE rooted at the
launcher.
"""

from __future__ import annotations

import ctypes
import subprocess
import time
from ctypes import wintypes

k32 = ctypes.WinDLL("kernel32", use_last_error=True)
psapi = ctypes.WinDLL("psapi", use_last_error=True)
ntdll = ctypes.WinDLL("ntdll")
iphlp = ctypes.WinDLL("iphlpapi")

PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
PROCESS_VM_READ = 0x0010
TH32CS_SNAPPROCESS = 0x2


class PROCESSENTRY32W(ctypes.Structure):
    _fields_ = [
        ("dwSize", wintypes.DWORD),
        ("cntUsage", wintypes.DWORD),
        ("th32ProcessID", wintypes.DWORD),
        ("th32DefaultHeapID", ctypes.c_size_t),
        ("th32ModuleID", wintypes.DWORD),
        ("cntThreads", wintypes.DWORD),
        ("th32ParentProcessID", wintypes.DWORD),
        ("pcPriClassBase", ctypes.c_long),
        ("dwFlags", wintypes.DWORD),
        ("szExeFile", ctypes.c_wchar * 260),
    ]


class PROCESS_MEMORY_COUNTERS_EX(ctypes.Structure):
    _fields_ = [
        ("cb", wintypes.DWORD),
        ("PageFaultCount", wintypes.DWORD),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
        ("PrivateUsage", ctypes.c_size_t),
    ]


class MIB_TCPSTATS(ctypes.Structure):
    _fields_ = [(n, wintypes.DWORD) for n in (
        "RtoAlgorithm", "RtoMin", "RtoMax", "MaxConn", "ActiveOpens", "PassiveOpens", "AttemptFails",
        "EstabResets", "CurrEstab", "InSegs", "OutSegs", "RetransSegs", "InErrs", "OutRsts", "NumConns")]


k32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
k32.OpenProcess.restype = wintypes.HANDLE


def snapshot_processes() -> list[tuple[int, int, str]]:
    """(pid, parent pid, exe) for every process."""
    snap = k32.CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0)
    out = []
    try:
        e = PROCESSENTRY32W()
        e.dwSize = ctypes.sizeof(e)
        ok = k32.Process32FirstW(snap, ctypes.byref(e))
        while ok:
            out.append((e.th32ProcessID, e.th32ParentProcessID, e.szExeFile))
            ok = k32.Process32NextW(snap, ctypes.byref(e))
    finally:
        k32.CloseHandle(snap)
    return out


def tree(root: int) -> list[int]:
    """root and all its descendants (pid reuse is guarded by the caller
    only ever asking about live processes it started)."""
    procs = snapshot_processes()
    kids: dict[int, list[int]] = {}
    for pid, ppid, _ in procs:
        kids.setdefault(ppid, []).append(pid)
    out, stack = [], [root]
    while stack:
        p = stack.pop()
        out.append(p)
        stack.extend(k for k in kids.get(p, []) if k != p)
    return out


class Proc:
    """An open handle to one process for cheap repeated sampling."""

    def __init__(self, pid: int):
        self.pid = pid
        self.h = k32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_VM_READ, False, pid)
        if not self.h:
            self.h = k32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)

    def close(self):
        if self.h:
            k32.CloseHandle(self.h)
            self.h = None

    def cpu(self) -> tuple[float, float]:
        c, e, k, u = (wintypes.FILETIME() for _ in range(4))
        if not self.h or not k32.GetProcessTimes(self.h, ctypes.byref(c), ctypes.byref(e), ctypes.byref(k),
                                                  ctypes.byref(u)):
            return 0.0, 0.0
        f = lambda t: (t.dwHighDateTime << 32 | t.dwLowDateTime) / 1e7  # noqa: E731
        return f(u), f(k)

    def mem(self) -> dict:
        m = PROCESS_MEMORY_COUNTERS_EX()
        m.cb = ctypes.sizeof(m)
        if not self.h or not psapi.GetProcessMemoryInfo(self.h, ctypes.byref(m), m.cb):
            return {}
        return {"ws": m.WorkingSetSize, "peak_ws": m.PeakWorkingSetSize, "private": m.PrivateUsage}

    def handles(self) -> int:
        n = wintypes.DWORD()
        if self.h and k32.GetProcessHandleCount(self.h, ctypes.byref(n)):
            return n.value
        return 0


# --- per-thread accounting: SystemProcessInformation (x64 layouts) --------
_SPI = 5
_PROC_HDR = 0x100  # sizeof(SYSTEM_PROCESS_INFORMATION) on x64
_THREAD = 0x50  # sizeof(SYSTEM_THREAD_INFORMATION) on x64
_buf = ctypes.create_string_buffer(1 << 21)


def thread_stats(pids: set[int]) -> dict[int, dict]:
    """{pid: {threads, ctx_switches}} from one system snapshot."""
    global _buf
    need = wintypes.ULONG()
    while True:
        st = ntdll.NtQuerySystemInformation(_SPI, _buf, len(_buf), ctypes.byref(need))
        if st == 0:
            break
        if st & 0xFFFFFFFF == 0xC0000004:  # STATUS_INFO_LENGTH_MISMATCH
            _buf = ctypes.create_string_buffer(max(need.value, len(_buf)) * 2)
            continue
        return {}
    raw = _buf.raw
    out: dict[int, dict] = {}
    off = 0
    while True:
        nxt = int.from_bytes(raw[off:off + 4], "little")
        nthreads = int.from_bytes(raw[off + 4:off + 8], "little")
        pid = int.from_bytes(raw[off + 80:off + 88], "little")
        if pid in pids:
            cs = 0
            for t in range(nthreads):
                base = off + _PROC_HDR + t * _THREAD
                cs += int.from_bytes(raw[base + 64:base + 68], "little")
            out[pid] = {"threads": nthreads, "ctx_switches": cs}
        if nxt == 0:
            break
        off += nxt
    return out


def tcp_stats() -> dict:
    s = MIB_TCPSTATS()
    iphlp.GetTcpStatisticsEx(ctypes.byref(s), 2)  # AF_INET
    return {n: getattr(s, n) for n, _ in s._fields_}


class TreeSampler:
    """Samples a process tree; values are summed over its live members."""

    def __init__(self, root: int):
        self.root = root
        self.procs: dict[int, Proc] = {}
        self.dead_cpu = [0.0, 0.0]

    def refresh(self):
        live = set(tree(self.root))
        for pid in list(self.procs):
            if pid not in live:
                self.procs.pop(pid).close()
        for pid in live:
            if pid not in self.procs:
                self.procs[pid] = Proc(pid)

    def sample(self) -> dict:
        self.refresh()
        user = kern = 0.0
        ws = private = handles = 0
        for p in self.procs.values():
            u, k = p.cpu()
            user += u
            kern += k
            m = p.mem()
            ws += m.get("ws", 0)
            private += m.get("private", 0)
            handles += p.handles()
        ts = thread_stats(set(self.procs))
        return {
            "t": time.perf_counter(),
            "cpu_user": user,
            "cpu_kernel": kern,
            "ws": ws,
            "private": private,
            "handles": handles,
            "threads": sum(v["threads"] for v in ts.values()),
            "ctx_switches": sum(v["ctx_switches"] for v in ts.values()),
            "nprocs": len(self.procs),
        }

    def close(self):
        for p in self.procs.values():
            p.close()
        self.procs.clear()


def kill_tree(pid: int) -> None:
    """Terminate a process tree WE started (taskkill /T on its root)."""
    subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True)


if __name__ == "__main__":
    import os

    me = os.getpid()
    s = TreeSampler(me)
    print(s.sample())
    print(thread_stats({me}))
    print(tcp_stats())
