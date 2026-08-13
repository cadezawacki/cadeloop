#!/usr/bin/env python3
"""Process-level watchdog: run a command, kill its whole tree on timeout.

    python run_with_timeout.py SECONDS -- cmd arg1 arg2 ...

Exists because in-process timeouts (pytest-timeout's thread method) need
the GIL — a native-code wedge that HOLDS the GIL stalls them forever.
This supervises from outside the wedged process: on timeout the entire
tree is killed (taskkill /T /F on Windows, killpg elsewhere) and exit
code 124 is returned, so an orchestrated run always makes progress.
"""

import os
import subprocess
import sys


def main() -> int:
    if len(sys.argv) < 4 or sys.argv[2] != "--":
        print("usage: run_with_timeout.py SECONDS -- cmd args...", file=sys.stderr)
        return 2
    timeout = float(sys.argv[1])
    cmd = sys.argv[3:]
    kwargs = {}
    if sys.platform != "win32":
        kwargs["start_new_session"] = True
    proc = subprocess.Popen(cmd, **kwargs)
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        print(
            f"\n*** WATCHDOG: no exit after {timeout:.0f}s — killing tree: {' '.join(cmd)} ***",
            flush=True,
        )
        if sys.platform == "win32":
            subprocess.run(
                ["taskkill", "/PID", str(proc.pid), "/T", "/F"], capture_output=True
            )
        else:
            import signal

            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        proc.wait()
        return 124


if __name__ == "__main__":
    sys.exit(main())
