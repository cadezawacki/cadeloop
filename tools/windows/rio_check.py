"""Fast RIO availability gate for validate.ps1.

Exit 0 and print the backend name if Loop(backend="rio") constructs;
exit 3 with the error otherwise. The orchestrator uses this to decide in
~2s whether to run or SKIP the RIO-dependent steps, instead of letting
each of them fail slowly against a machine where RIO cannot initialize
(see 04b-rio-probe.log for the per-call diagnosis on such machines).
"""

import pathlib
import sys

try:
    from cadeloop.loop import Loop  # installed wheel first
except ImportError:
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "python"))
    from cadeloop.loop import Loop  # noqa: E402

try:
    lp = Loop(backend="rio")
except OSError as e:
    print(f"rio unavailable: {e}")
    sys.exit(3)
name = lp.stats()["backend"]
lp.close()
print(f"rio available: backend={name}")
