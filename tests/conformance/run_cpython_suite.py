#!/usr/bin/env python3
"""R-120 conformance runner: execute CPython's own asyncio test suite
against cadeloop.

Requires a CPython build that ships the ``test`` package (python.org
installers do; many distro packages don't -- then this script exits 0 with a
notice and the self-contained subset in this directory is the fallback).

Usage:
    python tests/conformance/run_cpython_suite.py [suite ...]

Each suite runs in a process of its own, and the run proves it exercised
cadeloop:

* ``asyncio.set_event_loop_policy(None)`` -- which every test_asyncio
  module calls in ``tearDownModule``, and some tests in ``tearDown`` --
  is pinned back to cadeloop's policy. Without the pin the first module
  to finish silently switched every later one to the stdlib loop, so a
  green run tested almost nothing.
* The loop-implementation test classes construct their loops directly
  (``asyncio.SelectorEventLoop(...)``) and so never touch the policy.
  Each is replaced by a subclass whose loop factory returns a cadeloop
  loop (``REBOUND`` below); the stdlib originals are not run, since they
  test the stdlib's loop.
* A suite that constructs no cadeloop loop at all fails.

Skips are read from ``skiplist.txt``: one ``module.TestClass.test_name``
per line (``TestClass.test_name`` matches in any module), each with a
``# reason`` on the same line -- an entry without one is an error. Skips
are for tests of the stdlib loop's own internals, which cadeloop does not
have; the list must shrink monotonically across milestones (R-120).
"""

import argparse
import os
import pathlib
import subprocess
import sys
import time
import unittest

SUITES_BY_MILESTONE = {
    # M0: scheduling semantics only.
    "M0": ["test.test_asyncio.test_base_events", "test.test_asyncio.test_events"],
    # M1+: enable as transports land.
    "M1": [
        "test.test_asyncio.test_streams",
        "test.test_asyncio.test_sock_lowlevel",
        "test.test_asyncio.test_server",
        "test.test_asyncio.test_buffered_proto",
        "test.test_asyncio.test_sendfile",
    ],
    "M2": ["test.test_asyncio.test_tasks"],
    "M4": ["test.test_asyncio.test_sslproto", "test.test_asyncio.test_ssl"],
    "M5": ["test.test_asyncio.test_subprocess", "test.test_asyncio.test_unix_events"],
}
# Informational only (which milestone this project is at) -- it does NOT
# gate which suites run: every suite of every milestone runs.
CURRENT_MILESTONE = "M5"

# Loop-implementation test classes, by module: for each, the name the
# cadeloop-bound subclass runs under, the hook through which the class
# builds its loop ("create_event_loop" / "new_loop" are the mixins'
# factory hooks; "setUp" classes build `self.loop` in setUp themselves),
# and the per-platform variants of the class in order of preference. The
# first variant this platform has is rebound -- running the same tests
# once per selector flavour would only repeat them -- and none of the
# stdlib originals runs.
_LOOP_VARIANTS = [
    "EPollEventLoopTests",
    "KqueueEventLoopTests",
    "ProactorEventLoopTests",
    "PollEventLoopTests",
    "SelectEventLoopTests",
]
REBOUND = {
    "test_events": [("CadeloopEventLoopTests", "create_event_loop", _LOOP_VARIANTS)],
    "test_sock_lowlevel": [("CadeloopSockTests", "create_event_loop", _LOOP_VARIANTS)],
    "test_sendfile": [("CadeloopSendfileTests", "create_event_loop", _LOOP_VARIANTS)],
    "test_buffered_proto": [
        (
            "CadeloopBufferedProtoTests",
            "new_loop",
            ["BufferedProtocolSelectorTests", "BufferedProtocolProactorTests"],
        )
    ],
    "test_server": [
        ("CadeloopStartServerTests", "new_loop", ["SelectorStartServerTests", "ProactorStartServerTests"])
    ],
    "test_sslproto": [
        ("CadeloopStartTLSTests", "new_loop", ["SelectorStartTLSTests", "ProactorStartTLSTests"])
    ],
    "test_unix_events": [
        ("CadeloopUnixSocketTests", "setUp", ["SelectorEventLoopUnixSocketTests"]),
        ("CadeloopUnixSockSendfileTests", "setUp", ["SelectorEventLoopUnixSockSendfileTests"]),
    ],
}


def all_suites() -> list[str]:
    seen: list[str] = []
    for suite_list in SUITES_BY_MILESTONE.values():
        for name in suite_list:
            if name not in seen:
                seen.append(name)
    return seen


def load_skiplist(path: pathlib.Path) -> set[str]:
    skips = set()
    if not path.exists():
        return skips
    for lineno, raw in enumerate(path.read_text().splitlines(), 1):
        entry, _, reason = raw.partition("#")
        entry = entry.strip()
        if not entry:
            continue
        if not reason.strip():
            raise SystemExit(f"{path.name}:{lineno}: skip of {entry!r} has no '# reason'")
        skips.add(entry)
    return skips


# --------------------------------------------------------------------- #
# child: one suite, in this process                                     #
# --------------------------------------------------------------------- #


def _pin_policy():
    """Map set_event_loop_policy(None) back to cadeloop's policy, and
    count the cadeloop loops the suite builds."""
    import asyncio
    from asyncio import events

    import cadeloop

    real_set = events.set_event_loop_policy

    def set_event_loop_policy(policy):
        return real_set(cadeloop.EventLoopPolicy() if policy is None else policy)

    events.set_event_loop_policy = set_event_loop_policy
    asyncio.set_event_loop_policy = set_event_loop_policy
    set_event_loop_policy(None)

    counter = [0]
    real_init = cadeloop.Loop.__init__

    def counting_init(self, *args, **kwargs):
        counter[0] += 1
        real_init(self, *args, **kwargs)

    cadeloop.Loop.__init__ = counting_init
    return counter


def _rebind(module):
    """Cadeloop-bound subclasses of the module's loop-implementation test
    classes. Returns ({original class name: subclass or None}, [names]):
    None marks a platform variant that is dropped, not run."""
    import cadeloop

    short = module.__name__.rpartition(".")[2]
    out = {}
    names = []
    for new_name, hook, variants in REBOUND.get(short, []):
        present = [v for v in variants if getattr(module, v, None) is not None]
        if not present:
            continue
        base = getattr(module, present[0])
        if hook == "create_event_loop":
            body = {"create_event_loop": lambda self: cadeloop.new_event_loop()}
        elif hook == "new_loop":
            body = {"new_loop": lambda self: cadeloop.new_event_loop()}
        else:
            body = {"setUp": _rebound_setup(base)}
        sub = type(new_name, (base,), body)
        sub.__module__ = module.__name__
        sub.__qualname__ = new_name
        out[present[0]] = sub
        for other in present[1:]:
            out[other] = None
        names.append(f"{new_name}({present[0]})")
    return out, names


def _rebound_setup(base):
    """For classes whose setUp builds `asyncio.SelectorEventLoop()`
    itself: run that setUp with the selector loop class swapped for
    cadeloop's, so whatever else it prepares stays as written."""
    import asyncio

    import cadeloop

    def setUp(self):
        real = asyncio.SelectorEventLoop
        asyncio.SelectorEventLoop = lambda *a, **k: cadeloop.new_event_loop()
        try:
            base.setUp(self)
        finally:
            asyncio.SelectorEventLoop = real

    return setUp


def run_child(suite_name: str, skiplist: set[str]) -> int:
    counter = _pin_policy()
    import importlib

    try:
        module = importlib.import_module(suite_name)
    except unittest.SkipTest as exc:
        print(f"== {suite_name}: skipped on this platform ({exc})")
        return 0
    short = suite_name.rpartition(".")[2]
    rebound, rebound_names = _rebind(module)

    loader = unittest.TestLoader()
    tests = unittest.TestSuite()
    skipped = []

    def keep(test):
        cls = type(test)
        key = f"{cls.__name__}.{test._testMethodName}"
        if key in skiplist or f"{short}.{key}" in skiplist:
            skipped.append(key)
            return False
        return True

    for name, obj in vars(module).items():
        if not (isinstance(obj, type) and issubclass(obj, unittest.TestCase)):
            continue
        if obj.__module__ != module.__name__:
            continue  # imported from elsewhere: runs with its own module
        cls = rebound.get(name, obj)
        if cls is None:
            continue
        for test in loader.loadTestsFromTestCase(cls):
            if keep(test):
                tests.addTest(test)

    print(f"== {suite_name}: {tests.countTestCases()} tests, {len(skipped)} skipped by skiplist")
    if rebound_names:
        print("   rebound to cadeloop: " + ", ".join(rebound_names))
    result = unittest.TextTestRunner(verbosity=1).run(tests)
    print(f"   cadeloop loops constructed: {counter[0]}")
    if counter[0] == 0:
        print(f"!! {suite_name} never constructed a cadeloop loop: it did not test cadeloop")
        return 1
    return 0 if result.wasSuccessful() else 1


# --------------------------------------------------------------------- #
# parent: one process per suite                                         #
# --------------------------------------------------------------------- #


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("suites", nargs="*")
    parser.add_argument("--child", help=argparse.SUPPRESS)
    parser.add_argument("--timeout", type=float, default=900.0, help="seconds per suite")
    args = parser.parse_args(argv)

    try:
        import test.test_asyncio  # noqa: F401
    except ImportError:
        print(
            "NOTICE: this CPython build does not ship the `test` package; "
            "skipping the CPython conformance suite. The self-contained "
            "subset (pytest tests/conformance) still applies."
        )
        return 0

    skiplist = load_skiplist(pathlib.Path(__file__).with_name("skiplist.txt"))
    if args.child:
        return run_child(args.child, skiplist)

    suites = args.suites or all_suites()
    failed = []
    for suite_name in suites:
        started = time.monotonic()
        try:
            proc = subprocess.run(
                [sys.executable, os.path.abspath(__file__), "--child", suite_name],
                timeout=args.timeout,
            )
            ok = proc.returncode == 0
        except subprocess.TimeoutExpired:
            print(f"!! {suite_name} timed out after {args.timeout:.0f}s")
            ok = False
        print(f"== {suite_name}: {'ok' if ok else 'FAILED'} in {time.monotonic() - started:.1f}s\n")
        if not ok:
            failed.append(suite_name)
    if failed:
        print("FAILED suites: " + ", ".join(failed))
        return 1
    print(f"all {len(suites)} suites passed")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
