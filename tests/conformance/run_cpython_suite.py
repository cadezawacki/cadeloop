#!/usr/bin/env python3
"""R-120 conformance runner: execute CPython's own asyncio test suite
against cadeloop.

Requires a CPython build that ships the ``test`` package (python.org
installers do; many distro packages don't — then this script exits 0 with a
notice and the self-contained subset in this directory is the fallback).

Usage:
    python tests/conformance/run_cpython_suite.py [suite ...]

Skips are read from ``skiplist.txt`` (one ``TestClass.test_name`` per line,
``#`` comments carry the mandatory justification; the list must shrink
monotonically across milestones — R-120).
"""

import pathlib
import sys
import unittest

SUITES_BY_MILESTONE = {
    # M0: scheduling semantics only.
    "M0": ["test.test_asyncio.test_base_events", "test.test_asyncio.test_events"],
    # M1+: enable as transports land.
    "M1": ["test.test_asyncio.test_streams", "test.test_asyncio.test_sock_lowlevel"],
    "M2": ["test.test_asyncio.test_tasks"],
    "M4": ["test.test_asyncio.test_sslproto"],
    "M5": ["test.test_asyncio.test_subprocess"],
}
# Informational only (which milestone this project is at) — does NOT
# gate which suites run below. It used to (SUITES_BY_MILESTONE[
# CURRENT_MILESTONE] alone), which is exactly why test_streams/
# test_sock_lowlevel/test_tasks/test_sslproto never ran in CI despite
# M1/M2/M4 being complete: CURRENT_MILESTONE was never advanced past
# "M0" once those suites were added. The default below now runs every
# suite across every milestone unconditionally, so a future milestone's
# suite addition takes effect the moment it's added to the dict above.
CURRENT_MILESTONE = "M5"


def all_suites() -> list[str]:
    seen: list[str] = []
    for suite_list in SUITES_BY_MILESTONE.values():
        for name in suite_list:
            if name not in seen:
                seen.append(name)
    return seen


def load_skiplist(path: pathlib.Path) -> set[str]:
    skips = set()
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.split("#", 1)[0].strip()
            if line:
                skips.add(line)
    return skips


def main(argv: list[str]) -> int:
    try:
        import test.test_asyncio  # noqa: F401
    except ImportError:
        print(
            "NOTICE: this CPython build does not ship the `test` package; "
            "skipping the CPython conformance suite. The self-contained "
            "subset (pytest tests/conformance) still applies."
        )
        return 0

    import cadeloop

    # Route the suite's loop construction through cadeloop.
    import asyncio

    asyncio.set_event_loop_policy(cadeloop.EventLoopPolicy())

    skiplist = load_skiplist(pathlib.Path(__file__).with_name("skiplist.txt"))
    suites = argv or all_suites()

    loader = unittest.TestLoader()
    runner = unittest.TextTestRunner(verbosity=1)
    total_failures = 0
    for suite_name in suites:
        print(f"== {suite_name} (skiplist: {len(skiplist)} entries)")
        suite = loader.loadTestsFromName(suite_name)
        filtered = unittest.TestSuite()

        def add_filtered(s):
            for item in s:
                if isinstance(item, unittest.TestSuite):
                    add_filtered(item)
                else:
                    key = f"{type(item).__name__}.{item._testMethodName}"
                    if key not in skiplist:
                        filtered.addTest(item)

        add_filtered(suite)
        result = runner.run(filtered)
        total_failures += len(result.failures) + len(result.errors)
    return 1 if total_failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
