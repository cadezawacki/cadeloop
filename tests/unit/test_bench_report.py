"""The benchmark report's statistics (bench/harness, bench/suite).

The published tables rank servers and put medals on the leaders. These
tests pin the rules that decide when one server is ahead of another: a
confidence interval for the median of the runs, and a place shared when
two intervals overlap.
"""

from __future__ import annotations

import math
import os
import sys

import pytest

BENCH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "bench")
sys.path.insert(0, os.path.join(BENCH, "harness"))
sys.path.insert(0, os.path.join(BENCH, "suite"))

from tables import mark_ranks  # noqa: E402


def _ranks(vals, hib=True, ivs=None):
    rows = [{"cells": [{} for _ in vals]}]
    mark_ranks(rows, hib, [vals], axis="row", intervals=[ivs] if ivs else None)
    return [c.get("rank") for c in rows[0]["cells"]]


def test_ranks_without_intervals_are_medals_by_value():
    assert _ranks([10, 8, 8, 5, 3]) == [1, 2, 2, None, None]
    assert _ranks([5, 5, 5, 1]) == [None] * 4  # most tie for first: nobody stands out
    assert _ranks([3, 9], hib=False) == [1, None]
    assert _ranks([7, None, 4, 2]) == [1, None, 2, None]


def test_overlapping_intervals_share_a_place():
    # A and B overlap; C is clearly behind both; D, last, never gets a badge.
    ivs = [(95, 105), (92, 101), (70, 80), (40, 50), (30, 35)]
    assert _ranks([100, 96, 75, 45, 32], ivs=ivs) == [1, 1, 3, None, None]


def test_a_place_counts_only_those_clearly_ahead():
    # A~B and B~C, but A is clear of C: only A is known to be ahead of C.
    ivs = [(100, 110), (95, 104), (88, 99), (10, 20), (5, 9)]
    assert _ranks([105, 99, 93, 15, 7], ivs=ivs) == [1, 1, 2, None, None]


def test_intervals_when_lower_is_better():
    ivs = [(10, 12), (11, 14), (30, 40), (50, 60), (70, 80)]
    assert _ranks([11, 12, 35, 55, 75], hib=False, ivs=ivs) == [1, 1, 3, None, None]


def test_a_missing_interval_falls_back_to_the_values():
    ivs = [(95, 105), None, (70, 80)]
    assert _ranks([100, 96, 75], ivs=ivs) == [1, 2, None]


def test_cells_that_show_the_same_value_share_a_place_whatever_their_intervals():
    # "21 MiB" twice: the intervals are apart, the printed values are not.
    ivs = [(20.6, 20.9), (21.1, 21.4), (30, 31), (40, 41), (50, 51)]
    assert _ranks([21, 21, 30, 40, 50], hib=False, ivs=ivs) == [1, 1, 3, None, None]


def test_most_cells_tied_within_their_intervals_marks_nothing():
    ivs = [(90, 110), (88, 108), (85, 111), (10, 20)]
    assert _ranks([100, 98, 97, 15], ivs=ivs) == [None] * 4


def test_columns_rank_with_intervals_too():
    rows = [{"cells": [{}]} for _ in range(4)]
    intervals = [[(95, 105)], [(92, 101)], [(55, 65)], [(15, 25)]]
    mark_ranks(rows, True, [[100], [97], [60], [20]], axis="col", intervals=intervals)
    assert [r["cells"][0].get("rank") for r in rows] == [1, 1, 3, None]


# --------------------------------------------------------------------- #
# the confidence interval of a median                                    #
# --------------------------------------------------------------------- #


def test_median_ci_is_the_order_statistic_interval():
    from report import median_ci

    # Nine runs: [x2, x8] covers the median with probability 1 - 2*10/512.
    lo, hi, cover = median_ci([5, 1, 9, 3, 7, 2, 8, 4, 6])
    assert (lo, hi) == (2, 8)
    assert cover == pytest.approx(1 - 20 / 512)
    # Three runs cannot reach 95%: the whole range, and its honest 75%.
    lo, hi, cover = median_ci([10, 30, 20])
    assert (lo, hi, cover) == (10, 30, 0.75)
    # Six: the range covers 96.9%; nothing narrower reaches 95%.
    lo, hi, cover = median_ci([1, 2, 3, 4, 5, 6])
    assert (lo, hi) == (1, 6) and cover == pytest.approx(1 - 2 / 64)
    assert median_ci([42]) == (42, 42, 0.0)


@pytest.mark.parametrize("n", [3, 5, 7, 9, 11, 15, 21])
def test_median_ci_coverage_is_what_it_says(n):
    """Simulated: the interval contains the true median at least as often
    as the stated coverage (within sampling error), for any distribution."""
    import random

    from report import median_ci

    rng = random.Random(n)
    trials, inside = 4000, 0
    stated = None
    for _ in range(trials):
        xs = [rng.lognormvariate(0, 0.5) for _ in range(n)]  # true median: 1
        lo, hi, stated = median_ci(xs)
        inside += lo <= 1.0 <= hi
    observed = inside / trials
    assert observed >= stated - 4 * math.sqrt(stated * (1 - stated) / trials) - 1e-9, (n, observed, stated)
    assert stated >= 0.95 or n < 6
