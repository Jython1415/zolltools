"""Tests for strtools.py"""

import time
import random
import importlib

import pytest
from scipy import stats

import zolltools.nemsis.locationcodes as locationcodes


@pytest.mark.slow
def test_get_mapping_performance():
    """
    measuring performance of get_mapping

    Takes a sample of the execution time for the first and successive calls to
    `get_mapping` and determines that successive calls are at least
    `min_exp_speedup` times faster than the first calls within a certain
    confidence interval (see `alpha` and comparison to p-value in `assert`
    statement).

    `min_exp_speedup` was determined with preliminary testing. See gh 74
    """

    alpha = 0.05  # confidence threshold
    min_exp_speedup = 40  # successive reads should be min_exp_speedup times faster
    num_data_points = 10_000
    data = {"successive": [], "adjusted-first-read": []}
    for _ in range(num_data_points):
        importlib.reload(locationcodes)
        successive_read_upper_bound = 100

        # Record for the first read of the mapping
        start_time = time.perf_counter_ns()
        _ = locationcodes.get_mapping()
        end_time = time.perf_counter_ns()
        adjusted_first_read = (end_time - start_time) / min_exp_speedup

        # Record a later of the mapping (randomly selected)
        read_to_test = random.randint(2, successive_read_upper_bound)
        for _ in range(read_to_test - 2):
            _ = locationcodes.get_mapping()
        start_time = time.perf_counter_ns()
        _ = locationcodes.get_mapping()
        end_time = time.perf_counter_ns()
        successive_read = end_time - start_time

        # Record measurements
        data["successive"].append(successive_read)
        data["adjusted-first-read"].append(adjusted_first_read)

    t_check = stats.ttest_ind(
        data["adjusted-first-read"],
        data["successive"],
        equal_var=False,
        alternative="greater",
    )
    assert t_check[1] < alpha


def test_get_mapping_correctness():
    """testing output of get_mapping"""

    mapping = locationcodes.get_mapping()

    # Check a value
    expected = (
        "Non-institutional (private) residence as the place of "
        "occurrence of the external cause"
    )
    result = mapping["Y92.0"]
    assert result == expected

    # Check length
    expected = 246
    result = len(mapping)
    assert result == expected
