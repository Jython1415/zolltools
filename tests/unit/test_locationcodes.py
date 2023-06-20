"""Tests for strtools.py"""

import time
import random
import importlib

import pytest
from scipy import stats
from zolltools.nemsis import locationcodes


@pytest.mark.slow
def test_get_mapping_performance():
    """
    measuring performance of get_mapping

    Takes a sample of the execution time for the first and successive calls to
    `get_mapping` and determines if successive calls are at least
    `min_exp_speedup` times faster than the first calls within a certain
    confidence interval (see `alpha` and comparison to p-value in `assert`
    statement).

    `min_exp_speedup` was determined with preliminary testing. See gh 74
    """

    alpha = 0.05 # confidence threshold
    min_exp_speedup = 10
    num_data_points = 100 # see gh 74 for reasoning
    data = {"successive": [], "adjusted-first-read": []}
    for _ in range(num_data_points):
        importlib.reload(locationcodes)
        successive_read_lower_bound_incl = 100
        successive_read_upper_bound_incl = 200 # see gh 74 for boundary reasoning

        # Record the first read of the mapping
        start_time = time.perf_counter_ns()
        _ = locationcodes.get_mapping()
        end_time = time.perf_counter_ns()
        adjusted_first_read = (end_time - start_time) / min_exp_speedup

        # Record a later read of the mapping (randomly selected)
        read_to_test = random.randint(
            successive_read_lower_bound_incl, successive_read_upper_bound_incl
        )
        for _ in range(read_to_test - 2): # -2, first read and the next (below)
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
    p_value = t_check[1]
    assert p_value < alpha


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
