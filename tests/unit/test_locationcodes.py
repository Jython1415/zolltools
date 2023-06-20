"""Tests for strtools.py"""

import time
import timeit
import importlib
import pandas as pd
from scipy import stats

# import hypothesis as hp
# import hypothesis.strategies as st
import zolltools.nemsis.locationcodes as locationcodes


def test_get_mapping_performance():
    """measuring performance of get_mapping"""

    alpha = 0.05             # confidence threshold
    min_exp_speedup = 40     # successive reads should be min_exp_speedup times faster
    num_data_points = 10_000
    data = {"first": [], "successive": [], "adjusted": []}
    for _ in range(num_data_points):
        importlib.reload(locationcodes)
        timeit_repeat_num = 1000
        seconds_to_ns = 1e9

        # Record for the first read of the mapping
        start_time = time.perf_counter_ns()
        _ = locationcodes.get_mapping()
        end_time = time.perf_counter_ns()
        first_read = end_time - start_time
        adjusted_first_read = first_read / min_exp_speedup

        # Record a second read of the mapping
        successive_read = (
            timeit.timeit(
                "_ = locationcodes.get_mapping()",
                number=timeit_repeat_num,
                globals=globals(),
            )
            / timeit_repeat_num
            * seconds_to_ns
        )

        # Record measurements
        data["first"].append(first_read)
        data["successive"].append(successive_read)
        data["adjusted"].append(adjusted_first_read)

    t_check = stats.ttest_ind(
        data["adjusted"], data["successive"], equal_var=False, alternative="greater"
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
