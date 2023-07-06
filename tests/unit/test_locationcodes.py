"""Tests for locationcodes.py"""

import time
import random
import importlib

import pytest
from scipy import stats
from zolltools.nemsis import locationcodes


@pytest.mark.slow
def test_get_mapping_performance():
    """
    Measures the performance of get_mapping. Checks speed-up for successive
    calls to the method.

    Takes a sample of the execution time for the first and successive calls to
    `get_mapping` and determines if successive calls are at least
    `min_exp_speedup` times faster than the first calls. The confidence of this
    assertion is measured by a t-test, with the assertion being that the p-value
    must be less than `alpha`.

    `min_exp_speedup` was determined with preliminary testing. See gh 74
    """

    alpha = 0.05  # max p-value (exclusive)
    min_exp_speedup = 10
    num_data_points = 100  # see gh 74 for reasoning
    data = {"successive": [], "adjusted-first-read": []}
    for _ in range(num_data_points):
        importlib.reload(locationcodes)
        successive_read_lower_bound_incl = 100
        successive_read_upper_bound_incl = 200  # see gh 74 for boundary reasoning

        # Record the first read of the mapping
        start_time = time.perf_counter_ns()
        _ = locationcodes.get_mapping()
        end_time = time.perf_counter_ns()
        adjusted_first_read = (end_time - start_time) / min_exp_speedup

        # Record a later read of the mapping (randomly selected)
        nth_read_to_test = random.randint(
            successive_read_lower_bound_incl, successive_read_upper_bound_incl
        )
        for _ in range(nth_read_to_test - 2):  # -2, first read and the next (below)
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
    """Tests the result of from get_mapping"""

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


def test_get_code_set_correctness():
    """Tests the correctness of get_code_set"""

    expected = set(
        [
            "Y92.0",
            "Y92.00",
            "Y92.000",
            "Y92.001",
            "Y92.002",
            "Y92.003",
            "Y92.007",
            "Y92.008",
            "Y92.009",
            "Y92.01",
            "Y92.010",
            "Y92.011",
            "Y92.012",
            "Y92.013",
            "Y92.014",
            "Y92.015",
            "Y92.016",
            "Y92.017",
            "Y92.018",
            "Y92.019",
            "Y92.02",
            "Y92.020",
            "Y92.021",
            "Y92.022",
            "Y92.023",
            "Y92.024",
            "Y92.025",
            "Y92.026",
            "Y92.027",
            "Y92.028",
            "Y92.029",
            "Y92.03",
            "Y92.030",
            "Y92.031",
            "Y92.032",
            "Y92.038",
            "Y92.039",
            "Y92.04",
            "Y92.040",
            "Y92.041",
            "Y92.042",
            "Y92.043",
            "Y92.044",
            "Y92.045",
            "Y92.046",
            "Y92.048",
            "Y92.049",
            "Y92.09",
            "Y92.090",
            "Y92.091",
            "Y92.092",
            "Y92.093",
            "Y92.094",
            "Y92.095",
            "Y92.096",
            "Y92.098",
            "Y92.099",
            "Y92.1",
            "Y92.10",
            "Y92.11",
            "Y92.110",
            "Y92.111",
            "Y92.112",
            "Y92.113",
            "Y92.114",
            "Y92.115",
            "Y92.116",
            "Y92.118",
            "Y92.119",
            "Y92.12",
            "Y92.120",
            "Y92.121",
            "Y92.122",
            "Y92.123",
            "Y92.124",
            "Y92.125",
            "Y92.126",
            "Y92.128",
            "Y92.129",
            "Y92.13",
            "Y92.130",
            "Y92.131",
            "Y92.133",
            "Y92.135",
            "Y92.136",
            "Y92.137",
            "Y92.138",
            "Y92.139",
            "Y92.14",
            "Y92.140",
            "Y92.141",
            "Y92.142",
            "Y92.143",
            "Y92.146",
            "Y92.147",
            "Y92.148",
            "Y92.149",
            "Y92.15",
            "Y92.150",
            "Y92.151",
            "Y92.152",
            "Y92.153",
            "Y92.154",
            "Y92.155",
            "Y92.156",
            "Y92.157",
            "Y92.158",
            "Y92.159",
            "Y92.16",
            "Y92.160",
            "Y92.161",
            "Y92.162",
            "Y92.163",
            "Y92.168",
            "Y92.169",
            "Y92.19",
            "Y92.190",
            "Y92.191",
            "Y92.192",
            "Y92.193",
            "Y92.194",
            "Y92.195",
            "Y92.196",
            "Y92.197",
            "Y92.198",
            "Y92.199",
            "Y92.2",
            "Y92.21",
            "Y92.210",
            "Y92.211",
            "Y92.212",
            "Y92.213",
            "Y92.214",
            "Y92.215",
            "Y92.218",
            "Y92.219",
            "Y92.22",
            "Y92.23",
            "Y92.230",
            "Y92.231",
            "Y92.232",
            "Y92.233",
            "Y92.234",
            "Y92.238",
            "Y92.239",
            "Y92.24",
            "Y92.240",
            "Y92.241",
            "Y92.242",
            "Y92.243",
            "Y92.248",
            "Y92.25",
            "Y92.250",
            "Y92.251",
            "Y92.252",
            "Y92.253",
            "Y92.254",
            "Y92.258",
            "Y92.26",
            "Y92.29",
            "Y92.3",
            "Y92.31",
            "Y92.310",
            "Y92.311",
            "Y92.312",
            "Y92.318",
            "Y92.32",
            "Y92.320",
            "Y92.321",
            "Y92.322",
            "Y92.328",
            "Y92.33",
            "Y92.330",
            "Y92.331",
            "Y92.34",
            "Y92.39",
            "Y92.4",
            "Y92.41",
            "Y92.410",
            "Y92.411",
            "Y92.412",
            "Y92.413",
            "Y92.414",
            "Y92.415",
            "Y92.48",
            "Y92.480",
            "Y92.481",
            "Y92.482",
            "Y92.488",
            "Y92.5",
            "Y92.51",
            "Y92.510",
            "Y92.511",
            "Y92.512",
            "Y92.513",
            "Y92.52",
            "Y92.520",
            "Y92.521",
            "Y92.522",
            "Y92.523",
            "Y92.524",
            "Y92.53",
            "Y92.530",
            "Y92.531",
            "Y92.532",
            "Y92.538",
            "Y92.59",
            "Y92.6",
            "Y92.61",
            "Y92.62",
            "Y92.63",
            "Y92.64",
            "Y92.65",
            "Y92.69",
            "Y92.7",
            "Y92.71",
            "Y92.72",
            "Y92.73",
            "Y92.74",
            "Y92.79",
            "Y92.8",
            "Y92.81",
            "Y92.810",
            "Y92.811",
            "Y92.812",
            "Y92.813",
            "Y92.814",
            "Y92.815",
            "Y92.816",
            "Y92.818",
            "Y92.82",
            "Y92.820",
            "Y92.821",
            "Y92.828",
            "Y92.83",
            "Y92.830",
            "Y92.831",
            "Y92.832",
            "Y92.833",
            "Y92.834",
            "Y92.838",
            "Y92.84",
            "Y92.85",
            "Y92.86",
            "Y92.89",
            "Y92.9",
        ]
    )

    result = locationcodes.get_code_set()  # pylint: disable=no-member
    assert result == expected
