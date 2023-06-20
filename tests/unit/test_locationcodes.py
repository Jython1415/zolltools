"""Tests for strtools.py"""

import timeit
import hypothesis as hp
import hypothesis.strategies as st
import zolltools.nemsis.locationcodes as locationcodes


def test_get_mapping_performance():
    """measuring performance of get_mapping"""

    result = timeit.timeit(
        "_ = locationcodes.get_mapping()", globals=globals(), number=1_000_000
    )
    print(result)


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
