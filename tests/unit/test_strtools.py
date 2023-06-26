"""Tests for strtools.py"""

import pytest
import hypothesis as hp
import hypothesis.strategies as st
from zolltools import strtools


@hp.given(prefix=st.text())
def test_removeprefix_empty_string(prefix: str):
    """removeprefix test for empty string"""

    assert strtools.removeprefix("", prefix) == ""


@hp.given(string=st.text())
def test_removeprefix_empty_prefix(string: str):
    """removeprefix test for empty prefix"""

    assert strtools.removeprefix(string, "") == string


@pytest.mark.parametrize(
    ("string", "prefix", "expected"),
    [
        ("abcabcabc", "abc", "abcabc"),
        ("abc123456", "abc", "123456"),
        ("abc", "a", "bc"),
    ],
)
def test_removeprefix_with_matching_prefix(string, prefix, expected):
    """removeprefix test with a prefix that matches"""

    assert strtools.removeprefix(string, prefix) == expected


@pytest.mark.parametrize(
    ("string", "prefix"),
    [
        ("abcabcabc", "bac"),
        ("abc123456", "123"),
        ("abc123456", "456"),
    ],
)
def test_removeprefix_with_nonmatching_prefix(string, prefix):
    """removeprefix test with a prefix that does not match"""

    assert strtools.removeprefix(string, prefix) == string


@hp.given(suffix=st.text())
def test_removesuffix_empty_string(suffix: str):
    """removesuffix test for empty string"""

    assert strtools.removesuffix("", suffix) == ""


@hp.given(string=st.text())
def test_removesuffix_empty_suffix(string: str):
    """removesuffix test for empty suffix"""

    assert strtools.removesuffix(string, "") == string


@pytest.mark.parametrize(
    ("string", "suffix", "expected"),
    [
        ("abcabcabc", "abc", "abcabc"),
        ("abc123456", "456", "abc123"),
        ("abc", "c", "ab"),
    ],
)
def test_removesuffix_with_matching_suffix(string, suffix, expected):
    """removesuffix test with a suffix that matches"""

    assert strtools.removesuffix(string, suffix) == expected


@pytest.mark.parametrize(
    ("string", "suffix"),
    [
        ("abcabcabc", "bac"),
        ("abc123456", "123"),
        ("abc123456", "abc"),
    ],
)
def test_removesuffix_with_nonmatching_suffix(string, suffix):
    """removesuffix test with a suffix that does not match"""

    assert strtools.removesuffix(string, suffix) == string


def main():
    """Run when file is executed"""


if __name__ == "__main__":
    main()
