"""Tests for strtools.py"""

import pytest
import hypothesis as hp
import hypothesis.strategies as st
import zolltools.strtools as strtools


@hp.given(prefix=st.text())
def test_removeprefix_empty_string(prefix: str):
    """removeprefix test for empty string"""

    assert strtools.removeprefix("", prefix) == ""


@hp.given(string=st.text())
def test_removeprefix_empty_prefix(string: str):
    """removeprefix test for empty prefix"""

    assert strtools.removeprefix(string, "") == string


@pytest.mark.parametrize(
    ("input", "prefix", "expected"),
    [
        ("abcabcabc", "abc", "abcabc"),
        ("abc123456", "abc", "123456"),
        ("abc", "a", "bc"),
    ],
)
def test_removeprefix_with_matching_prefix(input, prefix, expected):
    """removeprefix test with a prefix that matches"""

    assert strtools.removeprefix(input, prefix) == expected


@pytest.mark.parametrize(
    ("input", "prefix"),
    [
        ("abcabcabc", "bac"),
        ("abc123456", "123"),
        ("abc123456", "456"),
    ],
)
def test_removeprefix_with_nonmatching_prefix(input, prefix):
    """removeprefix test with a prefix that does not match"""

    assert strtools.removeprefix(input, prefix) == input


@hp.given(suffix=st.text())
def test_removesuffix_empty_string(suffix: str):
    """removesuffix test for empty string"""

    assert strtools.removesuffix("", suffix) == ""


@hp.given(string=st.text())
def test_removesuffix_empty_suffix(string: str):
    """removesuffix test for empty suffix"""

    assert strtools.removesuffix(string, "") == string

@pytest.mark.parametrize(
    ("input", "suffix", "expected"),
    [
        ("abcabcabc", "abc", "abcabc"),
        ("abc123456", "456", "abc123"),
        ("abc", "c", "ab"),
    ]
)
def test_removesuffix_with_matching_suffix(input, suffix, expected):
    """removesuffix test with a suffix that matches"""

    assert strtools.removesuffix(input, suffix) == expected


@pytest.mark.parametrize(
    ("input", "suffix"),
    [
        ("abcabcabc", "bac"),
        ("abc123456", "123"),
        ("abc123456", "abc"),
    ]
)
def test_removesuffix_with_nonmatching_suffix(input, suffix):
    """removesuffix test with a suffix that does not match"""

    assert strtools.removesuffix(input, suffix) == input
    assert strtools.removesuffix(input, suffix) == input
    assert strtools.removesuffix(input, suffix) == input


def main():
    """Run when file is executed"""


if __name__ == "__main__":
    main()
