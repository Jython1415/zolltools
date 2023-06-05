"""Tests for strtools.py"""

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


def test_removeprefix_with_matching_prefix():
    """removeprefix test with a prefix that matches"""

    assert strtools.removeprefix("abcabcabc", "abc") == "abcabc"
    assert strtools.removeprefix("abc123456", "abc") == "123456"
    assert strtools.removeprefix("abc", "a") == "bc"


def test_removeprefix_with_nonmatching_prefix():
    """removeprefix test with a prefix that does not match"""

    assert strtools.removeprefix("abcabcabc", "bac") == "abcabcabc"
    assert strtools.removeprefix("abc123456", "123") == "abc123456"
    assert strtools.removeprefix("abc123456", "456") == "abc123456"


@hp.given(suffix=st.text())
def test_removesuffix_empty_string(suffix: str):
    """removesuffix test for empty string"""

    assert strtools.removesuffix("", suffix) == ""


@hp.given(string=st.text())
def test_removesuffix_empty_suffix(string: str):
    """removesuffix test for empty suffix"""

    assert strtools.removesuffix(string, "") == string


def test_removesuffix_with_matching_suffix():
    """removesuffix test with a suffix that matches"""

    assert strtools.removesuffix("abcabcabc", "abc") == "abcabc"
    assert strtools.removesuffix("abc123456", "456") == "abc123"
    assert strtools.removesuffix("abc", "c") == "ab"


def test_removesuffix_with_nonmatching_suffix():
    """removesuffix test with a suffix that does not match"""

    assert strtools.removesuffix("abcabcabc", "bac") == "abcabcabc"
    assert strtools.removesuffix("abc123456", "123") == "abc123456"
    assert strtools.removesuffix("abc123456", "abc") == "abc123456"


def main():
    """Run when file is executed"""


if __name__ == "__main__":
    main()
