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

@hp.given(suffix=st.text())
def test_removesuffix_empty_string(suffix: str):
    """removesuffix test for empty string"""

    assert strtools.removesuffix("", suffix) == ""


@hp.given(string=st.text())
def test_removesuffix_empty_suffix(string: str):
    """removesuffix test for empty suffix"""
    
    assert strtools.removesuffix(string, "") == string


def main():
    """Run when file is executed"""


if __name__ == "__main__":
    main()
