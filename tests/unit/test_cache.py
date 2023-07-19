"""Tests for cache.py"""

import tempfile
from typing import Any
from pathlib import Path

import numpy as np
import pandas as pd
import hypothesis as hp
import hypothesis.strategies as st
from zolltools import cache as zch

st_integer_or_text = st.one_of(st.integers(), st.text())
st_df_3x3 = st.just(pd.DataFrame(np.arange(1, 10).reshape(3, 3)))
st_any_object = st.one_of(
    st_integer_or_text,
    st_df_3x3,
    st.dictionaries(st_integer_or_text, st_integer_or_text),
)


def _assert_object_equals(left: Any, right: Any) -> None:
    """
    Checks if two objects based on `st_any_object` are equal. Has logic to
    make sure data frames are compared correctly.

    :param left: the first value to compare.
    :param right: the second value to compare.
    """

    if isinstance(left, pd.DataFrame):
        assert left.equals(right)
    elif isinstance(right, pd.DataFrame):
        assert right.equals(left)
    else:
        assert left == right


@hp.given(obj_to_store=st_any_object)
def test_load_object_storage(obj_to_store) -> None:
    """
    Tests the load function with various objects.
    """

    with tempfile.TemporaryDirectory(dir=Path.cwd()) as dir_name:
        folder = Path(dir_name)
        load_1, _ = zch.load(None, lambda _: obj_to_store, 0, folder=folder)
        load_2, _ = zch.load(None, lambda _: obj_to_store, 0, folder=folder)
        _assert_object_equals(obj_to_store, load_1)
        _assert_object_equals(obj_to_store, load_2)


@hp.given(state=st_any_object)
def test_load_state_usage(state) -> None:
    """
    Tests the load function with various types of states
    """

    obj_to_store = "test_object"

    with tempfile.TemporaryDirectory(dir=Path.cwd()) as dir_name:
        folder = Path(dir_name)

        def reload(prev_state, state) -> bool:
            if isinstance(prev_state, pd.DataFrame):
                return prev_state.equals(state)
            return prev_state == state

        load_1, _ = zch.load(
            state, lambda _: obj_to_store, 1, folder=folder, reload=reload
        )
        load_2, _ = zch.load(
            state, lambda _: obj_to_store, 1, folder=folder, reload=reload
        )
        _assert_object_equals(obj_to_store, load_1)
        _assert_object_equals(obj_to_store, load_2)
