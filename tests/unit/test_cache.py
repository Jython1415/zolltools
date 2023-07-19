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
            state, lambda _: obj_to_store, 0, folder=folder, reload=reload
        )
        load_2, _ = zch.load(
            state, lambda _: obj_to_store, 0, folder=folder, reload=reload
        )
        _assert_object_equals(obj_to_store, load_1)
        _assert_object_equals(obj_to_store, load_2)


@hp.given(initial_state=st.integers())
def test_load_meaningful_generate_parameter(initial_state) -> None:
    """
    Tests the load function with a generate function that actually performs a
    computation.
    """

    with tempfile.TemporaryDirectory(dir=Path.cwd()) as dir_name:
        folder = Path(dir_name)
        expected_object = initial_state + 1
        load_1, _ = zch.load(initial_state, lambda x: x + 1, 0, folder=folder)
        load_2, _ = zch.load(initial_state, lambda x: x + 1, 0, folder=folder)
        _assert_object_equals(expected_object, load_1)
        _assert_object_equals(expected_object, load_2)


@hp.given(id_1=st.integers(), id_2=st.integers())
def test_load_unique_id_parameter(id_1, id_2) -> None:
    """
    Tests the load function with two objects and two IDs to determine if the
    objects are stored independently and can be retrieved independently.
    """

    hp.assume(id_1 != id_2)

    with tempfile.TemporaryDirectory(dir=Path.cwd()) as dir_name:
        folder = Path(dir_name)
        id_1_loads = set()
        id_2_loads = set()
        load, _ = zch.load(None, lambda _: id_1, id_1, folder=folder)
        id_1_loads.add(load)
        load, _ = zch.load(None, lambda _: id_2, id_2, folder=folder)
        id_2_loads.add(load)
        load, _ = zch.load(None, lambda _: id_1, id_1, folder=folder)
        id_1_loads.add(load)
        load, _ = zch.load(None, lambda _: id_2, id_2, folder=folder)
        id_2_loads.add(load)
        assert id_1 in id_1_loads
        assert len(id_1_loads) == 1
        assert id_2 in id_2_loads
        assert len(id_2_loads) == 1
