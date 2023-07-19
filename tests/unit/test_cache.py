"""Tests for cache.py"""

import numpy as np
import pandas as pd
import hypothesis as hp
import hypothesis.strategies as st
from zolltools import cache as zch

st_integer_or_text = st.one_of(st.integers(), st.text())
st_df_3x3 = st.just(pd.DataFrame(np.arange(1, 10).reshape(3, 3)))
st_object_to_store = st.one_of(
    st_integer_or_text,
    st_df_3x3,
    st.dictionaries(st_integer_or_text, st_integer_or_text),
)


@hp.given(obj_to_store=st_object_to_store)
def test_load_object_storage(obj_to_store) -> None:
    """
    Tests the load function with various objects.
    """

    initial_load = zch.load(None, lambda _: obj_to_store, 0)
    assert obj_to_store == initial_load
    second_load = zch.load(None, lambda _: obj_to_store, 0)
    assert obj_to_store == second_load
