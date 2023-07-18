"""Tests for cache.py"""

import numpy as np
import pandas as pd
import hypothesis as hp
import hypothesis.strategies as st
from zolltools import cache as zch

integer_or_text = st.one_of(st.integers(), st.text())
df_3x3 = st.just(pd.DataFrame(np.arange(1, 10).reshape(3, 3)))


@hp.given(
    obj_to_store=st.one_of(
        integer_or_text,
        df_3x3,
        st.dictionaries(integer_or_text, integer_or_text),
    )
)
def test_load_object_storage(obj_to_store) -> None:
    """
    Tests the load function with various objects.
    """

    initial_load = zch.load(None, lambda _: obj_to_store, 0)
    assert obj_to_store == initial_load
    second_load = zch.load(None, lambda _: obj_to_store, 0)
    assert obj_to_store == second_load
