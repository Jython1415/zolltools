"""Tests for pqtools.py"""

import os
import shutil
import random
import contextlib
from pathlib import Path

import pytest
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from zolltools.db import pqtools


@contextlib.contextmanager
def _temporary_parquet_table_context_manager(frame: pd.DataFrame) -> Path:
    """
    Context manager for a temporary parquet table (used for testing). The
    context manager will delete the temporary directory and table upon closing.

    :param frame: the data frame to make into a parquet table
    :returns: the path to the temporary table
    """

    randomizer = random.randint(1, 100_000)
    table_id = str(
        (pd.util.hash_pandas_object(frame).sum() % 900_000) + randomizer
    ).zfill(6)
    temporary_directory = Path.cwd().joinpath(f"tmp_parquet_table_{table_id}")
    temporary_directory.mkdir(exist_ok=True)
    table = pa.Table.from_pandas(frame)
    table_path = temporary_directory.joinpath(f"{table_id}.parquet")
    pq.write_table(table, table_path)
    try:
        yield table_path
    finally:
        os.remove(table_path)
        shutil.rmtree(temporary_directory)


@pytest.fixture(scope="module")
def tmp_table_3x3() -> tuple[Path, pd.DataFrame]:
    """
    Fixture of a temporary parquet table to use for tests. The frame stored as a
    parquet table was of the integers 1-9 arranged in a 3x3 grid.

    :returns: (path to table, data frame that was stored as a parquet table)
    """

    frame = pd.DataFrame(np.arange(1, 10).reshape(3, 3))
    with _temporary_parquet_table_context_manager(frame) as table_path:
        yield (table_path, frame)


@pytest.fixture(scope="module")
def tmp_table_3x3_named_cols() -> tuple[Path, pd.DataFrame]:
    """
    Fixture for a temporary parquet table to use for tests.The frame stored as a
    parquet table was of the integers 1-9 arranged in a 3x3 grid with column
    names "a", "b", and "c".

    :returns: (path to table, data frame that was stored as a parquet table)"""

    frame = pd.DataFrame(np.arange(1, 10).reshape(3, 3), columns=["a", "b", "c"])
    with _temporary_parquet_table_context_manager(frame) as table_path:
        yield (table_path, frame)


def test_get_table(
    tmp_table_3x3: tuple[Path, pd.DataFrame]
):  # pylint: disable=redefined-outer-name
    """
    Tests get_table with a simple table

    :param tmp_table_3x3: a pytest fixture
    """

    table_path, frame = tmp_table_3x3
    data_dir = table_path.parent
    pq_config = pqtools.ParquetManager.Config(data_dir)
    pq_reader = pqtools.Reader(pq_config)
    loaded_frame = pq_reader.get_table(table_path.name.removesuffix(".parquet"))
    pd.testing.assert_frame_equal(frame, loaded_frame)


def test_get_table_warning(
    tmp_table_3x3: tuple[Path, pd.DataFrame]
):  # pylint: disable=redefined-outer-name
    """
    Tests if get_table returns a warning when the requested table is too large.
    """

    table_path, frame = tmp_table_3x3
    data_dir = table_path.parent
    pq_config = pqtools.ParquetManager.Config(data_dir, default_target_in_memory_size=1)
    pq_reader = pqtools.Reader(pq_config)
    with pytest.raises(MemoryError):
        _ = pq_reader.get_table(table_path.name.removesuffix(".parquet"))

    pq_config = pqtools.ParquetManager.Config(
        data_dir, default_target_in_memory_size=1000
    )
    pq_reader = pqtools.Reader(pq_config)
    loaded_frame = pq_reader.get_table(table_path.name.removesuffix(".parquet"))
    pd.testing.assert_frame_equal(frame, loaded_frame)


def test_get_column(
    tmp_table_3x3_named_cols: tuple[Path, pd.DataFrame]
):  # pylint: disable=redefined-outer-name
    """
    Tests get_column method
    """

    table_path, frame = tmp_table_3x3_named_cols
    data_dir = table_path.parent
    pq_config = pqtools.ParquetManager.Config(data_dir)
    pq_reader = pqtools.Reader(pq_config)
    assert list(frame.columns) == pq_reader.get_columns(
        table_path.name.removesuffix(".parquet")
    )
