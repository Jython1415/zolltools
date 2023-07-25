"""Tests for pqtools.py"""

import os
import shutil
import random
import hashlib
import tempfile
import contextlib
from pathlib import Path
from typing import Optional, Generator

import pytest
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from zolltools.db import pqtools


@contextlib.contextmanager
def _temporary_parquet_table_context_manager(
    frame: pd.DataFrame,
) -> Generator[Path, None, None]:
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


@contextlib.contextmanager
def _temp_table_helper(
    frame: pd.DataFrame, directory: Path
) -> Generator[Path, None, None]:
    """
    Context manager for a temporary parquet table.

    :param frame: the data frame to store as a parquet table.
    :param directory: the directory to store the temporary table.
    :returns: the path to the temporary table.
    """

    frame_hash = hashlib.sha256(
        pd.util.hash_pandas_object(frame, index=True).values
    ).hexdigest()[:5]
    file_name = f"{frame_hash}.parquet"
    table_path = directory.joinpath(file_name)
    table = pa.Table.from_pandas(frame)
    pq.write_table(table, table_path)
    try:
        yield table_path
    finally:
        os.remove(table_path)


@contextlib.contextmanager
def _temp_table(
    frame: pd.DataFrame, directory: Optional[Path] = None
) -> Generator[Path, None, None]:
    """
    Context manager for a temporary parquet table with an optional directory
    parameter.

    :param frame: the data frame to store as a parquet table.
    :param directory: the directory to store the temporary table. If
    unspecified, the table will be stored in a newly created temporary
    directory which will be deleted upon closing.
    :returns: the path to the temporary table.
    """

    if directory is None:
        with (
            tempfile.TemporaryDirectory() as dir_name,
            _temp_table_helper(frame, Path(dir_name)) as table_path,
        ):
            try:
                yield table_path
            finally:
                pass
    else:
        with _temp_table_helper(frame, directory) as table_path:
            try:
                yield table_path
            finally:
                pass


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
    assert isinstance(table_path, Path)
    assert isinstance(frame, pd.DataFrame)
    data_dir = table_path.parent
    pq_config = pqtools.ParquetManager.Config(data_dir)
    pq_reader = pqtools.Reader(pq_config)
    loaded_frame = pq_reader.get_table(table_path)
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
        _ = pq_reader.get_table(table_path)

    pq_config = pqtools.ParquetManager.Config(
        data_dir, default_target_in_memory_size=1000
    )
    pq_reader = pqtools.Reader(pq_config)
    loaded_frame = pq_reader.get_table(table_path)
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
    assert list(frame.columns) == pq_reader.get_columns(table_path)


def test_find_table_success() -> None:
    """
    Tests the find_table method to confirm it returns the correct table.
    """

    frame1 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    frame2 = pd.DataFrame({"a": [1, 2, 3], "c": [7, 8, 9]})

    with tempfile.TemporaryDirectory() as dir_name:
        dir_path = Path(dir_name)
        with (
            _temp_table(frame1, dir_path) as table1_path,
            _temp_table(frame2, dir_path) as table2_path,
        ):
            pq_config = pqtools.ParquetManager.Config(dir_path)
            pq_reader = pqtools.Reader(pq_config)
            assert table1_path == pq_reader.find_table("b")
            assert table2_path == pq_reader.find_table("c")


def test_find_table_no_matching_table() -> None:
    """
    Tests the find_table method to confirm it raises the expected exception when
    no matching table can be found.
    """

    frame1 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    frame2 = pd.DataFrame({"a": [1, 2, 3], "c": [7, 8, 9]})

    with tempfile.TemporaryDirectory() as dir_name:
        dir_path = Path(dir_name)
        with (
            _temp_table(frame1, dir_path) as _,
            _temp_table(frame2, dir_path) as _,
        ):
            pq_config = pqtools.ParquetManager.Config(dir_path)
            pq_reader = pqtools.Reader(pq_config)
            search_column = "d"
            with pytest.raises(
                LookupError, match=f"No table with column {search_column}"
            ):
                pq_reader.find_table("d")


def test_find_table_multiple_matching_tables() -> None:
    """
    Tests the find_table method to confirm it raises a LookupError when multiple
    matching tables are found.
    """

    frame1 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    frame2 = pd.DataFrame({"a": [1, 2, 3], "b": [7, 8, 9]})

    with tempfile.TemporaryDirectory() as dir_name:
        dir_path = Path(dir_name)
        with (
            _temp_table(frame1, dir_path) as table1_path,
            _temp_table(frame2, dir_path) as table2_path,
        ):
            pq_config = pqtools.ParquetManager.Config(dir_path)
            pq_reader = pqtools.Reader(pq_config)
            search_column = "b"
            table_list = f"{table1_path.name}, {table2_path.name}"
            with pytest.raises(
                LookupError,
                match=f"Multiple tables with column {search_column}: {table_list}",
            ):
                pq_reader.find_table("b")
