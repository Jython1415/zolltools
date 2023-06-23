"""Tests for pqtools.py"""

import os
import contextlib
from pathlib import Path
from typing import Tuple

import pytest
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from zolltools.db import pqtools


@contextlib.contextmanager
def temporary_parquet_table_context_manager(frame: pd.DataFrame) -> Path:
    """Docstring"""

    table_id = str(pd.util.hash_pandas_object(frame).sum() % 1_000_000).zfill(6)
    temporary_directory = Path.cwd().joinpath(f"tmp_parquet_table_{table_id}")
    temporary_directory.mkdir(exist_ok=True)
    table = pa.Table.from_pandas(frame)
    table_path = temporary_directory.joinpath(f"{table_id}.parquet")
    pq.write_table(table, table_path)
    try:
        yield table_path
    finally:
        os.remove(table_path)
        temporary_directory.rmdir()


@pytest.fixture(scope="module")
def tmp_table_3x3() -> Tuple[Path, pd.DataFrame]:
    """docstring"""

    frame = pd.DataFrame(np.arange(1, 10).reshape(3, 3))
    with temporary_parquet_table_context_manager(frame) as table_path:
        yield (table_path, frame)


def test_get_table(
    tmp_table_3x3: Tuple[Path, pd.DataFrame]
):  # pylint: disable=redefined-outer-name
    """docstring"""

    table_path, frame = tmp_table_3x3
    data_dir = table_path.parent
    pq_config = pqtools.ParquetManager.Config(data_dir)
    pq_reader = pqtools.Reader(pq_config)
    loaded_frame = pq_reader.get_table(  # pylint: disable=assignment-from-no-return
        table_path.name.removesuffix(".parquet"), tmp=False
    )
    pd.testing.assert_frame_equal(frame, loaded_frame)
