"""Module for reading and managing parquet files in a database."""

import math
from pathlib import Path
import os
from types import GeneratorType
import pyarrow.parquet as pq


class Reader:
    """Class to read and interface with a database of parquet files"""

    def __init__(
        self, db_path: Path = Path.cwd(), default_target_in_memory_size: int = int(1e8)
    ):
        """Initializes a new Reader object"""
        self.db_path = db_path
        self.default_target_in_memory_size = default_target_in_memory_size
        self.tmp_path = db_path.joinpath("tmp")
        if not self.tmp_path.exists():
            os.mkdir(self.tmp_path)

    def _get_parquet_file(self, pq_name: str) -> pq.ParquetFile:
        """Returns the parquet file object given a path to a parquet file"""
        return pq.ParquetFile(self.db_path.joinpath(f"{pq_name}.parquet"))

    def _calc_chunk_size(self, pq_name: str, target_in_memory_size=None) -> int:
        """Returns the optimal chunk size for reading a parquet file.
        Estimates the number of rows that will keep the in-memory size of the chunk
        close to the target_in_memory_size (or default_target_in_memory_size if the
        value is not provided).
        """
        if target_in_memory_size is None:
            target_in_memory_size = self.default_target_in_memory_size
        pq_file = self._get_parquet_file(pq_name)
        pq_iter = pq_file.iter_batches(batch_size=1)
        row = next(pq_iter).to_pandas()
        size = row.memory_usage(index=True, deep=True).sum()
        return math.floor(target_in_memory_size / size)

    @staticmethod
    def _pq_generator(pq_iter):
        """Generator that wrap an iterator for reading a parquet file in chunks.
        The generator returns pandas.DataFrame objects instead of parquet batches."""
        for batch in pq_iter:
            yield batch.to_pandas()

    def get_reader(self, pq_name: str, target_in_memory_size=None) -> GeneratorType:
        """Returns an iterator that yields data frames of chunks of the input file."""
        if target_in_memory_size is None:
            target_in_memory_size = self.default_target_in_memory_size
        chunk_size = self._calc_chunk_size(pq_name)
        pq_file = self._get_parquet_file(pq_name)
        pq_iter = pq_file.iter_batches(batch_size=chunk_size)

        return Reader._pq_generator(pq_iter)
