"""Module for reading parquet files in a database."""

import math
from pathlib import Path
import os
from types import GeneratorType
import pyarrow.parquet as pq
import pandas as pd
from ..strtools import removesuffix


class ParquetManagerConfig:
    """Class that represents the configuration of a parquet manager"""

    def __init__(
        self, db_path: Path = Path.cwd(), default_target_in_memory_size: int = int(1e8)
    ):
        """Initializes a new Reader object"""
        self.db_path = db_path
        self.default_target_in_memory_size = default_target_in_memory_size
        self.tmp_path = db_path.joinpath("tmp")
        if not self.tmp_path.exists():
            os.mkdir(self.tmp_path)


class ParquetManager:
    """Class that represents an object that interfaces with a parquet database"""

    def __init__(self, config: ParquetManagerConfig):
        self.config = config

    def _get_parquet_file(self, pq_name: str, tmp=False) -> tuple:
        """
        Returns the parquet file object and Path object given the name (w/o the file extension)
        of a parquet file.

        :param pq_name: name (w/o file extension) of the file to read
        :param tmp: bool of whether to read a temporary file or an original database file
        :returns: (pq.ParquetFile, Path)
        """
        file_name = f"{pq_name}.parquet"
        if tmp:
            file_path = self.config.tmp_path.joinpath(file_name)
        else:
            file_path = self.config.db_path.joinpath(file_name)

        return (pq.ParquetFile(file_path), file_path)

    def _calc_chunk_size(self, pq_name: str, target_in_memory_size=None) -> int:
        """Returns the optimal chunk size for reading a parquet file.
        Estimates the number of rows that will keep the in-memory size of the chunk
        close to the target_in_memory_size (or default_target_in_memory_size if the
        value is not provided).
        """
        if target_in_memory_size is None:
            target_in_memory_size = self.config.default_target_in_memory_size
        pq_file, _ = self._get_parquet_file(pq_name)
        pq_iter = pq_file.iter_batches(batch_size=1)
        row = next(pq_iter).to_pandas()
        size = row.memory_usage(index=True, deep=True).sum()
        return math.floor(target_in_memory_size / size)

    def get_tables(self, tmp=True) -> list:
        """
        Returns a list of the names of parquet tables in the database.
        The default search location is the temporary tables, but that can
        be changed with the `tmp` parameter

        :param tmp: True to search temporary tables, False to search the
        primary database
        :returns: a list of names (str)
        """

        dir_path = self.config.tmp_path if tmp else self.config.db_path
        return sorted(
            [
                removesuffix(path.name, ".parquet")
                for path in list(dir_path.glob("*.parquet"))
            ]
        )

    def get_table_paths(self, tmp=False) -> list:
        """Returns a list of parquet files as Path objects. The tmp parameter determines
        what directory will be searched.
        """
        if tmp:
            dir_path = self.config.tmp_path
        else:
            dir_path = self.config.db_path
        return list(dir_path.glob("*.parquet"))


class Reader(ParquetManager):
    """Class to read and interface with a database of parquet files"""

    @staticmethod
    def _pq_generator(pq_iter) -> pd.DataFrame:
        """Generator that wrap an iterator for reading a parquet file in chunks.
        The generator returns pandas.DataFrame objects instead of parquet batches.
        """
        for batch in pq_iter:
            yield batch.to_pandas()

    def get_reader(
        self, pq_name: str, columns=None, target_in_memory_size=None, tmp=False
    ) -> GeneratorType:
        """
        Returns an iterator that yields data frames of chunks of the input file.

        :param pq_name: name (w/o file extension) of the file to read
        :param columns: columns of the table to read. If set to None, all columns will be read
        :param target_in_memory_size: target size for each chunk (data frame) in memory
        """
        if target_in_memory_size is None:
            target_in_memory_size = self.config.default_target_in_memory_size
        chunk_size = self._calc_chunk_size(pq_name)
        pq_file, _ = self._get_parquet_file(pq_name, tmp)
        pq_iter = pq_file.iter_batches(batch_size=chunk_size, columns=columns)

        return Reader._pq_generator(pq_iter)


class Writer(ParquetManager):
    """Module for writing and erasing parquet files to the database"""

    def save(self, frame: pd.DataFrame, name: str) -> tuple:
        """
        Saves a data frame to the temporary storage folder, and returns the name
        and path to the file.

        :param frame: pandas data frame to save
        :param name: name for the temporary table
        :returns: (name, Path object pointing to file)
        """
        parquet_path = self.config.tmp_path.joinpath(f"{name}.parquet")
        frame.to_parquet(parquet_path, engine="fastparquet", index=False)
        return (removesuffix(parquet_path.name, ".parquet"), parquet_path)

    def remove(self, pq_name) -> bool:
        """
        Removes a temporary table if it exists. Returns success of the operation.

        :param pq_name: name (w/o file extension) of the file to read
        :returns: bool indicating success of the removal
        """

        _, pq_path = self._get_parquet_file(pq_name, tmp=True)
        if pq_path.exists():
            os.remove(pq_path)
            return True
        return False
