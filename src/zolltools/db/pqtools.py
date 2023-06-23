"""Module for reading and writing parquet files."""

import os
import math
import logging
from pathlib import Path
from typing import Tuple, Union
from types import GeneratorType

import pandas as pd
import pyarrow.parquet as pq
from ..strtools import removesuffix

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class ParquetManager:
    """Class that represents an object that interfaces with a parquet database"""

    class Config:  # pylint: disable=too-few-public-methods
        """Class that represents the configuration of a parquet manager"""

        def __init__(
            self,
            db_path: Path = Path.cwd(),
            default_target_in_memory_size: int = int(1e8),
        ):
            """
            Initializes a new Config object

            :param db_path: the path to the folder containing the parquet files
            :param default_target_in_memory_size: the default target memory size
            to use when loading chunks of large tables into memory
            """

            self.db_path = db_path
            self.default_target_in_memory_size = default_target_in_memory_size
            self.tmp_path = db_path.joinpath("tmp")
            if not self.tmp_path.exists():
                os.mkdir(self.tmp_path)
            logger.debug(
                "ParquetManagerConfig.__init__: new configuration created %s",
                repr(self),
            )

        def __str__(self):
            """Returns a user-friendly string representation"""

            return (
                f"Config\n\tDatabase: {self.db_path}\n\t"
                f"Temporary: {self.tmp_path}\n\t"
                f"Target Memory: {self.default_target_in_memory_size}"
            )

        def __repr__(self):
            """
            Returns a developer-friendly string representation. Format is
            "Config({db_path}, {tmp_path}, {default_target_in_memory_size})"
            """

            return (
                f"Config({self.db_path}, {self.tmp_path}, "
                f"{self.default_target_in_memory_size})"
            )

    def __init__(self, config: Config):
        """
        Initializes a new ParquetManager with a configuration

        :param config: the configuration to use
        """

        self.config = config

    def _get_dir_path(self, tmp: bool):
        """
        Returns the directory path depending on whether `tmp` (temporary
        directory) is selected or not.

        :param tmp: When `False`, returns `self.config.db_path`, otherwise
        returns `self.config.tmp_path`
        :returns: path to directory
        """

        return self.config.tmp_path if tmp else self.config.db_path

    def _get_parquet_file(self, pq_name: str, tmp=True) -> Tuple[pq.ParquetFile, Path]:
        """
        Returns the parquet file object and Path object given the name (w/o the
        file extension) of a parquet file.

        :param pq_name: name (w/o file extension) of the file to read
        :param tmp: bool of whether to read a temporary file or an original
        database file
        :returns: (pq.ParquetFile, Path)
        """

        file_name = f"{pq_name}.parquet"
        dir_path = self._get_dir_path(tmp)
        file_path = dir_path.joinpath(file_name)
        logger.debug(
            "ParquetManager._get_parquet_file: returned path and "
            "pyarrow.parquet.ParquetFile for %s",
            file_path,
        )

        return (pq.ParquetFile(file_path), file_path)

    def _calc_chunk_size(
        self, pq_name: str, tmp=True, target_in_memory_size=None
    ) -> int:
        """
        Returns the optimal chunk size for reading a parquet file.
        Estimates the number of rows that will keep the in-memory size of the
        chunk close to the target_in_memory_size (or
        `default_target_in_memory_size` if the value is not provided).

        :param pq_name: the file to calculate chunk size for
        :param tmp: the directory the file is in
        :param target_in_memory_size: the target in-memory size for the chunk
        :returns: the integer number of rows that should be included in each
        chunk to get the in-memory size of the chunk close to the
        `target_in_memory_size`
        """

        log_prefix = "ParquetManager._calc_chunk_size"

        if target_in_memory_size is None:
            target_in_memory_size = self.config.default_target_in_memory_size
        pq_file, _ = self._get_parquet_file(pq_name, tmp=tmp)
        pq_iter = pq_file.iter_batches(batch_size=1)
        row = next(pq_iter).to_pandas()
        size = row.memory_usage(index=True, deep=True).sum()
        num_rows = math.floor(target_in_memory_size / size)
        logger.debug(
            "%s: calculated chunk size to be %d rows",
            log_prefix,
            num_rows,
        )
        return num_rows

    def get_tables(self, tmp=True) -> list:
        """
        Returns a list of the names of parquet tables in the database.
        The default search location is the temporary tables, but that can
        be changed with the `tmp` parameter

        :param tmp: True to search temporary tables, False to search the
        primary database
        :returns: a list of names (str)
        """

        dir_path = self._get_dir_path(tmp)
        logger.debug("ParquetManager.get_tables: reading from %s", dir_path)
        return sorted(
            [
                removesuffix(path.name, ".parquet")
                for path in list(dir_path.glob("*.parquet"))
            ]
        )

    def get_table_paths(self, tmp=True) -> list:
        """
        Returns a list of parquet files as Path objects. The tmp parameter
        determines what directory will be searched.

        :param tmp: determines which directory will be searched
        ;returns: list of Path objects
        """

        dir_path = self._get_dir_path(tmp)
        logger.debug("ParquetManager.get_table_paths: reading from %s", dir_path)
        return list(dir_path.glob("*.parquet"))


class Reader(ParquetManager):
    """Class to read and interface with a database of parquet files"""

    @staticmethod
    def _pq_generator(pq_iter) -> pd.DataFrame:
        """
        Generator that wrap an iterator for reading a parquet file in chunks.
        The generator returns pandas.DataFrame objects instead of parquet
        batches.

        :param pq_iter: the iterator for the parquet file to wrap
        """

        for batch in pq_iter:
            yield batch.to_pandas()

    def get_metadata(self):
        """Docstring"""

        return None

    def get_table(
        self, pq_name: str, tmp: bool, columns: Union[list[str], None] = None
    ) -> pd.DataFrame:
        """
        Gets a table (`pq_name`) from the database.

        :param pq_name: the name (w/o file extension) of the file to read
        :param tmp: whether to get the table from temporary storage or from the
        data directory
        :param columns: the columns to read from the table
        :returns: a data frame representing the table that was read
        """

        pq_file, _ = self._get_parquet_file(pq_name, tmp)
        table = pq_file.read(columns=columns, use_pandas_metadata=True)

        return table.to_pandas()

    def get_reader(
        self, pq_name: str, columns=None, target_in_memory_size=None, tmp=True
    ) -> GeneratorType:
        """
        Returns an iterator that yields data frames of chunks of the input file.

        :param pq_name: name (w/o file extension) of the file to read
        :param columns: columns of the table to read. If set to None, all
        columns will be read
        :param target_in_memory_size: target size for each chunk (data frame) in
        memory
        :param tmp: whether to get the table from temporary storage or from the
        data directory
        """

        if target_in_memory_size is None:
            target_in_memory_size = self.config.default_target_in_memory_size
        chunk_size = self._calc_chunk_size(pq_name, tmp=tmp)
        pq_file, pq_path = self._get_parquet_file(pq_name, tmp)
        pq_iter = pq_file.iter_batches(batch_size=chunk_size, columns=columns)
        logger.info(
            "Reader.get_reader: Returning reader for %s with chunk size %d",
            pq_path,
            chunk_size,
        )

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
        logger.info("Writer.save: saved %s to %s", name, parquet_path)
        return (name, parquet_path)

    def remove(self, pq_name) -> bool:
        """
        Removes a temporary table if it exists. Returns success of the
        operation.

        :param pq_name: name (w/o file extension) of the file to read
        :returns: bool indicating success of the removal
        """

        log_prefix = "Writer.remove"

        _, pq_path = self._get_parquet_file(pq_name, tmp=True)
        if pq_path.exists():
            os.remove(pq_path)
            logger.info("%s: removed %s", log_prefix, pq_name)
            return True
        logger.info("%s: %s could not be found at %s", log_prefix, pq_name, pq_path)
        return False
