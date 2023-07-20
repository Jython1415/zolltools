"""Module for reading and writing parquet files."""

import os
import math
import logging
from pathlib import Path
from typing import Optional, Generator

import pandas as pd
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class ParquetManager:
    """
    Class for interfacing with a directory of parquet tables.
    """

    class Config:  # pylint: disable=too-few-public-methods
        """
        Class that represents the configuration of a parquet manager.
        """

        def __init__(
            self,
            dir_path: Path = Path.cwd(),
            default_target_in_memory_size: int = int(1e8),
            enforce_directory: bool = True,
        ) -> None:
            """
            Initializes a new Config object.

            :param dir_path: the path to the directory containing the parquet
            files.
            :param default_target_in_memory_size: the default target memory size
            to use when loading chunks of large tables into memory.
            :param enforce_directory: whether to raise errors when a method
            attempts to access a file outside of `dir_path`.
            """

            self.dir_path = dir_path
            self.default_target_in_memory_size = default_target_in_memory_size
            self.enforce_directory = enforce_directory
            logger.debug(
                "ParquetManagerConfig.__init__: new configuration created %s",
                repr(self),
            )

        def __str__(self):
            """
            Returns a user-friendly string representation.
            """

            return (
                f"Config\n\tDirectory: {self.dir_path}\n\t"
                f"Target Memory: {self.default_target_in_memory_size}"
            )

        def __repr__(self):
            """
            Returns a developer-friendly string representation. Format is
            "Config({dir_path}, {default_target_in_memory_size})".
            """

            return f"Config({self.dir_path}, {self.default_target_in_memory_size})"

    def __init__(self, config: Config):
        """
        Initializes a new ParquetManager with a configuration.

        :param config: the configuration to use.
        """

        self.config = config

    def _enforce_directory(self, file: Path) -> None:
        if self.config.enforce_directory and file.parent != self.config.dir_path:
            raise ValueError(f"{file} is not in the directory {self.config.dir_path}")

    def _calc_row_size(self, file: Path) -> int:
        """
        Returns the size (in bytes) of a row of a parquet file when converted to
        a pandas data frame. A single row of the parquet file (`pq_file`) is
        read into memory for this calculation.

        :param pq_file: the parquet file to read a row of.
        :returns: the size of the row in memory as a row of a pandas data frame.
        """

        pq_file = pq.ParquetFile(file)
        pq_iter = pq_file.iter_batches(batch_size=1)
        row = next(pq_iter).to_pandas()
        return row.memory_usage(index=True, deep=True).sum()

    def _calc_file_size(self, file: Path) -> int:
        """
        Returns the size (in bytes) of a parquet file in memory when it is
        converted to a pandas data frame. Only a single row of the parquet file
        is loaded into memory for this calculation.

        :param pq_file: the parquet file to estimate the size of.
        :returns: the estimated size of the parquet file in memory as a pandas
        data frame.
        """

        row_size = self._calc_row_size(file)
        pq_file = pq.ParquetFile(file)
        num_rows = pq_file.metadata.num_rows
        return row_size * num_rows

    def _calc_chunk_size(
        self, file: Path, target_in_memory_size: Optional[int] = None
    ) -> int:
        """
        Returns the optimal chunk size for reading a parquet file.
        Estimates the number of rows that will keep the in-memory size of the
        chunk close to the `target_in_memory_size` (or
        `default_target_in_memory_size` if the value is not provided).

        :param file: the file to calculate chunk size for.
        :param target_in_memory_size: the target in-memory size for the chunk.
        :returns: the number of rows that should be included in each
        chunk to get the in-memory size of the chunk close to the
        `target_in_memory_size`.
        """

        log_prefix = "ParquetManager._calc_chunk_size"

        if target_in_memory_size is None:
            target_in_memory_size = self.config.default_target_in_memory_size
        assert target_in_memory_size is not None
        size = self._calc_row_size(file)
        num_rows = math.floor(target_in_memory_size / size)
        logger.debug(
            "%s: calculated chunk size to be %d rows",
            log_prefix,
            num_rows,
        )
        return num_rows

    def get_columns(self, file: Path) -> list[str]:
        """
        Returns the columns in a parquet table.

        :param file: the file to get columns for.
        :returns: the columns in the table.
        """

        self._enforce_directory(file)
        pq_file = pq.ParquetFile(file)
        return [column.name for column in pq_file.schema]

    def get_tables(self) -> list[Path]:
        """
        Returns a list of the paths of parquet tables in the directory.

        :returns: a list of files.
        """

        dir_path = self.config.dir_path
        logger.debug("ParquetManager.get_tables: reading from %s", dir_path)
        return list(dir_path.glob("*.parquet"))


class Reader(ParquetManager):
    """
    Class to read and interface with a directory of parquet files.
    """

    @staticmethod
    def _pq_generator(pq_iter) -> pd.DataFrame:
        """
        Generator that wrap an iterator for reading a parquet file in chunks.
        The generator returns pandas.DataFrame objects instead of parquet
        batches.

        :param pq_iter: the iterator for the parquet file to wrap.
        """

        for batch in pq_iter:
            yield batch.to_pandas()

    def get_metadata(self) -> None:
        """Docstring"""

        return None

    def get_table(
        self,
        file: Path,
        columns: Optional[list[str]] = None,
        suppress_error: bool = False,
    ) -> pd.DataFrame:
        """
        Gets a table from the directory.

        :param file: the file to read.
        :param columns: the columns to read from the table. Default is None, and
        all columns will be read.
        :param suppress_error: set to True to suppress the warning for memory
        usage.
        :returns: a data frame representing the table that was read.
        """

        self._enforce_directory(file)
        pq_file = pq.ParquetFile(file)
        estimated_file_size = self._calc_file_size(pq_file)
        file_size_limit = self.config.default_target_in_memory_size
        if not suppress_error and estimated_file_size > file_size_limit:
            raise MemoryError(
                f"Estimated file size, {estimated_file_size}, is greater than "
                f"the file size limit, {file_size_limit}."
            )
        table = pq_file.read(columns=columns, use_pandas_metadata=True)

        return table.to_pandas()

    def get_reader(
        self, file: Path, columns=None, target_in_memory_size: Optional[int] = None
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Returns an iterator that yields data frames of chunks of the input file.

        :param file: the file to read.
        :param columns: columns of the table to read. If set to None, all
        columns will be read.
        :param target_in_memory_size: target size for each chunk (data frame) in
        memory.
        """

        self._enforce_directory(file)
        if target_in_memory_size is None:
            target_in_memory_size = self.config.default_target_in_memory_size
        assert target_in_memory_size is not None
        chunk_size = self._calc_chunk_size(file)
        pq_file = pq.ParquetFile(file)
        pq_iter = pq_file.iter_batches(batch_size=chunk_size, columns=columns)
        logger.info(
            "Reader.get_reader: Returning reader for %s with chunk size %d",
            file,
            chunk_size,
        )

        return Reader._pq_generator(pq_iter)


class Writer(ParquetManager):
    """
    Module for writing and erasing parquet files to the directory.
    """

    def save(self, frame: pd.DataFrame, file: Path) -> None:
        """
        Saves a data frame and returns the path to the file.

        :param frame: pandas data frame to save.
        :param file: the path to save the file table to.
        """

        self._enforce_directory(file)
        frame.to_parquet(file, engine="fastparquet", index=False)
        logger.info("Writer.save: saved %s", file)

    def remove(self, file: Path) -> bool:
        """
        Removes a table if it exists.

        :param file: the file to remove.
        :returns: True if successful, False otherwise.
        """

        log_prefix = "Writer.remove"

        self._enforce_directory(file)
        if file.exists():
            os.remove(file)
            logger.info("%s: removed %s", log_prefix, file)
            return True
        logger.info("%s: %s could not be found", log_prefix, file)
        return False
