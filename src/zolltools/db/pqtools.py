"""Module for reading and writing parquet files."""

import os
import math
import logging
from typing import Union
from pathlib import Path
from types import GeneratorType

import pandas as pd
import pyarrow.parquet as pq

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
        ) -> None:
            """
            Initializes a new Config object

            :param db_path: the path to the folder containing the parquet files
            :param default_target_in_memory_size: the default target memory size
            to use when loading chunks of large tables into memory
            """

            self.db_path = db_path
            self.default_target_in_memory_size = default_target_in_memory_size
            logger.debug(
                "ParquetManagerConfig.__init__: new configuration created %s",
                repr(self),
            )

        def __str__(self):
            """Returns a user-friendly string representation"""

            return (
                f"Config\n\tDatabase: {self.db_path}\n\t"
                f"Target Memory: {self.default_target_in_memory_size}"
            )

        def __repr__(self):
            """
            Returns a developer-friendly string representation. Format is
            "Config({db_path}, {default_target_in_memory_size})"
            """

            return f"Config({self.db_path}, {self.default_target_in_memory_size})"

    def __init__(self, config: Config):
        """
        Initializes a new ParquetManager with a configuration

        :param config: the configuration to use
        """

        self.config = config

    def _calc_row_size(self, pq_file: pq.ParquetFile) -> int:
        """
        Returns the size (in bytes) of a row of a parquet file when converted to
        a pandas data frame. A single row of the parquet file (`pq_file`) is
        read into memory for this calculation.

        :param pq_file: the parquet file to read a row of.
        :returns: the size of the row in memory as a row of a pandas data frame
        """

        pq_iter = pq_file.iter_batches(batch_size=1)
        row = next(pq_iter).to_pandas()
        return row.memory_usage(index=True, deep=True).sum()

    def _calc_file_size(self, pq_file: pq.ParquetFile) -> int:
        """
        Returns the size (in bytes) of a parquet file in memory when it is
        converted to a pandas data frame. Only a single row of the parquet file
        is loaded into memory for this calculation.

        :param pq_file: the parquet file to estimate the size of.
        :returns: the estimated size of the parquet file in memory as a pandas
        data frame
        """

        row_size = self._calc_row_size(pq_file)
        num_rows = pq_file.metadata.num_rows
        return row_size * num_rows

    def _calc_chunk_size(self, file: Path, target_in_memory_size=None) -> int:
        """
        Returns the optimal chunk size for reading a parquet file.
        Estimates the number of rows that will keep the in-memory size of the
        chunk close to the target_in_memory_size (or
        `default_target_in_memory_size` if the value is not provided).

        :param file: the file to calculate chunk size for
        :param target_in_memory_size: the target in-memory size for the chunk
        :returns: the integer number of rows that should be included in each
        chunk to get the in-memory size of the chunk close to the
        `target_in_memory_size`
        """

        log_prefix = "ParquetManager._calc_chunk_size"

        if target_in_memory_size is None:
            target_in_memory_size = self.config.default_target_in_memory_size
        pq_file = pq.ParquetFile(file)
        size = self._calc_row_size(pq_file)
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

        pq_file = pq.ParquetFile(file)
        return [column.name for column in pq_file.schema]

    def get_tables(self) -> list[Path]:
        """
        Returns a list of the names of parquet tables in the database.

        :returns: a list of files.
        """

        dir_path = self.config.db_path
        logger.debug("ParquetManager.get_tables: reading from %s", dir_path)
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
        self, file: Path, columns: Union[list[str], None] = None
    ) -> pd.DataFrame:
        """
        Gets a table from the database.

        :param file: the file to read
        :param columns: the columns to read from the table
        :returns: a data frame representing the table that was read
        """

        pq_file = pq.ParquetFile(file)
        estimated_file_size = self._calc_file_size(file)
        file_size_limit = self.config.default_target_in_memory_size
        if estimated_file_size > file_size_limit:
            raise MemoryError(
                f"Estimated file size, {estimated_file_size}, is greater than "
                f"the file size limit, {file_size_limit}."
            )
        table = pq_file.read(columns=columns, use_pandas_metadata=True)

        return table.to_pandas()

    def get_reader(
        self, file: Path, columns=None, target_in_memory_size=None
    ) -> GeneratorType:
        """
        Returns an iterator that yields data frames of chunks of the input file.

        :param file: the file to read.
        :param columns: columns of the table to read. If set to None, all
        columns will be read
        :param target_in_memory_size: target size for each chunk (data frame) in
        memory
        """

        if target_in_memory_size is None:
            target_in_memory_size = self.config.default_target_in_memory_size
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
    """Module for writing and erasing parquet files to the database"""

    def save(self, frame: pd.DataFrame, file: Path) -> Path:
        """
        Saves a data frame and returns the path to the file.

        :param frame: pandas data frame to save
        :param file: the path to save the file table to.
        :returns: (name, Path object pointing to file)
        """

        parquet_path = file
        frame.to_parquet(parquet_path, engine="fastparquet", index=False)
        logger.info("Writer.save: saved %s", parquet_path)
        return parquet_path

    def remove(self, file: Path) -> bool:
        """
        Removes a table if it exists. Returns success of the
        operation.

        :param file: the file to remove.
        :returns: bool indicating success of the removal
        """

        log_prefix = "Writer.remove"

        if file.exists():
            os.remove(file)
            logger.info("%s: removed %s", log_prefix, file)
            return True
        logger.info("%s: %s could not be found", log_prefix, file)
        return False
