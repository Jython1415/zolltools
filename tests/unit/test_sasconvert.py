"""Tests for sasconvert.py"""

from pathlib import Path
import hypothesis as hp
import hypothesis.strategies as st

from zolltools.sasconvert import Converter  # type: ignore


@hp.given(parquet_file_name=st.text())
def test_get_sas_path_with_name(parquet_file_name: str) -> None:
    """_get_sas_path test with a file name"""

    if "/" not in parquet_file_name:
        assert (
            Converter._get_sas_path(  # pylint: disable=W0212
                Path.cwd().joinpath(f"{parquet_file_name}.parquet")
            ).name
            == f"{parquet_file_name}.sas7bdat"
        )


@hp.given(sas_file_name=st.text())
def test_get_parquet_path_with_name(sas_file_name: str) -> None:
    """_get_parquet_path test with a file name"""

    if "/" not in sas_file_name:
        assert (
            Converter._get_parquet_path(  # pylint: disable=W0212
                Path.cwd().joinpath(f"{sas_file_name}.sas7bdat")
            ).name
            == f"{sas_file_name}.parquet"
        )
