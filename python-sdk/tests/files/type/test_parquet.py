import pathlib
import tempfile
from unittest import mock

import pandas as pd

from astro.dataframes.load_options import PandasLoadOptions, PandasParquetLoadOptions
from astro.dataframes.pandas import PandasDataframe
from astro.files.types import ParquetFileType
from astro.settings import NEED_CUSTOM_SERIALIZATION

sample_file = pathlib.Path(pathlib.Path(__file__).parent.parent.parent, "data/sample.parquet")


def test_read_parquet_file():
    """Test reading of parquet file from local location"""
    path = str(sample_file.absolute())
    parquet_type = ParquetFileType(path)
    with open(path, mode="rb") as file:
        df = parquet_type.export_to_dataframe(file)
    assert df.shape == (3, 2)
    if NEED_CUSTOM_SERIALIZATION:
        assert isinstance(df, PandasDataframe)
    else:
        assert isinstance(df, pd.DataFrame)


@mock.patch("astro.files.types.parquet.ParquetFileType._convert_remote_file_to_byte_stream")
@mock.patch("astro.files.types.parquet.pd.read_parquet")
def test_read_parquet_file_with_pandas_opts(mock_read_parquet, mock_file_to_byte):
    """Test pandas option get pass to read_parquet"""
    path = str(sample_file.absolute())
    parquet_type = ParquetFileType(path, load_options=PandasLoadOptions(columns=["col1"]))
    stream = b"12345"
    mock_file_to_byte.return_value = stream
    with open(path, mode="rb") as file:
        parquet_type.export_to_dataframe(file)
    mock_read_parquet.assert_called_once_with(stream, columns=["col1"])


@mock.patch("astro.files.types.parquet.ParquetFileType._convert_remote_file_to_byte_stream")
@mock.patch("astro.files.types.parquet.pd.read_parquet")
def test_read_parquet_file_with_pandas_opts_with_deprecated_load_options(
    mock_read_parquet, mock_file_to_byte
):
    """Test pandas option get pass to read_parquet"""
    path = str(sample_file.absolute())
    parquet_type = ParquetFileType(path, load_options=PandasParquetLoadOptions(columns=["col1"]))
    stream = b"12345"
    mock_file_to_byte.return_value = stream
    with open(path, mode="rb") as file:
        parquet_type.export_to_dataframe(file)
    mock_read_parquet.assert_called_once_with(stream, columns=["col1"])


def test_write_parquet_file():
    """Test writing of parquet file from local location"""
    with tempfile.NamedTemporaryFile() as temp_file:
        path = temp_file.name
        data = {
            "id": [1, 2, 3],
            "name": ["First", "Second", "Third with unicode पांचाल"],
        }
        df = pd.DataFrame(data=data)

        parquet_type = ParquetFileType(path)
        parquet_type.create_from_dataframe(stream=temp_file, df=df)
        assert pd.read_parquet(temp_file).shape == (3, 2)
