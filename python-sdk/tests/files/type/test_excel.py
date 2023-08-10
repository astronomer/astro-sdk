import pathlib
import tempfile
from unittest import mock

import pandas as pd

from astro.dataframes.load_options import PandasLoadOptions
from astro.dataframes.pandas import PandasDataframe
from astro.files.types import XLSXFileType

sample_file = pathlib.Path(pathlib.Path(__file__).parent.parent.parent, "data/sample.xlsx")


def test_read_excel_file():
    """Test reading of excel file from local location"""
    path = str(sample_file.absolute())
    excel_type = XLSXFileType(path)
    with open(path, "rb") as file:
        df = excel_type.export_to_dataframe(file)
    assert df.shape == (3, 2)
    assert isinstance(df, PandasDataframe)


@mock.patch("astro.files.types.excel.pd.read_excel")
def test_read_excel_file_with_pandas_opts(mock_read_excel):
    """Test pandas option get pass to read_excel"""
    path = str(sample_file.absolute())
    excel_type = XLSXFileType(path, load_options=PandasLoadOptions())
    with open(path, "rb") as file:
        excel_type.export_to_dataframe(file)
    mock_read_excel.assert_called_once_with(file)


def test_write_excel_file():
    """Test writing of excel file from local location"""
    with tempfile.NamedTemporaryFile() as temp_file:
        path = temp_file.name
        data = {
            "id": [1, 2, 3],
            "name": ["First", "Second", "Third with unicode पांचाल"],
        }
        df = pd.DataFrame(data=data)

        excel_type = XLSXFileType(path)
        excel_type.create_from_dataframe(stream=temp_file, df=df)
        assert pd.read_excel(path).shape == (3, 2)
