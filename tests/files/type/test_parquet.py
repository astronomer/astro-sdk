import pathlib
import tempfile

import pandas as pd

from astro.files.type import ParquetFileType

sample_file = pathlib.Path(
    pathlib.Path(__file__).parent.parent.parent, "data/sample.parquet"
)


def test_read_parquet_file():
    """Test reading of parquet file from local location"""
    path = str(sample_file.absolute())
    parquet_type = ParquetFileType(path)
    with open(path, mode="rb") as file:
        df = parquet_type.export_to_dataframe(file, normalize_config=None)
    assert df.shape == (3, 2)


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
