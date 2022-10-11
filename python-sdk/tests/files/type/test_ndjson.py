import json
import pathlib
import tempfile

import pandas as pd

from astro.files.types import NDJSONFileType

sample_file = pathlib.Path(pathlib.Path(__file__).parent.parent.parent, "data/sample.ndjson")


def test_read_ndjson_file():
    """Test reading of ndjson file from local location"""
    path = str(sample_file.absolute())
    json_type = NDJSONFileType(path)
    with open(path) as file:
        df = json_type.export_to_dataframe(file)
    assert df.shape == (3, 2)


def test_write_ndjson_file():
    """Test writing of ndjson file from local location"""
    with tempfile.NamedTemporaryFile() as temp_file:
        path = temp_file.name
        data = {
            "id": [1, 2, 3],
            "name": ["First", "Second", "Third with unicode पांचाल"],
        }
        df = pd.DataFrame(data=data)

        json_type = NDJSONFileType(path)
        json_type.create_from_dataframe(stream=temp_file, df=df)

        count = 0
        with open(temp_file.name) as file:
            for _, line in enumerate(file):
                assert len(json.loads(line).keys()) == 2
                count = count + 1
        assert count == 3


def test_ndjson_file_nrows():
    sample_file = pathlib.Path(pathlib.Path(__file__).parent.parent.parent, "data/sample.ndjson")

    file = NDJSONFileType(sample_file)
    # Case 1 : when the file have sufficient rows
    with open(sample_file) as stream:
        df = file.export_to_dataframe(stream, nrows=2)
        assert df.shape[0] == 2

    # Case 2 : when the file don't have sufficient rows
    with open(sample_file) as stream:
        df = file.export_to_dataframe(stream, nrows=5)
        assert df.shape[0] == 3

    # Case 3 : when the user don't pass nrows
    with open(sample_file) as stream:
        df = file.export_to_dataframe(stream, chunksize=10)
        assert df.shape[0] == 3


def test_the_order_of_rows_getting_loaded_ndjson_file_nrows():
    """
    Verify that the rows of a dataframe loaded from a file are top n rows.
    """
    sample_file = pathlib.Path(pathlib.Path(__file__).parent.parent.parent, "data/order_check_nrows.ndjson")
    file = NDJSONFileType(sample_file)
    with open(sample_file) as stream:
        df = file.export_to_dataframe(stream, nrows=5)
        df = df.sort_values(by="id")
        assert (df["id"] == [1, 2, 3, 4, 5]).all()

    sample_file = pathlib.Path(pathlib.Path(__file__).parent.parent.parent, "data/sample.ndjson")
    file = NDJSONFileType(sample_file)
    with open(sample_file) as stream:
        df = file.export_to_dataframe(stream, nrows=5)
        df = df.sort_values(by="id")
        assert (df["id"] == [1, 2, 3]).all()
