import json
import pathlib
import tempfile

import pandas as pd

from astro.files.type import NDJSONFileType

sample_file = pathlib.Path(
    pathlib.Path(__file__).parent.parent.parent, "data/sample.ndjson"
)


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
