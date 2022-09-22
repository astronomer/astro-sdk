import pathlib
import tempfile

import pandas as pd
from astro.files.types import JSONFileType

sample_file = pathlib.Path(
    pathlib.Path(__file__).parent.parent.parent, "data/sample.json"
)


def test_read_json_file():
    """Test reading of json file from local location"""
    path = str(sample_file.absolute())
    json_type = JSONFileType(path)
    with open(path) as file:
        df = json_type.export_to_dataframe(file)
    assert df.shape == (3, 2)


def test_write_json_file():
    """Test writing of json file from local location"""
    with tempfile.NamedTemporaryFile() as temp_file:
        path = temp_file.name
        data = {
            "id": [1, 2, 3],
            "name": ["First", "Second", "Third with unicode पांचाल"],
        }
        df = pd.DataFrame(data=data)

        json_type = JSONFileType(path)
        json_type.create_from_dataframe(stream=temp_file, df=df)
        assert pd.read_json(path).shape == (3, 2)
