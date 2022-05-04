import pathlib
import tempfile

import pandas as pd

from astro.files.type import Json

sample_file = pathlib.Path(
    pathlib.Path(__file__).parent.parent.parent, "data/sample.json"
)


def test_read_json_file():
    path = str(sample_file.absolute())
    json_type = Json(path)
    with open(path) as file:
        df = json_type.read_to_dataframe(file, normalize_config=None)
    assert df.shape == (3, 2)


def test_write_json_file():
    with tempfile.NamedTemporaryFile() as temp_file:
        path = temp_file.name
        data = {
            "id": [1, 2, 3],
            "name": ["First", "Second", "Third with unicode पांचाल"],
        }
        df = pd.DataFrame(data=data)

        json_type = Json(path)
        json_type.write_from_dataframe(stream=temp_file, df=df)
        assert pd.read_json(path).shape == (3, 2)
