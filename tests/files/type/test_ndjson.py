import json
import pathlib
import tempfile

import pandas as pd

from astro.files.type import NdJson

sample_file = pathlib.Path(
    pathlib.Path(__file__).parent.parent.parent, "data/sample.ndjson"
)


def test_read_ndjson_file():
    path = str(sample_file.absolute())
    json_type = NdJson(path)
    with open(path) as file:
        df = json_type.read(file, normalize_config=None)
    assert df.shape == (3, 2)


def test_write_ndjson_file():
    with tempfile.NamedTemporaryFile() as temp_file:
        path = temp_file.name
        data = {
            "id": [1, 2, 3],
            "name": ["First", "Second", "Third with unicode पांचाल"],
        }
        df = pd.DataFrame(data=data)

        json_type = NdJson(path)
        json_type.write(stream=temp_file, df=df)

        with open(temp_file.name) as file:
            for count, line in enumerate(file):
                assert len(json.loads(line).keys()) == 2
        # Since count starts from 0.
        assert (count + 1) == 3
