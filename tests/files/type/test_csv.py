import pathlib
import tempfile

import pandas as pd

from astro.files.type import CSVFileType

sample_file = pathlib.Path(
    pathlib.Path(__file__).parent.parent.parent, "data/sample.csv"
)


def test_read_csv_file():
    path = str(sample_file.absolute())
    csv_type = CSVFileType(path)
    with open(path) as file:
        df = csv_type.read_to_dataframe(file, normalize_config=None)
    assert df.shape == (3, 2)


def test_write_csv_file():
    with tempfile.NamedTemporaryFile() as temp_file:
        path = temp_file.name
        data = {
            "id": [1, 2, 3],
            "name": ["First", "Second", "Third with unicode पांचाल"],
        }
        df = pd.DataFrame(data=data)

        csv_type = CSVFileType(path)
        csv_type.write_from_dataframe(stream=temp_file, df=df)
        assert pd.read_csv(path).shape == (3, 2)
