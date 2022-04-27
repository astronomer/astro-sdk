import pathlib

import pytest

from astro.constants import FileType
from astro.files import File
from astro.files.type import get_filetype

sample_file = pathlib.Path(pathlib.Path(__file__).parent.parent, "data/sample.csv")
sample_filepaths_per_filetype = [
    (FileType.CSV, "sample.csv"),
    (FileType.JSON, "sample.json"),
    (FileType.NDJSON, "sample.ndjson"),
    (FileType.PARQUET, "sample.parquet"),
]
sample_filetypes = [items[0] for items in sample_filepaths_per_filetype]
sample_filepaths = [items[1] for items in sample_filepaths_per_filetype]
sample_filepaths_ids = [items[0].value for items in sample_filepaths_per_filetype]


def test_get_size():
    assert File(sample_file).get_size() == 65


def describe_get_filetype():
    @pytest.mark.parametrize(
        "expected_filetype,filepath",
        sample_filepaths_per_filetype,
        ids=sample_filepaths_ids,
    )
    def with_supported_filetypes(expected_filetype, filepath):
        assert get_filetype(filepath) == expected_filetype

    def with_unsupported_filetype_raises_exception():
        unsupported_filetype = "sample.inexistent"
        with pytest.raises(ValueError) as exc_info:
            get_filetype(unsupported_filetype)
        expected_msg = (
            "Unsupported filetype 'inexistent' from file 'sample.inexistent'."
        )
        assert exc_info.value.args[0] == expected_msg
