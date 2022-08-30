import pathlib

import pytest
from astro.constants import FileType

from astro.files.types import get_filetype

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


@pytest.mark.parametrize(
    "expected_filetype,filepath",
    sample_filepaths_per_filetype,
    ids=sample_filepaths_ids,
)
def test_get_filetype_with_supported_filetypes(expected_filetype, filepath):
    """Test all the supported file types, where file type is inferred via file extension."""
    assert get_filetype(filepath) == expected_filetype


def test_get_filetype_with_path_which_is_missing_extension():
    """Test should raise an exception when file type cannot be determined via extension"""
    with pytest.raises(ValueError) as e:
        get_filetype("s3://fake-bucket/fake-object"),

        expected_error = "Missing file extension"
        assert expected_error in str(e.value)


def test_get_filetype_with_unsupported_filetype_raises_exception():
    """Test all the unsupported file types, where file type is inferred via file extension."""
    unsupported_filetype = "sample.inexistent"
    with pytest.raises(ValueError) as exc_info:
        get_filetype(unsupported_filetype)
    expected_msg = "Unsupported filetype 'inexistent' from file 'sample.inexistent'."
    assert exc_info.value.args[0] == expected_msg
