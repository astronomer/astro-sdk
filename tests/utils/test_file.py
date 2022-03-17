import pathlib
from unittest.mock import patch

import pytest

from astro.constants import FileType
from astro.utils.file import get_filetype, get_size, is_binary, is_small

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
    assert get_size(sample_file) == 65


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


def describe_is_binary():
    @pytest.mark.parametrize(
        "filetype",
        sample_filetypes[:-1],
        ids=sample_filepaths_ids[:-1],
    )
    def with_non_binary_files(filetype):
        assert not is_binary(filetype)

    def with_parquet():
        filetype = FileType.PARQUET
        assert is_binary(filetype)


def describe_is_small():
    def with_real_small_value():
        assert is_small(sample_file)

    @patch("astro.utils.file.get_size", return_value=512000)
    def with_mock_small_value(mock_get_size):
        assert is_small("pretend-file")

    @patch("astro.utils.file.get_size", return_value=512001)
    def with_mock_big_value(mock_get_size):
        assert not is_small("pretend-file")
