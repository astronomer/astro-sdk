from __future__ import annotations

import pathlib

from universal_transfer_operator.constants import FileType as FileTypeConstants
from universal_transfer_operator.datasets.file.types.base import FileTypes
from universal_transfer_operator.datasets.file.types.csv import CSVFileTypes
from universal_transfer_operator.datasets.file.types.json import JSONFileTypes
from universal_transfer_operator.datasets.file.types.ndjson import NDJsonFileTypes
from universal_transfer_operator.datasets.file.types.parquet import ParquetFileTypes


def create_file_type(
    path: str,
    filetype: FileTypeConstants | None = None,
    normalize_config: dict | None = None,
) -> FileTypes:
    """Factory method to create FileType super objects based on the file extension in path or filetype specified."""
    filetype_to_class: dict[FileTypeConstants, type[FileTypes]] = {
        FileTypeConstants.CSV: CSVFileTypes,
        FileTypeConstants.JSON: JSONFileTypes,
        FileTypeConstants.NDJSON: NDJsonFileTypes,
        FileTypeConstants.PARQUET: ParquetFileTypes,
    }
    if not filetype:
        filetype = get_filetype(path)

    try:
        return filetype_to_class[filetype](path=path, normalize_config=normalize_config)
    except KeyError:
        raise ValueError(
            f"Non supported file type provided {filetype}, file_type should be among {', '.join(FileTypeConstants)}."
        )


def get_filetype(filepath: str | pathlib.PosixPath) -> FileTypeConstants:
    """
    Return a FileType given the filepath. Uses a native strategy, using the file extension.
    :param filepath: URI or Path to a file
    :type filepath: str or pathlib.PosixPath
    :return: The filetype (e.g. csv, ndjson, json, parquet)
    :rtype: universal_transfer_operator.constants.FileType
    """
    if isinstance(filepath, pathlib.PosixPath):
        extension = filepath.suffix[1:]
    else:
        extension = ""
        tokenized_path = filepath.split(".")
        if len(tokenized_path) > 1:
            extension = tokenized_path[-1]

    if extension == "":
        raise ValueError(
            f"Missing file extension, cannot automatically determine filetype from path '{filepath}'."
            f" Please pass the 'filetype' param with the explicit filetype (e.g. csv, ndjson, etc.)."
        )

    try:
        return FileTypeConstants(extension)
    except ValueError:
        raise ValueError(f"Unsupported filetype '{extension}' from file '{filepath}'.")
