from __future__ import annotations

import pathlib

from astro.constants import SUPPORTED_FILE_EXTENSIONS
from astro.constants import FileType as FileTypeConstants
from astro.files.types.base import FileType
from astro.files.types.csv import CSVFileType
from astro.files.types.json import JSONFileType
from astro.files.types.ndjson import NDJSONFileType
from astro.files.types.parquet import ParquetFileType
from astro.files.types.pattern import PatternFileType


def create_file_type(
    path: str,
    filetype: FileTypeConstants | None = None,
    normalize_config: dict | None = None,
) -> FileType:
    """Factory method to create FileType super objects based on the file extension in path or filetype specified."""
    filetype_to_class: dict[FileTypeConstants, type[FileType]] = {
        FileTypeConstants.CSV: CSVFileType,
        FileTypeConstants.JSON: JSONFileType,
        FileTypeConstants.NDJSON: NDJSONFileType,
        FileTypeConstants.PARQUET: ParquetFileType,
        FileTypeConstants.PATTERN: PatternFileType,
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
    Return a FileType given the filepath. Uses a naive strategy, using the file extension.

    :param filepath: URI or Path to a file
    :type filepath: str or pathlib.PosixPath
    :return: The filetype (e.g. csv, ndjson, json, parquet)
    :rtype: astro.constants.FileType
    """

    if not isinstance(filepath, pathlib.PosixPath):
        filepath = pathlib.PosixPath(filepath)

    extension = filepath.suffix

    if extension == "":
        return FileTypeConstants("pattern")

    try:
        if extension in SUPPORTED_FILE_EXTENSIONS:
            return FileTypeConstants(extension[1:])
    except ValueError:
        raise ValueError(f"Unsupported filetype '{extension}' from file '{filepath}'.")
