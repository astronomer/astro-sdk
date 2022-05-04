import pathlib
from typing import Dict, Type, Union

from astro.constants import FileType as FileTypeConstants
from astro.files.type.base import FileType
from astro.files.type.csv import CSVFileType
from astro.files.type.json import JSONFileType
from astro.files.type.ndjson import NDJSONFileType
from astro.files.type.parquet import ParquetFileType


def create_file_type(path: str, filetype: Union[FileTypeConstants, None] = None):
    filetype_to_class: Dict[FileTypeConstants, Type[FileType]] = {
        FileTypeConstants.CSV: CSVFileType,
        FileTypeConstants.JSON: JSONFileType,
        FileTypeConstants.NDJSON: NDJSONFileType,
        FileTypeConstants.PARQUET: ParquetFileType,
    }
    if not filetype:
        filetype = get_filetype(path)

    try:
        return filetype_to_class[filetype](path)
    except KeyError:
        raise ValueError(
            f"Non supported file type provided {filetype}, file_type should be among {', '.join(FileTypeConstants)}."
        )


def get_filetype(filepath: Union[str, pathlib.PosixPath]) -> FileTypeConstants:
    """
    Return a FileType given the filepath. Uses a naive strategy, using the file extension.

    :param filepath: URI or Path to a file
    :type filepath: str or pathlib.PosixPath
    :return: The filetype (e.g. csv, ndjson, json, parquet)
    :rtype: astro.constants.FileType
    """
    if isinstance(filepath, pathlib.PosixPath):
        extension = filepath.suffix[1:]
    else:
        extension = filepath.split(".")[-1]

    try:
        return FileTypeConstants(extension)
    except ValueError:
        raise ValueError(f"Unsupported filetype '{extension}' from file '{filepath}'.")
