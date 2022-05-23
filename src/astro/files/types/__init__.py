import pathlib
from typing import Dict, Optional, Type, Union

from astro.constants import FileType as FileTypeConstants
from astro.files.types.base import FileType
from astro.files.types.csv import CSVFileType
from astro.files.types.json import JSONFileType
from astro.files.types.ndjson import NDJSONFileType
from astro.files.types.parquet import ParquetFileType


def create_file_type(
    path: str,
    filetype: Union[FileTypeConstants, None] = None,
    normalize_config: Optional[dict] = None,
):
    """Factory method to create FileType super objects based on the file extension in path or filetype specified."""
    filetype_to_class: Dict[FileTypeConstants, Type[FileType]] = {
        FileTypeConstants.CSV: CSVFileType,
        FileTypeConstants.JSON: JSONFileType,
        FileTypeConstants.NDJSON: NDJSONFileType,
        FileTypeConstants.PARQUET: ParquetFileType,
    }
    if not filetype:
        filetype = get_filetype(path)

    try:
        return filetype_to_class[filetype](path=path, normalize_config=normalize_config)
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
