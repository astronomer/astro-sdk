import os
import pathlib
from typing import Union

from astro.constants import LOAD_DATAFRAME_BYTES_LIMIT, FileType


def get_size(filepath: str) -> int:
    """
    Return the size (bytes) of the given file.

    :param filepath: Path to a file in the filesystem
    :type filepath: str
    :return: File size in bytes
    :rtype: int
    """
    path = pathlib.Path(filepath)
    return os.path.getsize(path)


def get_filetype(filepath: Union[str, pathlib.PosixPath]) -> FileType:
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
        return FileType[extension.upper()]
    except KeyError:
        raise ValueError(f"Unsupported filetype '{extension}' from file '{filepath}'.")


def is_binary(filetype: FileType) -> bool:
    """
    Return a FileType given the filepath. Uses a naive strategy, using the file extension.

    :param filetype: File type
    :type filetype: astro.constants.FileType
    :return: True or False
    :rtype: bool
    """
    if filetype == FileType.PARQUET:
        return True
    return False


def is_small(filepath: str) -> bool:
    """
    Checks if a file is small enough to be loaded into a Pandas dataframe in memory efficiently.
    This value was obtained through performance tests.

    :param filepath: Path to a file in the filesystem
    :type filepath: str
    :return: If the file is small enough
    :rtype: boolean
    """
    size_in_bytes = get_size(filepath)
    return bool(size_in_bytes <= LOAD_DATAFRAME_BYTES_LIMIT)
