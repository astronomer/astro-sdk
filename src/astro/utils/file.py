import os
import pathlib

from astro.constants import LOAD_DATAFRAME_BYTES_LIMIT, FileType


def get_size(filepath):
    """
    Return the size (bytes) of the given file.

    :param filepath: Path to a file in the filesystem
    :type filepath: str
    :return: File size in bytes
    :rtype: int
    """
    filepath = pathlib.Path(filepath)
    return os.path.getsize(filepath)


def get_filetype(filepath):
    """
    Return a FileType given the filepath. Uses a naive strategy, using the file extension.

    :param filepath: URI or Path to a file
    :type filepath: str
    :return: The filetype (e.g. csv, ndjson, json, parquet)
    :rtype: astro.constants.FileType
    """
    extension = filepath.split(".")[-1]
    try:
        filetype = getattr(FileType, extension.upper())
    except AttributeError:
        raise ValueError(f"Unsupported filetype '{extension}' from file '{filepath}'.")
    return filetype


def is_binary(filetype):
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


def is_small(filepath):
    """
    Checks if a file is small enough to be loaded into a Pandas dataframe in memory efficently.
    This value was obtained through performance tests.

    :param filepath: Path to a file in the filesystem
    :type filepath: str
    :return: If the file is small enough
    :rtype: boolean
    """
    size_in_bytes = get_size(filepath)
    return size_in_bytes <= LOAD_DATAFRAME_BYTES_LIMIT
