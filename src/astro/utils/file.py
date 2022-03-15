import os
import pathlib

from astro.constants import LOAD_DATAFRAME_BYTES_LIMIT


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
    return size_in_bytes < LOAD_DATAFRAME_BYTES_LIMIT
