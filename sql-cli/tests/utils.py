from pathlib import Path
from typing import List


def list_dir(dir_name: str) -> List[Path]:
    """
    Return sorted list of files and directories available in the given directory.

    :param dir_name: Source directory name

    :returns: Sorted list of files and directories within the given directory.

    """
    return sorted([path.relative_to(dir_name) for path in Path(dir_name).rglob("*")])
