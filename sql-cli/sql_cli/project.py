import os
import shutil
from pathlib import Path

BASE_SOURCE_DIR = Path(Path(os.path.realpath(__file__)).parent.parent, "include/base/")


class Project:
    """
    Abstracts methods related to a SQL CLI Project.
    """

    @staticmethod
    def initialise(target_dir: Path) -> None:
        """
        Initialise a SQL CLI project, creating expected directories and files.
        """
        shutil.copytree(src=BASE_SOURCE_DIR, dst=target_dir, dirs_exist_ok=True)
