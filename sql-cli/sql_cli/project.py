import os
import shutil
from pathlib import Path
from typing import Optional

from sql_cli.configuration import DEFAULT_ENVIRONMENT, Config

BASE_SOURCE_DIR = Path(os.path.realpath(__file__)).parent.parent / "include/base/"


class Project:
    """
    SQL CLI Project.
    """

    def __init__(
        self,
        directory: Path,
        airflow_home: Optional[Path] = None,
        airflow_dags_folder: Optional[Path] = None,
    ) -> None:
        self.directory = directory
        self.airflow_home = airflow_home
        self.airflow_dags_folder = airflow_dags_folder

    def _update_config(self) -> None:
        """
        Sets custom Airflow configuration in case the user is not using the default values.

        :param airflow_home: Custom user-defined Airflow Home directory
        :param airflow_dags_folder: Custom user-defined Airflow DAGs folder
        """
        config = Config(environment=DEFAULT_ENVIRONMENT, project_dir=self.directory)
        if self.airflow_home is not None:
            config.write_value_to_yaml("airflow", "home", str(self.airflow_home))
        if self.airflow_dags_folder is not None:
            config.write_value_to_yaml("airflow", "dags_folder", str(self.airflow_dags_folder))

    def initialise(self) -> None:
        """
        Initialise a SQL CLI project, creating expected directories and files.

        :param airflow_home: Custom user-defined Airflow Home directory
        :param airflow_dags_folder: Custom user-defined Airflow DAGs folder
        """
        shutil.copytree(
            src=BASE_SOURCE_DIR,
            dst=self.directory,
            ignore=shutil.ignore_patterns(".gitkeep"),
            dirs_exist_ok=True,
        )
        self._update_config()
