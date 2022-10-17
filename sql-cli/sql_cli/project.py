import logging
import os
import shutil
from pathlib import Path
from typing import Optional

from airflow.utils import db

from sql_cli.configuration import Config
from sql_cli.constants import DEFAULT_AIRFLOW_HOME, DEFAULT_DAGS_FOLDER, DEFAULT_ENVIRONMENT
from sql_cli.utils.airflow import env_vars

BASE_SOURCE_DIR = Path(os.path.realpath(__file__)).parent.parent / "include/base/"

MANDATORY_PATHS = {Path("config/default/configuration.yml"), Path("workflows"), Path(".airflow/airflow.db")}


class Project:
    """
    SQL CLI Project.
    """

    workflows_directory = Path("workflows")

    def __init__(
        self,
        directory: Path,
        airflow_home: Optional[Path] = None,
        airflow_dags_folder: Optional[Path] = None,
    ) -> None:
        self.directory = directory
        self._airflow_home = airflow_home
        self._airflow_dags_folder = airflow_dags_folder

    @property
    def airflow_home(self) -> Path:
        """
        Folder which contains the Airflow database and configuration.
        Can be either user-defined, during initialisation, or the default one.

        This is used by flow validate and flow run.

        :returns: The path to the Airflow home directory.
        """
        return self._airflow_home or Path(self.directory, DEFAULT_AIRFLOW_HOME)

    @property
    def airflow_dags_folder(self) -> Path:
        """
        Folder which contains the generated Airflow DAG files.
        Can be eitehr user-defined, during initialisation, or the default one.

        This is used by flow generate and flow run.

        :returns: The path to the Airflow DAGs directory.
        """
        return self._airflow_dags_folder or Path(self.directory, DEFAULT_DAGS_FOLDER)

    def _update_config(self) -> None:
        """
        Sets custom Airflow configuration in case the user is not using the default values.

        :param airflow_home: Custom user-defined Airflow Home directory
        :param airflow_dags_folder: Custom user-defined Airflow DAGs folder
        """
        config = Config(environment=DEFAULT_ENVIRONMENT, project_dir=self.directory)
        if self._airflow_home is not None:
            config.write_value_to_yaml("airflow", "home", self._airflow_home.as_posix())
        if self._airflow_dags_folder is not None:
            config.write_value_to_yaml("airflow", "dags_folder", self._airflow_dags_folder.as_posix())

    def _initialise_airflow(self) -> None:
        """
        Create an Airflow database and configuration in the self.airflow_home folder, or upgrade them,
        if they already exist.
        """
        overrides = {
            "AIRFLOW_HOME": self.airflow_home,
            "AIRFLOW__CORE__DAGS_FOLDER": self.airflow_dags_folder,
            "AIRFLOW__CORE__LOAD_EXAMPLES": False,
        }
        with env_vars(overrides):
            db.upgradedb()

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
        self._initialise_airflow()

    def is_valid_project(self) -> bool:
        """
        Check if self.directory contains the necessary paths which make it qualify as a valid SQL CLI project.

        The mandatory paths are sql_cli.project.MANDATORY_PATHS
        """
        existing_paths = {path.relative_to(self.directory) for path in Path(self.directory).rglob("*")}
        return MANDATORY_PATHS.issubset(existing_paths)

    def load_config(self, environment: str = DEFAULT_ENVIRONMENT) -> None:
        """
        Given a self.directory and an environment, load to the configuration ad paths to the Project instance.

        :param environment: string referencing the desired environment, uses "default" unless specified
        """
        if not self.is_valid_project():
            logging.exception("This is not a valid SQL project. Please, use `flow init`")
        config = Config(environment=environment, project_dir=self.directory).from_yaml_to_config()
        self._airflow_home = Path(str(config.airflow_home))
        self._airflow_dags_folder = Path(str(config.airflow_dags_folder))
