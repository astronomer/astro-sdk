from __future__ import annotations

import os
import shutil
from configparser import ConfigParser
from pathlib import Path
from typing import Any

from airflow.models.connection import Connection

from sql_cli.configuration import Config, convert_to_connection
from sql_cli.constants import DEFAULT_AIRFLOW_HOME, DEFAULT_DAGS_FOLDER, DEFAULT_ENVIRONMENT
from sql_cli.exceptions import InvalidProject
from sql_cli.utils.airflow import disable_examples

BASE_SOURCE_DIR = Path(os.path.realpath(__file__)).parent.parent / "include/base/"

MANDATORY_PATHS = {
    Path("config/default/configuration.yml"),
    Path("workflows"),
    Path(".airflow/default/airflow.db"),
}


class Project:
    """
    SQL CLI Project.
    """

    workflows_directory = Path("workflows")

    def __init__(
        self,
        directory: Path,
        airflow_home: Path | None = None,
        airflow_dags_folder: Path | None = None,
    ) -> None:
        self.directory = directory
        self._airflow_home = airflow_home
        self._airflow_dags_folder = airflow_dags_folder
        self.connections: list[Connection] = []

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

    @property
    def airflow_config(self) -> dict[str, Any]:
        """
        Retrieve the Airflow configuration for the currently set environment.

        :returns: A Python dictionary containing the Airflow configuration.
        """
        filename = self.airflow_home / "airflow.cfg"
        parser = ConfigParser()
        parser.read(filename)
        return {section: dict(parser.items(section)) for section in parser.sections()}

    def update_config(self, environment: str = DEFAULT_ENVIRONMENT) -> None:
        """
        Sets custom Airflow configuration in case the user is not using the default values.

        :param airflow_home: Custom user-defined Airflow Home directory
        :param airflow_dags_folder: Custom user-defined Airflow DAGs folder
        """
        config = Config(environment=environment, project_dir=self.directory)
        config = config.from_yaml_to_config()

        if self._airflow_home is not None:
            config.write_value_to_yaml("airflow", "home", str(self._airflow_home.resolve()))
        if self._airflow_dags_folder is not None:
            config.write_value_to_yaml("airflow", "dags_folder", str(self._airflow_dags_folder.resolve()))

        config.connections[0]["host"] = str(self.directory / config.connections[0]["host"])
        config.write_config_to_yaml()

    def _initialise_airflow(self) -> None:
        """
        Create an Airflow database and configuration in the self.airflow_home folder, or upgrade them,
        if they already exist.
        """
        os.environ.pop("AIRFLOW_HOME", None)
        os.environ.pop("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", None)

        # TODO: In future we want to replace this by either:
        # - python-native approach or
        # - subprocess
        os.system(  # skipcq: BAN-B605
            f"AIRFLOW_HOME={self.airflow_home} "
            f"AIRFLOW__CORE__DAGS_FOLDER={self.airflow_dags_folder} "
            "AIRFLOW__CORE__LOAD_EXAMPLES=False "
            "airflow db init > /dev/null 2>&1"
        )

    def _remove_unnecessary_airflow_files(self) -> None:
        """
        Delete Airflow generated paths which are not necessary for the SQL CLI (scheduler & webserver-related).
        """
        logs_folder = self.airflow_home / "logs"
        shutil.rmtree(logs_folder)

        webserver_config = self.airflow_home / "webserver_config.py"
        webserver_config.unlink()

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
        self.update_config()
        self._initialise_airflow()
        disable_examples(self.airflow_home)
        self._remove_unnecessary_airflow_files()

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
            raise InvalidProject("This is not a valid SQL project. Please, use `flow init`")
        config = Config(environment=environment, project_dir=self.directory).from_yaml_to_config()
        if config.airflow_home:
            self._airflow_home = Path(config.airflow_home).resolve()
        if config.airflow_dags_folder:
            self._airflow_dags_folder = Path(config.airflow_dags_folder).resolve()
        self.connections = [convert_to_connection(c) for c in config.connections]
