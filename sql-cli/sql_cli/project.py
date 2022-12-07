from __future__ import annotations

import os
import shutil
from configparser import ConfigParser
from pathlib import Path
from typing import Any

from airflow.models.connection import Connection

from sql_cli.configuration import Config, convert_to_connection
from sql_cli.constants import (
    DEFAULT_AIRFLOW_HOME,
    DEFAULT_DAGS_FOLDER,
    DEFAULT_ENVIRONMENT,
    GLOBAL_CONFIG,
    SQLITE_CONN_TYPE,
)
from sql_cli.exceptions import InvalidProject
from sql_cli.utils.airflow import initialise as initialise_airflow, reload as reload_airflow

BASE_SOURCE_DIR = Path(os.path.realpath(__file__)).parent.parent / "include/base/"

MANDATORY_PATHS = {
    Path("config/default/configuration.yml"),
    Path("config/global/configuration.yml"),
    Path("workflows"),
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
        self._airflow_home = airflow_home or Path(self.directory, DEFAULT_AIRFLOW_HOME)
        self._airflow_dags_folder = airflow_dags_folder or Path(self.directory, DEFAULT_DAGS_FOLDER)
        self.connections: list[Connection] = []

    @property
    def airflow_home(self) -> Path:
        """
        Folder which contains the Airflow database and configuration.
        Can be either user-defined, during initialisation, or the default one.

        This is used by flow validate and flow run.

        :returns: The path to the Airflow home directory.
        """
        return self._airflow_home

    @property
    def airflow_dags_folder(self) -> Path:
        """
        Folder which contains the generated Airflow DAG files.
        Can be eitehr user-defined, during initialisation, or the default one.

        This is used by flow generate and flow run.

        :returns: The path to the Airflow DAGs directory.
        """
        return self._airflow_dags_folder

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

    def _initialise_global_config(self) -> None:
        """
        Initialises global config file that includes configuration to be shared across environments including the
        airflow config.
        """
        config = Config(environment=GLOBAL_CONFIG, project_dir=self.directory)
        global_env_filepath = config.get_global_config_filepath()
        # If the `Airflow Home` directory does not exist, Airflow initialisation flow takes care of creating the
        # directory. We rely on this behaviour and hence do not raise an exception if the path specified as
        # `Airflow Home` does not exist.
        config.write_value_to_yaml("airflow", "home", str(self._airflow_home.resolve()), global_env_filepath)
        if not Path.exists(self._airflow_dags_folder):
            raise FileNotFoundError(f"Specified DAGs directory {self._airflow_dags_folder} does not exist.")
        config.write_value_to_yaml(
            "airflow", "dags_folder", str(self._airflow_dags_folder.resolve()), global_env_filepath
        )

    def transform_env_config(self, environment: str = DEFAULT_ENVIRONMENT) -> None:
        """
        Transforms environment specific configurations post project initialisation.
        :param environment: the environment for which the configuration has to be updated
        """
        config = Config(environment=environment, project_dir=self.directory)
        config = config.from_yaml_to_config()
        for connection in config.connections:
            if connection["conn_type"] == SQLITE_CONN_TYPE:
                host_path = connection["host"]
                if os.path.isabs(host_path):
                    continue
                # The example workflows have relative paths for the host URLs for SQLite connections. Additionally, the
                # user might also sometimes set relative paths for the host from the initialised project directory. Such
                # paths need to be converted to absolute paths so that the connections work successfully.
                resolved_host_path = self.directory / host_path
                if not resolved_host_path.exists():
                    raise FileNotFoundError(
                        f"The relative file path {host_path} was resolved into {resolved_host_path} but it's a failed "
                        f"resolution as the path does not exist."
                    )
                connection["host"] = str(resolved_host_path)
        config.write_config_to_yaml()

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
        """
        shutil.copytree(
            src=BASE_SOURCE_DIR,
            dst=self.directory,
            ignore=shutil.ignore_patterns(".gitkeep"),
            dirs_exist_ok=True,
        )
        self._initialise_global_config()
        self.transform_env_config()
        initialise_airflow(self.airflow_home, self.airflow_dags_folder)
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
        reload_airflow(self.airflow_home)
        self.connections = [convert_to_connection(c) for c in config.connections]
