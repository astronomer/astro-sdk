from __future__ import annotations

import os
import shutil
from configparser import ConfigParser
from pathlib import Path
from typing import Any

from airflow.models.connection import Connection

from sql_cli.configuration import Config
from sql_cli.connections import convert_to_connection
from sql_cli.constants import (
    DEFAULT_BASE_AIRFLOW_HOME,
    DEFAULT_DAGS_FOLDER,
    DEFAULT_DATA_DIR,
    DEFAULT_ENVIRONMENT,
)
from sql_cli.exceptions import InvalidProject
from sql_cli.utils.airflow import (
    initialise as initialise_airflow,
    reload as reload_airflow,
    remove_unnecessary_files,
)

BASE_SOURCE_DIR = Path(os.path.realpath(__file__)).parent / "include/base/"

MANDATORY_PATHS = {
    Path("config/default/configuration.yml"),
    Path("config/global.yml"),
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
        data_dir: Path | None = None,
    ) -> None:
        self.directory = directory
        self._airflow_home = airflow_home
        self._airflow_dags_folder = airflow_dags_folder or Path(self.directory, DEFAULT_DAGS_FOLDER)
        self._data_dir = data_dir or Path(self.directory, DEFAULT_DATA_DIR)
        self.connections: list[Connection] = []

    def get_env_airflow_home(self, env: str = DEFAULT_ENVIRONMENT) -> Path:
        """
        Return Airflow home for desired environment.

        :param env: SQL CLI environment
        :returns: Path to environment-specific Airflow home
        """
        default_path = self.directory / DEFAULT_BASE_AIRFLOW_HOME / env
        return self._airflow_home or default_path

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
    def data_dir(self) -> Path:
        """
        Folder which contains additional data files.
        Can be either user-defined, during initialisation, or the default one.

        This is used by flow init for copying the data files.

        :returns: The path to the data directory.
        """
        return self._data_dir

    @property
    def environments_list(self) -> list[str]:
        """
        Return list with the names of the existing environments in the current project.

        :returns: A list with strings representing the existing environments.
        """
        config_dir = self.directory / "config"
        return [subpath.name for subpath in config_dir.iterdir() if subpath.is_dir()]

    def get_env_airflow_config(self, env: str) -> dict[str, Any]:
        """
        Retrieve the Airflow configuration for the currently set environment.

        :returns: A Python dictionary containing the Airflow configuration.
        """
        airflow_home = self.get_env_airflow_home(env)
        filename = airflow_home / "airflow.cfg"
        parser = ConfigParser()
        parser.read(filename)
        return {section: dict(parser.items(section)) for section in parser.sections()}

    def _initialise_global_config(self) -> None:
        """
        Initialises global config YAML file that includes configuration to be shared across environments including the
        airflow config.
        """
        config = Config(project_dir=self.directory)
        global_config_filepath = config.get_global_config_filepath()
        config.write_value_to_yaml(
            "general", "data_dir", self._data_dir.resolve().as_posix(), global_config_filepath
        )

        # If the `Airflow Home` directory does not exist, Airflow initialisation flow takes care of creating the
        # directory. We rely on this behaviour and hence do not raise an exception if the path specified as
        # `Airflow Home` does not exist.
        if not Path.exists(self._airflow_dags_folder):
            raise FileNotFoundError(f"Specified DAGs directory {self._airflow_dags_folder} does not exist.")
        config.write_value_to_yaml(
            "airflow", "dags_folder", str(self._airflow_dags_folder.resolve()), global_config_filepath
        )

    def _initialise_env_config(self, environment: str) -> None:
        """
        Initialises the environment YAML config file that includes environment-specific values.

        :param environment: Environment to set the configuration
        """
        config = Config(environment=environment, project_dir=self.directory)
        config_filepath = config.get_env_config_filepath()
        # If the `Airflow Home` directory does not exist, Airflow initialisation flow takes care of creating the
        # directory. We rely on this behaviour and hence do not raise an exception if the path specified as
        # `Airflow Home` does not exist.
        config.write_value_to_yaml(
            "airflow", "home", str(self.get_env_airflow_home(environment)), config_filepath
        )

    def _initialise_airflow_for_environment(self, env: str) -> None:
        """
        Initialise Airflow home for given environment.

        :param env: SQL CLI environment
        """
        airflow_home = self.get_env_airflow_home(env)
        initialise_airflow(airflow_home, self.airflow_dags_folder)
        remove_unnecessary_files(airflow_home)

    def initialise(self) -> None:
        """
        Initialise a SQL CLI project, creating expected directories and files.
        """
        excludes = [".gitkeep"]
        if self.data_dir != self.directory / DEFAULT_DATA_DIR:
            # Use user-provided data directory
            excludes.append(DEFAULT_DATA_DIR)
            shutil.copytree(
                src=BASE_SOURCE_DIR / DEFAULT_DATA_DIR,
                dst=self.data_dir,
                dirs_exist_ok=True,
            )
        shutil.copytree(
            src=BASE_SOURCE_DIR,
            dst=self.directory,
            ignore=shutil.ignore_patterns(*excludes),
            dirs_exist_ok=True,
        )
        self._initialise_global_config()
        for env in self.environments_list:
            self._initialise_env_config(env)
            self._initialise_airflow_for_environment(env)

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
        else:
            # Scenario in which the end-user created an environment configuration after the project initialisation
            # and it did not specify the Airflow home property.
            self._initialise_env_config(environment)
            config = Config(environment=environment, project_dir=self.directory).from_yaml_to_config()

        if not self.get_env_airflow_home(environment).exists():
            # Scenario in which the end-user created an environment configuration after the project initialisation.
            # In these cases, we need to initialise Airflow (folder and files) so the user can run commands such
            # as validate and run.
            self._initialise_airflow_for_environment(environment)

        if config.airflow_dags_folder:
            self._airflow_dags_folder = Path(config.airflow_dags_folder).resolve()

        if config.data_dir:
            self._data_dir = Path(config.data_dir).resolve()

        reload_airflow(self.get_env_airflow_home(environment))
        self.connections = [
            convert_to_connection(connection, self._data_dir) for connection in config.connections
        ]
