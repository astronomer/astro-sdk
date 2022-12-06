from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from airflow.models.connection import Connection

from sql_cli.constants import CONFIG_DIR, CONFIG_FILENAME, GLOBAL_CONFIG


def convert_to_connection(conn: dict[str, Any]) -> Connection:
    """
    Convert the SQL CLI connection dictionary into an Airflow Connection instance.

    :param conn: SQL CLI connection dictionary
    :returns: Connection object
    """
    from airflow.api_connexion.schemas.connection_schema import connection_schema

    c = conn.copy()
    c["connection_id"] = c["conn_id"]
    c.pop("conn_id")
    return Connection(**connection_schema.load(c))


@dataclass
class Config:
    """
    SQL CLI Project configuration class to support reading and writing to configuration file.
    """

    project_dir: Path
    environment: str

    connections: list[dict[str, Connection]] = field(default_factory=list)
    airflow_home: str | None = None
    airflow_dags_folder: str | None = None

    def get_env_config_filepath(self) -> Path:
        """
        Return environment specific configuration.yaml filepath.

        :returns: The path to the desired YAML configuration file
        """
        return self.project_dir / CONFIG_DIR / self.environment / CONFIG_FILENAME

    def get_global_config_filepath(self) -> Path:
        """
        Return global configuration.yaml filepath which is shared across environments.

        :return: The path to the desired global YAML configuration file
        """
        return self.project_dir / CONFIG_DIR / GLOBAL_CONFIG / CONFIG_FILENAME

    @staticmethod
    def from_yaml_to_dict(filepath: Path) -> dict[str, Any]:
        """
        Return a dict with the contents of the given configuration.yaml

        :param filepath: Path of the desired configuration.yaml to read contents from.

        :returns: Content of the YAML configuration file as a python dictionary.
        """
        with open(filepath) as fp:
            yaml_with_env = os.path.expandvars(fp.read())
            yaml_config = yaml.safe_load(yaml_with_env)
        return yaml_config or {}

    def from_yaml_to_config(self) -> Config:
        """Returns a Config instance with the contents of the environment specific and global configuration.yaml"""
        env_yaml_config = self.from_yaml_to_dict(self.get_env_config_filepath())
        global_yaml_config = self.from_yaml_to_dict(self.get_global_config_filepath())
        return Config(
            project_dir=self.project_dir,
            environment=self.environment,
            airflow_home=global_yaml_config.get("airflow", {}).get("home"),
            airflow_dags_folder=global_yaml_config.get("airflow", {}).get("dags_folder"),
            connections=env_yaml_config["connections"],
        )

    def write_value_to_yaml(self, section: str, key: str, value: str, filepath: Path) -> None:
        """
        Write a particular key/value to the desired configuration.yaml.

        Example:
        ```
            [section]
            key = value
        ```

        :param section: Section within the YAML file where the key/value will be recorded
        :param key: Key within the YAML file associated to the value to be recorded.
        :param value: Value associated to the key in the YAML file.
        :param filepath: Path of the desired configuration.yaml to write contents to.
        """
        yaml_config = self.from_yaml_to_dict(filepath)
        yaml_config.setdefault(section, {})
        yaml_config[section][key] = value

        with filepath.open(mode="w") as fp:
            yaml.dump(yaml_config, fp)

    def write_config_to_yaml(self) -> None:
        """
        Write Config instance's key-values to respective environment specific and global configuration.yml.

        It may happen that some method wishes to transform the config instance's values and wants to persist them back
        to the configuration files. This method serves as a utility method that can be used to write back the Config
        instances' environment specific keys to the environment configuration.yml and global keys to the global
        configuration.yml file.

        E.g. When a project is initialised, the example workflow containing SQLite connection refer to the database
        using relative paths. However, for the connection to be established successfully, it needs absolute path, so the
        project initialisation flow reads the default yaml into config instance, transforms the config instance to
        expand those relative paths to absolute paths and then calls this utility to persists the transformed instance.
        """
        env_config_filepath = self.get_env_config_filepath()
        env_yaml_config = self.from_yaml_to_dict(env_config_filepath)
        env_yaml_config["connections"] = self.connections
        with env_config_filepath.open(mode="w") as fp:
            yaml.dump(env_yaml_config, fp)

        global_config_filepath = self.get_global_config_filepath()
        global_yaml_config = self.from_yaml_to_dict(global_config_filepath)
        if self.airflow_home:
            global_yaml_config["airflow"]["home"] = self.airflow_home
        if self.airflow_dags_folder:
            global_yaml_config["airflow"]["dags_folder"] = self.airflow_dags_folder
        with global_config_filepath.open(mode="w") as fp:
            yaml.dump(global_yaml_config, fp)
