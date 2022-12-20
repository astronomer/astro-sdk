from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from airflow.models.connection import Connection
from dotenv import load_dotenv

from sql_cli.constants import CONFIG_DIR, CONFIG_FILENAME, GLOBAL_CONFIG_FILENAME


@dataclass
class Config:
    """
    SQL CLI Project configuration class to support reading and writing to configuration file.
    """

    project_dir: Path
    environment: str | None = None

    connections: list[dict[str, Connection]] = field(default_factory=list)
    airflow_home: str | None = None
    airflow_dags_folder: str | None = None
    data_dir: str | None = None

    def get_env_config_filepath(self) -> Path | None:
        """
        Return environment specific configuration.yaml filepath.

        :returns: The path to the desired YAML configuration file
        """
        if not self.environment:
            return None
        return self.project_dir / CONFIG_DIR / self.environment / CONFIG_FILENAME

    def get_global_config_filepath(self) -> Path:
        """
        Return global configuration.yaml filepath which is shared across environments.

        :return: The path to the desired global YAML configuration file
        """
        return self.project_dir / CONFIG_DIR / GLOBAL_CONFIG_FILENAME

    def from_yaml_to_dict(self, filepath: Path | None) -> dict[str, Any]:
        """
        Return a dict with the contents of the given configuration.yaml

        :param filepath: Path of the desired configuration.yaml to read contents from.

        :returns: Content of the YAML configuration file as a python dictionary.
        """
        if not filepath:
            return {}
        load_dotenv(self.project_dir / ".env")
        with filepath.open() as fp:
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
            data_dir=global_yaml_config.get("general", {}).get("data_dir"),
            connections=env_yaml_config.get("connections", []),
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
