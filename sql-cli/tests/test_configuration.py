from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import yaml

from sql_cli.configuration import Config

unique_config_file = "/tmp/" + str(uuid4())


def test_from_yaml_to_config():
    config_reference = Config(
        project_dir=Path(__file__).parent.parent / "include/base", environment="default"
    )
    config_from_file = config_reference.from_yaml_to_config()
    assert isinstance(config_from_file, Config)
    assert config_from_file.project_dir == config_reference.project_dir
    assert config_from_file.environment == config_reference.environment
    assert config_from_file.connections


@patch(
    "sql_cli.configuration.Config.from_yaml_to_dict",
    return_value={"airflow": {"home": "", "dags_folder": ""}},
)
@patch("sql_cli.configuration.Config.get_filepath", return_value=unique_config_file)
def test_write_config_to_yaml(mock_to_dict, mock_get_path, tmp_path):
    config = Config(
        project_dir=tmp_path,
        environment="neverland",
        airflow_home="/tmp/home",
        airflow_dags_folder="/tmp/dags",
    )
    config.write_config_to_yaml()
    with open(unique_config_file) as fp:
        yaml_config = yaml.safe_load(fp.read())
    assert yaml_config["airflow"]["home"] == "/tmp/home"
    assert yaml_config["airflow"]["dags_folder"] == "/tmp/dags"
