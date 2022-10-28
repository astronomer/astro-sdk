from pathlib import Path
from tempfile import gettempdir
from unittest.mock import patch
from uuid import uuid4

import yaml

from sql_cli.configuration import Config


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
@patch("sql_cli.configuration.Config.get_filepath")
def test_write_config_to_yaml(mock_get_filepath, mock_from_yaml_to_dict, tmp_path):
    tmp_dir = gettempdir()
    mock_get_filepath.return_value = f"{tmp_dir}/{uuid4().hex}"
    config = Config(
        project_dir=tmp_path,
        environment="neverland",
        airflow_home=f"{tmp_dir}/home",
        airflow_dags_folder=f"{tmp_dir}/dags",
    )
    config.write_config_to_yaml()
    with open(mock_get_filepath.return_value) as fp:
        yaml_config = yaml.safe_load(fp.read())
    assert yaml_config["airflow"]["home"] == f"{tmp_dir}/home"
    assert yaml_config["airflow"]["dags_folder"] == f"{tmp_dir}/dags"
