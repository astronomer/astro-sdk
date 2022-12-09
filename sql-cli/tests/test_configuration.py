from pathlib import Path

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
