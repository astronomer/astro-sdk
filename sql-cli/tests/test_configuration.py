from sql_cli.configuration import Config
from sql_cli.project import BASE_SOURCE_DIR


def test_from_yaml_to_config():
    config_reference = Config(project_dir=BASE_SOURCE_DIR, environment="default")
    config_from_file = config_reference.from_yaml_to_config()
    assert isinstance(config_from_file, Config)
    assert config_from_file.project_dir == config_reference.project_dir
    assert config_from_file.environment == config_reference.environment
    assert config_from_file.connections


def test_from_yaml_to_config_without_env():
    config_reference = Config(project_dir=BASE_SOURCE_DIR)
    config_from_file = config_reference.from_yaml_to_config()
    assert isinstance(config_from_file, Config)
    assert config_from_file.project_dir == config_reference.project_dir
    assert config_from_file.environment == "default"
