import json

import pytest

from sql_cli.cli.config import InvalidConfigException, _get, _set


def test__get_with_key(initialised_project):
    computed = _get("airflow_home", initialised_project.directory, env="default", as_json=False)
    expected = (initialised_project.directory / ".airflow/default").as_posix()
    assert computed == expected


def test__get_as_json(initialised_project):
    computed = _get("", initialised_project.directory, env="dev", as_json=True)
    expected = {
        "global": {
            "airflow": {
                "dags_folder": str(initialised_project.directory / ".airflow/dags"),
            },
            "general": {"data_dir": str(initialised_project.directory / "data")},
        },
        "dev": {
            "airflow": {
                "home": str(initialised_project.directory / ".airflow/dev"),
            }
        },
    }
    assert computed == json.dumps(expected)


def test__get_invalid(initialised_project):
    with pytest.raises(InvalidConfigException) as err:
        _get("", initialised_project.directory, env="default", as_json=False)
    assert str(err.value) == "Please, either define a key or use the --as-json flag"


def test__set_invalid(initialised_project):
    with pytest.raises(InvalidConfigException) as err:
        _set("not-deploy", "", "", "", "")
    assert str(err.value) == "The key not-deploy is not supported yet. Only deploy is currently supported."
