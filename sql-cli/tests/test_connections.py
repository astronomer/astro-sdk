import logging
import os

import pytest

from sql_cli.connections import SQL_CLI_PROJECT_DIRECTORY, _load_yaml_connections, validate_connections


def test_validate_connections(caplog):
    logging.info("SQL_CLI_PROJECT_DIRECTORY Is %s", SQL_CLI_PROJECT_DIRECTORY)
    logging.info(os.popen("echo $PWD").read())
    logging.info(os.popen("echo $PYTHONPATH").read())
    os.system("echo 'y' | airflow db reset")
    validate_connections()

    postgres_conn_id = "postgres_conn"
    postgres_conn_id_formatted_string = " " * (25 - len(postgres_conn_id)) + postgres_conn_id
    failed_connection_log = f"Validating connection {postgres_conn_id_formatted_string} FAILED"
    assert failed_connection_log in caplog.text

    sqlite_conn_id = "sqlite_conn"
    sqlite_conn_id_formatted_string = " " * (25 - len(sqlite_conn_id)) + sqlite_conn_id
    added_connection_log = f"Validating connection {sqlite_conn_id_formatted_string} PASSED and ADDED"
    assert added_connection_log in caplog.text

    validate_connections()
    replaced_connection_log = f"Validating connection {sqlite_conn_id_formatted_string} PASSED and REPLACED"
    assert replaced_connection_log in caplog.text


def test_validate_connections_config_file_not_found_exception():
    with pytest.raises(FileNotFoundError):
        validate_connections(environment="UnknownEnvironment")


def test_validate_connections_config_file_does_not_contain_connection(caplog):
    unknown_connection_id = "UnknownConnection"
    validate_connections(connection=unknown_connection_id)
    assert f"Config file does not contain given connection {unknown_connection_id}" in caplog.text


def test__load_yaml_connections():
    connections = _load_yaml_connections(environment="default")
    sqlite_conn_dict = {
        "conn_id": "sqlite_conn",
        "conn_type": "sqlite",
        "host": "/tmp/sqlite.db",
        "schema": None,
        "login": None,
        "password": None,
    }
    assert sqlite_conn_dict in connections
