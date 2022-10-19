import pytest

from sql_cli.connections import CONNECTION_ID_OUTPUT_STRING_WIDTH, validate_connections


def test_specific_connection(initialised_project, capsys):
    sqlite_conn_id = "sqlite_conn"

    validate_connections(project=initialised_project, connection_id=sqlite_conn_id)
    captured = capsys.readouterr()

    sqlite_conn_id_formatted_string = sqlite_conn_id + " " * (
        CONNECTION_ID_OUTPUT_STRING_WIDTH - len(sqlite_conn_id)
    )
    added_connection_log = f"Validating connection {sqlite_conn_id_formatted_string} PASSED"
    assert added_connection_log in captured.out


def test_validate_connections_config_file_not_found_exception(initialised_project):
    with pytest.raises(FileNotFoundError):
        validate_connections(project=initialised_project, environment="UnknownEnvironment")


def test_validate_connections_config_file_does_not_contain_connection(initialised_project, capsys):
    unknown_connection_id = "UnknownConnection"
    validate_connections(project=initialised_project, connection_id=unknown_connection_id)
    captured = capsys.readouterr()
    assert f"Config file does not contain given connection {unknown_connection_id}" in captured.out
