import os
from unittest.mock import patch

from sql_cli.connections import CONNECTION_ID_OUTPUT_STRING_WIDTH, validate_connections
from tests.utils import get_connection_by_id


@patch("sql_cli.connections.Path.is_file", return_value=True)
@patch("sql_cli.connections.Connection.test_connection", return_value=(True, ""))
@patch.dict(os.environ, {}, clear=True)
def test_validate_connections(
    mock_test_connection,
    mock_is_file,
    connections,
    initialised_project,
    capsys,
):
    connection_id = "sqlite_conn"
    validate_connections(connections=connections, connection_id=connection_id)

    captured = capsys.readouterr()
    assert f"Validating connection {connection_id:{CONNECTION_ID_OUTPUT_STRING_WIDTH}} PASSED" in captured.out
    sqlite_connection = get_connection_by_id(connections, connection_id)
    assert f"AIRFLOW_CONN_{sqlite_connection.conn_id.upper()}" in os.environ
    assert os.environ[f"AIRFLOW_CONN_{sqlite_connection.conn_id.upper()}"] == sqlite_connection.get_uri()


@patch("sql_cli.connections.Path.is_file", return_value=False)
@patch("sql_cli.connections.Connection.test_connection", return_value=(False, ""))
@patch.dict(os.environ, {}, clear=True)
def test_validate_connections_config_file_does_not_contain_connection(
    mock_test_connection,
    mock_is_file,
    connections,
    initialised_project,
    capsys,
):
    unknown_connection_id = "UnknownConnection"
    validate_connections(connections=connections, connection_id=unknown_connection_id)

    captured = capsys.readouterr()
    assert f"Config file does not contain given connection {unknown_connection_id}" in captured.out
    assert f"AIRFLOW_CONN_{unknown_connection_id.upper()}" not in os.environ


@patch("sql_cli.connections.Path.is_file", return_value=True)
@patch("sql_cli.connections.Connection.test_connection", return_value=(True, ""))
@patch.dict(os.environ, {}, clear=True)
def test_validate_connections_multiple_connections(
    mock_test_connection,
    mock_is_file,
    multiple_connections,
    initialised_project,
    capsys,
):
    validate_connections(connections=multiple_connections)
    captured = capsys.readouterr()
    assert captured.out.count("PASSED") == 2
    assert "AIRFLOW_CONN_SQLITE_CONN1" in os.environ
    assert "AIRFLOW_CONN_SQLITE_CONN2" in os.environ


@patch("sql_cli.connections.Path.is_file", return_value=True)
@patch("sql_cli.connections.Connection.test_connection", return_value=(True, ""))
@patch.dict(os.environ, {}, clear=True)
def test_validate_connections_multiple_connections_single_specified(
    mock_test_connection,
    mock_is_file,
    multiple_connections,
    initialised_project,
    capsys,
):
    validate_connections(connections=multiple_connections, connection_id="sqlite_conn1")
    captured = capsys.readouterr()
    assert captured.out.count("PASSED") == 1
    assert "AIRFLOW_CONN_SQLITE_CONN1" in os.environ
    assert "AIRFLOW_CONN_SQLITE_CONN2" not in os.environ
