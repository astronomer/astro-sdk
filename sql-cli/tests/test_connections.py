from unittest.mock import patch

from sql_cli.connections import CONNECTION_ID_OUTPUT_STRING_WIDTH, validate_connections


@patch("sql_cli.connections.Path.is_file", return_value=True)
@patch("sql_cli.connections.Connection.test_connection", return_value=(True, ""))
@patch("sql_cli.connections.create_session")
def test_validate_connections(
    mock_create_session,
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


@patch("sql_cli.connections.Path.is_file", return_value=False)
@patch("sql_cli.connections.Connection.test_connection", return_value=(False, ""))
@patch("sql_cli.connections.create_session")
def test_validate_connections_config_file_does_not_contain_connection(
    mock_create_session,
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
