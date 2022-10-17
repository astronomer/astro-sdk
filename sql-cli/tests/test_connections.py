from unittest import mock

import pytest

from sql_cli.connections import CONNECTION_ID_OUTPUT_STRING_WIDTH, validate_connections

REDSHIFT_CONNECTION_ENV_VARS = {
    "REDSHIFT_DATABASE": "fake_database",
    "REDSHIFT_HOST": "fake.redshift.amazonaws.com",
    "REDSHIFT_USERNAME": "fake_user",
    "REDSHIFT_PASSWORD": "fake_password",
}


@pytest.mark.parametrize(
    "conn_id, test_connection_output",
    [
        ("redshift_conn", (False, "test connection failed")),
        ("sqlite_conn", (True, "test_connection passed")),
    ],
)
@mock.patch("sql_cli.connections.Connection.test_connection")
def test_validate_connections(mock_test_connection, conn_id, test_connection_output, caplog):
    mock_test_connection.return_value = test_connection_output
    validate_connections()

    conn_id_formatted_string = conn_id + " " * (CONNECTION_ID_OUTPUT_STRING_WIDTH - len(conn_id))
    validation_status_string = "PASSED" if test_connection_output[0] else "FAILED"
    connection_log = f"Validating connection {conn_id_formatted_string} {validation_status_string}"
    assert connection_log in caplog.text


def test_specific_connection(caplog):
    sqlite_conn_id = "sqlite_conn"
    validate_connections(connection_id=sqlite_conn_id)

    sqlite_conn_id_formatted_string = sqlite_conn_id + " " * (
        CONNECTION_ID_OUTPUT_STRING_WIDTH - len(sqlite_conn_id)
    )
    added_connection_log = f"Validating connection {sqlite_conn_id_formatted_string} PASSED"
    assert added_connection_log in caplog.text


def test_validate_connections_config_file_not_found_exception():
    with pytest.raises(FileNotFoundError):
        validate_connections(environment="UnknownEnvironment")


def test_validate_connections_config_file_does_not_contain_connection(caplog):
    unknown_connection_id = "UnknownConnection"
    validate_connections(connection_id=unknown_connection_id)
    assert f"Config file does not contain given connection {unknown_connection_id}" in caplog.text
