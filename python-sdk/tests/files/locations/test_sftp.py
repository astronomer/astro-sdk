import json
from unittest.mock import patch

import pytest
from airflow.models.connection import Connection
from airflow.providers.sftp.hooks.sftp import SFTPHook

from astro.exceptions import PermissionNotSetError
from astro.files.locations import create_file_location


@patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
def test_get_transport_params_for_sftp(mock_sftp_hook):
    """test get_transport_params() method which should return connect_kwargs when keyfile is passed"""
    mock_sftp_hook.return_value = Connection(
        conn_id="sftp_default",
        conn_type="test",
        login=1234,
        host="localhost",
        extra=json.dumps({"key_file": "/some/local/path/rsa.pem"}),
    )
    path = "sftp://bucket/some-file"
    location = create_file_location(path)
    credentials = location.transport_params
    assert credentials == {"connect_kwargs": {"key_filename": "/some/local/path/rsa.pem"}}


@patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
def test_get_transport_params_for_sftp_no_value(mock_sftp_hook):
    """test get_transport_params() method raises exception when no credentials is passed"""
    mock_sftp_hook.return_value = Connection(
        conn_id="sftp_default",
        conn_type="test",
        login=1234,
        host="localhost",
    )
    path = "sftp://bucket/some-file"
    location = create_file_location(path)
    with pytest.raises(PermissionNotSetError) as exec_info:
        location.transport_params
    assert str(exec_info.value) == "SFTP credentials are not set in the connection."


@patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
def test_get_transport_params_for_sftp_password(mock_sftp_hook):
    """test get_transport_params() method when password is sent in connection"""
    mock_sftp_hook.return_value = Connection(
        conn_id="sftp_default", conn_type="test", login=1234, host="localhost", password="test"
    )
    path = "sftp://bucket/some-file"
    location = create_file_location(path)
    credentials = location.transport_params
    assert credentials == {"connect_kwargs": {"password": "test"}}


@patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_tree_map")
@patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_connection")
def test_get_paths_from_sftp(mock_sftp_conn, mock_list):
    """Get the list of files from the sftp path"""
    mock_sftp_conn.return_value = Connection(
        uri="sftp://user@host:1234",
    )
    mock_list.return_value = (["some/sample.csv"],)
    location = create_file_location("sftp://some/")
    assert sorted(location.paths) == sorted(["sftp://user@host:1234/some/sample.csv"])


@pytest.mark.parametrize(
    "return_value, path",
    [
        (110, "sftp://bucket/some-file"),
        (-1, "sftp://wrong_path/to_file"),
    ],
)
@patch("airflow.providers.sftp.hooks.sftp.SFTPHook.get_conn")
def test_size(mock_get_conn, return_value, path):
    """Test get_size() of for SFTP file."""

    mock_get_conn.return_value.stat.return_value.st_size = return_value
    location = create_file_location(path)
    assert location.size == return_value


def test_hook():
    """Test whether SFTPHook is being called or not."""
    location = create_file_location("sftp://bucket/some-file")
    hook = location.hook
    assert isinstance(hook, SFTPHook)
