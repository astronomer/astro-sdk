import ftplib
from unittest.mock import Mock, patch

import pytest
from airflow.providers.ftp.hooks.ftp import FTPHook

from astro.files.locations import create_file_location


def test_get_transport_params_for_ftp():
    """test get_transport_params() whether empty dict is been sent or not"""
    path = "ftp://bucket/some-file"
    location = create_file_location(path)
    credentials = location.transport_params
    assert credentials == {}


@patch("airflow.providers.ftp.hooks.ftp.FTPHook.get_size")
def test_size(mock_get_conn):
    """Test whether get_size() will return the file size correctly"""

    mock_get_conn.return_value = 110
    location = create_file_location("ftp://user@host/some/sample.csv")
    assert location.size == 110
    mock_get_conn.assert_called_once_with("/some/sample.csv")


def test_hook():
    """Test whether FTP hook is being called or not."""
    location = create_file_location("ftp://user@host/some")
    hook = location.hook
    assert isinstance(hook, FTPHook)


@pytest.mark.parametrize(
    "response, file_location",
    [
        (["/some/sample.csv"], "ftp://user@host/some"),
        ([], "ftp://some/sample.csv"),
    ],
)
@patch("airflow.providers.ftp.hooks.ftp.FTPHook.get_connection")
@patch("airflow.providers.ftp.hooks.ftp.FTPHook.get_conn")
def test_get_paths_from_ftp(mock_ftp_conn, mock_ftp_connection, response, file_location):
    """Get the list of files from the ftp path"""
    mock_ftp_client = Mock(ftplib.FTP)
    mock_ftp_conn.return_value = mock_ftp_client
    mock_ftp_client.nlst.return_value = response
    mock_ftp_connection.return_value.get_uri.return_value = "ftp://user@host:1234"
    location = create_file_location(file_location)
    assert sorted(location.paths) == sorted(["ftp://user@host:1234/some/sample.csv"])
