import ftplib
from unittest.mock import Mock, patch

from airflow.providers.ftp.hooks.ftp import FTPHook

from astro.files.locations import create_file_location


def test_get_transport_params_for_ftp():  # skipcq: PYL-W0612, PTC-W0065
    """test get_transport_params() method which should empty dict"""
    path = "ftp://bucket/some-file"
    location = create_file_location(path)
    credentials = location.transport_params
    assert credentials == {}


@patch("airflow.providers.ftp.hooks.ftp.FTPHook.get_size")
def test_size(mock_get_conn):
    """Test get_size() of for FTP file."""

    mock_get_conn.return_value = 110
    location = create_file_location("ftp://user@host/some/sample.csv")
    assert location.size == 110


def test_hook():
    """Test whether FTP is being called or not."""
    location = create_file_location("ftp://user@host/some")
    hook = location.hook
    assert isinstance(hook, FTPHook)


@patch("airflow.providers.ftp.hooks.ftp.FTPHook.get_connection")
@patch("airflow.providers.ftp.hooks.ftp.FTPHook.get_conn")
def test_get_paths_from_ftp_from_directory(mock_ftp_conn, mock_ftp_connection):
    """Get the list of files from the ftp path"""
    mock_ftp_client = Mock(ftplib.FTP)
    mock_ftp_conn.return_value = mock_ftp_client
    mock_ftp_client.nlst.return_value = ["/some/sample.csv"]
    mock_ftp_connection.return_value.get_uri.return_value = "ftp://user@host:1234"
    location = create_file_location("ftp://user@host/some")
    assert sorted(location.paths) == sorted(["ftp://user@host:1234/some/sample.csv"])


@patch("airflow.providers.ftp.hooks.ftp.FTPHook.get_connection")
@patch("airflow.providers.ftp.hooks.ftp.FTPHook.get_conn")
def test_get_paths_from_ftp_file(mock_ftp_conn, mock_ftp_connection):
    """Get the file from the ftp path"""
    mock_ftp_client = Mock(ftplib.FTP)
    mock_ftp_conn.return_value = mock_ftp_client
    mock_ftp_client.nlst.return_value = []
    mock_ftp_connection.return_value.get_uri.return_value = "ftp://user@host:1234"
    location = create_file_location("ftp://user@host/some/sample.csv")
    assert sorted(location.paths) == sorted(["ftp://user@host:1234/some/sample.csv"])
