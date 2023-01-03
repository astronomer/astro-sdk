"""Tests specific to the Sqlite Database implementation."""
from unittest import mock

from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

from astro.databases.aws.redshift import RedshiftDatabase
from astro.table import Metadata, Table


@mock.patch("redshift_connector.connect", autospec=True)
@mock.patch("airflow.hooks.base.BaseHook.get_connection", autospec=True)
def test_hook_with_db_from_table(mock_conn, redshift_conn):
    mock_conn.return_value = Connection(conn_id="test_conn", extra={})
    table = Table(conn_id="test", metadata=Metadata(database="dev"))
    db = RedshiftDatabase(conn_id="test", table=table)
    hook = db.hook
    assert isinstance(hook, RedshiftSQLHook)
    # TODO: Remove comment when RedshiftSQLHook in Airflow start using the kwargs
    # redshift_conn.assert_called_once_with({"database": "dev"})
