import re
from unittest import mock
from urllib.parse import urlparse

import pytest
import sqlalchemy
from airflow.exceptions import AirflowException
from airflow.models import Connection

from astro.databases import create_database
from astro.databases.mysql import MysqlDatabase
from astro.settings import MYSQL_SCHEMA
from astro.table import Metadata, Table

DEFAULT_CONN_ID = "mysql_default"


@pytest.mark.parametrize("conn_id", [DEFAULT_CONN_ID])
def test_create_database(conn_id):
    """Test creation of database"""
    database = create_database(conn_id)
    assert isinstance(database, MysqlDatabase)


@pytest.mark.parametrize(
    "conn_id,expected_uri",
    [
        (DEFAULT_CONN_ID, re.compile(r"^(mysql://)(.*)$")),
    ],
    ids=[DEFAULT_CONN_ID],
)
def test_mysql_sqlalchemy_engine(conn_id, expected_uri):
    """Test getting a mysql based sqla engine."""
    database = MysqlDatabase(conn_id)
    engine = database.sqlalchemy_engine
    assert isinstance(engine, sqlalchemy.engine.base.Engine)
    url = urlparse(str(engine.url))
    assert re.match(expected_uri, url.geturl())


@mock.patch("airflow.hooks.base.BaseHook.get_connection", autospec=True)
def test_mysql_with_database_and_schema_in_metadata(mock_conn):
    mock_conn.return_value = Connection(conn_id="test_conn", extra={})
    table = Table(conn_id="test", metadata=Metadata(database="dev", schema="dev"))
    with pytest.raises(AirflowException) as exc:
        db = MysqlDatabase(conn_id="test", table=table)
        assert db.hook.schema is None

    assert (
        str(exc.value) == "You have provided both database and schema in Metadata."
        "Enter only schema while connecting to MySQL!"
    )


@mock.patch("airflow.hooks.base.BaseHook.get_connection", autospec=True)
def test_mysql_with_only_schema_in_metadata(mock_conn):
    mock_conn.return_value = Connection(conn_id="test_conn", extra={})
    table = Table(conn_id="test", metadata=Metadata(schema="dev"))
    db = MysqlDatabase(conn_id="test", table=table)
    assert db.hook.schema == "dev"


@mock.patch("airflow.hooks.base.BaseHook.get_connection", autospec=True)
def test_create_table_only_with_database(mock_conn):
    mock_conn.return_value = Connection(conn_id="test_conn", extra={})
    table = Table(conn_id="test", metadata=Metadata(database="dev"))
    with pytest.raises(AirflowException) as exc:
        db = MysqlDatabase(conn_id="test", table=table)
        assert db.hook.schema is None

    assert (
        str(exc.value)
        == "You have provided database in Metadata.Enter only schema while connecting to MySQL!"
    )


@mock.patch("airflow.hooks.base.BaseHook.get_connection", autospec=True)
def test_create_table_only_without_metadata(mock_conn):
    mock_conn.return_value = Connection(conn_id="test_conn", extra={})
    table = Table(conn_id="test")
    db = MysqlDatabase(conn_id="test", table=table)
    assert db.hook.schema == MYSQL_SCHEMA
