from urllib.parse import urlparse

import pytest
import sqlalchemy

from astro.databases import get_database_from_conn_id
from astro.databases.sqlite import Database as SqliteDatabase
from astro.sql.tables import Table

DEFAULT_CONN_ID = "sqlite_default"
CUSTOM_CONN_ID = "sqlite_conn"
SUPPORTED_CONN_IDS = [DEFAULT_CONN_ID, CUSTOM_CONN_ID]


@pytest.mark.parametrize("conn_id", SUPPORTED_CONN_IDS)
def test_get_database_from_conn_id(conn_id):
    database = get_database_from_conn_id(conn_id)
    assert isinstance(database, SqliteDatabase)


@pytest.mark.parametrize(
    "conn_id,expected_uri",
    [
        (DEFAULT_CONN_ID, "//tmp/sqlite_default.db"),
        (CUSTOM_CONN_ID, "////tmp/sqlite.db"),
    ],
    ids=SUPPORTED_CONN_IDS,
)
def test_sqlalchemy_engine(conn_id, expected_uri):
    database = SqliteDatabase(conn_id)
    engine = database.sqlalchemy_engine
    assert isinstance(engine, sqlalchemy.engine.base.Engine)
    url = urlparse(str(engine.url))
    assert url.path == expected_uri


def test_run_sql():
    statement = "SELECT    1 + 1;"
    database = SqliteDatabase(DEFAULT_CONN_ID)
    response = database.run_sql(statement)
    assert response.first()[0] == 2


def test_create_table():
    sample_table = Table(
        columns=[
            sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column(
                "name", sqlalchemy.String(60), nullable=False, key="name"
            ),
        ]
    )
    database = SqliteDatabase(DEFAULT_CONN_ID)
    database.drop_table(sample_table)
    statement = f"PRAGMA table_info({sample_table.name});"
    response = database.run_sql(statement)
    assert response.first() is None

    database.create_table(sample_table)
    response = database.run_sql(statement)
    assert response.first() == (0, "id", "INTEGER", 1, None, 1)
