"""Tests specific to the SqlServer Database implementation."""
import pathlib
import re
from urllib.parse import urlparse

import pandas as pd
import pytest
import sqlalchemy

from astro.constants import Database
from astro.databases import create_database
from astro.databases.mssql import MssqlDatabase
from astro.settings import SCHEMA
from astro.table import Metadata, Table

DEFAULT_CONN_ID = "mssql_default"
CUSTOM_CONN_ID = "mssql_conn"
SUPPORTED_CONN_IDS = [DEFAULT_CONN_ID, CUSTOM_CONN_ID]
CWD = pathlib.Path(__file__).parent

TEST_TABLE = Table()


@pytest.mark.parametrize("conn_id", SUPPORTED_CONN_IDS)
def test_create_database(conn_id):
    """Test creation of database"""
    database = create_database(conn_id)
    assert isinstance(database, MssqlDatabase)


@pytest.mark.parametrize(
    "conn_id,expected_uri",
    [
        (DEFAULT_CONN_ID, re.compile(r"^(mssql\+pymssql://)(.*):1433$")),
        (
            CUSTOM_CONN_ID,
            r"^(mssql\+pymssql://)(.*)(astroserver.database.windows.net:1433/astrodb)$",
        ),
    ],
    ids=SUPPORTED_CONN_IDS,
)
def test_mssql_sqlalchemy_engine(conn_id, expected_uri):
    """Test getting a mssql based sqla engine."""
    database = MssqlDatabase(conn_id)
    engine = database.sqlalchemy_engine
    assert isinstance(engine, sqlalchemy.engine.base.Engine)
    url = urlparse(str(engine.url))
    assert re.match(expected_uri, url.geturl())


@pytest.mark.integration
def test_mssql_run_sql():
    """Test run_sql against mssql database"""
    statement = "SELECT 1 + 1;"
    database = MssqlDatabase(conn_id=CUSTOM_CONN_ID)
    response = database.run_sql(statement, handler=lambda x: x.first())
    assert response[0] == 2


@pytest.mark.integration
def test_table_exists_raises_exception():
    """Test if table exists in mssql database"""
    database = MssqlDatabase(conn_id=CUSTOM_CONN_ID)
    table = Table(name="non-existing-table", metadata=Metadata(schema=SCHEMA))
    assert not database.table_exists(table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.MSSQL,
            "table": Table(
                metadata=Metadata(schema=SCHEMA),
                columns=[
                    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column("name", sqlalchemy.String(60), nullable=False, key="name"),
                ],
            ),
        }
    ],
    indirect=True,
    ids=["mssql"],
)
def test_mssql_create_table_with_columns(database_table_fixture):
    """Test table creation with columns data"""
    database, table = database_table_fixture

    statement = f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='{table.name}'"
    response = database.run_sql(statement, handler=lambda x: x.first())
    assert response is None

    database.create_table(table)
    response = database.run_sql(statement, handler=lambda x: x.fetchall())
    rows = response
    assert len(rows) == 2
    assert rows[0][0:4] == (
        "astrodb",
        SCHEMA,
        f"{table.name}",
        "id",
    )
    assert rows[1][0:4] == (
        "astrodb",
        SCHEMA,
        f"{table.name}",
        "name",
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.MSSQL},
    ],
    indirect=True,
    ids=["mssql"],
)
def test_load_pandas_dataframe_to_table(database_table_fixture):
    """Test load_pandas_dataframe_to_table against mssql"""
    database, table = database_table_fixture

    pandas_dataframe = pd.DataFrame(data={"id": [1, 2]})
    database.load_pandas_dataframe_to_table(pandas_dataframe, table)

    statement = f"SELECT * FROM {database.get_table_qualified_name(table)};"
    response = database.run_sql(statement, handler=lambda x: x.fetchall())

    rows = response
    assert len(rows) == 2
    assert rows[0] == (1,)
    assert rows[1] == (2,)
