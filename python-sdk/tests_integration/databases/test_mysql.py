"""Tests specific to the MySql Database implementation."""

import pathlib

import pandas as pd
import pytest
import sqlalchemy

from astro.constants import Database
from astro.databases.mysql import MysqlDatabase
from astro.settings import SCHEMA
from astro.table import Metadata, Table

DEFAULT_CONN_ID = "mysql_default"
CUSTOM_CONN_ID = "mysql_conn"
SUPPORTED_CONN_IDS = [DEFAULT_CONN_ID, CUSTOM_CONN_ID]
CWD = pathlib.Path(__file__).parent

TEST_TABLE = Table()


@pytest.mark.integration
def test_mysql_run_sql():
    """Test run_sql against mysql database"""
    statement = "SELECT 1 + 1;"
    database = MysqlDatabase(conn_id=CUSTOM_CONN_ID)
    response = database.run_sql(statement, handler=lambda x: x.first())
    assert response[0] == 2


@pytest.mark.integration
def test_table_exists_raises_exception():
    """Test if table exists in mysql database"""
    database = MysqlDatabase(conn_id=CUSTOM_CONN_ID)
    table = Table(name="non-existing-table", metadata=Metadata(schema=SCHEMA))
    assert not database.table_exists(table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.MYSQL,
            "table": Table(
                metadata=Metadata(database=SCHEMA, schema=SCHEMA),
                columns=[
                    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column("name", sqlalchemy.String(60), nullable=False, key="name"),
                ],
            ),
        }
    ],
    indirect=True,
    ids=["mysql"],
)
def test_mysql_create_table_with_columns(database_table_fixture):
    """Test table creation with columns data"""
    database, table = database_table_fixture

    statement = (
        f"SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE table_name='{table.name}'"
    )
    response = database.run_sql(statement, handler=lambda x: x.first())
    assert response is None

    database.create_table(table)
    response = database.run_sql(statement, handler=lambda x: x.fetchall())
    rows = response
    assert len(rows) == 2
    assert rows[0][:3] == (
        SCHEMA,
        f"{table.name}",
        "id",
    )
    assert rows[1][:3] == (
        SCHEMA,
        f"{table.name}",
        "name",
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.MYSQL},
    ],
    indirect=True,
    ids=["mysql"],
)
def test_load_pandas_dataframe_to_table(database_table_fixture):
    """Test load_pandas_dataframe_to_table against mysql"""
    database, table = database_table_fixture

    pandas_dataframe = pd.DataFrame(data={"id": [1, 2]})
    database.load_pandas_dataframe_to_table(pandas_dataframe, table)

    statement = f"SELECT id FROM {database.get_table_qualified_name(table)};"
    response = database.run_sql(statement, handler=lambda x: x.fetchall())

    rows = response
    assert len(rows) == 2
    assert rows[0] == (1,)
    assert rows[1] == (2,)
