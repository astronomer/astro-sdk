"""
Tests specific to the Sqlite Database implementation.
"""
import pathlib
from urllib.parse import urlparse

import pandas as pd
import pytest
import sqlalchemy
from pandas.testing import assert_frame_equal

from astro.databases import get_database_from_conn_id
from astro.databases.sqlite import SqliteDatabase
from astro.sql.tables import Table
from tests.operators import utils as test_utils

DEFAULT_CONN_ID = "sqlite_default"
CUSTOM_CONN_ID = "sqlite_conn"
SUPPORTED_CONN_IDS = [DEFAULT_CONN_ID, CUSTOM_CONN_ID]
CWD = pathlib.Path(__file__).parent


TEST_TABLE = Table()


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
def test_sqlite_sqlalchemy_engine(conn_id, expected_uri):
    database = SqliteDatabase(conn_id)
    engine = database.sqlalchemy_engine
    assert isinstance(engine, sqlalchemy.engine.base.Engine)
    url = urlparse(str(engine.url))
    assert url.path == expected_uri


@pytest.mark.integration
def test_sqlite_run_sql():
    statement = "SELECT 1 + 1;"
    database = SqliteDatabase()
    response = database.run_sql(statement)
    assert response.first()[0] == 2


@pytest.mark.integration
def test_sqlite_create_and_drop_table_with_columns():
    # TODO: use table fixture (teardown) and split this test into two - one for creating and another for dropping
    # Move this test to a place where we can use parametrize
    sample_table = Table(
        columns=[
            sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column(
                "name", sqlalchemy.String(60), nullable=False, key="name"
            ),
        ]
    )
    database = SqliteDatabase()
    database.drop_table(sample_table)
    statement = f"PRAGMA table_info({sample_table.name});"
    response = database.run_sql(statement)
    assert response.first() is None

    database.create_table(sample_table)
    response = database.run_sql(statement)
    rows = response.fetchall()
    assert len(rows) == 2
    assert rows[0] == (0, "id", "INTEGER", 1, None, 1)
    assert rows[1] == (1, "name", "VARCHAR(60)", 1, None, 0)
    database.drop_table(sample_table)

    response = database.run_sql(statement)
    assert response.first() is None


@pytest.mark.integration
def test_load_pandas_dataframe_to_table():
    # TODO: use table fixture (teardown)
    database = SqliteDatabase()
    data = {"id": [1, 2]}
    pandas_dataframe = pd.DataFrame(data=data)
    target_table = Table()
    database.load_pandas_dataframe_to_table(pandas_dataframe, target_table)

    statement = f"SELECT * FROM {target_table.name};"
    response = database.run_sql(statement)
    rows = response.fetchall()
    assert len(rows) == 2
    assert rows[0] == (1,)
    assert rows[1] == (2,)


@pytest.mark.integration
def test_load_file_to_table():
    # TODO: use table fixture (teardown)
    database = SqliteDatabase()
    filepath = pathlib.Path(CWD.parent, "data/sample.csv")
    target_table = Table()
    database.load_file_to_table(filepath, target_table)

    df = database.hook.get_pandas_df(f"SELECT * FROM {target_table.name}")
    assert len(df) == 3
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    df = df.rename(columns=str.lower)
    df = df.astype({"id": "int64"})
    expected = expected.astype({"id": "int64"})
    assert_frame_equal(df, expected)


@pytest.mark.integration
def test_export_table_to_file_file_already_exists_raises_exception():
    database = SqliteDatabase()
    filepath = pathlib.Path(CWD.parent, "data/sample.csv")
    source_table = Table()
    with pytest.raises(FileExistsError) as exception_info:
        database.export_table_to_file(source_table, filepath)
    err_msg = exception_info.value.args[0]
    assert err_msg.startswith("The file")
    assert err_msg.endswith("tests/data/sample.csv already exists.")


@pytest.mark.integration
def test_export_table_to_file_overrides_existing_file():
    # TODO: use table fixture (teardown)
    # setup
    df = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    populated_table = Table()
    database = SqliteDatabase()
    database.load_pandas_dataframe_to_table(df, populated_table)

    # actual test
    filepath = str(pathlib.Path(CWD.parent, "data/sample.csv"))
    database.export_table_to_file(populated_table, filepath, if_exists="replace")

    df = test_utils.load_to_dataframe(filepath, "csv")
    assert len(df) == 3
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    assert df.rename(columns=str.lower).equals(expected)


def test_export_table_to_file_in_the_cloud():
    # TODO: use table fixture (teardown)
    # TODO: use remote_file fixture (teardown)

    df = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    populated_table = Table()
    database = SqliteDatabase()
    database.load_pandas_dataframe_to_table(df, populated_table)

    # actual test
    filepath = "gs://dag-authoring/test/sample.csv"
    database.export_table_to_file(
        populated_table,
        filepath,
        # target_file_conn_id="google_cloud_default",
        if_exists="replace",
    )


def test_create_table_from_select_statement():
    # TODO: use table fixture (teardown)
    database = SqliteDatabase()
    filepath = pathlib.Path(CWD.parent, "data/sample.csv")
    original_table = Table()
    database.load_file_to_table(filepath, original_table)

    # actual test
    statement = "SELECT * FROM {} WHERE id = 1;".format(
        database.get_table_qualified_name(original_table)
    )
    target_table = Table()
    database.create_table_from_select_statement(statement, target_table)

    df = database.hook.get_pandas_df(f"SELECT * FROM {target_table.name}")
    assert len(df) == 1
    expected = pd.DataFrame([{"id": 1, "name": "First"}])
    df = df.rename(columns=str.lower)
    df = df.astype({"id": "int64"})
    expected = expected.astype({"id": "int64"})
    assert_frame_equal(df, expected)
