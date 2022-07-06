"""Tests specific to the Sqlite Database implementation."""
import os
import pathlib

import pandas as pd
import pytest
import sqlalchemy
from sqlalchemy.exc import ProgrammingError

from astro.constants import Database
from astro.databases import create_database
from astro.databases.snowflake import SnowflakeDatabase
from astro.exceptions import NonExistentTableException
from astro.files import File
from astro.settings import SCHEMA
from astro.sql.table import Metadata, Table
from astro.utils.load import copy_remote_file_to_local
from tests.sql.operators import utils as test_utils

DEFAULT_CONN_ID = "snowflake_default"
CUSTOM_CONN_ID = "snowflake_conn"
SUPPORTED_CONN_IDS = [CUSTOM_CONN_ID]
CWD = pathlib.Path(__file__).parent

TEST_TABLE = Table()


@pytest.mark.parametrize("conn_id", SUPPORTED_CONN_IDS)
def test_create_database(conn_id):
    """Test creation of database"""
    database = create_database(conn_id)
    assert isinstance(database, SnowflakeDatabase)


@pytest.mark.integration
def test_snowflake_run_sql():
    """Test run_sql against snowflake database"""
    statement = "SELECT 1 + 1;"
    database = SnowflakeDatabase(conn_id=CUSTOM_CONN_ID)
    response = database.run_sql(statement)
    assert response.first()[0] == 2


@pytest.mark.integration
def test_table_exists_raises_exception():
    """Test if table exists in snowflake database"""
    database = SnowflakeDatabase(conn_id=CUSTOM_CONN_ID)
    table = Table(name="inexistent-table", metadata=Metadata(schema=SCHEMA))
    assert not database.table_exists(table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "table": Table(
                metadata=Metadata(schema=SCHEMA),
                columns=[
                    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column(
                        "name", sqlalchemy.String(60), nullable=False, key="name"
                    ),
                ],
            ),
        }
    ],
    indirect=True,
    ids=["snowflake"],
)
def test_snowflake_create_table_with_columns(database_table_fixture):
    """Test table creation with columns data"""
    database, table = database_table_fixture

    statement = f"DESC TABLE {database.get_table_qualified_name(table)}"
    with pytest.raises(ProgrammingError) as e:
        database.run_sql(statement)
    assert e.match("does not exist or not authorized")

    database.create_table(table)
    response = database.run_sql(statement)
    rows = response.fetchall()
    assert len(rows) == 2
    assert rows[0] == (
        "ID",
        "NUMBER(38,0)",
        "COLUMN",
        "N",
        "IDENTITY START 1 INCREMENT 1",
        "Y",
        "N",
        None,
        None,
        None,
        None,
    )
    assert rows[1] == (
        "NAME",
        "VARCHAR(60)",
        "COLUMN",
        "N",
        None,
        "N",
        "N",
        None,
        None,
        None,
        None,
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
        },
    ],
    indirect=True,
    ids=["snowflake"],
)
def test_load_pandas_dataframe_to_table(database_table_fixture):
    """Test load_pandas_dataframe_to_table against snowflake"""
    database, table = database_table_fixture

    pandas_dataframe = pd.DataFrame(data={"id": [1, 2]})
    database.load_pandas_dataframe_to_table(pandas_dataframe, table)

    statement = f"SELECT * FROM {database.get_table_qualified_name(table)}"
    response = database.run_sql(statement)

    rows = response.fetchall()
    assert len(rows) == 2
    assert rows[0] == (1,)
    assert rows[1] == (2,)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        },
    ],
    indirect=True,
    ids=["snowflake"],
)
def test_load_file_to_table(database_table_fixture):
    """Test loading on files to snowflake database"""
    database, target_table = database_table_fixture
    filepath = str(pathlib.Path(CWD.parent, "data/sample.csv"))
    database.load_file_to_table(File(filepath), target_table, {})

    df = database.hook.get_pandas_df(
        f"SELECT * FROM {database.get_table_qualified_name(target_table)}"
    )
    assert len(df) == 3
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    test_utils.assert_dataframes_are_equal(df, expected)


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        },
    ],
    indirect=True,
    ids=["snowflake"],
)
def test_export_table_to_file_file_already_exists_raises_exception(
    database_table_fixture,
):
    """
    Test export_table_to_file_file() where the end file already exists, should result in exception
    when the override option is False
    """
    database, source_table = database_table_fixture
    filepath = pathlib.Path(CWD.parent, "data/sample.csv")
    with pytest.raises(FileExistsError) as exception_info:
        database.export_table_to_file(source_table, File(str(filepath)))
    err_msg = exception_info.value.args[0]
    assert err_msg.endswith(f"The file {filepath} already exists.")


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
            "table": Table(
                metadata=Metadata(
                    schema=os.getenv("SNOWFLAKE_SCHEMA", SCHEMA),
                    database=os.getenv("SNOWFLAKE_DATABASE", "snowflake"),
                )
            ),
        },
    ],
    indirect=True,
    ids=["snowflake"],
)
def test_export_table_to_file_overrides_existing_file(database_table_fixture):
    """
    Test export_table_to_file_file() where the end file already exists,
    should result in overriding the existing file
    """
    database, populated_table = database_table_fixture

    filepath = str(pathlib.Path(CWD.parent, "data/sample.csv"))
    database.export_table_to_file(populated_table, File(filepath), if_exists="replace")

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


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        }
    ],
    indirect=True,
    ids=["snowflake"],
)
def test_export_table_to_pandas_dataframe_non_existent_table_raises_exception(
    database_table_fixture,
):
    """Test export_table_to_file_file() where the table don't exist, should result in exception"""
    database, non_existent_table = database_table_fixture

    with pytest.raises(NonExistentTableException) as exc_info:
        database.export_table_to_pandas_dataframe(non_existent_table)
    error_message = exc_info.value.args[0]
    assert error_message.startswith("The table")
    assert error_message.endswith("does not exist")


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
        }
    ],
    indirect=True,
    ids=["snowflake"],
)
@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "google", "file_create": False}],
    indirect=True,
    ids=["google"],
)
def test_export_table_to_file_in_the_cloud(
    database_table_fixture, remote_files_fixture
):
    """Test export_table_to_file_file() where end file location is in cloud object stores"""
    object_path = remote_files_fixture[0]
    database, populated_table = database_table_fixture

    database.export_table_to_file(
        populated_table,
        File(object_path),
        if_exists="replace",
    )

    filepath = copy_remote_file_to_local(object_path)
    df = pd.read_csv(filepath)
    assert len(df) == 3
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    test_utils.assert_dataframes_are_equal(df, expected)
    os.remove(filepath)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
        }
    ],
    indirect=True,
    ids=["snowflake"],
)
def test_create_table_from_select_statement(database_table_fixture):
    """Test table creation via select statement"""
    database, original_table = database_table_fixture

    statement = "SELECT * FROM {} WHERE id = 1;".format(
        database.get_table_qualified_name(original_table)
    )
    target_table = Table(metadata=Metadata(schema=SCHEMA))
    database.create_table_from_select_statement(statement, target_table)

    df = database.hook.get_pandas_df(
        f"SELECT * FROM {database.get_table_qualified_name(target_table)}"
    )
    assert len(df) == 1
    expected = pd.DataFrame([{"id": 1, "name": "First"}])
    test_utils.assert_dataframes_are_equal(df, expected)
    database.drop_table(target_table)
