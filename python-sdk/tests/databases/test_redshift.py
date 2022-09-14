"""Tests specific to the Sqlite Database implementation."""
import os
import pathlib
import re
from urllib.parse import urlparse

import pandas as pd
import pytest
import sqlalchemy
from astro.constants import Database, FileType
from astro.databases import create_database
from astro.databases.aws.redshift import RedshiftDatabase
from astro.exceptions import NonExistentTableException
from astro.files import File
from astro.settings import SCHEMA
from astro.sql.table import Metadata, Table
from astro.utils.load import copy_remote_file_to_local
from tests.sql.operators import utils as test_utils

DEFAULT_CONN_ID = "redshift_default"
CUSTOM_CONN_ID = "redshift_conn"
SUPPORTED_CONN_IDS = [DEFAULT_CONN_ID, CUSTOM_CONN_ID]
CWD = pathlib.Path(__file__).parent

TEST_TABLE = Table()


@pytest.mark.parametrize("conn_id", SUPPORTED_CONN_IDS)
def test_create_database(conn_id):
    """Test creation of database"""
    database = create_database(conn_id)
    assert isinstance(database, RedshiftDatabase)


@pytest.mark.parametrize(
    "conn_id,expected_uri",
    [
        (DEFAULT_CONN_ID, re.compile(r"^(redshift\+redshift_connector://)(.*)")),
        (
            CUSTOM_CONN_ID,
            r"^(redshift\+redshift_connector://)(.*)(us-east-2.redshift.amazonaws.com:5439/dev)$",
        ),
    ],
    ids=SUPPORTED_CONN_IDS,
)
def test_redshift_sqlalchemy_engine(conn_id, expected_uri):
    """Test getting a redshift based sqlalchemy engine."""
    database = RedshiftDatabase(conn_id)
    engine = database.sqlalchemy_engine
    assert isinstance(engine, sqlalchemy.engine.base.Engine)
    url = urlparse(str(engine.url))
    assert re.match(expected_uri, url.geturl())


@pytest.mark.integration
def test_redshift_run_sql():
    """Test run_sql against redshift database"""
    statement = "SELECT 1 + 1;"
    database = RedshiftDatabase(conn_id=CUSTOM_CONN_ID)
    response = database.run_sql(statement)
    assert response.first()[0] == 2


@pytest.mark.integration
def test_inexistent_table_returns_false_on_table_exists_check():
    """Test if table exists in redshift database"""
    database = RedshiftDatabase(conn_id=CUSTOM_CONN_ID)
    table = Table(name="inexistent-table", metadata=Metadata(schema=SCHEMA))
    assert not database.table_exists(table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.REDSHIFT,
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
    ids=["redshift"],
)
def test_redshift_create_table_with_columns(database_table_fixture):
    """Test table creation with columns data"""
    database, table = database_table_fixture

    statement = (
        f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='{table.name}'"
    )
    response = database.run_sql(statement)
    assert response.first() is None

    database.create_table(table)
    response = database.run_sql(statement)
    rows = response.fetchall()
    assert len(rows) == 2
    assert rows[0][0:4] == (
        "dev",
        SCHEMA,
        f"{table.name}",
        "id",
    )
    assert rows[1][0:4] == (
        "dev",
        SCHEMA,
        f"{table.name}",
        "name",
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.REDSHIFT},
    ],
    indirect=True,
    ids=["redshift"],
)
def test_load_pandas_dataframe_to_table(database_table_fixture):
    """Test load_pandas_dataframe_to_table against redshift"""
    database, table = database_table_fixture

    pandas_dataframe = pd.DataFrame(data={"id": [1, 2]})
    database.load_pandas_dataframe_to_table(pandas_dataframe, table)

    statement = f"SELECT * FROM {database.get_table_qualified_name(table)};"
    response = database.run_sql(statement)

    rows = response.fetchall()
    assert len(rows) == 2
    assert rows[0] == (1,)
    assert rows[1] == (2,)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.REDSHIFT},
    ],
    indirect=True,
    ids=["redshift"],
)
def test_load_file_to_table(database_table_fixture):
    """Test loading on files to redshift database"""
    database, target_table = database_table_fixture
    filepath = str(pathlib.Path(CWD.parent, "data/sub_folder/"))
    database.load_file_to_table(File(filepath, filetype=FileType.CSV), target_table, {})

    df = database.hook.get_pandas_df(
        f"SELECT * FROM {database.get_table_qualified_name(target_table)}"
    ).sort_values(by="id")
    assert len(df) == 3
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    ).sort_values(by="id")
    test_utils.assert_dataframes_are_equal(df, expected)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.REDSHIFT,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        },
    ],
    indirect=True,
    ids=["redshift"],
)
def test_load_file_from_cloud_to_table(database_table_fixture):
    """Test loading of files from S3 bucket folder to Redshift database."""
    database, target_table = database_table_fixture
    database.load_file_to_table(
        File(
            "s3://astro-sdk-redshift/data_redshift/",
            conn_id="aws_conn",
            filetype=FileType.CSV,
        ),
        target_table,
        use_native_support=False,
    )

    df = database.hook.get_pandas_df(
        f"SELECT * FROM {database.get_table_qualified_name(target_table)}"
    ).sort_values(by="id")
    assert len(df) == 3
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    ).sort_values(by="id")
    test_utils.assert_dataframes_are_equal(df, expected)


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.REDSHIFT},
    ],
    indirect=True,
    ids=["redshift"],
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
            "database": Database.REDSHIFT,
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
        },
    ],
    indirect=True,
    ids=["redshift"],
)
def test_export_table_to_file_overrides_existing_file(database_table_fixture):
    """
    Test export_table_to_file() where the end file already exists,
    should result in overriding the existing file
    """
    filepath = "/tmp/file_to_override.csv"

    previous_dataframe = pd.DataFrame(
        [{"id": 1, "name": "xyz"}, {"id": 2, "name": "abc"}]
    )
    previous_dataframe.to_csv(filepath)

    df = test_utils.load_to_dataframe(filepath, "csv").sort_values(by="id")

    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    ).sort_values(by="id")

    assert not df.rename(columns=str.lower).equals(expected)

    database, populated_table = database_table_fixture
    database.export_table_to_file(populated_table, File(filepath), if_exists="replace")

    df = test_utils.load_to_dataframe(filepath, "csv").sort_values(by="id")
    assert len(df) == 3
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    ).sort_values(by="id")
    assert df.rename(columns=str.lower).equals(expected)
    os.remove(filepath)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [{"database": Database.REDSHIFT}],
    indirect=True,
    ids=["redshift"],
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
            "database": Database.REDSHIFT,
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
        }
    ],
    indirect=True,
    ids=["redshift"],
)
@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "amazon", "file_create": False}],
    indirect=True,
    ids=["amazon_s3"],
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
    df = pd.read_csv(filepath).sort_values(by="id")
    assert len(df) == 3
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    ).sort_values(by="id")
    test_utils.assert_dataframes_are_equal(df, expected)
    os.remove(filepath)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.REDSHIFT,
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
        }
    ],
    indirect=True,
    ids=["redshift"],
)
def test_create_table_from_select_statement(database_table_fixture):
    """Test table creation via select statement"""
    database, original_table = database_table_fixture

    table_qualified_name = database.get_table_qualified_name(original_table)
    statement = f"SELECT * FROM {table_qualified_name} WHERE id = 1;"
    target_table = original_table.create_similar_table()
    database.create_table_from_select_statement(statement, target_table)

    df = database.hook.get_pandas_df(
        f"SELECT * FROM {database.get_table_qualified_name(target_table)}"
    ).sort_values(by="id")
    assert len(df) == 1
    expected = pd.DataFrame([{"id": 1, "name": "First"}]).sort_values(by="id")
    test_utils.assert_dataframes_are_equal(df, expected)
    database.drop_table(target_table)
