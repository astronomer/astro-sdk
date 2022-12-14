"""Tests all Database implementations."""
import pathlib
from unittest import mock

import pandas as pd
import pytest

from astro.constants import Database
from astro.databases.base import BaseDatabase
from astro.dataframes.pandas import PandasDataframe
from astro.files import File
from astro.settings import SCHEMA
from astro.table import Metadata, Table

CWD = pathlib.Path(__file__).parent


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        },
        {
            "database": Database.POSTGRES,
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
            "table": Table(metadata=Metadata(schema=SCHEMA.lower())),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
            "table": Table(metadata=Metadata(schema=SCHEMA.lower())),
        },
        {
            "database": Database.SNOWFLAKE,
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        },
        {
            "database": Database.SQLITE,
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
            "table": Table(),
        },
        {
            "database": Database.DELTA,
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
            "table": Table(),
        },
    ],
    indirect=True,
    ids=["bigquery", "postgres", "redshift", "snowflake", "sqlite", "delta"],
)
def test_export_table_to_pandas_dataframe(
    database_table_fixture,
):
    """Test export_table_to_pandas_dataframe() where the table exists"""
    database, table = database_table_fixture

    df = database.export_table_to_pandas_dataframe(table)
    assert len(df) == 3
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    # Due to a weird minor difference with how databricks creates dataframes, this is a workaround
    # to ensure equality when the actual data inside the DF is equal.
    assert df.rename(columns=str.lower).to_dict() == expected.to_dict()
    assert isinstance(df, PandasDataframe)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.MSSQL,
            "file": File(str(pathlib.Path(CWD.parent, "data/sample_without_unicode.csv"))),
            "table": Table(metadata=Metadata(schema=SCHEMA.lower())),
        },
    ],
    indirect=True,
    ids=["mssql"],
)
def test_export_table_to_pandas_dataframe_mssql(
    database_table_fixture,
):
    """Test export_table_to_pandas_dataframe() where the table exists"""
    database, table = database_table_fixture

    df = database.export_table_to_pandas_dataframe(table)
    assert len(df) == 2
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
        ]
    )
    # Due to a weird minor difference with how databricks creates dataframes, this is a workaround
    # to ensure equality when the actual data inside the DF is equal.
    assert df.rename(columns=str.lower).to_dict() == expected.to_dict()
    assert isinstance(df, PandasDataframe)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.BIGQUERY},
        {"database": Database.POSTGRES},
        {"database": Database.REDSHIFT},
        {"database": Database.SNOWFLAKE},
        {"database": Database.SQLITE},
        {"database": Database.DELTA},
        {"database": Database.MSSQL},
    ],
    indirect=True,
    ids=["bigquery", "postgres", "redshift", "snowflake", "sqlite", "delta", "mssql"],
)
def test_load_pandas_dataframe_to_table_with_append(database_table_fixture):
    """Load Pandas Dataframe to a SQL table with append strategy"""
    database, table = database_table_fixture

    pandas_dataframe = pd.DataFrame(data={"id": [1, 2]})
    database.load_pandas_dataframe_to_table(
        source_dataframe=pandas_dataframe,
        target_table=table,
        if_exists="append",
    )

    rows = database.fetch_all_rows(table)
    assert len(rows) == 2
    assert rows[0] == (1,)
    assert rows[1] == (2,)

    database.load_pandas_dataframe_to_table(
        source_dataframe=pandas_dataframe,
        target_table=table,
        if_exists="append",
    )

    rows = database.fetch_all_rows(table)
    assert len(rows) == 4
    assert rows[0] == (1,)
    assert rows[1] == (2,)
    assert rows[2] == (1,)
    assert rows[3] == (2,)

    database.drop_table(table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.BIGQUERY},
        {"database": Database.POSTGRES},
        {"database": Database.REDSHIFT},
        {"database": Database.SNOWFLAKE},
        {"database": Database.SQLITE},
        {"database": Database.DELTA},
    ],
    indirect=True,
    ids=["bigquery", "postgres", "redshift", "snowflake", "sqlite", "delta"],
)
@pytest.mark.parametrize("row_count", [0, 100])
@mock.patch.object(BaseDatabase, "run_sql")
def test_fetch_all_rows(mock_run_sql, database_table_fixture, row_count):
    db, table = database_table_fixture
    db.run_sql = mock_run_sql
    db.fetch_all_rows(table, row_count)
    select_statements = [m.args[0] for m in mock_run_sql.mock_calls if m.args and "SELECT" in m.args[0]]
    assert len(select_statements) == 1
    if row_count > -1:
        assert f"LIMIT {row_count}" in select_statements[0]
    else:
        assert f"LIMIT {row_count}" not in select_statements[0]


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.MSSQL},
    ],
    indirect=True,
    ids=["mssql"],
)
@pytest.mark.parametrize("row_count", [0, 100])
@mock.patch.object(BaseDatabase, "run_sql")
def test_fetch_all_rows_mssql(mock_run_sql, database_table_fixture, row_count):
    db, table = database_table_fixture
    db.run_sql = mock_run_sql
    db.fetch_all_rows(table, row_count)
    select_statements = [m.args[0] for m in mock_run_sql.mock_calls if m.args and "SELECT" in m.args[0]]
    assert len(select_statements) == 1
    assert f"TOP {row_count}" in select_statements[0]


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.BIGQUERY},
        {"database": Database.POSTGRES},
        {"database": Database.REDSHIFT},
        {"database": Database.SNOWFLAKE},
        {"database": Database.SQLITE},
        {"database": Database.MSSQL},
    ],
    indirect=True,
    ids=["bigquery", "postgres", "redshift", "snowflake", "sqlite", "mssql"],
)
def test_load_pandas_dataframe_to_table_with_replace(database_table_fixture):
    """Load Pandas Dataframe to a SQL table with replace strategy"""
    database, table = database_table_fixture

    pandas_dataframe = pd.DataFrame(data={"id": [1, 2, 3]})
    database.load_pandas_dataframe_to_table(
        source_dataframe=pandas_dataframe,
        target_table=table,
    )

    rows = database.fetch_all_rows(table)
    assert len(rows) == 3
    assert rows[0] == (1,)
    assert rows[1] == (2,)

    pandas_dataframe = pd.DataFrame(data={"id": [3, 4]})
    database.load_pandas_dataframe_to_table(
        source_dataframe=pandas_dataframe,
        target_table=table,
    )

    rows = database.fetch_all_rows(table)
    assert len(rows) == 2
    assert rows[0] == (3,)
    assert rows[1] == (4,)

    database.drop_table(table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        },
        {
            "database": Database.POSTGRES,
            "table": Table(metadata=Metadata(schema=SCHEMA.lower())),
        },
        {
            "database": Database.SNOWFLAKE,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        },
        {"database": Database.SQLITE, "table": Table()},
        {"database": Database.DELTA, "table": Table()},
        {"database": Database.MSSQL, "table": Table(metadata=Metadata(schema=SCHEMA))},
    ],
    indirect=True,
    ids=["bigquery", "postgres", "snowflake", "sqlite", "delta", "mssql"],
)
@mock.patch("astro.files.base.File.export_to_dataframe")
@mock.patch("astro.files.base.File.export_to_dataframe_via_byte_stream")
def test_export_to_dataframe_via_byte_stream_is_called_for(
    export_to_dataframe_via_byte_stream,
    export_to_dataframe,
    database_table_fixture,
):
    """Test export_to_dataframe_via_byte_stream() is called in case the db is postgres."""
    database, _ = database_table_fixture
    file = File(str(pathlib.Path(CWD.parent, "data/sample.csv")))

    database.get_dataframe_from_file(file)
    if database.sql_type == "postgresql":
        export_to_dataframe_via_byte_stream.assert_called()
    else:
        export_to_dataframe.assert_called()
