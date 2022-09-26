"""Tests all Database implementations."""
import pathlib
from unittest import mock

import pandas as pd
import pytest
from astro.constants import Database
from astro.files import File
from astro.settings import SCHEMA
from astro.sql.table import Metadata, Table

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
    ],
    indirect=True,
    ids=["bigquery", "postgres", "redshift", "snowflake", "sqlite"],
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
    assert df.rename(columns=str.lower).equals(expected)


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
    ],
    indirect=True,
    ids=["bigquery", "postgres", "redshift", "snowflake", "sqlite"],
)
def test_export_to_dataframe_via_byte_stream_is_called_for_postgres(
    database_table_fixture,
):
    """Test export_to_dataframe_via_byte_stream() is called in case the db is postgres."""
    database, table = database_table_fixture
    file = File(str(pathlib.Path(CWD.parent, "data/sample.csv")))

    with mock.patch(
        "astro.files.base.File.export_to_dataframe"
    ) as export_to_dataframe, mock.patch(
        "astro.files.base.File.export_to_dataframe_via_byte_stream"
    ) as export_to_dataframe_via_byte_stream:
        database.get_dataframe_from_file(file)
        if database.sql_type == "postgresql":
            export_to_dataframe_via_byte_stream.assert_called()
        else:
            export_to_dataframe.assert_called()
