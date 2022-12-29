import json
import os
import pathlib

import pandas as pd
import pytest

from astro.constants import Database, FileType
from astro.files import File
from astro.files.locations import create_file_location
from astro.settings import SCHEMA
from astro.table import Metadata, Table
from astro.utils.load import copy_remote_file_to_local
from tests.sql.operators import utils as test_utils

DEFAULT_CONN_ID = "sftp_conn"
CWD = pathlib.Path(__file__).parent


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.POSTGRES,
            "file": File(str(pathlib.Path(CWD.parent.parent, "data/sample.csv"))),
        },
        {
            "database": Database.SNOWFLAKE,
            "file": File(str(pathlib.Path(CWD.parent.parent, "data/sample.csv"))),
            "table": Table(
                metadata=Metadata(
                    schema=os.getenv("SNOWFLAKE_SCHEMA", SCHEMA),
                    database=os.getenv("SNOWFLAKE_DATABASE", "snowflake"),
                )
            ),
        },
        {
            "database": Database.SQLITE,
            "file": File(str(pathlib.Path(CWD.parent.parent, "data/sample.csv"))),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(str(pathlib.Path(CWD.parent.parent, "data/sample.csv"))),
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(str(pathlib.Path(CWD.parent.parent, "data/sample.csv"))),
        },
    ],
    indirect=True,
    ids=["postgres", "snowflake", "sqlite", "bigquery", "redshift"],
)
def test_export_table_to_file_in_the_cloud(database_table_fixture):
    """Test export_table_to_file_file() where end file location is in cloud object stores"""
    object_path = "sftp://upload/test.csv"
    final_path = "sftp://foo:pass@localhost:2222/upload/test.csv"
    database, populated_table = database_table_fixture
    database.export_table_to_file(
        populated_table,
        File(object_path, conn_id=DEFAULT_CONN_ID),
        if_exists="replace",
    )

    filepath = copy_remote_file_to_local(
        source_filepath=final_path, transport_params={"connect_kwargs": {"password": "pass"}}
    )
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
    "path, filetype",
    [
        ("sftp://upload/filename", FileType.CSV),
        ("sftp://upload/filename", FileType.JSON),
        ("sftp://upload/filename", FileType.NDJSON),
        ("sftp://upload/filename", FileType.PARQUET),
    ],
)
def test_create_from_dataframe(path, filetype):
    """Test get_size() of for SFTP file."""
    data = {"id": [1, 2, 3], "name": ["First", "Second", "Third with unicode पांचाल"]}
    df = pd.DataFrame(data=data)
    path = path + "." + filetype.value
    final_path = "sftp://foo:pass@localhost:2222/upload/filename." + filetype.value
    location = create_file_location(path, DEFAULT_CONN_ID)
    location.create_from_dataframe("upload/filename." + filetype.value, df, filetype)
    filepath = copy_remote_file_to_local(
        source_filepath=final_path, is_binary=True, transport_params={"connect_kwargs": {"password": "pass"}}
    )
    count = 0
    if filetype == FileType.CSV:
        df = pd.read_csv(filepath)
        count = len(df)
    elif filetype == FileType.JSON:
        df = pd.read_json(filepath)
        count = len(df)
    elif filetype == FileType.NDJSON:
        with open(filepath) as file:
            for _, line in enumerate(file):
                assert len(json.loads(line).keys()) == 2
                count = count + 1
    elif filetype == FileType.PARQUET:
        df = pd.read_parquet(filepath)
        count = len(df)
    assert count == 3
    os.remove(filepath)
