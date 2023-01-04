import os
import pathlib

import airflow
import pandas as pd
import pytest

from astro.constants import Database
from astro.files import File
from astro.settings import SCHEMA
from astro.table import Metadata, Table
from astro.utils.load import copy_remote_file_to_local
from tests.sql.operators import utils as test_utils

DEFAULT_CONN_ID = "ftp_conn"
CWD = pathlib.Path(__file__).parent


@pytest.mark.integration
@pytest.mark.skipif(airflow.__version__ < "2.3.0", reason="Require Airflow version >= 2.3.0")
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
def test_export_table_to_file_in_the_ftp(database_table_fixture):
    """Test export_table_to_file() where end file location is in FTP"""
    object_path = "ftp://upload/test_ftp.csv"
    final_path = "ftp://foo:pass@localhost:21/upload/test_ftp.csv"
    database, populated_table = database_table_fixture
    database.export_table_to_file(
        populated_table,
        File(object_path, conn_id=DEFAULT_CONN_ID),
        if_exists="replace",
    )

    filepath = copy_remote_file_to_local(source_filepath=final_path, transport_params={})
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
