import os
import pathlib
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from astro import sql as aql
from astro.constants import SUPPORTED_FILE_TYPES, Database
from astro.files import File
from astro.settings import SCHEMA
from astro.sql import export_to_file
from astro.table import Table

from ..operators import utils as test_utils

CWD = pathlib.Path(__file__).parent


def s3fs_creds():
    """Structure s3fs credentials from Airflow connection.
    s3fs enables pandas to write to s3
    """
    # To-do: clean-up how S3 creds are passed to s3fs
    return {
        "key": os.environ["AWS_ACCESS_KEY_ID"],
        "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
    }


@pytest.mark.integration
@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "google"}, {"provider": "amazon"}, {"provider": "azure"}],
    indirect=True,
    ids=["google_gcs", "amazon_s3", "azure_blob_storage"],
)
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
def test_export_to_file_dbs_to_remote_file(sample_dag, database_table_fixture, remote_files_fixture):
    _, test_table = database_table_fixture
    file_uri = remote_files_fixture[0]

    if file_uri.startswith("wasb"):
        file_ = File(file_uri, conn_id="wasb_default_conn")
    else:
        file_ = File(file_uri)
    assert not file_.exists()

    with sample_dag:
        export_to_file(
            input_data=test_table,
            output_file=file_,
            if_exists="replace",
        )
    test_utils.run_dag(sample_dag)

    assert file_.exists()
    assert file_.size


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
def test_save_all_db_tables_to_local_file_exists_overwrite_false(sample_dag, database_table_fixture):
    _, test_table = database_table_fixture
    with tempfile.NamedTemporaryFile(suffix=".csv") as temp_file, pytest.raises(FileExistsError):
        with sample_dag:
            export_to_file(
                input_data=test_table,
                output_file=File(path=temp_file.name),
                if_exists="exception",
            )
        test_utils.run_dag(sample_dag)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "file": File(path=str(CWD) + "/../../data/homes.csv"),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../data/homes.csv"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/homes.csv"),
        },
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../data/homes.csv"),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/homes.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "google"}, {"provider": "amazon"}],
    indirect=True,
    ids=["google", "amazon"],
)
def test_save_table_remote_file_exists_overwrite_false(
    sample_dag, database_table_fixture, remote_files_fixture
):
    _, test_table = database_table_fixture
    with pytest.raises(FileExistsError):
        with sample_dag:
            export_to_file(
                input_data=test_table,
                output_file=File(path=remote_files_fixture[0]),
                if_exists="exception",
            )
        test_utils.run_dag(sample_dag)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
@pytest.mark.parametrize("file_type", SUPPORTED_FILE_TYPES)
def test_export_file(sample_dag, database_table_fixture, file_type):
    _, test_table = database_table_fixture
    with tempfile.TemporaryDirectory() as tmp_dir:
        filepath = Path(tmp_dir, f"sample.{file_type}")
        with sample_dag:
            export_to_file(
                input_data=test_table,
                output_file=File(path=str(filepath)),
                if_exists="exception",
            )
        test_utils.run_dag(sample_dag)

        df = test_utils.load_to_dataframe(filepath, file_type)
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
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
    ],
    indirect=True,
    ids=["postgresql", "redshift"],
)
def test_populate_table_metadata(sample_dag, database_table_fixture):
    """
    Test default populating of table fields in export_file op.
    """
    _, test_table = database_table_fixture
    test_table.metadata.schema = None

    @aql.dataframe
    def validate(table: Table):
        assert table.metadata.schema == SCHEMA

    with sample_dag:
        aql.export_to_file(
            input_data=test_table,
            output_file=File(path="/tmp/saved_df.csv"),
            if_exists="replace",
        )
        validate(test_table)

    test_utils.run_dag(sample_dag)
