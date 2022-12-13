import os
import pathlib
import tempfile
from pathlib import Path

import boto3
import pandas as pd
import pytest
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from astro import sql as aql
from astro.constants import SUPPORTED_FILE_TYPES, Database, FileType
from astro.files import File
from astro.settings import SCHEMA
from astro.sql import ExportTableToFileOperator, export_file, export_table_to_file
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
def test_save_all_db_tables_to_s3(sample_dag, database_table_fixture):
    _creds = s3fs_creds()
    file_name = f"{test_utils.get_table_name('test_save')}.csv"

    output_file_path = f"s3://tmp9/{file_name}"

    db, test_table = database_table_fixture
    with sample_dag:
        export_file(
            input_data=test_table,
            output_file=File(path=output_file_path, conn_id="aws_default"),
            if_exists="replace",
        )
    test_utils.run_dag(sample_dag)

    df = db.export_table_to_pandas_dataframe(test_table)
    # # Read output CSV
    df_file = pd.read_csv(output_file_path, storage_options=s3fs_creds())

    assert len(df_file) == 47
    assert (df["sell"] == df_file["sell"]).all()

    # Delete object from S3
    s3 = boto3.Session(_creds["key"], _creds["secret"]).resource("s3")
    s3.Object("tmp9", file_name).delete()


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
def test_save_all_db_tables_to_gcs(sample_dag, database_table_fixture):
    database, test_table = database_table_fixture
    file_name = f"{test_utils.get_table_name('test_save')}.csv"
    bucket = "dag-authoring"

    output_file_path = f"gs://{bucket}/test/{file_name}"

    with sample_dag:
        export_table_to_file(
            input_data=test_table,
            output_file=File(path=output_file_path, conn_id="google_cloud_default"),
            if_exists="replace",
        )
    test_utils.run_dag(sample_dag)
    df = database.export_table_to_pandas_dataframe(source_table=test_table)

    assert (df["sell"].sort_values() == [129, 138, 142, 175, 232]).all()

    hook = GCSHook(gcp_conn_id="google_cloud_default")
    hook.delete(bucket, f"test/{file_name}")


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
            export_table_to_file(
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
            export_table_to_file(
                input_data=test_table,
                output_file=File(path=remote_files_fixture[0], conn_id="aws_default"),
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
            export_table_to_file(
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
        aql.export_table_to_file(
            input_data=test_table,
            output_file=File(path="/tmp/saved_df.csv"),
            if_exists="replace",
        )
        validate(test_table)

    test_utils.run_dag(sample_dag)


def test_raise_exception_for_invalid_input_type():
    with pytest.raises(ValueError) as exc_info:
        ExportTableToFileOperator(
            task_id="task_id",
            input_data=123,
            output_file=File(
                path="gs://astro-sdk/workspace/openlineage_export_file.csv",
                conn_id="bigquery",
                filetype=FileType.CSV,
            ),
            if_exists="replace",
        ).execute(context=None)
    expected_msg = "Expected input_table to be Table or dataframe. Got <class 'int'>"
    assert exc_info.value.args[0] == expected_msg


# TODO: Remove this test in astro-sdk 1.4
def test_warnings_message():
    from astro.sql.operators.export_file import ExportFileOperator, export_file

    with pytest.warns(
        expected_warning=DeprecationWarning,
        match="""This class is deprecated.
            Please use `astro.sql.operators.export_table_to_file.ExportTableToFileOperator`.
            And, will be removed in astro-sdk-python>=1.4.0.""",
    ):
        ExportFileOperator(
            task_id="task_id",
            input_data=123,
            output_file=File(
                path="gs://astro-sdk/workspace/openlineage_export_file.csv",
                conn_id="bigquery",
                filetype=FileType.CSV,
            ),
            if_exists="replace",
        )

    with pytest.warns(
        expected_warning=DeprecationWarning,
        match="""This decorator is deprecated.
        Please use `astro.sql.operators.export_table_to_file.export_table_to_file`.
        And, will be removed in astro-sdk-python>=1.4.0.""",
    ):
        export_file(input_data=Table(), output_file=File(path="/tmp/saved_df.csv"), if_exists="replace")
