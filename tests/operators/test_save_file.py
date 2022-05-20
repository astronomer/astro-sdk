"""
Unittest module to test Load File function.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:
    AWS_ACCESS_KEY_ID=KEY \
    AWS_SECRET_ACCESS_KEY=SECRET \
    python3 -m unittest tests.operators.test_save_file.TestSaveFile.test_save_postgres_table_to_local

"""
import logging
import os
import pathlib
import tempfile
from pathlib import Path

import boto3
import pandas as pd
import pytest
from airflow.exceptions import BackfillUnfinished
from airflow.utils import timezone

import astro.dataframe as adf
import astro.sql as aql
from astro.constants import SUPPORTED_DATABASES, SUPPORTED_FILE_TYPES, Database
from astro.files import File
from astro.settings import SCHEMA

# Import Operator
from astro.sql.operators.save_file import save_file
from astro.sql.table import Metadata
from astro.utils.dependencies import gcs
from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
INPUT_TABLE_NAME = test_utils.get_table_name("save_file_test_table")
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


def test_save_dataframe_to_local(sample_dag):
    @adf
    def make_df():
        return pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    with sample_dag:
        df = make_df()
        aql.save_file(
            input_data=df,
            output_file=File(path="/tmp/saved_df.csv"),
            if_exists="replace",
        )
    test_utils.run_dag(sample_dag)

    df = pd.read_csv("/tmp/saved_df.csv")
    assert df.equals(pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]}))


@pytest.mark.parametrize("sql_server", [Database.SQLITE.value], indirect=True)
@pytest.mark.parametrize(
    "table_fixture",
    [{}],
    indirect=True,
    ids=["table"],
)
def test_save_temp_table_to_local(sample_dag, sql_server, table_fixture):
    data_path = str(CWD) + "/../data/homes.csv"
    with sample_dag:
        table = aql.load_file(
            input_file=File(path=data_path), output_table=table_fixture
        )
        aql.save_file(
            input_data=table,
            output_file=File(path="/tmp/saved_df.csv"),
            if_exists="replace",
        )
    test_utils.run_dag(sample_dag)

    output_df = pd.read_csv("/tmp/saved_df.csv")
    input_df = pd.read_csv(data_path)
    assert input_df.equals(output_df)


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "table_fixture",
    [
        {
            "path": str(CWD) + "/../data/homes.csv",
            "load_table": True,
            "param": {
                "metadata": Metadata(schema=SCHEMA),
                "name": test_utils.get_table_name("test_stats_check_1"),
            },
        }
    ],
    indirect=True,
    ids=["temp_table"],
)
def test_save_all_db_tables_to_S3(sample_dag, table_fixture, sql_server):

    _creds = s3fs_creds()
    sql_name, hook = sql_server
    file_name = f"{test_utils.get_table_name('test_save')}.csv"

    OUTPUT_FILE_PATH = f"s3://tmp9/{file_name}"

    with sample_dag:
        save_file(
            input_data=table_fixture,
            output_file=File(path=OUTPUT_FILE_PATH, conn_id="aws_default"),
            if_exists="replace",
        )
    test_utils.run_dag(sample_dag)

    df = test_utils.get_dataframe_from_table(sql_name, table_fixture, hook)

    # # Read output CSV
    df_file = pd.read_csv(OUTPUT_FILE_PATH, storage_options=s3fs_creds())

    assert len(df_file) == 47
    if sql_name != "snowflake":
        assert (df["sell"] == df_file["sell"]).all()
    else:
        assert (df["SELL"] == df_file["sell"]).all()

    # Delete object from S3
    s3 = boto3.Session(_creds["key"], _creds["secret"]).resource("s3")
    s3.Object("tmp9", file_name).delete()


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "table_fixture",
    [
        {
            "path": str(CWD) + "/../data/homes2.csv",
            "load_table": True,
            "param": {
                "metadata": Metadata(schema=SCHEMA),
                "name": test_utils.get_table_name("test_stats_check_1"),
            },
        }
    ],
    indirect=True,
    ids=["temp_table"],
)
def test_save_all_db_tables_to_GCS(sample_dag, table_fixture, sql_server):

    sql_name, hook = sql_server
    file_name = f"{test_utils.get_table_name('test_save')}.csv"
    bucket = "dag-authoring"

    OUTPUT_FILE_PATH = f"gs://{bucket}/test/{file_name}"

    with sample_dag:
        save_file(
            input_data=table_fixture,
            output_file=File(path=OUTPUT_FILE_PATH, conn_id="google_cloud_default"),
            if_exists="replace",
        )
    test_utils.run_dag(sample_dag)

    df = test_utils.get_dataframe_from_table(sql_name, table_fixture, hook)

    if sql_name != "snowflake":
        assert (df["sell"].sort_values() == [129, 138, 142, 175, 232]).all()
    else:
        assert (df["SELL"].sort_values() == [129, 138, 142, 175, 232]).all()

    hook = gcs.GCSHook(gcp_conn_id="google_cloud_default")
    hook.delete(bucket, f"test/{file_name}")


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "table_fixture",
    [
        {
            "path": str(CWD) + "/../data/homes.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "metadata": Metadata(schema=SCHEMA),
                "name": test_utils.get_table_name("test_stats_check_1"),
            },
        }
    ],
    indirect=True,
    ids=["table"],
)
def test_save_all_db_tables_to_local_file_exists_overwrite_false(
    sample_dag, table_fixture, sql_server, caplog
):
    with tempfile.NamedTemporaryFile(suffix=".csv") as temp_file:
        with pytest.raises(BackfillUnfinished):
            with sample_dag:
                save_file(
                    input_data=table_fixture,
                    output_file=File(path=temp_file.name),
                    if_exists="exception",
                )
            test_utils.run_dag(sample_dag)
        expected_error = f"{temp_file.name} file already exists."
        assert expected_error in caplog.text


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "table_fixture",
    [
        {
            "path": str(CWD) + "/../data/homes.csv",
            "load_table": True,
            "param": {
                "metadata": Metadata(schema=SCHEMA),
                "name": test_utils.get_table_name("test_stats_check_1"),
            },
        }
    ],
    indirect=True,
    ids=["table"],
)
@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "google"}, {"provider": "amazon"}],
    indirect=True,
    ids=["google", "amazon"],
)
def test_save_table_remote_file_exists_overwrite_false(
    sample_dag, table_fixture, sql_server, remote_files_fixture, caplog
):

    with pytest.raises(BackfillUnfinished):
        with sample_dag:
            save_file(
                input_data=table_fixture,
                output_file=File(path=remote_files_fixture[0], conn_id="aws_default"),
                if_exists="exception",
            )
        test_utils.run_dag(sample_dag)

    expected_error = f"{remote_files_fixture[0]} file already exists."
    assert expected_error in caplog.text


@pytest.mark.parametrize("sql_server", [Database.SQLITE.value], indirect=True)
@pytest.mark.parametrize(
    "table_fixture",
    [
        {
            "path": str(CWD) + "/../data/homes2.csv",
            "load_table": True,
            "param": {
                "metadata": Metadata(schema=SCHEMA),
                "name": test_utils.get_table_name("test_save"),
            },
        }
    ],
    indirect=True,
    ids=["temp_table"],
)
def test_unique_task_id_for_same_path(sample_dag, sql_server, table_fixture):
    file_name = f"{test_utils.get_table_name('output')}.csv"
    OUTPUT_FILE_PATH = str(CWD) + f"/../data/{file_name}"

    tasks = []
    with sample_dag:
        for i in range(4):
            params = {
                "input_data": table_fixture,
                "output_file": File(path=OUTPUT_FILE_PATH),
                "if_exists": "replace",
            }

            if i == 3:
                params["task_id"] = "task_id"
            task = save_file(**params)
            tasks.append(task)
    test_utils.run_dag(sample_dag)

    assert tasks[0].operator.task_id != tasks[1].operator.task_id
    assert tasks[0].operator.task_id == f"save_file_{file_name.replace('.','_')}"
    assert tasks[1].operator.task_id == f"save_file_{file_name.replace('.','_')}__1"
    assert tasks[2].operator.task_id == f"save_file_{file_name.replace('.','_')}__2"
    assert tasks[3].operator.task_id == "task_id"

    os.remove(OUTPUT_FILE_PATH)


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize("file_type", SUPPORTED_FILE_TYPES)
@pytest.mark.parametrize(
    "table_fixture",
    [
        {
            "path": str(CWD) + "/../data/sample.csv",
            "load_table": True,
            "param": {
                "metadata": Metadata(schema=SCHEMA),
                "name": test_utils.get_table_name("test_stats_check_1"),
            },
        }
    ],
    indirect=True,
    ids=["test-table"],
)
def test_save_file(sample_dag, sql_server, file_type, table_fixture):

    with tempfile.TemporaryDirectory() as tmp_dir:
        filepath = Path(tmp_dir, f"sample.{file_type}")
        with sample_dag:
            save_file(
                input_data=table_fixture,
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
