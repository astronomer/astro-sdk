"""
Unittest module to test Agnostic Load File function.

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
from astro.settings import SCHEMA

# Import Operator
from astro.sql.operators.agnostic_save_file import save_file
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
        d = {"col1": [1, 2], "col2": [3, 4]}
        return pd.DataFrame(data=d)

    with sample_dag:
        df = make_df()
        aql.save_file(input=df, output_file_path="/tmp/saved_df.csv", overwrite=True)
    test_utils.run_dag(sample_dag)

    df = pd.read_csv("/tmp/saved_df.csv")
    assert df.equals(pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]}))


@pytest.mark.parametrize("sql_server", [Database.SQLITE.value], indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [{"is_temp": True}],
    indirect=True,
    ids=["temp_table"],
)
def test_save_temp_table_to_local(sample_dag, sql_server, test_table):
    data_path = str(CWD) + "/../data/homes.csv"
    with sample_dag:
        table = aql.load_file(path=data_path, file_conn_id="", output_table=test_table)
        aql.save_file(input=table, output_file_path="/tmp/saved_df.csv", overwrite=True)
    test_utils.run_dag(sample_dag)

    output_df = pd.read_csv("/tmp/saved_df.csv")
    input_df = pd.read_csv(data_path)
    assert input_df.equals(output_df)


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "schema": SCHEMA,
                "table_name": test_utils.get_table_name("test_stats_check_1"),
            },
        }
    ],
    indirect=True,
    ids=["temp_table"],
)
def test_save_all_db_tables_to_S3(sample_dag, test_table, sql_server):

    _creds = s3fs_creds()
    sql_name, hook = sql_server
    file_name = f"{test_utils.get_table_name('test_save')}.csv"

    OUTPUT_FILE_PATH = f"s3://tmp9/{file_name}"

    with sample_dag:
        save_file(
            input=test_table,
            output_file_path=OUTPUT_FILE_PATH,
            output_conn_id="aws_default",
            overwrite=True,
        )
    test_utils.run_dag(sample_dag)

    df = test_utils.get_dataframe_from_table(sql_name, test_table, hook)

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
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "schema": SCHEMA,
                "table_name": test_utils.get_table_name("test_stats_check_1"),
            },
        }
    ],
    indirect=True,
    ids=["temp_table"],
)
def test_save_all_db_tables_to_local_file_exists_overwrite_false(
    sample_dag, test_table, sql_server, caplog
):
    with tempfile.NamedTemporaryFile() as temp_file:
        with pytest.raises(BackfillUnfinished):
            with sample_dag:
                save_file(
                    input=test_table,
                    output_file_path=temp_file.name,
                    output_conn_id=None,
                    overwrite=False,
                )
            test_utils.run_dag(sample_dag)
        expected_error = f"{temp_file.name} file already exists."
        assert expected_error in caplog.text


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "schema": SCHEMA,
                "table_name": test_utils.get_table_name("test_stats_check_1"),
            },
        }
    ],
    indirect=True,
    ids=["temp_table"],
)
@pytest.mark.parametrize(
    "remote_file",
    [{"name": "google"}, {"name": "amazon"}],
    indirect=True,
    ids=["google_gcs", "amazon_s3"],
)
def test_save_table_remote_file_exists_overwrite_false(
    sample_dag, test_table, sql_server, remote_file, caplog
):
    conn_id, object_paths = remote_file

    with pytest.raises(BackfillUnfinished):
        with sample_dag:
            save_file(
                input=test_table,
                output_file_path=object_paths[0],
                output_conn_id="aws_default",
                overwrite=False,
            )
        test_utils.run_dag(sample_dag)

    expected_error = f"{object_paths[0]} file already exists."
    assert expected_error in caplog.text


@pytest.mark.parametrize("sql_server", [Database.SQLITE.value], indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "is_temp": False,
            "path": str(CWD) + "/../data/homes2.csv",
            "load_table": True,
            "param": {
                "schema": SCHEMA,
                "table_name": test_utils.get_table_name("test_save"),
            },
        }
    ],
    indirect=True,
    ids=["temp_table"],
)
def test_unique_task_id_for_same_path(sample_dag, sql_server, test_table):
    file_name = f"{test_utils.get_table_name('output')}.csv"
    OUTPUT_FILE_PATH = str(CWD) + f"/../data/{file_name}"

    tasks = []
    with sample_dag:
        for i in range(4):
            params = {
                "input": test_table,
                "output_file_path": OUTPUT_FILE_PATH,
                "output_conn_id": None,
                "overwrite": True,
            }

            if i == 3:
                params["task_id"] = "task_id"
            task = save_file(**params)
            tasks.append(task)
    test_utils.run_dag(sample_dag)

    assert tasks[0].operator.task_id != tasks[1].operator.task_id
    assert tasks[1].operator.task_id == "save_file___1"
    assert tasks[2].operator.task_id == "save_file___2"
    assert tasks[3].operator.task_id == "task_id"

    os.remove(OUTPUT_FILE_PATH)


def load_to_dataframe(filepath, file_type):
    read = {
        "parquet": pd.read_parquet,
        "csv": pd.read_csv,
        "json": pd.read_json,
        "ndjson": pd.read_json,
    }
    read_params = {"ndjson": {"lines": True}}
    mode = {"parquet": "rb"}
    with open(filepath, mode.get(file_type, "r")) as fp:
        return read[file_type](fp, **read_params.get(file_type, {}))


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize("file_type", SUPPORTED_FILE_TYPES)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/sample.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "schema": SCHEMA,
                "table_name": test_utils.get_table_name("test_stats_check_1"),
            },
        }
    ],
    indirect=True,
    ids=["test-table"],
)
def test_save_file(sample_dag, sql_server, file_type, test_table):
    sql_name, sql_hook = sql_server

    with tempfile.TemporaryDirectory() as tmp_dir:
        filepath = Path(tmp_dir, f"sample.{file_type}")
        with sample_dag:
            save_file(
                input=test_table,
                output_file_path=str(filepath),
                output_file_format=file_type,
                output_conn_id=None,
                overwrite=False,
            )
        test_utils.run_dag(sample_dag)

        df = load_to_dataframe(filepath, file_type)
        assert len(df) == 3
        expected = pd.DataFrame(
            [
                {"id": 1, "name": "First"},
                {"id": 2, "name": "Second"},
                {"id": 3, "name": "Third with unicode पांचाल"},
            ]
        )
        assert df.rename(columns=str.lower).equals(expected)
