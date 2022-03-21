"""
Unittest module to test Agnostic Load File function.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:
    AWS_ACCESS_KEY_ID=AKIAZG42HVH6Z3B6ELRB \
    AWS_SECRET_ACCESS_KEY=SgwfrcO2NdKpeKhUG77K%2F6B2HuRJJopbHPV84NbY \
    python3 -m unittest tests.operators.test_agnostic_load_file.TestAgnosticLoadFile.test_aql_local_file_to_postgres

"""
import copy
import logging
import os
import pathlib
import unittest.mock
from unittest import mock

import pandas as pd
import pytest
from airflow.exceptions import BackfillUnfinished, DuplicateTaskIdFound
from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from pandas.util.testing import assert_frame_equal

from astro.settings import SCHEMA
from astro.sql.operators.agnostic_load_file import AgnosticLoadFile, load_file
from astro.sql.table import Table, TempTable
from astro.utils.dependencies import gcs, s3
from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
OUTPUT_TABLE_NAME = test_utils.get_table_name("load_file_test_table")
OUTPUT_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
CWD = pathlib.Path(__file__).parent


def get_dataframe_from_table(sql_name: str, tmp_table: Table, hook):
    if sql_name == "bigquery":
        client = bigquery.Client()
        query_job = client.query(
            f"SELECT * FROM astronomer-dag-authoring.{tmp_table.qualified_name()}"
        )
        df = query_job.to_dataframe()
    else:
        df = pd.read_sql(
            f"SELECT * FROM {tmp_table.qualified_name()}",
            con=hook.get_conn(),
        )
    return df


@pytest.mark.integration
@pytest.mark.parametrize(
    "sql_server", ["snowflake", "postgres", "bigquery", "sqlite"], indirect=True
)
def test_aql_http_path_file_to_postgres(sample_dag, tmp_table, sql_server):
    sql_name, hook = sql_server

    with sample_dag:
        load_file(
            path="https://raw.githubusercontent.com/astro-projects/astro/main/tests/data/homes_main.csv",
            file_conn_id="",
            output_table=tmp_table,
        )
    test_utils.run_dag(sample_dag)

    df = get_dataframe_from_table(sql_name, tmp_table, hook)
    assert df.shape == (3, 9)


@pytest.mark.integration
@pytest.mark.parametrize(
    "remote_file",
    [{"name": "google"}, {"name": "amazon"}],
    indirect=True,
    ids=["gcs", "s3"],
)
@pytest.mark.parametrize(
    "sql_server", ["snowflake", "postgres", "bigquery", "sqlite"], indirect=True
)
def test_aql_s3_file_to_postgres(sample_dag, tmp_table, sql_server, remote_file):
    sql_name, hook = sql_server
    file_conn_id, file_uri = remote_file

    with sample_dag:
        load_file(path=file_uri[0], file_conn_id=file_conn_id, output_table=tmp_table)
    test_utils.run_dag(sample_dag)

    df = get_dataframe_from_table(sql_name, tmp_table, hook)
    df = df.sort_values(by=["name"])

    assert df.iloc[0].to_dict()["name"] == "First"


@pytest.mark.integration
@pytest.mark.parametrize(
    "sql_server", ["snowflake", "postgres", "bigquery", "sqlite"], indirect=True
)
def test_aql_replace_existing_table(sample_dag, tmp_table, sql_server):
    sql_name, hook = sql_server
    data_path_1 = str(CWD) + "/../data/homes.csv"
    data_path_2 = str(CWD) + "/../data/homes2.csv"
    with sample_dag:
        task_1 = load_file(path=data_path_1, file_conn_id="", output_table=tmp_table)
        task_2 = load_file(path=data_path_2, file_conn_id="", output_table=tmp_table)
        task_1 >> task_2
    test_utils.run_dag(sample_dag)

    df = get_dataframe_from_table(sql_name, tmp_table, hook)
    data_df = pd.read_csv(data_path_2)

    assert df.shape == data_df.shape


@pytest.mark.integration
@pytest.mark.parametrize(
    "sql_server", ["snowflake", "postgres", "bigquery", "sqlite"], indirect=True
)
def test_aql_local_file_with_no_table_name(sample_dag, tmp_table, sql_server):
    sql_name, hook = sql_server
    data_path = str(CWD) + "/../data/homes.csv"
    with sample_dag:
        load_file(path=data_path, file_conn_id="", output_table=tmp_table)
    test_utils.run_dag(sample_dag)

    df = get_dataframe_from_table(sql_name, tmp_table, hook)
    data_df = pd.read_csv(data_path)

    assert df.shape == data_df.shape


@pytest.mark.unitest
def test_unique_task_id_for_same_path(sample_dag):
    OUTPUT_TABLE_NAME = "expected_table_from_csv_1"

    tasks = []

    with sample_dag:
        for index in range(4):
            params = {
                "path": str(CWD) + "/../data/homes.csv",
                "file_conn_id": "",
                "output_table": Table(
                    OUTPUT_TABLE_NAME,
                    database="pagila",
                    conn_id="postgres_conn",
                ),
            }
            if index == 3:
                params["task_id"] = "task_id"

            task = load_file(**params)
            tasks.append(task)

    test_utils.run_dag(sample_dag)

    assert tasks[0].operator.task_id != tasks[1].operator.task_id
    assert tasks[1].operator.task_id == "load_file___1"
    assert tasks[2].operator.task_id == "load_file___2"
    assert tasks[3].operator.task_id == "task_id"


@pytest.mark.unitest
def test_path_validation():
    test_table = [
        {"input": "gs://mybucket/puppy.jpg", "output": True},
        {"input": "S3://mybucket/puppy.jpg", "output": True},
        {
            "input": "https://my-bucket.s3.us-west-2.amazonaws.com/puppy.png",
            "output": True,
        },
        {"input": "/etc/someFile/randomFileName.csv", "output": False},
        {"input": "\x00", "output": False},
        {"input": "a" * 256, "output": False},
    ]

    for test in test_table:
        assert AgnosticLoadFile.validate_path(test["input"]) == test["output"]


@pytest.mark.unitest
@mock.patch.dict(
    os.environ,
    {"AWS_ACCESS_KEY_ID": "abcd", "AWS_SECRET_ACCESS_KEY": "@#$%@$#ASDH@Ksd23%SD546"},
)
def test_aws_decode():
    from astro.utils.cloud_storage_creds import parse_s3_env_var

    k, v = parse_s3_env_var()
    assert v == "@#$%@$#ASDH@Ksd23%SD546"


@pytest.mark.parametrize("sql_server", ["sqlite"], indirect=True)
def test_load_file_templated_filename(sample_dag, sql_server):
    database_name, sql_hook = sql_server

    sql_server_params = copy.deepcopy(
        test_utils.SQL_SERVER_HOOK_PARAMETERS[database_name]
    )
    conn_id_value = sql_server_params.pop(
        test_utils.SQL_SERVER_CONNECTION_KEY[database_name]
    )
    sql_server_params["conn_id"] = conn_id_value

    task_params = {
        "path": str(CWD) + "/../data/{{ var.value.foo }}/example.csv",
        "file_conn_id": "",
        "output_table": Table(
            table_name=OUTPUT_TABLE_NAME + "_{{ var.value.foo }}", **sql_server_params
        ),
    }

    test_utils.create_and_run_task(sample_dag, load_file, (), task_params)
    df = sql_hook.get_pandas_df(
        f"SELECT * FROM {OUTPUT_TABLE_NAME}_templated_file_name"
    )
    assert len(df) == 3


@pytest.mark.parametrize(
    "remote_file",
    [{"name": "google", "count": 2}, {"name": "amazon", "count": 2}],
    ids=["google", "amazon"],
    indirect=True,
)
def test_aql_load_file_pattern(remote_file, sample_dag):
    file_conn_id, file_prefix = remote_file
    filename = pathlib.Path(CWD.parent, "data/sample.csv")
    OUTPUT_TABLE_NAME = f"expected_table_from_gcs_csv__{file_conn_id}"

    test_df_rows = pd.read_csv(filename).shape[0]

    hook_target = PostgresHook(postgres_conn_id="postgres_conn", schema="pagila")
    cur = hook_target.get_cursor()
    cur.execute(f"DROP TABLE IF EXISTS public.{OUTPUT_TABLE_NAME} CASCADE;")

    with sample_dag:
        load_file(
            path=file_prefix[0][0:-5],
            file_conn_id=file_conn_id,
            output_table=Table(
                OUTPUT_TABLE_NAME,
                database="pagila",
                conn_id="postgres_conn",
                schema="public",
            ),
        )
    test_utils.run_dag(sample_dag)

    # Read table from db
    df = pd.read_sql(f"SELECT * FROM {OUTPUT_TABLE_NAME}", con=hook_target.get_conn())
    assert test_df_rows * 2 == df.shape[0]


def test_aql_load_file_local_file_pattern(sample_dag):
    filename = str(CWD.parent) + "/data/homes_pattern_1.csv"
    OUTPUT_TABLE_NAME = f"test_aql_load_file_pattern_table"

    test_df_rows = pd.read_csv(filename).shape[0]

    hook_target = PostgresHook(postgres_conn_id="postgres_conn", schema="pagila")
    cur = hook_target.get_cursor()
    cur.execute(f"DROP TABLE IF EXISTS public.{OUTPUT_TABLE_NAME} CASCADE;")

    with sample_dag:
        load_file(
            path=str(CWD.parent) + "/data/homes_pattern_*",
            file_conn_id="",
            output_table=Table(
                OUTPUT_TABLE_NAME,
                database="pagila",
                conn_id="postgres_conn",
                schema="public",
            ),
        )
    test_utils.run_dag(sample_dag)

    # Read table from db
    df = pd.read_sql(f"SELECT * FROM {OUTPUT_TABLE_NAME}", con=hook_target.get_conn())
    assert test_df_rows * 2 == df.shape[0]


@pytest.mark.integration
@pytest.mark.parametrize("sql_server", ["sqlite"], indirect=True)
@pytest.mark.parametrize(
    "remote_file", [{"name": "google"}, {"name": "amazon"}], indirect=True
)
def test_load_file_using_file_connection(sample_dag, remote_file, sql_server):
    database_name, sql_hook = sql_server
    file_conn_id, file_uri = remote_file

    sql_server_params = test_utils.get_default_parameters(database_name)

    task_params = {
        "path": file_uri[0],
        "file_conn_id": file_conn_id,
        "output_table": Table(table_name=OUTPUT_TABLE_NAME, **sql_server_params),
    }

    test_utils.create_and_run_task(sample_dag, load_file, (), task_params)
    df = sql_hook.get_pandas_df(f"SELECT * FROM {OUTPUT_TABLE_NAME}")
    assert len(df) == 3


@pytest.mark.unitest
def test_load_file_using_file_connection_fails_inexistent_conn(caplog, sample_dag):
    database_name = "postgres"
    file_conn_id = "fake_conn"
    file_uri = "s3://fake-bucket/fake-object"

    sql_server_params = test_utils.get_default_parameters(database_name)

    task_params = {
        "path": file_uri,
        "file_conn_id": file_conn_id,
        "output_table": Table(table_name=OUTPUT_TABLE_NAME, **sql_server_params),
    }
    with pytest.raises(BackfillUnfinished) as exec_info:
        test_utils.create_and_run_task(sample_dag, load_file, (), task_params)
    expected_error = "Failed to execute task: The conn_id `fake_conn` isn't defined."
    assert expected_error in caplog.text


def create_task_parameters(database_name, file_type):
    sql_server_params = test_utils.get_default_parameters(database_name)

    task_params = {
        "path": str(pathlib.Path(CWD.parent, f"data/sample.{file_type}")),
        "file_conn_id": "",
        "output_table": Table(table_name=OUTPUT_TABLE_NAME, **sql_server_params),
    }
    return task_params


@pytest.mark.parametrize(
    "sql_server", ["snowflake", "postgres", "bigquery", "sqlite"], indirect=True
)
@pytest.mark.parametrize("file_type", ["parquet", "ndjson", "json", "csv"])
def test_load_file(sample_dag, sql_server, file_type):
    database_name, sql_hook = sql_server

    task_params = create_task_parameters(database_name, file_type)
    test_utils.create_and_run_task(sample_dag, load_file, (), task_params)

    qualified_name = task_params["output_table"].qualified_name()
    df = sql_hook.get_pandas_df(f"SELECT * FROM {qualified_name}")

    assert len(df) == 3
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    df = df.rename(columns=str.lower)
    df = df.astype({"id": "int64"})
    expected = expected.astype({"id": "int64"})
    assert_frame_equal(df, expected)


@pytest.mark.parametrize(
    "sql_server", ["bigquery", "postgres", "snowflake"], indirect=True
)
def test_load_file_chunks(sample_dag, sql_server):
    file_type = "csv"
    database_name, sql_hook = sql_server

    chunk_function = {
        "bigquery": "pandas.DataFrame.to_gbq",
        "postgres": "pandas.DataFrame.to_sql",
        "snowflake": "snowflake.connector.pandas_tools.write_pandas",
    }[database_name]

    chunk_size_argument = {
        "bigquery": "chunksize",
        "postgres": "chunksize",
        "snowflake": "chunk_size",
    }[database_name]

    with mock.patch(chunk_function) as mock_chunk_function:
        task_params = create_task_parameters(database_name, file_type)
        test_utils.create_and_run_task(sample_dag, load_file, (), task_params)

    _, kwargs = mock_chunk_function.call_args
    assert kwargs[chunk_size_argument] == 1000000
