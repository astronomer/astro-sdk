"""
Unittest module to test Agnostic Load File function.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:
    AWS_ACCESS_KEY_ID=AKIAZG42HVH6Z3B6ELRB \
    AWS_SECRET_ACCESS_KEY=SgwfrcO2NdKpeKhUG77K%2F6B2HuRJJopbHPV84NbY \
    python3 -m unittest tests.operators.test_agnostic_load_file.TestAgnosticLoadFile.test_aql_local_file_to_postgres

"""
import logging
import os
import pathlib
from unittest import mock

import pandas as pd
import pytest
from airflow.exceptions import BackfillUnfinished
from airflow.utils import timezone
from pandas.testing import assert_frame_equal

from astro.constants import Database
from astro.databases import create_database
from astro.sql.operators.agnostic_load_file import load_file
from astro.sql.tables import Table
from astro.utils.dependencies import gcs, s3
from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
OUTPUT_TABLE_NAME = test_utils.get_table_name("load_file_test_table")
OUTPUT_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
CWD = pathlib.Path(__file__).parent


@pytest.mark.integration
@pytest.mark.parametrize(
    "sql_server", ["snowflake", "postgres", "bigquery", "sqlite"], indirect=True
)
@pytest.mark.parametrize(
    "test_table",
    [{"is_temp": True}, {"is_temp": False}],
    indirect=True,
    ids=["temp_table", "named_table"],
)
def test_load_file_with_http_path_file(sample_dag, test_table, sql_server):
    sql_name, hook = sql_server
    with sample_dag:
        load_file(
            path="https://raw.githubusercontent.com/astro-projects/astro/main/tests/data/homes_main.csv",
            file_conn_id="",
            output_table=test_table,
        )
    test_utils.run_dag(sample_dag)

    df = test_utils.get_dataframe_from_table(sql_name, test_table, hook)
    assert df.shape == (3, 9)


@pytest.mark.integration
@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "google"}, {"provider": "amazon"}],
    indirect=True,
    ids=["google_gcs", "amazon_s3"],
)
@pytest.mark.parametrize(
    "sql_server", ["snowflake", "postgres", "bigquery", "sqlite"], indirect=True
)
@pytest.mark.parametrize(
    "test_table",
    [{"is_temp": True}, {"is_temp": False}],
    indirect=True,
    ids=["temp_table", "named_table"],
)
def test_aql_load_remote_file_to_dbs(
    sample_dag, test_table, sql_server, remote_files_fixture
):
    sql_name, hook = sql_server
    file_uri = remote_files_fixture[0]

    with sample_dag:
        load_file(path=file_uri, output_table=test_table)
    test_utils.run_dag(sample_dag)

    df = test_utils.get_dataframe_from_table(sql_name, test_table, hook)

    # Workaround for snowflake capitalized col names
    sort_cols = "name"
    if sort_cols not in df.columns:
        sort_cols = sort_cols.upper()

    df = df.sort_values(by=[sort_cols])

    assert df.iloc[0].to_dict()[sort_cols] == "First"


@pytest.mark.integration
@pytest.mark.parametrize(
    "sql_server", ["snowflake", "postgres", "bigquery", "sqlite"], indirect=True
)
@pytest.mark.parametrize(
    "test_table",
    [{"is_temp": True}, {"is_temp": False}],
    indirect=True,
    ids=["temp_table", "named_table"],
)
def test_aql_replace_existing_table(sample_dag, test_table, sql_server):
    sql_name, hook = sql_server
    data_path_1 = str(CWD) + "/../data/homes.csv"
    data_path_2 = str(CWD) + "/../data/homes2.csv"
    with sample_dag:
        task_1 = load_file(path=data_path_1, output_table=test_table)
        task_2 = load_file(path=data_path_2, output_table=test_table)
        task_1 >> task_2
    test_utils.run_dag(sample_dag)

    df = test_utils.get_dataframe_from_table(sql_name, test_table, hook)
    data_df = pd.read_csv(data_path_2)

    assert df.shape == data_df.shape


@pytest.mark.integration
@pytest.mark.parametrize(
    "sql_server", ["snowflake", "postgres", "bigquery", "sqlite"], indirect=True
)
@pytest.mark.parametrize(
    "test_table",
    [{"is_temp": True}, {"is_temp": False}],
    indirect=True,
    ids=["temp_table", "named_table"],
)
def test_aql_local_file_with_no_table_name(sample_dag, test_table, sql_server):
    sql_name, hook = sql_server
    data_path = str(CWD) + "/../data/homes.csv"
    with sample_dag:
        load_file(path=data_path, output_table=test_table)
    test_utils.run_dag(sample_dag)

    df = test_utils.get_dataframe_from_table(sql_name, test_table, hook)
    data_df = pd.read_csv(data_path)

    assert df.shape == data_df.shape


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


@mock.patch.dict(
    os.environ,
    {"AWS_ACCESS_KEY_ID": "abcd", "AWS_SECRET_ACCESS_KEY": "@#$%@$#ASDH@Ksd23%SD546"},
)
def test_aws_decode():
    from astro.utils.cloud_storage_creds import parse_s3_env_var

    k, v = parse_s3_env_var()
    assert v == "@#$%@$#ASDH@Ksd23%SD546"


@pytest.mark.parametrize("sql_server", ["sqlite"], indirect=True)
def test_load_file_templated_filename(sample_dag, sql_server, test_table):
    database_name, sql_hook = sql_server
    with sample_dag:
        load_file(
            path=str(CWD) + "/../data/{{ var.value.foo }}/example.csv",
            file_conn_id="",
            output_table=test_table,
        )
    test_utils.run_dag(sample_dag)

    database = create_database(test_table.conn_id)
    qualified_name = database.get_table_qualified_name(test_table)
    df = sql_hook.get_pandas_df(f"SELECT * FROM {qualified_name}")
    assert len(df) == 3


@pytest.mark.integration
@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "google", "file_count": 2}, {"provider": "amazon", "file_count": 2}],
    ids=["google", "amazon"],
    indirect=True,
)
@pytest.mark.parametrize("sql_server", ["sqlite"], indirect=True)
def test_aql_load_file_pattern(
    remote_files_fixture, sample_dag, test_table, sql_server
):
    remote_object_uri = remote_files_fixture[0]
    filename = pathlib.Path(CWD.parent, "data/sample.csv")
    sql_name, hook = sql_server

    with sample_dag:
        load_file(
            path=remote_object_uri[0:-5],
            output_table=test_table,
        )
    test_utils.run_dag(sample_dag)

    df = test_utils.get_dataframe_from_table(sql_name, test_table, hook)
    test_df_rows = pd.read_csv(filename).shape[0]

    assert test_df_rows * 2 == df.shape[0]


@pytest.mark.integration
@pytest.mark.parametrize("sql_server", ["postgres"], indirect=True)
def test_aql_load_file_local_file_pattern(sample_dag, test_table, sql_server):
    filename = str(CWD.parent) + "/data/homes_pattern_1.csv"
    database_name, sql_hook = sql_server

    test_df_rows = pd.read_csv(filename).shape[0]

    with sample_dag:
        load_file(
            path=str(CWD.parent) + "/data/homes_pattern_*",
            file_conn_id="",
            output_table=test_table,
        )
    test_utils.run_dag(sample_dag)

    # Read table from db
    database = create_database(test_table.conn_id)
    qualified_name = database.get_table_qualified_name(test_table)
    df = pd.read_sql(f"SELECT * FROM {qualified_name}", con=sql_hook.get_conn())
    assert test_df_rows * 2 == df.shape[0]


@pytest.mark.integration
@pytest.mark.parametrize("sql_server", ["sqlite"], indirect=True)
@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "google"}, {"provider": "amazon"}],
    indirect=True,
    ids=["google", "amazon"],
)
def test_load_file_using_file_connection(
    sample_dag, remote_files_fixture, sql_server, test_table
):
    database_name, sql_hook = sql_server
    file_uri = remote_files_fixture[0]
    if file_uri.startswith("s3"):
        file_conn_id = s3.S3Hook.default_conn_name
    else:
        file_conn_id = gcs.GCSHook.default_conn_name
    with sample_dag:
        load_file(
            path=file_uri,
            file_conn_id=file_conn_id,
            output_table=test_table,
        )
    test_utils.run_dag(sample_dag)

    database = create_database(test_table.conn_id)
    qualified_name = database.get_table_qualified_name(test_table)
    df = sql_hook.get_pandas_df(f"SELECT * FROM {qualified_name}")
    assert len(df) == 3


@pytest.mark.parametrize("sql_server", ["postgres"], indirect=True)
def test_load_file_using_file_connection_fails_nonexistent_conn(
    caplog, sample_dag, sql_server
):
    database_name = "postgres"
    file_conn_id = "fake_conn"
    file_uri = "s3://fake-bucket/fake-object"

    sql_server_params = test_utils.get_default_parameters(database_name)

    task_params = {
        "path": file_uri,
        "file_conn_id": file_conn_id,
        "output_table": Table(name=OUTPUT_TABLE_NAME, **sql_server_params),
    }
    with pytest.raises(BackfillUnfinished):
        with sample_dag:
            load_file(**task_params)
        test_utils.run_dag(sample_dag)

    expected_error = "Failed to execute task: The conn_id `fake_conn` isn't defined."
    assert expected_error in caplog.text


@pytest.mark.parametrize(
    "sql_server", ["snowflake", "postgres", "bigquery", "sqlite"], indirect=True
)
@pytest.mark.parametrize("file_type", ["parquet", "ndjson", "json", "csv"])
def test_load_file(sample_dag, sql_server, file_type, test_table):
    database_name, sql_hook = sql_server

    with sample_dag:
        load_file(
            path=str(pathlib.Path(CWD.parent, f"data/sample.{file_type}")),
            file_conn_id="",
            output_table=test_table,
        )
    test_utils.run_dag(sample_dag)

    database = create_database(test_table.conn_id)
    qualified_name = database.get_table_qualified_name(test_table)
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


@pytest.mark.integration
@pytest.mark.parametrize(
    "sql_server",
    [
        "postgres",
        "bigquery",
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "test_table",
    [{"is_temp": False, "param": {"schema": "custom_schema"}}],
    indirect=True,
)
@pytest.mark.parametrize("file_type", ["csv"])
def test_load_file_with_named_schema(sample_dag, sql_server, file_type, test_table):
    database_name, sql_hook = sql_server

    with sample_dag:
        load_file(
            path=str(pathlib.Path(CWD.parent, f"data/sample.{file_type}")),
            file_conn_id="",
            output_table=test_table,
        )
    test_utils.run_dag(sample_dag)
    database = create_database(test_table.conn_id)
    qualified_name = database.get_table_qualified_name(test_table)
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


@pytest.mark.integration
@pytest.mark.parametrize(
    "sql_server", ["bigquery", "postgres", "snowflake"], indirect=True
)
def test_load_file_chunks(sample_dag, sql_server, test_table):
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
        with sample_dag:
            load_file(
                path=str(pathlib.Path(CWD.parent, f"data/sample.{file_type}")),
                file_conn_id="",
                output_table=test_table,
            )
        test_utils.run_dag(sample_dag)

    _, kwargs = mock_chunk_function.call_args
    assert kwargs[chunk_size_argument] == 1000000


@pytest.mark.parametrize(
    "sql_server", [Database.POSTGRES.value, Database.BIGQUERY.value], indirect=True
)
def test_aql_nested_ndjson_file_with_default_sep_param(
    sample_dag, sql_server, test_table
):
    """Test the flattening of single level nested ndjson, with default separator '_'."""
    _, hook = sql_server
    with sample_dag:
        load_file(
            path=str(CWD) + "/../data/github_single_level_nested.ndjson",
            output_table=test_table,
        )
    test_utils.run_dag(sample_dag)

    database = create_database(test_table.conn_id)
    qualified_name = database.get_table_qualified_name(test_table)
    df = hook.get_pandas_df(f"SELECT * FROM {qualified_name}")
    assert df.shape == (1, 36)
    assert "payload_size" in df.columns


@pytest.mark.parametrize("sql_server", [Database.BIGQUERY.value], indirect=True)
def test_aql_nested_ndjson_file_to_bigquery_explicit_sep_params(
    sample_dag, sql_server, test_table
):
    """Test the flattening of single level nested ndjson, with explicit separator '___'."""
    _, hook = sql_server
    with sample_dag:
        load_file(
            path=str(CWD) + "/../data/github_single_level_nested.ndjson",
            output_table=test_table,
            ndjson_normalize_sep="___",
        )
    test_utils.run_dag(sample_dag)

    database = create_database(test_table.conn_id)
    qualified_name = database.get_table_qualified_name(test_table)
    df = hook.get_pandas_df(f"SELECT * FROM {qualified_name}")
    assert df.shape == (1, 36)
    assert "payload___size" in df.columns


@pytest.mark.parametrize("sql_server", [Database.BIGQUERY.value], indirect=True)
def test_aql_nested_ndjson_file_to_bigquery_explicit_illegal_sep_params(
    sample_dag, sql_server, test_table
):
    """Test the flattening of single level nested ndjson, with explicit separator illegal '.',
    since '.' is not acceptable in col names in bigquery.
    """
    _, hook = sql_server
    with sample_dag:
        load_file(
            path=str(CWD) + "/../data/github_single_level_nested.ndjson",
            output_table=test_table,
            ndjson_normalize_sep=".",
        )
    test_utils.run_dag(sample_dag)

    database = create_database(test_table.conn_id)
    qualified_name = database.get_table_qualified_name(test_table)
    df = hook.get_pandas_df(f"SELECT * FROM {qualified_name}")
    assert df.shape == (1, 36)
    assert "payload_size" in df.columns


@pytest.mark.parametrize("sql_server", [Database.POSTGRES.value], indirect=True)
def test_aql_multilevel_nested_ndjson_file_default_params(
    sample_dag, sql_server, test_table, caplog
):
    """
    Test the flattening of multilevel level nested ndjson, with default '_'.
    Expected to fail since we do not support flattening of multilevel ndjson.
    """
    with pytest.raises(BackfillUnfinished):
        with sample_dag:
            load_file(
                path=str(CWD) + "/../data/github_multi_level_nested.ndjson",
                output_table=test_table,
            )
        test_utils.run_dag(sample_dag)
    expected_error = "can't adapt type 'dict"
    assert expected_error in caplog.text
