"""
Unittest module to test Load File function.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:
    AWS_ACCESS_KEY_ID=<id> \
    AWS_SECRET_ACCESS_KEY=<key> \
    python3 -m unittest tests.operators.test_load_file.TestLoadFile.test_aql_local_file_to_postgres

"""
import importlib
import os
import pathlib
from unittest import mock

import pandas as pd
import pytest
from airflow.exceptions import BackfillUnfinished
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from astro import sql as aql
from astro.airflow.datasets import DATASET_SUPPORT
from astro.constants import Database, FileType
from astro.exceptions import IllegalLoadToDatabaseException
from astro.files import File
from astro.settings import SCHEMA
from astro.sql.operators.load_file import load_file
from astro.sql.table import Metadata, Table
from pandas.testing import assert_frame_equal

from tests.sql.operators import utils as test_utils

OUTPUT_TABLE_NAME = test_utils.get_table_name("load_file_test_table")
CWD = pathlib.Path(__file__).parent


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
        },
        {
            "database": Database.BIGQUERY,
        },
        {
            "database": Database.POSTGRES,
        },
        {
            "database": Database.SQLITE,
        },
        {
            "database": Database.REDSHIFT,
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
def test_load_file_with_http_path_file(sample_dag, database_table_fixture):
    db, test_table = database_table_fixture
    with sample_dag:
        load_file(
            input_file=File(
                "https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/homes_main.csv"
            ),
            output_table=test_table,
        )
    test_utils.run_dag(sample_dag)

    df = db.export_table_to_pandas_dataframe(test_table)
    assert df.shape == (3, 9)


@mock.patch.dict(
    os.environ, {"AIRFLOW__ASTRO_SDK__DATAFRAME_ALLOW_UNSAFE_STORAGE": "False"}
)
def test_unsafe_loading_of_dataframe(sample_dag):
    from astro import settings

    importlib.reload(settings)

    with pytest.raises(IllegalLoadToDatabaseException):
        load_file(
            input_file=File(
                "https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/homes_main.csv"
            ),
        ).operator.execute({})


@pytest.mark.integration
@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "google"}, {"provider": "amazon"}],
    indirect=True,
    ids=["google_gcs", "amazon_s3"],
)
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
        },
        {
            "database": Database.BIGQUERY,
        },
        {
            "database": Database.POSTGRES,
        },
        {
            "database": Database.SQLITE,
        },
        {
            "database": Database.REDSHIFT,
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
def test_aql_load_remote_file_to_dbs(
    sample_dag, database_table_fixture, remote_files_fixture
):
    db, test_table = database_table_fixture
    file_uri = remote_files_fixture[0]

    with sample_dag:
        load_file(
            input_file=File(file_uri), output_table=test_table, use_native_support=False
        )
    test_utils.run_dag(sample_dag)

    df = db.export_table_to_pandas_dataframe(test_table)

    # Workaround for snowflake capitalized col names
    sort_cols = "name"
    if sort_cols not in df.columns:
        sort_cols = sort_cols.upper()

    df = df.sort_values(by=[sort_cols])

    assert df.iloc[0].to_dict()[sort_cols] == "First"


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
def test_aql_replace_existing_table(sample_dag, database_table_fixture):
    db, test_table = database_table_fixture
    data_path_1 = str(CWD) + "/../../data/homes.csv"
    data_path_2 = str(CWD) + "/../../data/homes2.csv"
    with sample_dag:
        task_1 = load_file(input_file=File(data_path_1), output_table=test_table)
        task_2 = load_file(input_file=File(data_path_2), output_table=test_table)
        task_1 >> task_2  # skipcq: PYL-W0104
    test_utils.run_dag(sample_dag)

    df = db.export_table_to_pandas_dataframe(test_table)
    data_df = pd.read_csv(data_path_2)

    assert df.shape == data_df.shape


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
        },
        {
            "database": Database.BIGQUERY,
        },
        {
            "database": Database.POSTGRES,
        },
        {
            "database": Database.SQLITE,
        },
        {
            "database": Database.REDSHIFT,
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
def test_aql_local_file_with_no_table_name(sample_dag, database_table_fixture):
    db, test_table = database_table_fixture
    data_path = str(CWD) + "/../../data/homes.csv"
    with sample_dag:
        load_file(input_file=File(data_path), output_table=test_table)
    test_utils.run_dag(sample_dag)

    df = db.export_table_to_pandas_dataframe(test_table)
    data_df = pd.read_csv(data_path)

    assert df.shape == data_df.shape


def test_unique_task_id_for_same_path(sample_dag):
    tasks = []

    with sample_dag:
        for index in range(4):
            params = {
                "input_file": File(path=str(CWD) + "/../../data/homes.csv"),
                "output_table": Table(
                    conn_id="postgres_conn", metadata=Metadata(database="pagila")
                ),
            }
            if index == 3:
                params["task_id"] = "task_id"

            task = load_file(**params)
            tasks.append(task)

    test_utils.run_dag(sample_dag)

    assert tasks[0].operator.task_id != tasks[1].operator.task_id
    assert tasks[1].operator.task_id == "load_file__1"
    assert tasks[2].operator.task_id == "load_file__2"
    assert tasks[3].operator.task_id == "task_id"


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SQLITE,
        },
    ],
    indirect=True,
    ids=["sqlite"],
)
def test_load_file_templated_filename(sample_dag, database_table_fixture):
    db, test_table = database_table_fixture
    with sample_dag:
        load_file(
            input_file=File(
                path=str(CWD) + "/../../data/{{ var.value.foo }}/example.csv"
            ),
            output_table=test_table,
        )
    test_utils.run_dag(sample_dag)

    df = db.export_table_to_pandas_dataframe(test_table)
    assert len(df) == 3


@pytest.mark.integration
@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "google", "file_count": 2}, {"provider": "amazon", "file_count": 2}],
    ids=["google", "amazon"],
    indirect=True,
)
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SQLITE,
        },
    ],
    indirect=True,
    ids=["sqlite"],
)
def test_aql_load_file_pattern(
    remote_files_fixture, sample_dag, database_table_fixture
):
    remote_object_uri = remote_files_fixture[0]
    filename = pathlib.Path(CWD.parent, "../data/sample.csv")
    db, test_table = database_table_fixture

    with sample_dag:
        load_file(
            input_file=File(path=remote_object_uri[0:-5], filetype=FileType.CSV),
            output_table=test_table,
        )
    test_utils.run_dag(sample_dag)

    df = db.export_table_to_pandas_dataframe(test_table)
    test_df_rows = pd.read_csv(filename).shape[0]

    assert test_df_rows * 2 == df.shape[0]


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.POSTGRES,
        },
    ],
    indirect=True,
    ids=["postgres"],
)
def test_aql_load_file_local_file_pattern(sample_dag, database_table_fixture):
    filename = str(CWD.parent) + "/../data/homes_pattern_1.csv"
    db, test_table = database_table_fixture

    test_df_rows = pd.read_csv(filename).shape[0]

    with sample_dag:
        load_file(
            input_file=File(
                path=str(CWD.parent) + "/../data/homes_pattern_*", filetype=FileType.CSV
            ),
            output_table=test_table,
        )
    test_utils.run_dag(sample_dag)

    # Read table from db
    df = db.export_table_to_pandas_dataframe(test_table)
    assert test_df_rows * 2 == df.shape[0]


def test_aql_load_file_local_file_pattern_dataframe(sample_dag):
    filename = str(CWD.parent) + "/../data/homes_pattern_1.csv"
    filename_2 = str(CWD.parent) + "/../data/homes_pattern_2.csv"

    test_df = pd.read_csv(filename)
    test_df_2 = pd.read_csv(filename_2)
    test_df = pd.concat([test_df, test_df_2])

    @aql.dataframe
    def validate(input_df: pd.DataFrame):
        assert isinstance(input_df, pd.DataFrame)
        assert test_df.shape == input_df.shape
        assert test_df.sort_values("sell").equals(input_df.sort_values("sell"))
        print(input_df)

    with sample_dag:
        loaded_df = load_file(
            input_file=File(
                path=str(CWD.parent) + "/../data/homes_pattern_*", filetype=FileType.CSV
            ),
        )
        validate(loaded_df)

    test_utils.run_dag(sample_dag)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SQLITE,
        },
    ],
    indirect=True,
    ids=["sqlite"],
)
@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "google"}, {"provider": "amazon"}],
    indirect=True,
    ids=["google", "amazon"],
)
def test_load_file_using_file_connection(
    sample_dag, remote_files_fixture, database_table_fixture
):
    db, test_table = database_table_fixture
    file_uri = remote_files_fixture[0]
    if file_uri.startswith("s3"):
        file_conn_id = S3Hook.default_conn_name
    else:
        file_conn_id = GCSHook.default_conn_name
    with sample_dag:
        load_file(
            input_file=File(path=file_uri, conn_id=file_conn_id),
            output_table=test_table,
        )
    test_utils.run_dag(sample_dag)

    df = db.export_table_to_pandas_dataframe(test_table)
    assert len(df) == 3


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.POSTGRES,
        },
    ],
    indirect=True,
    ids=["postgresql"],
)
def test_load_file_using_file_connection_fails_nonexistent_conn(
    caplog, sample_dag, database_table_fixture
):
    database_name = "postgres"
    file_conn_id = "fake_conn"
    file_uri = "s3://fake-bucket/fake-object.csv"

    sql_server_params = test_utils.get_default_parameters(database_name)

    task_params = {
        "input_file": File(path=file_uri, conn_id=file_conn_id),
        "output_table": Table(name=OUTPUT_TABLE_NAME, **sql_server_params),
    }
    with pytest.raises(BackfillUnfinished):
        with sample_dag:
            load_file(**task_params)
        test_utils.run_dag(sample_dag)

    expected_error = "Failed to execute task: The conn_id `fake_conn` isn't defined."
    assert expected_error in caplog.text


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
        },
        {
            "database": Database.BIGQUERY,
        },
        {
            "database": Database.POSTGRES,
        },
        {
            "database": Database.SQLITE,
        },
        {
            "database": Database.REDSHIFT,
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
@pytest.mark.parametrize("file_type", ["parquet", "ndjson", "json", "csv"])
def test_load_file(sample_dag, database_table_fixture, file_type):
    db, test_table = database_table_fixture

    # Using the use_native_support=False here since the dataset
    # used requires other optional params by local to Bigquery native path.
    with sample_dag:
        load_file(
            input_file=File(
                path=str(pathlib.Path(CWD.parent, f"../data/sample.{file_type}"))
            ),
            output_table=test_table,
            use_native_support=False,
        )
    test_utils.run_dag(sample_dag)

    df = db.export_table_to_pandas_dataframe(test_table)

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
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
        },
        {
            "database": Database.POSTGRES,
        },
        {
            "database": Database.REDSHIFT,
        },
    ],
    indirect=True,
    ids=[
        "bigquery",
        "postgresql",
        "redshift",
    ],
)
@pytest.mark.parametrize("file_type", ["csv"])
def test_load_file_with_named_schema(sample_dag, database_table_fixture, file_type):
    db, test_table = database_table_fixture
    test_table.metadata.schema = "custom_schema"

    with sample_dag:
        load_file(
            input_file=File(
                path=str(pathlib.Path(CWD.parent, f"../data/sample.{file_type}"))
            ),
            output_table=test_table,
        )
    test_utils.run_dag(sample_dag)
    df = db.export_table_to_pandas_dataframe(test_table)
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
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
        },
        {
            "database": Database.BIGQUERY,
        },
        {
            "database": Database.REDSHIFT,
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "redshift"],
)
def test_load_file_chunks(sample_dag, database_table_fixture):
    file_type = "csv"
    db, test_table = database_table_fixture

    chunk_function = {
        "bigquery": "pandas.DataFrame.to_gbq",
        "snowflake": "snowflake.connector.pandas_tools.write_pandas",
        "redshift": "pandas.DataFrame.to_sql",
    }[db.sql_type]

    chunk_size_argument = {
        "bigquery": "chunksize",
        "snowflake": "chunk_size",
        "redshift": "chunksize",
    }[db.sql_type]

    with mock.patch(chunk_function) as mock_chunk_function:
        with sample_dag:
            load_file(
                input_file=File(
                    path=str(pathlib.Path(CWD.parent, f"../data/sample.{file_type}"))
                ),
                output_table=test_table,
                use_native_support=False,
            )
        test_utils.run_dag(sample_dag)

    _, kwargs = mock_chunk_function.call_args
    assert kwargs[chunk_size_argument] == 1000000


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
        },
        {
            "database": Database.BIGQUERY,
        },
        {
            "database": Database.POSTGRES,
        },
        {
            "database": Database.SQLITE,
        },
        {
            "database": Database.REDSHIFT,
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
def test_aql_nested_ndjson_file_with_default_sep_param(
    sample_dag, database_table_fixture
):
    """Test the flattening of single level nested ndjson, with default separator '_'."""
    db, test_table = database_table_fixture
    with sample_dag:
        # Using the use_native_support=False here since the dataset
        # used requires other optional params by local to Bigquery native path.
        load_file(
            input_file=File(
                path=str(CWD) + "/../../data/github_single_level_nested.ndjson"
            ),
            output_table=test_table,
            use_native_support=False,
        )
    test_utils.run_dag(sample_dag)

    df = db.export_table_to_pandas_dataframe(test_table)
    assert df.shape == (1, 36)
    assert "payload_size" in df.columns


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
        },
        {
            "database": Database.REDSHIFT,
        },
    ],
    indirect=True,
    ids=["bigquery", "redshift"],
)
def test_aql_nested_ndjson_file_to_database_explicit_sep_params(
    sample_dag, database_table_fixture
):
    """Test the flattening of single level nested ndjson, with explicit separator '___'."""
    db, test_table = database_table_fixture
    with sample_dag:
        # Using the use_native_support=False here since the dataset
        # used requires other optional params by local to Bigquery native path.
        load_file(
            input_file=File(
                path=str(CWD) + "/../../data/github_single_level_nested.ndjson"
            ),
            output_table=test_table,
            use_native_support=False,
            ndjson_normalize_sep="___",
        )
    test_utils.run_dag(sample_dag)

    df = db.export_table_to_pandas_dataframe(test_table)
    assert df.shape == (1, 36)
    assert "payload___size" in df.columns


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
        },
        {
            "database": Database.REDSHIFT,
        },
    ],
    indirect=True,
    ids=[
        "bigquery",
        "redshift",
    ],
)
def test_aql_nested_ndjson_file_to_database_explicit_illegal_sep_params(
    sample_dag, database_table_fixture
):
    """Test the flattening of single level nested ndjson, with explicit separator illegal '.',
    since '.' is not acceptable in col names in bigquery.
    """
    db, test_table = database_table_fixture
    with sample_dag:
        # Using the use_native_support=False here since the dataset
        # used requires other optional params by local to Bigquery native path.
        load_file(
            input_file=File(
                path=str(CWD) + "/../../data/github_single_level_nested.ndjson"
            ),
            output_table=test_table,
            ndjson_normalize_sep=".",
            use_native_support=False,
        )
    test_utils.run_dag(sample_dag)

    df = db.export_table_to_pandas_dataframe(test_table)
    assert df.shape == (1, 36)
    assert "payload_size" in df.columns


def test_populate_table_metadata(sample_dag):
    """
    Test default populating of table fields in load_fil op.
    """

    @aql.dataframe
    def validate(table: Table):
        assert table.metadata.schema == SCHEMA

    with sample_dag:
        output_table = load_file(
            input_file=File(path=str(pathlib.Path(CWD.parent, "../data/sample.csv"))),
            output_table=Table(conn_id="postgres_conn_pagila"),
        )
        validate(output_table)
    test_utils.run_dag(sample_dag)


@pytest.mark.parametrize(
    "invalid_path",
    [
        "/tmp/cklcdklscdksl.csv",
        "/tmp/cklcdklscdksl/*.csv",
    ],
)
def test_load_file_should_fail_loudly(sample_dag, invalid_path, caplog):
    """
    load_file() operator is expected to fail for files which don't exist and 'if_file_doesnt_exist' is having exception
    strategy selected.
    """

    with pytest.raises(BackfillUnfinished):
        with sample_dag:
            _ = load_file(
                input_file=File(path=invalid_path),
                output_table=Table(conn_id="postgres_conn_pagila"),
            )
        test_utils.run_dag(sample_dag)
    expected_error = f"File(s) not found for path/pattern '{invalid_path}'"
    assert expected_error in caplog.text


def is_dict_subset(superset: dict, subset: dict) -> bool:
    """
    Compare superset and subset to check if the latter is a subset of former.
    Note: dict1 <= dict2 was not working on multilevel nested dicts.
    """
    for key, val in subset.items():
        print(key, val)
        if isinstance(val, dict):
            if key not in superset:
                return False
            result = is_dict_subset(superset[key], subset[key])
            if not result:
                return False
        elif superset[key] != val:
            return False
    return True


@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "google"}, {"provider": "amazon"}, {"provider": "local"}],
    indirect=True,
    ids=["google_gcs", "amazon_s3", "local"],
)
@pytest.mark.parametrize(
    "database_table_fixture",
    [{"database": Database.BIGQUERY, "table": Table(conn_id="bigquery")}],
    indirect=True,
    ids=["bigquery"],
)
def test_aql_load_file_optimized_path_method_called(
    sample_dag, database_table_fixture, remote_files_fixture
):
    """
    Verify the correct method is getting called for specific source and destination.
    """
    db, test_table = database_table_fixture
    file_uri = remote_files_fixture[0]

    # (source, destination) : {
    #   method_path: where source is file source path and destination is database
    # and method_path is the path to method
    #   expected_kwargs: subset of all the kwargs that are passed to method mentioned in the method_path
    #   expected_args:  List of all the args that are passed to method mentioned in the method_path
    # }
    file = File(file_uri)
    optimised_path_to_method = {
        ("gs", "bigquery",): {
            "method_path": "astro.databases.google.bigquery.BigqueryDatabase.load_gs_file_to_table",
            "expected_kwargs": {
                "source_file": file,
                "target_table": test_table,
            },
            "expected_args": (),
        },
        ("s3", "bigquery",): {
            "method_path": "astro.databases.google.bigquery.BigqueryDatabase.load_s3_file_to_table",
            "expected_kwargs": {
                "source_file": file,
                "target_table": test_table,
            },
            "expected_args": (),
        },
        ("local", "bigquery",): {
            "method_path": "astro.databases.google.bigquery.BigqueryDatabase.load_local_file_to_table",
            "expected_kwargs": {
                "source_file": file,
                "target_table": test_table,
            },
            "expected_args": (),
        },
    }

    if file_uri.find(":") >= 0:
        source = file_uri.split(":")[0]
    else:
        source = "local"

    destination = db.sql_type
    mock_path = optimised_path_to_method[(source, destination)]["method_path"]
    expected_kwargs = optimised_path_to_method[(source, destination)]["expected_kwargs"]
    expected_args = optimised_path_to_method[(source, destination)]["expected_args"]

    with mock.patch(mock_path) as method:
        load_file(
            input_file=file,
            output_table=test_table,
        ).operator.execute({})
        assert method.called
        assert is_dict_subset(superset=method.call_args.kwargs, subset=expected_kwargs)
        assert method.call_args.args == expected_args


@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "google"}, {"provider": "amazon"}, {"provider": "local"}],
    indirect=True,
    ids=["google_gcs", "amazon_s3", "local"],
)
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
        }
    ],
    indirect=True,
    ids=["bigquery"],
)
def test_aql_load_file_optimized_path_method_is_not_called(
    sample_dag, database_table_fixture, remote_files_fixture
):
    """
    Verify that the optimised path method is skipped in case use_native_support is set to False.
    """
    db, test_table = database_table_fixture
    file_uri = remote_files_fixture[0]

    # (source, destination) : {
    #   method_path: where source is file source path and destination is database
    # and method_path is the path to method
    #   expected_kwargs: subset of all the kwargs that are passed to method mentioned in the method_path
    #   expected_args:  List of all the args that are passed to method mentioned in the method_path
    # }
    optimised_path_to_method = {
        ("gs", "bigquery",): {
            "method_path": "astro.databases.google.bigquery.BigqueryDatabase.load_gs_file_to_table",
        },
        ("s3", "bigquery",): {
            "method_path": "astro.databases.google.bigquery.BigqueryDatabase.load_s3_file_to_table",
        },
        ("local", "bigquery"): {
            "method_path": "astro.databases.google.bigquery.BigqueryDatabase.load_local_file_to_table",
        },
    }
    if file_uri.find(":") >= 0:
        source = file_uri.split(":")[0]
    else:
        source = "local"
    destination = db.sql_type
    mock_path = optimised_path_to_method[(source, destination)]["method_path"]

    with mock.patch(mock_path) as method:
        load_file(
            input_file=File(file_uri), output_table=test_table, use_native_support=False
        ).operator.execute({})
        assert not method.called


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture,native_support_kwargs",
    [
        (
            {
                "database": Database.BIGQUERY,
            },
            {
                "ignore_unknown_values": True,
                "allow_jagged_rows": True,
                "skip_leading_rows": "1",
            },
        ),
        (
            {
                "database": Database.REDSHIFT,
            },
            {
                "IGNOREHEADER": 1,
                "REGION": "us-west-2",
                "IAM_ROLE": os.getenv("REDSHIFT_NATIVE_LOAD_IAM_ROLE_ARN"),
            },
        ),
    ],
    indirect=["database_table_fixture"],
    ids=["Bigquery", "Redshift"],
)
def test_aql_load_file_s3_native_path(
    sample_dag, database_table_fixture, native_support_kwargs
):
    """
    Verify that the optimised path method is skipped in case use_native_support is set to False.
    """
    db, test_table = database_table_fixture

    # We are using a preexisting file for integration test, since the dynamically populating
    # file on S3 results in file not found, since that file is not propagated to all the servers/clusters,
    # and we might hit a server where the file in not yet populated, resulting in file not found issue.
    load_file(
        input_file=File("s3://tmp9/homes_main.csv", conn_id="aws_conn"),
        output_table=test_table,
        use_native_support=True,
        native_support_kwargs=native_support_kwargs,
    ).operator.execute({})

    df = db.export_table_to_pandas_dataframe(test_table)
    assert df.shape == (3, 9)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
        },
        {
            "database": Database.REDSHIFT,
        },
    ],
    indirect=True,
    ids=["Bigquery", "redshift"],
)
def test_loading_local_file_to_database(database_table_fixture):
    """
    Verify that the optimised path method is skipped in case use_native_support is set to False.
    """
    path = str(CWD) + "/../../data/homes_main.csv"
    db, test_table = database_table_fixture
    load_file(
        input_file=File(path),
        output_table=test_table,
        use_native_support=True,
        native_support_kwargs={
            "skip_leading_rows": "1",
        },
    ).operator.execute({})

    database_df = db.export_table_to_pandas_dataframe(test_table)
    assert database_df.shape == (3, 9)


def test_aql_load_file_columns_names_capitalization_dataframe(sample_dag):
    filename = str(CWD.parent) + "/../data/homes_pattern_1.csv"

    @aql.dataframe
    def validate(input_df_1, input_df_2, input_df_3):
        assert isinstance(input_df_1, pd.DataFrame)
        assert isinstance(input_df_2, pd.DataFrame)
        assert isinstance(input_df_3, pd.DataFrame)
        assert all(x.isupper() for x in list(input_df_1.columns))
        assert all(x.islower() for x in list(input_df_2.columns))
        assert all(x.islower() for x in list(input_df_3.columns))

    with sample_dag:
        loaded_df_1 = load_file(
            input_file=File(path=filename, filetype=FileType.CSV),
            columns_names_capitalization="upper",
        )
        loaded_df_2 = load_file(
            input_file=File(path=filename, filetype=FileType.CSV),
            columns_names_capitalization="lower",
        )
        loaded_df_3 = load_file(
            input_file=File(path=filename, filetype=FileType.CSV),
            columns_names_capitalization="original",
        )
        validate(loaded_df_1, loaded_df_2, loaded_df_3)

    test_utils.run_dag(sample_dag)


@pytest.mark.parametrize(
    "database_table_fixture,native_support_kwargs",
    [
        (
            {
                "database": Database.REDSHIFT,
            },
            {
                "IGNOREHEADER": 1,
                "REGION": "us-east-1",
                "IAM_ROLE": os.getenv("REDSHIFT_NATIVE_LOAD_IAM_ROLE_ARN"),
            },
        ),
    ],
    indirect=["database_table_fixture"],
    ids=["redshift"],
)
def test_aql_load_column_name_mixed_case_json_file_to_dbs(
    database_table_fixture, native_support_kwargs
):
    """Test that json with mixed column name case loads fine natively to the database."""
    db, test_table = database_table_fixture

    # We are using a preexisting file for integration test, since the dynamically populating
    # file on S3 results in file not found, since that file is not propagated to all the servers/clusters,
    # and we might hit a server where the file in not yet populated, resulting in file not found issue.
    load_file(
        input_file=File("s3://astro-sdk/sample.ndjson", conn_id="aws_conn"),
        output_table=test_table,
        use_native_support=True,
        native_support_kwargs=native_support_kwargs,
    ).operator.execute({})

    df = db.export_table_to_pandas_dataframe(test_table)
    assert df.shape == (2, 2)


@pytest.mark.skipif(
    not DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4"
)
def test_inlets_outlets_supported_ds():
    """Test Datasets are set as inlets and outlets"""
    input_file = File("gs://bucket/object.csv")
    output_table = Table("test_name")
    task = aql.load_file(
        input_file=input_file,
        output_table=output_table,
    )
    assert task.operator.inlets == [input_file]
    assert task.operator.outlets == [output_table]


@pytest.mark.skipif(
    DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4"
)
def test_inlets_outlets_non_supported_ds():
    """Test inlets and outlets are not set if Datasets are not supported"""
    input_file = File("gs://bucket/object.csv")
    output_table = Table("test_name")
    task = aql.load_file(
        input_file=input_file,
        output_table=output_table,
    )
    assert task.operator.inlets == []
    assert task.operator.outlets == []


@pytest.mark.parametrize(
    "file",
    [
        File("gs://astro-sdk/workspace/sample.parquet"),
        File("gs://astro-sdk/workspace/sample.csv"),
        File("gs://astro-sdk/workspace/sample.ndjson"),
    ],
    ids=["parquet", "csv", "ndjson"],
)
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
        }
    ],
    indirect=True,
    ids=["Bigquery"],
)
def test_loading_gcs_file_to_database(database_table_fixture, file):
    """
    Test working of optimised path method with autodetect for files in GCS
    """
    db, test_table = database_table_fixture
    load_file(
        input_file=file,
        output_table=test_table,
        use_native_support=True,
    ).operator.execute({})

    database_df = db.export_table_to_pandas_dataframe(test_table)
    assert database_df.shape == (3, 2)
