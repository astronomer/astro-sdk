import pathlib
from unittest import mock

import pytest
from astro.constants import Database, FileType
from astro.databases import create_database
from astro.databases.base import BaseDatabase
from astro.files import File
from astro.settings import SCHEMA
from astro.sql.table import Metadata, Table
from pandas import DataFrame

CWD = pathlib.Path(__file__).parent


class DatabaseSubclass(BaseDatabase):
    pass


def test_subclass_missing_not_implemented_methods_raise_exception():
    db = DatabaseSubclass(conn_id="fake_conn_id")
    with pytest.raises(NotImplementedError):
        db.hook

    with pytest.raises(NotImplementedError):
        db.sqlalchemy_engine

    with pytest.raises(NotImplementedError):
        db.connection

    with pytest.raises(NotImplementedError):
        db.default_metadata

    with pytest.raises(NotImplementedError):
        db.run_sql("SELECT * FROM inexistent_table")


def test_create_table_using_native_schema_autodetection_not_implemented():
    db = DatabaseSubclass(conn_id="fake_conn_id")
    with pytest.raises(NotImplementedError):
        db.create_table_using_native_schema_autodetection(
            table=Table(), file=File(path="s3://bucket/key")
        )


def test_subclass_missing_load_pandas_dataframe_to_table_raises_exception():
    db = DatabaseSubclass(conn_id="fake_conn_id")
    table = Table()
    df = DataFrame()
    with pytest.raises(NotImplementedError):
        db.load_pandas_dataframe_to_table(df, table)


def test_create_table_using_columns_raises_exception():
    db = DatabaseSubclass(conn_id="fake_conn_id")
    table = Table()
    with pytest.raises(ValueError) as exc_info:
        db.create_table_using_columns(table)
    assert exc_info.match("To use this method, table.columns must be defined")


def test_check_schema_autodetection_is_supported():
    """
    Test the condition native schema autodetection for files and prefixes
    """
    db = create_database("gcp_conn")
    assert (
        db.check_schema_autodetection_is_supported(
            source_file=File(path="gs://bucket/prefix", filetype=FileType.CSV)
        )
    )

    assert (
        db.check_schema_autodetection_is_supported(
            source_file=File(path="gs://bucket/prefix/key.csv")
        )
    )

    assert (
        db.check_schema_autodetection_is_supported(
            source_file=File(path="s3://bucket/prefix/key.csv")
        )
        is False
    )


def test_subclass_missing_append_table_raises_exception():
    db = DatabaseSubclass(conn_id="fake_conn_id")
    source_table = Table()
    target_table = Table()
    with pytest.raises(NotImplementedError):
        db.append_table(source_table, target_table, source_to_target_columns_map={})


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
    [{"provider": "google"}],
    indirect=True,
    ids=["google_gcs"],
)
@pytest.mark.parametrize(
    "database_table_fixture",
    [{"database": Database.BIGQUERY, "table": Table(conn_id="bigquery")}],
    indirect=True,
    ids=["bigquery"],
)
def test_load_file_to_table_natively(
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
    optimised_path_to_method = {
        ("gs", "bigquery",): {
            "method_path": "airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_job",
            "expected_kwargs": {
                "configuration": {
                    "load": {
                        "sourceUris": [file_uri],
                        "destinationTable": {"tableId": test_table.name},
                    }
                }
            },
            "expected_args": (),
        }
    }

    source = file_uri.split(":")[0]
    destination = db.sql_type
    mock_path = optimised_path_to_method[(source, destination)]["method_path"]
    expected_kwargs = optimised_path_to_method[(source, destination)]["expected_kwargs"]
    expected_args = optimised_path_to_method[(source, destination)]["expected_args"]
    file = File(file_uri)

    with mock.patch(mock_path) as method:
        database = create_database(test_table.conn_id)
        database.load_file_to_table_natively(
            source_file=file,
            target_table=test_table,
        )
        assert database.is_native_load_file_available(
            source_file=file, target_table=test_table
        )
        assert method.called
        assert is_dict_subset(superset=method.call_args.kwargs, subset=expected_kwargs)
        assert method.call_args.args == expected_args


@mock.patch("astro.databases.base.BaseDatabase.drop_table")
@mock.patch("astro.databases.base.BaseDatabase.create_schema_if_needed")
@mock.patch("astro.databases.base.BaseDatabase.create_table")
@mock.patch(
    "astro.databases.base.BaseDatabase.load_file_to_table_natively_with_fallback"
)
@mock.patch("astro.databases.base.resolve_file_path_pattern")
def test_load_file_calls_resolve_file_path_pattern_with_filetype(
    resolve_file_path_pattern,
    load_file_to_table_natively_with_fallback,
    create_table,
    create_schema_if_needed,
    drop_table,
):
    resolve_file_path_pattern.return_value = [File(path="S3://somebucket/test.csv")]
    database = create_database("gcp_conn")
    database.load_file_to_table(
        input_file=File(path="S3://somebucket/test.csv"),
        output_table=Table(conn_id="gcp_conn", metadata=Metadata(schema=SCHEMA)),
        use_native_support=True,
    )
    assert resolve_file_path_pattern.call_args.kwargs["filetype"] == FileType.CSV
