from unittest import mock

import pytest

from astro.constants import Database, FileType
from astro.databases import create_database
from astro.files import File
from astro.settings import SCHEMA
from astro.table import Metadata, Table


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


@pytest.mark.integration
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
def test_load_file_to_table_natively(sample_dag, database_table_fixture, remote_files_fixture):
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
        (
            "gs",
            "bigquery",
        ): {
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
        assert database.is_native_load_file_available(source_file=file, target_table=test_table)
        assert method.called
        assert is_dict_subset(superset=method.call_args.kwargs, subset=expected_kwargs)
        assert method.call_args.args == expected_args


@pytest.mark.integration
@mock.patch("astro.databases.base.BaseDatabase.drop_table")
@mock.patch("astro.databases.base.BaseDatabase.create_schema_if_needed")
@mock.patch("astro.databases.base.BaseDatabase.create_table")
@mock.patch("astro.databases.base.BaseDatabase.load_file_to_table_natively_with_fallback")
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


@pytest.mark.integration
def test_database_with_check_for_minio_connection():
    """Test if the S3 path is passed with minio connection it recognizes it"""
    database = create_database("snowflake_conn")
    assert (
        database.check_for_minio_connection(
            input_file=File(path="S3://somebucket/test.csv", conn_id="minio_conn")
        )
        is True
    )

    assert (
        database.check_for_minio_connection(
            input_file=File(path="S3://somebucket/test.csv", conn_id="aws_conn")
        )
        is False
    )
