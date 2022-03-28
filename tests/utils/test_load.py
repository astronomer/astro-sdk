import pathlib
import tempfile
from filecmp import cmp

import pandas as pd
import pytest
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from pandas.testing import assert_frame_equal

from astro.constants import (
    SUPPORTED_DATABASES,
    SUPPORTED_FILE_TYPES,
    Database,
    FileType,
)
from astro.settings import SCHEMA
from astro.utils.database import get_sqlalchemy_engine
from astro.utils.load import (
    copy_remote_file_to_local,
    load_dataframe_into_sql_table,
    load_file_into_dataframe,
    load_file_into_sql_table,
    load_file_rows_into_dataframe,
)
from conftest import _create_unique_dag_id

table_name = _create_unique_dag_id()
CWD = pathlib.Path(__file__).parent
EXPECTED_DATA = pd.DataFrame(
    [
        {"id": 1, "name": "First"},
        {"id": 2, "name": "Second"},
        {"id": 3, "name": "Third with unicode पांचाल"},
    ]
)


def create_table(database, hook, table):
    if database == Database.BIGQUERY.value:
        hook.run(f"CREATE TABLE {table.qualified_name()} (ID int, Name string);")
    else:
        hook.run(f"DROP TABLE IF EXISTS {table.qualified_name()}")
        hook.run(f"CREATE TABLE {table.qualified_name()} (ID int, Name varchar(255));")


def describe_load_file_into_dataframe():
    def with_unsupported_inferred_filetype():
        filepath = __file__
        with pytest.raises(ValueError) as exc_info:
            load_file_into_dataframe(filepath=filepath)
        expected_msg_prefix = "Unsupported filetype 'py' from file '"
        expected_msg_suffix = "test_load.py'."
        error_msg = exc_info.value.args[0]
        assert error_msg.startswith(expected_msg_prefix)
        assert error_msg.endswith(expected_msg_suffix)

    def with_unsupported_explicit_filetype():
        filepath = __file__
        with pytest.raises(ValueError) as exc_info:
            load_file_into_dataframe(filepath, filetype="py")
        expected_msg_prefix = "Unable to load file '"
        expected_msg_suffix = "test_load.py' of type 'py'"
        error_msg = exc_info.value.args[0]
        assert error_msg.startswith(expected_msg_prefix)
        assert error_msg.endswith(expected_msg_suffix)

    @pytest.mark.parametrize("file_type", SUPPORTED_FILE_TYPES)
    def with_supported_filetype(file_type):
        filepath = pathlib.Path(CWD.parent, f"data/sample.{file_type}")
        hook = SqliteHook()
        computed = load_file_into_dataframe(filepath, hook=hook)
        assert len(computed) == 3
        computed = computed.rename(columns=str.lower)
        assert_frame_equal(computed, EXPECTED_DATA)

    def with_explicit_filetype():
        filepath = pathlib.Path(CWD.parent, "data/sample.parquet")
        computed = load_file_into_dataframe(filepath, FileType.PARQUET)
        assert len(computed) == 3
        computed = computed.rename(columns=str.lower)
        assert_frame_equal(computed, EXPECTED_DATA)


def describe_load_file_rows_into_dataframe():
    @pytest.mark.parametrize("file_type", SUPPORTED_FILE_TYPES)
    def with_rows_count_smaller_than_data_rows(file_type):
        filepath = pathlib.Path(CWD.parent, f"data/sample.{file_type}")
        computed = load_file_rows_into_dataframe(filepath, rows_count=1)

        assert len(computed) == 1
        expected = pd.DataFrame([{"id": 1, "name": "First"}])
        computed = computed.rename(columns=str.lower)
        assert_frame_equal(computed, expected)

    def with_explicit_filetype():
        filepath = pathlib.Path(CWD.parent, "data/sample.ndjson")
        computed = load_file_rows_into_dataframe(filepath, FileType.NDJSON)
        assert len(computed) == 3
        computed = computed.rename(columns=str.lower)
        assert_frame_equal(computed, EXPECTED_DATA)


def describe_load_file_into_sql_table():
    def with_unsupported_sqlite():
        hook = SqliteHook()
        engine = get_sqlalchemy_engine(hook)
        with pytest.raises(ValueError) as exc_info:
            load_file_into_sql_table(
                "", filetype=FileType.CSV, table_name="any", engine=engine
            )
        expected_msg = "Function not available for sqlite"
        assert exc_info.value.args[0] == expected_msg

    @pytest.mark.integration
    @pytest.mark.parametrize("sql_server", ["postgres"], indirect=True)
    @pytest.mark.parametrize(
        "test_table",
        [{"is_temp": False, "param": {"table_name": table_name}}],
        ids=["named_table"],
        indirect=True,
    )
    def with_supported_postgres(test_table, sql_server):
        database, hook = sql_server
        create_table(database, hook, test_table)

        filepath = pathlib.Path(CWD.parent, "data/sample.ndjson")
        engine = get_sqlalchemy_engine(hook)
        load_file_into_sql_table(
            filepath=filepath,
            filetype=FileType.NDJSON,
            table_name=test_table.table_name,
            engine=engine,
        )
        computed = hook.get_pandas_df(f"SELECT * FROM {test_table.table_name}")
        computed = computed.rename(columns=str.lower)
        assert_frame_equal(computed, EXPECTED_DATA)


def describe_load_dataframe_into_sql_table():
    @pytest.mark.integration
    @pytest.mark.parametrize("sql_server", ["postgres"], indirect=True)
    @pytest.mark.parametrize(
        "test_table",
        [{"is_temp": False, "param": {"table_name": table_name}}],
        ids=["named_table"],
        indirect=True,
    )
    def with_postgres_and_new_schema(session, test_table, sql_server):
        database, hook = sql_server
        create_table(database, hook, test_table)
        dataframe = pd.DataFrame([{"id": 27, "name": "Jim Morrison"}])
        test_table.schema = SCHEMA
        load_dataframe_into_sql_table(dataframe, test_table, hook)
        computed = hook.get_pandas_df(f"SELECT * FROM {test_table.qualified_name()}")
        computed = computed.rename(columns=str.lower)
        expected = pd.DataFrame(
            [
                {"id": 27, "name": "Jim Morrison"},
            ]
        )
        assert_frame_equal(computed, expected)
        hook.run(f"DROP TABLE IF EXISTS {test_table.qualified_name()}")

    @pytest.mark.integration
    @pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
    @pytest.mark.parametrize(
        "test_table",
        [{"is_temp": False, "param": {"table_name": table_name}}],
        ids=["named_table"],
        indirect=True,
    )
    def with_database(session, test_table, sql_server):
        database, hook = sql_server
        create_table(database, hook, test_table)
        dataframe = pd.DataFrame([{"id": 27, "name": "Jim Morrison"}])
        load_dataframe_into_sql_table(dataframe, test_table, hook)
        computed = hook.get_pandas_df(f"SELECT * FROM {test_table.qualified_name()}")
        expected = pd.DataFrame(
            [
                {"id": 27, "name": "Jim Morrison"},
            ]
        )
        computed = computed.rename(columns=str.lower)
        computed = computed.astype({"id": "int64"})
        expected = expected.astype({"id": "int64"})
        assert_frame_equal(computed, expected)


def describe_copy_remote_file_to_local():
    @pytest.mark.integration
    @pytest.mark.parametrize(
        "remote_file",
        [{"name": "google", "count": 1, "filetype": FileType.PARQUET}],
        indirect=True,
    )
    def with_binary_parquet_file_from_google_gcs(remote_file):
        _, object_uris = remote_file
        object_uri = object_uris[0]
        local_file = copy_remote_file_to_local(object_uri, is_binary=True)
        original_file = pathlib.Path(CWD.parent, "data/sample.parquet")
        assert cmp(local_file, original_file)

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "remote_file",
        [{"name": "amazon", "count": 1, "filetype": FileType.CSV}],
        indirect=True,
    )
    def with_non_binary_csv_file_from_amazon_s3(remote_file):
        _, object_uris = remote_file
        object_uri = object_uris[0]
        local_file = copy_remote_file_to_local(object_uri)
        original_file = pathlib.Path(CWD.parent, "data/sample.csv")
        assert cmp(local_file, original_file)

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "remote_file",
        [{"name": "google", "count": 1, "filetype": FileType.JSON}],
        indirect=True,
    )
    def with_target_filepath_from_google_gcs(remote_file):
        _, object_uris = remote_file
        object_uri = object_uris[0]
        with tempfile.NamedTemporaryFile() as target_fp:
            copy_remote_file_to_local(object_uri, target_filepath=target_fp.name)
            original_file = pathlib.Path(CWD.parent, "data/sample.json")
            assert cmp(target_fp.name, original_file)
