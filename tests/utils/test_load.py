import pathlib
import tempfile
from filecmp import cmp

import pandas as pd
import pytest
import sqlalchemy.types as types
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from pandas.testing import assert_frame_equal
from sqlalchemy import Column

from astro.constants import SUPPORTED_FILE_TYPES, Database, FileType
from astro.databases import create_database
from astro.databases.base import BaseDatabase
from astro.settings import SCHEMA
from astro.sql.table import Table, create_unique_table_name
from astro.utils.database import get_sqlalchemy_engine
from astro.utils.load import (
    copy_remote_file_to_local,
    load_dataframe_into_sql_table,
    load_file_into_dataframe,
    load_file_into_sql_table,
    load_file_rows_into_dataframe,
)

table_name = create_unique_table_name()
CWD = pathlib.Path(__file__).parent
EXPECTED_DATA = pd.DataFrame(
    [
        {"id": 1, "name": "First"},
        {"id": 2, "name": "Second"},
        {"id": 3, "name": "Third with unicode पांचाल"},
    ]
)


def create_table(database: BaseDatabase, table: Table):
    if database.sql_type == Database.BIGQUERY.value:
        table.columns = [
            Column(name="id", type_=types.Integer),
            Column(name="Name", type_=types.String),
        ]
    else:
        table.columns = [
            Column(name="id", type_=types.Integer),
            Column(name="Name", type_=types.String(255)),
        ]
    database.create_table(table)


def describe_load_file_into_dataframe():
    def with_unsupported_inferred_filetype():
        filepath = __file__
        with pytest.raises(ValueError) as exc_info:
            load_file_into_dataframe(filepath)
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
        computed = load_file_into_dataframe(filepath)
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
    @pytest.mark.parametrize(
        "database_table_fixture",
        [{"database": Database.POSTGRES, "table": Table(name=table_name)}],
        indirect=True,
        ids=["postgres"],
    )
    def with_supported_postgres(database_table_fixture):
        db, test_table = database_table_fixture
        db: BaseDatabase = db
        db.create_schema_if_needed(test_table.metadata.schema)
        create_table(db, test_table)

        filepath = pathlib.Path(CWD.parent, "data/sample.ndjson")
        load_file_into_sql_table(
            filepath=filepath,
            filetype=FileType.NDJSON,
            table_name=db.get_table_qualified_name(test_table),
            engine=db.sqlalchemy_engine,
        )
        computed = db.export_table_to_pandas_dataframe(test_table)
        computed = computed.rename(columns=str.lower)
        assert_frame_equal(computed, EXPECTED_DATA)


def describe_load_dataframe_into_sql_table():
    @pytest.mark.integration
    @pytest.mark.parametrize(
        "database_table_fixture",
        [{"database": Database.POSTGRES, "table": Table(name=table_name)}],
        indirect=True,
        ids=["postgres"],
    )
    def with_postgres_and_new_schema(session, database_table_fixture):
        db, test_table = database_table_fixture
        db.create_schema_if_needed(test_table.metadata.schema)
        create_table(db, test_table)
        dataframe = pd.DataFrame([{"id": 27, "name": "Jim Morrison"}])
        test_table.metadata.schema = SCHEMA
        load_dataframe_into_sql_table(dataframe, test_table, db.hook)
        computed = db.export_table_to_pandas_dataframe(test_table)
        computed = computed.rename(columns=str.lower)
        expected = pd.DataFrame(
            [
                {"id": 27, "name": "Jim Morrison"},
            ]
        )
        assert_frame_equal(computed, expected)

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "database_table_fixture",
        [
            {"database": Database.SNOWFLAKE, "table": Table(name=table_name)},
            {"database": Database.BIGQUERY, "table": Table(name=table_name)},
            {"database": Database.POSTGRES, "table": Table(name=table_name)},
            {"database": Database.SQLITE, "table": Table(name=table_name)},
        ],
        indirect=True,
        ids=["snowflake", "bigquery", "postgresql", "sqlite"],
    )
    def with_database(session, database_table_fixture):
        db, test_table = database_table_fixture
        db.create_schema_if_needed(test_table.metadata.schema)
        create_table(db, test_table)
        dataframe = pd.DataFrame([{"id": 27, "name": "Jim Morrison"}])
        load_dataframe_into_sql_table(dataframe, test_table, db.hook)
        db = create_database(test_table.conn_id)
        computed = db.export_table_to_pandas_dataframe(test_table)
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
        "remote_files_fixture",
        [{"provider": "google", "count": 1, "filetype": FileType.PARQUET}],
        indirect=True,
        ids=["google"],
    )
    def with_binary_parquet_file_from_google_gcs(remote_files_fixture):
        object_uri = remote_files_fixture[0]
        local_file = copy_remote_file_to_local(object_uri, is_binary=True)
        original_file = pathlib.Path(CWD.parent, "data/sample.parquet")
        assert cmp(local_file, original_file)

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "remote_files_fixture",
        [{"provider": "amazon", "count": 1, "filetype": FileType.CSV}],
        indirect=True,
        ids=["amazon"],
    )
    def with_non_binary_csv_file_from_amazon_s3(remote_files_fixture):
        object_uri = remote_files_fixture[0]
        local_file = copy_remote_file_to_local(object_uri)
        original_file = pathlib.Path(CWD.parent, "data/sample.csv")
        assert cmp(local_file, original_file)

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "remote_files_fixture",
        [{"provider": "google", "count": 1, "filetype": FileType.JSON}],
        indirect=True,
        ids=["google"],
    )
    def with_target_filepath(remote_files_fixture):
        object_uri = remote_files_fixture[0]
        with tempfile.NamedTemporaryFile() as target_fp:
            copy_remote_file_to_local(object_uri, target_filepath=target_fp.name)
            original_file = str(pathlib.Path(CWD.parent, "data/sample.json"))
            assert cmp(target_fp.name, original_file)
