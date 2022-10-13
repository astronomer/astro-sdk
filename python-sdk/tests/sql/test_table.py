import pickle
from datetime import datetime
from unittest import mock

import pytest
from airflow.models import DAG, Connection

from astro.sql import get_value_list
from astro.table import Metadata, Table, TempTable


def test_table_with_explicit_name():
    """Check that we respect the name of table when it's named an we set it to non-temp"""
    table = Table(conn_id="some_connection", name="some_name")
    assert table.conn_id == "some_connection"
    assert table.name == "some_name"
    assert not table.temp


def test_table_without_name():
    """Check that we create a name, that it is always the same name, and that we set temp name to True"""
    table = Table(conn_id="some_connection")
    assert table.conn_id == "some_connection"
    name1 = table.name
    name2 = table.name
    assert name1
    assert isinstance(name1, str)
    assert len(table.name) == 62
    assert name1 == name2
    assert table.temp


def test_table_without_name_and_schema():
    """Check that the table name is smaller when there is metadata associated to the table."""
    table = Table(conn_id="some_connection")
    table.metadata.schema = "abc"
    assert isinstance(table.name, str)
    assert len(table.name) == 59  # max length limit - len("abc.")
    assert table.temp


def test_table_name_set_after_initialization():
    """Check that the table is no longer considered temp when the name is set after initialization."""
    table = Table(conn_id="some_connection")
    assert table.temp
    table.name = "something"
    assert not table.temp


def test_table_name_with_temp_prefix():
    """Check that the table is no longer considered temp when the name is set after initialization."""
    table = Table(conn_id="some_connection")
    assert table.name.startswith("_tmp_")


@pytest.mark.parametrize(
    "metadata,expected_is_empty",
    [
        (Metadata(), True),
        (Metadata(schema="test"), False),
    ],
)
def test_is_empty_metadata(metadata, expected_is_empty):
    """Check that is_empty returns"""
    assert metadata.is_empty() == expected_is_empty


@pytest.mark.parametrize(
    "metadata,expected_metadata",
    [
        (
            {"schema": "test", "database": "db1"},
            Metadata(schema="test", database="db1"),
        ),
        (Metadata(schema="test"), Metadata(schema="test")),
    ],
)
def test_metadata_converter(metadata, expected_metadata):
    """Test you can pass a dict to metadata param"""
    table = Table(metadata=metadata)
    assert table.metadata == expected_metadata


def test_get_value_list():
    """Assert that get_file_list handle kwargs correctly"""
    dag = DAG(dag_id="dag1", start_date=datetime(2022, 1, 1))

    resp = get_value_list(sql="path", conn_id="conn", dag=dag)
    assert resp.operator.task_id == "get_value_list"

    resp = get_value_list(sql="path", conn_id="conn", dag=dag)
    assert resp.operator.task_id != "get_value_list"

    resp = get_value_list(sql="path", conn_id="conn", task_id="test")
    assert resp.operator.task_id == "test"


@pytest.mark.parametrize(
    "table,dataset_uri",
    [
        (Table(name="test_table"), "astro://@?table=test_table"),
        (
            Table(name="test_table", conn_id="test_conn"),
            "astro://test_conn@?table=test_table",
        ),
        (
            Table(
                name="test_table",
                conn_id="test_conn",
                metadata=Metadata(schema="schema", database="database"),
            ),
            "astro://test_conn@?table=test_table&schema=schema&database=database",
        ),
        (
            Table(
                name="test_table",
                conn_id="test_conn",
                metadata=Metadata(schema="schema"),
            ),
            "astro://test_conn@?table=test_table&schema=schema",
        ),
    ],
)
def test_table_to_datasets_uri(table, dataset_uri):
    """Verify that Table build and pass correct URI"""
    assert table.uri == dataset_uri


def test_table_to_datasets_extra():
    """Verify that extra is set"""
    table = Table(name="test_table", conn_id="test_conn", metadata=Metadata(schema="schema"))
    assert table.extra == {}


@pytest.mark.parametrize(
    "table",
    [
        Table(),
        Table("_tmp"),
        Table(name="_tmp", conn_id="test_conn"),
        Table(name="name", conn_id="test_conn", temp=True),
    ],
)
def test_temp_table(table):
    """Verify that temp table is generated if no name is passed or temp is set to True"""
    assert table.temp
    assert isinstance(table, TempTable)
    assert not isinstance(table, Table)


def test_if_table_object_can_be_pickled():
    """Verify if we can pickle Table object"""
    table = Table()
    assert pickle.loads(pickle.dumps(table)) == table


@pytest.mark.parametrize(
    "connection,name,namespace",
    [
        (
            Connection(conn_id="test_conn", conn_type="gcpbigquery", extra={"project": "astronomer-dag-authoring"}),
            "astro-sdk.dataset.test_tb",
            "bigquery",
        ),
        (
            Connection(
                conn_id="test_conn",
                conn_type="redshift",
                schema="astro",
                host="local",
                port=5439,
                login="astro-sdk",
                password="",
            ),
            "astro.test_tb",
            "redshift://local:5439",
        ),
        (
            Connection(
                conn_id="test_conn",
                conn_type="postgres",
                login="postgres",
                password="postgres",
                host="postgres",
                port=5432,
            ),
            "public.test_tb",
            "postgresql://postgres:5432",
        ),
        (
            Connection(
                conn_id="test_conn",
                conn_type="snowflake",
                host="local",
                port=443,
                login="astro-sdk",
                password="",
                schema="ci",
                extra={
                    "account": "astro-sdk",
                    "region": "us-east-1",
                    "role": "TEST_USER",
                    "warehouse": "TEST_ASTRO",
                    "database": "TEST_ASTRO",
                },
            ),
            "TEST_ASTRO.ci.test_tb",
            "snowflake://astro-sdk",
        ),
        (
            Connection(conn_id="test_conn", conn_type="sqlite", host="/tmp/sqlite.db"),
            "/tmp/sqlite.db.test_tb",
            "/tmp/sqlite.db",
        ),
    ],
)
@mock.patch("airflow.providers.google.cloud.utils.credentials_provider.get_credentials_and_project_id")
@mock.patch("airflow.hooks.base.BaseHook.get_connection")
def test_openlineage_dataset(mock_get_connection, gcp_cred, connection, name, namespace):
    mock_get_connection.return_value = connection
    gcp_cred.return_value = "astronomer-dag-authoring", "astronomer-dag-authoring"
    tb = Table(conn_id="test_conn", name="test_tb", metadata=Metadata(schema="dataset"))

    assert tb.openlineage_dataset_name() == name
    assert tb.openlineage_dataset_namespace() == namespace
