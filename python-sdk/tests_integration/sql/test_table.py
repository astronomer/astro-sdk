import socket
from unittest import mock

import pytest
from airflow.models import Connection

from astro.table import Metadata, Table


@pytest.mark.integration
@pytest.mark.parametrize(
    "connection,name,namespace,uri",
    [
        (
            Connection(
                conn_id="test_bq", conn_type="gcpbigquery", extra={"project": "astronomer-dag-authoring"}
            ),
            "astronomer-dag-authoring.dataset.test_tb",
            "bigquery",
            "bigquery:astronomer-dag-authoring.dataset.test_tb",
        ),
        (
            Connection(
                conn_id="test_redshift",
                conn_type="redshift",
                schema="astro",
                host="local",
                port=5439,
                login="astro-sdk",
                password="",
            ),
            "astro.test_tb",
            "redshift://local:5439",
            "redshift://local:5439/astro.test_tb",
        ),
        (
            Connection(
                conn_id="test_pg",
                conn_type="postgres",
                login="postgres",
                password="postgres",
                host="postgres",
                port=5432,
            ),
            "public.test_tb",
            "postgresql://postgres:5432",
            "postgresql://postgres:5432/public.test_tb",
        ),
        (
            Connection(
                conn_id="test_snow",
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
            "snowflake://astro-sdk/TEST_ASTRO.ci.test_tb",
        ),
        (
            Connection(conn_id="test_sqlite", conn_type="sqlite", host="/tmp/sqlite.db"),
            "/tmp/sqlite.db.test_tb",
            f"file://{socket.gethostbyname(socket.gethostname())}:22",
            f"file://{socket.gethostbyname(socket.gethostname())}:22/tmp/sqlite.db.test_tb",
        ),
        (
            Connection(conn_id="test_duckdb", conn_type="duckdb", host="/tmp/duckdb.db"),
            "/tmp/duckdb.db.test_tb",
            f"file://{socket.gethostbyname(socket.gethostname())}:22",
            f"file://{socket.gethostbyname(socket.gethostname())}:22/tmp/duckdb.db.test_tb",
        ),
        (
            Connection(
                conn_id="test_mssql",
                conn_type="mssql",
                host="someserver.com",
                schema="astrodb",
                port=1433,
                login="username",
                password="password",
            ),
            "astrodb.dataset.test_tb",
            "mssql://someserver.com:1433",
            "mssql://someserver.com:1433/astrodb.dataset.test_tb",
        ),
    ],
)
@mock.patch("airflow.providers.google.cloud.utils.credentials_provider.get_credentials_and_project_id")
@mock.patch("airflow.hooks.base.BaseHook.get_connection")
def test_openlineage_dataset(mock_get_connection, gcp_cred, connection, name, namespace, uri):
    """
    Test that name and namespace for lineage is correct for databases
    """
    mock_get_connection.return_value = connection
    gcp_cred.return_value = "astronomer-dag-authoring", "astronomer-dag-authoring"
    tb = Table(conn_id=connection.conn_id, name="test_tb", metadata=Metadata(schema="dataset"))

    assert tb.openlineage_dataset_name() == name
    assert tb.openlineage_dataset_namespace() == namespace
    assert tb.openlineage_dataset_uri() == uri
