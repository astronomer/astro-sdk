from unittest.mock import PropertyMock, patch

from airflow.models.connection import Connection

from astro.sql.operators.run_query import RunQueryOperator


@patch("sqlalchemy.engine.base.Connection._execute_context")
@patch("astro.databases.sqlite.SqliteDatabase.hook", new_callable=PropertyMock)
@patch("airflow.hooks.base.BaseHook.get_connection")
def test_run_query_sqlite(mock_get_conn, mock_create_engine, _execute_context):
    """Assert that if connection type is ``sqlite`` then sqlalchemy create_engine called with correct uri"""
    mock_get_conn.return_value = Connection(
        conn_id="conn", conn_type="sqlite", host="localhost"
    )
    op = RunQueryOperator(
        task_id="task1", sql_statement="select * from 1", conn_id="conn"
    )
    op.execute(None)
    mock_create_engine.assert_called_once()


@patch("astro.databases.postgres.PostgresDatabase.hook", new_callable=PropertyMock)
@patch("airflow.hooks.base.BaseHook.get_connection")
def test_run_query_postgres(mock_get_conn, sqlalchemy_engine):
    """Assert that if connection type is ``postgres`` then postgres hook getting called"""
    mock_get_conn.return_value = Connection(
        conn_id="conn", conn_type="postgres", host="localhost", schema="test"
    )
    op = RunQueryOperator(
        task_id="task1", sql_statement="select * from 1", conn_id="conn"
    )
    op.execute(None)
    sqlalchemy_engine.assert_called_once()


@patch("astro.databases.google.bigquery.BigqueryDatabase.sqlalchemy_engine", new_callable=PropertyMock)
@patch("astro.databases.google.bigquery.BigqueryDatabase.hook", new_callable=PropertyMock)
@patch("airflow.hooks.base.BaseHook.get_connection")
def test_run_query_bigquery(mock_get_conn, mock_bigquery_hook, sqlalchemy_engine):
    """Assert that if connection type is ``gcpbigquery`` then sqlalchemy create_engine called with correct uri"""
    mock_get_conn.return_value = Connection(conn_id="conn", conn_type="gcpbigquery")
    op = RunQueryOperator(
        task_id="task1", sql_statement="select * from 1", conn_id="conn"
    )
    op.execute(None)
    sqlalchemy_engine.assert_called_once()


@patch(
    "airflow.providers.snowflake.hooks.snowflake.SnowflakeHook.get_sqlalchemy_engine"
)
@patch("airflow.hooks.base.BaseHook.get_connection")
def test_run_query_snowflake(mock_get_conn, mock_snowflake_sqlalchemy_engine):
    """Assert that if connection type is ``snowflake`` then snowflake get_sqlalchemy_engine called"""
    mock_get_conn.return_value = Connection(conn_id="conn", conn_type="snowflake")
    op = RunQueryOperator(
        task_id="task1", sql_statement="select * from 1", conn_id="conn"
    )
    op.execute(None)
    mock_snowflake_sqlalchemy_engine.assert_called_once()
