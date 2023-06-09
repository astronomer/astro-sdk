from __future__ import annotations

import tempfile
from unittest import mock

from astro import sql as aql
from tests.sql.operators import utils as test_utils


class MockReturn:
    _scalar: list = []

    def scalar(self):
        return self._scalar


@mock.patch("astro.databases.base.BaseDatabase.connection")
def test_transform_calls_with_query_tag(run_sql, sample_dag):
    from astro.query_modifier import QueryModifier

    run_sql.execute.return_value = MockReturn()

    with sample_dag:

        @aql.transform(
            conn_id="sqlite_default",
            query_modifier=QueryModifier(pre_queries=["ALTER team_1", "ALTER team_2"]),
        )
        def dummy_method():
            return "SELECT 1+1"

        dummy_method()

    test_utils.run_dag(sample_dag)
    run_sql.method_calls[1].args[0].text.startswith("ALTER team_1")
    run_sql.method_calls[2].args[0].text.startswith("ALTER team_2")
    run_sql.method_calls[3].args[0].text.startswith("CREATE TABLE IF NOT EXISTS")
    run_sql.method_calls[3].args[0].text.endswith("AS SELECT 1+1")


@mock.patch("astro.databases.base.BaseDatabase.connection")
def test_transform_file_calls_with_query_tag(run_sql, sample_dag):
    from astro.query_modifier import QueryModifier

    run_sql.execute.return_value = MockReturn()

    with tempfile.NamedTemporaryFile(suffix=".sql") as tmp_file:
        tmp_file.write(b"SELECT 1+1")
        tmp_file.flush()

        with sample_dag:
            aql.transform_file(
                file_path=tmp_file.name,
                conn_id="sqlite_default",
                query_modifier=QueryModifier(pre_queries=["ALTER team_1", "ALTER team_2"]),
            )
        test_utils.run_dag(sample_dag)

        assert run_sql.method_calls[1].args[0].text.startswith("ALTER team_1")
        assert run_sql.method_calls[2].args[0].text.startswith("ALTER team_2")
        assert run_sql.method_calls[3].args[0].text.startswith("CREATE TABLE IF NOT EXISTS")
        assert run_sql.method_calls[3].args[0].text.endswith("AS SELECT 1+1")


@mock.patch("astro.databases.snowflake.SnowflakeDatabase.connection")
@mock.patch("astro.databases.snowflake.SnowflakeDatabase.hook")
@mock.patch("astro.databases.snowflake.SnowflakeDatabase.get_table_qualified_name")
@mock.patch("airflow.models.taskinstance.XCom")
@mock.patch("astro.sql.operators.base_decorator.BaseSQLDecoratedOperator.execute")
def test_transform_with_default_assume_schema_exists(
    mock_base, mock_xcom, mock_qualified_name, mock_hook, mock_connection, sample_dag
):
    mock_connection.execute.return_value = MockReturn()

    with sample_dag:

        @aql.transform(conn_id="snowflake_conn", assume_schema_exists=False)
        def dummy_method():
            return "SELECT 1+1"

        dummy_method()

    test_utils.run_dag(sample_dag)
    expected = (
        "SELECT SCHEMA_NAME from information_schema.schemata WHERE LOWER(SCHEMA_NAME) = %(schema_name)s;"
    )
    assert mock_hook.run.call_args[0][0] == expected


@mock.patch("astro.databases.snowflake.SnowflakeDatabase.connection")
@mock.patch("astro.databases.snowflake.SnowflakeDatabase.hook")
@mock.patch("astro.databases.snowflake.SnowflakeDatabase.get_table_qualified_name")
@mock.patch("airflow.models.taskinstance.XCom")
@mock.patch("astro.sql.operators.base_decorator.BaseSQLDecoratedOperator.execute")
def test_transform_with_assume_schema_exists_set_true(
    mock_base, mock_xcom, mock_qualified_name, mock_hook, mock_connection, sample_dag
):
    mock_connection.execute.return_value = MockReturn()

    with sample_dag:

        @aql.transform(conn_id="snowflake_conn", assume_schema_exists=True)
        def dummy_method():
            return "SELECT 1+1"

        dummy_method()

    test_utils.run_dag(sample_dag)
    assert not mock_hook.run.called
