import pathlib

import pytest

from astro.databases import create_database
from astro.settings import SCHEMA
from astro.sql.operators.sql_decorator import SqlDecoratedOperator
from astro.sql.tables import Metadata
from tests.operators import utils as test_utils

CWD = pathlib.Path(__file__).parent
TEST_SCHEMA = test_utils.get_table_name("test")


@pytest.mark.parametrize(
    "sql_server",
    [
        "snowflake",
        "postgres",
        "bigquery",
        "sqlite",
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes2.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "metadata": Metadata(schema=SCHEMA),
                "name": test_utils.get_table_name("test"),
            },
        }
    ],
    indirect=True,
)
def test_sql_decorator_basic_functionality(sample_dag, sql_server, test_table):
    """Test basic sql execution of SqlDecoratedOperator."""
    database = create_database(test_table.conn_id)
    qualified_name = database.get_table_qualified_name(test_table)

    def handler_func(result):
        """Result handler"""
        if result.fetchone()[0] != 240:
            raise ValueError

    def null_function():  # skipcq: PTC-W0049
        """dummy function"""

    with sample_dag:
        SqlDecoratedOperator(
            raw_sql=True,
            parameters={},
            task_id="SomeTask",
            op_args=(),
            handler=handler_func,
            python_callable=null_function,
            conn_id=test_table.conn_id,
            database=test_table.metadata.database,
            sql=f"SELECT list FROM {qualified_name} WHERE sell=232",
        )
    test_utils.run_dag(sample_dag)


@pytest.mark.parametrize(
    "sql_server",
    ["snowflake", "bigquery", "postgres"],
    indirect=True,
)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/sample.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "name": test_utils.get_table_name("test"),
            },
        }
    ],
    indirect=True,
)
def test_sql_decorator_does_not_create_schema_when_the_schema_exists(
    sample_dag, sql_server, test_table
):
    """Test basic sql execution of SqlDecoratedOperator."""
    _, hook = sql_server

    database = create_database(test_table.conn_id)
    qualified_name = database.get_table_qualified_name(test_table)

    sql_statement = f"SELECT * FROM {qualified_name} WHERE id=4"
    df = hook.get_pandas_df(sql_statement)
    assert df.empty

    with sample_dag:
        SqlDecoratedOperator(
            raw_sql=True,
            parameters={},
            task_id="SomeTask",
            op_args=(),
            conn_id=test_table.conn_id,
            database=test_table.metadata.database,
            python_callable=lambda: None,
            sql=f"INSERT INTO {qualified_name} (id, name) VALUES (4, 'New Person');",
        )
    test_utils.run_dag(sample_dag)

    df = hook.get_pandas_df(sql_statement).rename(columns=str.lower)
    assert df.to_dict("r") == [{"id": 4, "name": "New Person"}]


@pytest.mark.parametrize(
    "sql_server",
    ["postgres", "bigquery"],
    indirect=True,
)
@pytest.mark.parametrize("schema_fixture", [TEST_SCHEMA], indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/sample.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "metadata": Metadata(schema=TEST_SCHEMA),
                "name": test_utils.get_table_name("test"),
            },
        }
    ],
    indirect=True,
)
def test_sql_decorator_creates_schema_when_it_does_not_exist(
    sample_dag, sql_server, schema_fixture, test_table
):
    """Test basic sql execution of SqlDecoratedOperator."""
    _, hook = sql_server

    database = create_database(test_table.conn_id)
    qualified_name = database.get_table_qualified_name(test_table)

    sql_statement = f"SELECT * FROM {qualified_name} WHERE id=4"
    df = hook.get_pandas_df(sql_statement)
    assert df.empty

    with sample_dag:
        SqlDecoratedOperator(
            raw_sql=True,
            parameters={},
            task_id="SomeTask",
            op_args=(),
            conn_id=test_table.conn_id,
            database=test_table.metadata.database,
            python_callable=lambda: None,
            sql=f"INSERT INTO {qualified_name} (id, name) VALUES (4, 'New Person');",
        )
    test_utils.run_dag(sample_dag)

    df = hook.get_pandas_df(sql_statement)
    assert df.to_dict("r") == [{"id": 4, "name": "New Person"}]
