import pathlib

import pytest

from astro.settings import SCHEMA
from astro.sql.operators.sql_decorator import SqlDecoratedOperator
from tests.operators import utils as test_utils

CWD = pathlib.Path(__file__).parent


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
                "schema": SCHEMA,
                "table_name": test_utils.get_table_name("test"),
            },
        }
    ],
    indirect=True,
)
def test_sql_decorator_basic_functionality(sample_dag, sql_server, test_table):
    """Test basic sql execution of SqlDecoratedOperator."""

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
            database=test_table.database,
            sql=f"SELECT list FROM {test_table.qualified_name()} WHERE sell=232",
        )
    test_utils.run_dag(sample_dag)
