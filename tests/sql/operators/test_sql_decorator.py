import pathlib

import pytest

from astro.constants import Database
from astro.files import File
from astro.sql.operators.sql_decorator_legacy import SqlDecoratedOperator
from tests.sql.operators import utils as test_utils

CWD = pathlib.Path(__file__).parent
TEST_SCHEMA = test_utils.get_table_name("test")


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite"],
)
def test_sql_decorator_basic_functionality(sample_dag, database_table_fixture):
    """Test basic sql execution of SqlDecoratedOperator."""
    database, test_table = database_table_fixture
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
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql"],
)
def test_sql_decorator_does_not_create_schema_when_the_schema_exists(
    sample_dag,
    database_table_fixture,
):
    """Test basic sql execution of SqlDecoratedOperator."""
    database, test_table = database_table_fixture
    hook = database.hook
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
            schema=test_table.metadata.schema,
            python_callable=lambda: None,
            sql=f"INSERT INTO {qualified_name} (id, name) VALUES (4, 'New Person');",
        )
    test_utils.run_dag(sample_dag)

    df = hook.get_pandas_df(sql_statement).rename(columns=str.lower)
    assert df.to_dict("r") == [{"id": 4, "name": "New Person"}]


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/sample.csv"),
        },
    ],
    indirect=True,
    ids=["bigquery", "postgresql"],
)
@pytest.mark.parametrize("schemas_fixture", [TEST_SCHEMA], indirect=True)
def test_sql_decorator_creates_schema_when_it_does_not_exist(
    sample_dag,
    schemas_fixture,
    database_table_fixture,
):
    """Test basic sql execution of SqlDecoratedOperator."""
    database, test_table = database_table_fixture
    qualified_name = database.get_table_qualified_name(test_table)

    sql_statement = f"SELECT * FROM {qualified_name} WHERE id=4"
    df = database.hook.get_pandas_df(sql_statement)
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

    df = database.hook.get_pandas_df(sql_statement)
    assert df.to_dict("r") == [{"id": 4, "name": "New Person"}]
