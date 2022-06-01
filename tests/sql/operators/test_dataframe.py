import logging
import pathlib

import pandas
import pytest
from airflow.models.xcom import XCom
from airflow.utils import timezone

import astro.sql as aql
from astro.constants import Database
from astro.files import File
from astro.sql.table import Table
from tests.sql.operators import utils as test_utils

# Import Operator

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
CWD = pathlib.Path(__file__).parent


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
def test_dataframe_from_sql_basic(sample_dag, database_table_fixture):
    """Test basic operation of dataframe operator."""

    _, test_table = database_table_fixture

    @aql.dataframe
    def my_df_func(df: pandas.DataFrame):  # skipcq: PY-D0003
        return df.sell.count()

    with sample_dag:
        f = my_df_func(df=test_table)

    test_utils.run_dag(sample_dag)

    assert (
        XCom.get_one(execution_date=DEFAULT_DATE, key=f.key, task_id=f.operator.task_id)
        == 5
    )


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
def test_dataframe_from_sql_custom_task_id(sample_dag, database_table_fixture):
    """Test custom and taskId increment when same task is added multiple times."""

    _, test_table = database_table_fixture

    @aql.dataframe(task_id="foo")
    def my_df_func(df: pandas.DataFrame):  # skipcq: PY-D0003
        return df.sell.count()

    with sample_dag:
        for _ in range(5):
            # ensure we can create multiple tasks
            my_df_func(df=test_table)

    task_ids = [x.task_id for x in sample_dag.tasks]
    assert task_ids == ["foo", "foo__1", "foo__2", "foo__3", "foo__4"]


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
def test_dataframe_from_sql_basic_op_arg(sample_dag, database_table_fixture):
    """Test basic operation of dataframe operator with op_args."""

    _, test_table = database_table_fixture

    @aql.dataframe(
        conn_id=test_table.conn_id,
        database=getattr(test_table.metadata, "database", None),
    )
    def my_df_func(df: pandas.DataFrame):  # skipcq: PY-D0003
        return df.sell.count()

    with sample_dag:
        res = my_df_func(test_table)
    test_utils.run_dag(sample_dag)

    assert (
        XCom.get_one(
            execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
        )
        == 5
    )


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
def test_dataframe_from_sql_basic_op_arg_and_kwarg(
    sample_dag,
    database_table_fixture,
):
    """Test dataframe creation from table object in args and kwargs."""
    test_table = database_table_fixture[1]

    @aql.dataframe(
        conn_id=test_table.conn_id,
        database=getattr(test_table.metadata, "database", None),
    )
    def my_df_func(df_1: pandas.DataFrame, df_2: pandas.DataFrame):  # skipcq: PY-D0003
        return df_1.sell.count() + df_2.sell.count()

    with sample_dag:
        res = my_df_func(test_table, df_2=test_table)
    test_utils.run_dag(sample_dag)

    assert (
        XCom.get_one(
            execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
        )
        == 10
    )


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/homes_upper.csv"),
        },
    ],
    indirect=True,
    ids=["postgresql"],
)
@pytest.mark.parametrize(
    "identifiers_as_lower",
    [True, False],
    ids=["identifiers_as_lower=True", "identifiers_as_lower=False"],
)
def test_dataframe_with_lower_and_upper_case(
    sample_dag, database_table_fixture, identifiers_as_lower
):
    """
    Test dataframe operator 'identifiers_as_lower' param which converts
    all col names in lower case, which is useful to maintain consistency,
    since snowflake return all col name in caps.
    """
    _, test_table = database_table_fixture

    @aql.dataframe(identifiers_as_lower=identifiers_as_lower)
    def my_df_func(df: pandas.DataFrame):  # skipcq: PY-D0003
        return df.columns

    with sample_dag:
        res = my_df_func(df=test_table)
    test_utils.run_dag(sample_dag)

    columns = XCom.get_one(
        execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
    )
    assert all(x.islower() for x in columns) == identifiers_as_lower


def test_postgres_dataframe_without_table_arg(sample_dag):
    """Test dataframe operator without table argument"""

    @aql.dataframe
    def validate_result(df: pandas.DataFrame):  # skipcq: PY-D0003
        assert df.iloc[0].to_dict()["colors"] == "red"

    @aql.dataframe
    def sample_df():  # skipcq: PY-D0003
        return pandas.DataFrame(
            {"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]}
        )

    @aql.transform
    def sample_pg(input_table: Table):  # skipcq: PY-D0003
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        plain_df = sample_df()
        pg_df = sample_pg(
            conn_id="postgres_conn", database="pagila", input_table=plain_df
        )
        validate_result(pg_df)
    test_utils.run_dag(sample_dag)
