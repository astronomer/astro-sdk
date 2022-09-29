import pathlib

import astro.sql as aql
import pandas
import pytest
from airflow.models.xcom import XCom
from airflow.utils import timezone
from astro.airflow.datasets import DATASET_SUPPORT
from astro.constants import Database
from astro.files import File
from astro.sql.table import Table
from tests.sql.operators import utils as test_utils

# Import Operator

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
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
def test_dataframe_from_sql_basic(sample_dag, database_table_fixture):
    """Test basic operation of dataframe operator."""

    _, test_table = database_table_fixture

    @aql.dataframe
    def validate(num):
        assert num == 5

    @aql.dataframe
    def my_df_func(df: pandas.DataFrame):  # skipcq: PY-D0003
        return df.sell.count()

    with sample_dag:
        f = my_df_func(df=test_table)
        validate(f)

    test_utils.run_dag(sample_dag)


def test_dataframe_pass_list(sample_dag):
    """Test basic operation of dataframe operator."""

    @aql.dataframe
    def create_list():  # skipcq: PY-D0003
        return [1, 2, 3, 4, 5]

    @aql.dataframe
    def validate_list(input_list: list):
        assert input_list == [1, 2, 3, 4, 5]

    with sample_dag:
        validate_list(create_list())

    test_utils.run_dag(sample_dag)


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
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
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
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
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

    @aql.dataframe
    def validate(num: int):
        assert num == 5

    with sample_dag:
        res = my_df_func(test_table)
        validate(res)
    test_utils.run_dag(sample_dag)


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
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
    ],
    indirect=True,
    # ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
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

    @aql.dataframe
    def validate(num):
        assert num == 10

    with sample_dag:
        res = my_df_func(test_table, df_2=test_table)
        validate(res)
    test_utils.run_dag(sample_dag)


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


def test_columns_names_capitalization(sample_dag):
    """Test dataframe operator with columns_names_capitalization param"""

    @aql.dataframe(columns_names_capitalization="lower")
    def sample_df_1():  # skipcq: PY-D0003
        return pandas.DataFrame(
            {"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]}
        )

    @aql.dataframe(columns_names_capitalization="upper")
    def sample_df_2():  # skipcq: PY-D0003
        return pandas.DataFrame(
            {"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]}
        )

    @aql.dataframe(columns_names_capitalization="original")
    def sample_df_3():  # skipcq: PY-D0003
        return pandas.DataFrame(
            {"numbers": [1, 2, 3], "COLORS": ["red", "white", "blue"]}
        )

    with sample_dag:
        res_1 = sample_df_1()
        res_2 = sample_df_2()
        res_3 = sample_df_3()
    test_utils.run_dag(sample_dag)

    columns = XCom.get_one(
        execution_date=DEFAULT_DATE, key=res_1.key, task_id=res_1.operator.task_id
    )
    assert all(x.islower() for x in columns)

    columns = XCom.get_one(
        execution_date=DEFAULT_DATE, key=res_2.key, task_id=res_2.operator.task_id
    )
    assert all(x.isupper() for x in columns)

    columns = XCom.get_one(
        execution_date=DEFAULT_DATE, key=res_3.key, task_id=res_3.operator.task_id
    )
    cols = list(columns.columns)
    cols.sort()
    assert cols[1].islower()
    assert cols[0].isupper()


@pytest.mark.parametrize(
    "kwargs",
    [{"task_id": "task1", "queue": "new_1"}, {"queue": "new_2", "owner": "astro-sdk"}],
)
def test_pass_kwargs_to_base_operator(kwargs):
    """Test that kwargs passed to decorator are passed to BaseOperator"""

    @aql.dataframe(**kwargs)
    def sample_df_1():  # skipcq: PY-D0003
        return pandas.DataFrame(
            {"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]}
        )

    task1 = sample_df_1()
    assert all(getattr(task1.operator, k) == v for k, v in kwargs.items())


@pytest.mark.skipif(
    not DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4"
)
def test_inlets_outlets_supported_ds():
    """Test Datasets are set as inlets and outlets"""
    output_table = Table("test_name")

    @aql.dataframe()
    def sample_df_1(**kwargs):
        return pandas.DataFrame(
            {"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]}
        )

    task = sample_df_1(output_table=output_table)
    assert task.operator.outlets == [output_table]


@pytest.mark.skipif(
    DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4"
)
def test_inlets_outlets_non_supported_ds():
    """Test inlets and outlets are not set if Datasets are not supported"""
    output_table = Table("test_name")

    @aql.dataframe()
    def sample_df_1(**kwargs):
        return pandas.DataFrame(
            {"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]}
        )

    task = sample_df_1(output_table=output_table)
    assert task.operator.outlets == []
