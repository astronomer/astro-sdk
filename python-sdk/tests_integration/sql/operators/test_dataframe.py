import pathlib

import pandas
import pytest
from airflow.utils import timezone

from astro import sql as aql
from astro.constants import Database
from astro.custom_backend.astro_custom_backend import AstroCustomXcomBackend as XCom
from astro.files import File
from astro.table import Table

from ..operators import utils as test_utils

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
CWD = pathlib.Path(__file__).parent

test_df = pandas.DataFrame({"numbers": [1, 2, 3], "Colors": ["red", "white", "blue"]})
test_df_2 = pandas.DataFrame({"Numbers": [1, 2, 3], "Colors": ["red", "white", "blue"]})


@pytest.mark.integration
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
    def my_df_func(df: pandas.DataFrame):  # skipcq: PY-D0003
        from astro.dataframes.pandas import PandasDataframe

        assert isinstance(df, PandasDataframe)
        assert len(df) == 5
        return df.sell.count().tolist()

    with sample_dag:
        f = my_df_func(df=test_table)

    dr = test_utils.run_dag(sample_dag)

    assert XCom.get_one(run_id=dr.run_id, key=f.key, task_id=f.operator.task_id) == 5


@pytest.mark.integration
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
        assert len(df) == 5
        return df.sell.count()

    with sample_dag:
        for _ in range(5):
            # ensure we can create multiple tasks
            my_df_func(df=test_table)

    task_ids = [x.task_id for x in sample_dag.tasks]
    assert task_ids == ["foo", "foo__1", "foo__2", "foo__3", "foo__4"]


@pytest.mark.integration
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
        assert len(df) == 5
        return df.sell.count().tolist()

    with sample_dag:
        res = my_df_func(test_table)
    dr = test_utils.run_dag(sample_dag)

    assert XCom.get_one(run_id=dr.run_id, key=res.key, task_id=res.operator.task_id) == 5


@pytest.mark.integration
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
        assert len(df_1) == 5
        assert len(df_2) == 5
        return (df_1.sell.count() + df_2.sell.count()).tolist()

    with sample_dag:
        res = my_df_func(test_table, df_2=test_table)
    dr = test_utils.run_dag(sample_dag)

    assert XCom.get_one(run_id=dr.run_id, key=res.key, task_id=res.operator.task_id) == 10


@pytest.mark.integration
def test_postgres_dataframe_without_table_arg(sample_dag):
    """Test dataframe operator without table argument"""

    @aql.dataframe
    def validate_result(df: pandas.DataFrame):  # skipcq: PY-D0003
        assert len(df) == 3
        assert df.iloc[0].to_dict()["colors"] == "red"

    @aql.dataframe
    def sample_df():  # skipcq: PY-D0003
        return pandas.DataFrame({"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]})

    @aql.transform
    def sample_pg(input_table: Table):  # skipcq: PY-D0003
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        plain_df = sample_df()
        pg_df = sample_pg(conn_id="postgres_conn", database="pagila", input_table=plain_df)
        validate_result(pg_df)
    test_utils.run_dag(sample_dag)


@pytest.mark.integration
@pytest.mark.parametrize(
    "conn_id",
    [
        "bigquery",
        "postgres_conn",
        "redshift_conn",
        "snowflake_conn",
        "sqlite_conn",
    ],
)
def test_empty_dataframe_fail(sample_dag, conn_id):
    @aql.dataframe
    def get_empty_dataframe():
        empty_arr = []
        return pandas.DataFrame(empty_arr)

    with sample_dag:
        get_empty_dataframe(
            output_table=Table(
                conn_id=conn_id,
            )
        )

        aql.cleanup()
    with pytest.raises(ValueError) as exec_info:
        test_utils.run_dag(sample_dag)
    assert exec_info.value.args[0] == "Can't load empty dataframe"


@pytest.mark.integration
@pytest.mark.parametrize(
    "conn_id",
    [
        "bigquery",
        "postgres_conn",
        "redshift_conn",
        "snowflake_conn",
        "sqlite_conn",
    ],
)
def test_dataframe_replace_table_if_exist(sample_dag, conn_id):
    @aql.dataframe
    def get_empty_dataframe():
        arr = {"col1": [1, 2]}
        return pandas.DataFrame(data=arr)

    output_tb = Table(conn_id=conn_id)
    with sample_dag:
        get_empty_dataframe(output_table=output_tb)

    test_utils.run_dag(sample_dag)
    assert output_tb.row_count == 2
    # re-run dag to and make sure it is replacing table
    test_utils.run_dag(sample_dag)
    assert output_tb.row_count == 2

    # drop the table to avoid issue with concurrent test run
    with sample_dag:
        aql.drop_table(table=output_tb)
    test_utils.run_dag(sample_dag)


@pytest.mark.integration
@pytest.mark.parametrize(
    "conn_id",
    [
        "bigquery",
        "postgres_conn",
        "redshift_conn",
        "snowflake_conn",
        "sqlite_conn",
    ],
)
def test_dataframe_append_table_if_exist(sample_dag, conn_id):
    @aql.dataframe(if_exists="append")
    def get_empty_dataframe():
        arr = {"col1": [1, 2]}
        return pandas.DataFrame(data=arr)

    output_tb = Table(conn_id=conn_id)
    with sample_dag:
        get_empty_dataframe(output_table=output_tb)

    test_utils.run_dag(sample_dag)
    assert output_tb.row_count == 2
    # re-run dag to and make sure it is appending the table
    test_utils.run_dag(sample_dag)
    assert output_tb.row_count == 4

    # drop the table to avoid issue with concurrent test run
    with sample_dag:
        aql.drop_table(table=output_tb)
    test_utils.run_dag(sample_dag)
