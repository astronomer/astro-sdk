import logging
import os
import pathlib
import unittest.mock
from unittest import mock

import pandas
import pytest
from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.models.xcom import XCom
from airflow.utils import timezone
from airflow.utils.session import create_session

from astro import dataframe as df
from astro import sql as aql
from astro.constants import SUPPORTED_DATABASES
from astro.settings import SCHEMA
from astro.sql.table import Table
from tests.operators import utils as test_utils

# Import Operator

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
CWD = pathlib.Path(__file__).parent


# Mock the `conn_sample` Airflow connection
def drop_table(table_name, postgres_conn):
    cursor = postgres_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    postgres_conn.commit()
    cursor.close()
    postgres_conn.close()


@mock.patch.dict("os.environ", AIRFLOW__CORE__ENABLE_XCOM_PICKLING="True")
class TestDataframeFromSQL(unittest.TestCase):
    """
    Test Dataframe From SQL
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cwd = pathlib.Path(__file__).parent
        cls.OUTPUT_TABLE_NAME = test_utils.get_table_name("snowflake_decorator_test")
        aql.load_file(
            path=str(cwd) + "/../data/homes.csv",
            output_table=Table(
                table_name=cls.OUTPUT_TABLE_NAME,
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                conn_id="snowflake_conn",
            ),
        ).operator.execute({"run_id": "foo"})

    @classmethod
    def tearDownClass(cls):
        test_utils.drop_table_snowflake(
            table_name=cls.OUTPUT_TABLE_NAME,
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )

    def setUp(self):
        super().setUp()
        self.clear_run()
        self.addCleanup(self.clear_run)
        self.dag = DAG(
            "test_dag",
            default_args={
                "owner": "airflow",
                "start_date": DEFAULT_DATE,
            },
        )

    def clear_run(self):
        self.run = False

    def tearDown(self):
        super().tearDown()
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def create_and_run_task(self, decorator_func, op_args, op_kwargs):
        with self.dag:
            f = decorator_func(*op_args, **op_kwargs)

        test_utils.run_dag(self.dag)

        return f

    # def test_dataframe_from_sql_basic(self):
    #     @df
    #     def my_df_func(df: pandas.DataFrame):
    #         return df.actor_id.count()
    #
    #     with self.dag:
    #         f = my_df_func(
    #             df=Table(
    #                 "actor", conn_id="postgres_conn", database="pagila", schema="public"
    #             )
    #         )
    #
    #     test_utils.run_dag(self.dag)
    #
    #     assert (
    #         XCom.get_one(
    #             execution_date=DEFAULT_DATE, key=f.key, task_id=f.operator.task_id
    #         )
    #         == 200
    #     )

    # def test_dataframe_from_sql_custom_task_id(self):
    #     @df(task_id="foo")
    #     def my_df_func(df: pandas.DataFrame):
    #         return df.actor_id.count()
    #
    #     with self.dag:
    #         for i in range(5):
    #             # ensure we can create multiple tasks
    #             f = my_df_func(
    #                 df=Table(
    #                     "actor",
    #                     conn_id="postgres_conn",
    #                     database="pagila",
    #                     schema="public",
    #                 )
    #             )
    #
    #     task_ids = [x.task_id for x in self.dag.tasks]
    #     assert task_ids == ["foo", "foo__1", "foo__2", "foo__3", "foo__4"]
    #
    #     test_utils.run_dag(self.dag)
    #
    #     assert (
    #         XCom.get_one(
    #             execution_date=DEFAULT_DATE, key=f.key, task_id=f.operator.task_id
    #         )
    #         == 200
    #     )

    # def test_dataframe_from_sql_basic_op_arg(self):
    #     @df(conn_id="postgres_conn", database="pagila")
    #     def my_df_func(df: pandas.DataFrame):
    #         return df.actor_id.count()
    #
    #     res = self.create_and_run_task(
    #         my_df_func,
    #         (
    #             Table(
    #                 "actor", conn_id="postgres_conn", database="pagila", schema="public"
    #             ),
    #         ),
    #         {},
    #     )
    #     assert (
    #         XCom.get_one(
    #             execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
    #         )
    #         == 200
    #     )

    # def test_dataframe_from_sql_basic_op_arg_and_kwarg(self):
    #     @df(conn_id="postgres_conn", database="pagila")
    #     def my_df_func(actor_df: pandas.DataFrame, film_df: pandas.DataFrame):
    #         return actor_df.actor_id.count() + film_df.film_id.count()
    #
    #     res = self.create_and_run_task(
    #         my_df_func,
    #         (
    #             Table(
    #                 "actor", conn_id="postgres_conn", database="pagila", schema="public"
    #             ),
    #         ),
    #         {
    #             "film_df": Table(
    #                 "film", conn_id="postgres_conn", database="pagila", schema="public"
    #             )
    #         },
    #     )
    #     assert (
    #         XCom.get_one(
    #             execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
    #         )
    #         == 1200
    #     )

    def test_snow_dataframe_from_sql_basic(self):
        @df(identifiers_as_lower=False)
        def my_df_func(df: pandas.DataFrame):
            return df.LIVING.count()

        res = self.create_and_run_task(
            my_df_func,
            (),
            {
                "df": Table(
                    self.OUTPUT_TABLE_NAME,
                    conn_id="snowflake_conn",
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                )
            },
        )
        assert (
            XCom.get_one(
                execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
            )
            == 47
        )

    def test_snow_dataframe_with_case(self):
        identifiers_as_lower = True

        @df(identifiers_as_lower=identifiers_as_lower)
        def my_df_func(df: pandas.DataFrame):
            return df.columns

        res = self.create_and_run_task(
            my_df_func,
            (),
            {
                "df": Table(
                    self.OUTPUT_TABLE_NAME,
                    conn_id="snowflake_conn",
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                )
            },
        )
        columns = XCom.get_one(
            execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
        )
        assert all(x.islower() for x in columns) == identifiers_as_lower

    def test_snow_dataframe_without_case(self):
        identifiers_as_lower = False

        @df(identifiers_as_lower=identifiers_as_lower)
        def my_df_func(df: pandas.DataFrame):
            return df.columns

        res = self.create_and_run_task(
            my_df_func,
            (),
            {
                "df": Table(
                    self.OUTPUT_TABLE_NAME,
                    conn_id="snowflake_conn",
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                )
            },
        )
        columns = XCom.get_one(
            execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
        )
        assert all(x.islower() for x in columns) == identifiers_as_lower


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes2.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "schema": SCHEMA,
                "table_name": test_utils.get_table_name("test_stats_check_2"),
            },
        }
    ],
    indirect=True,
)
def test_dataframe_from_sql_basic(sample_dag, sql_server, test_table):
    sql_name, hook = sql_server

    @df
    def my_df_func(df: pandas.DataFrame):
        if sql_name != "snowflake":
            return df.sell.count()
        else:
            return df.SELL.count()

    with sample_dag:
        f = my_df_func(df=test_table)

    test_utils.run_dag(sample_dag)

    assert (
        XCom.get_one(execution_date=DEFAULT_DATE, key=f.key, task_id=f.operator.task_id)
        == 5
    )


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes2.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "schema": SCHEMA,
                "table_name": test_utils.get_table_name("test_stats_check_2"),
            },
        }
    ],
    indirect=True,
)
def test_dataframe_from_sql_custom_task_id(sample_dag, sql_server, test_table):
    sql_name, hook = sql_server

    @df(task_id="foo")
    def my_df_func(df: pandas.DataFrame):
        if sql_name != "snowflake":
            return df.sell.count()
        else:
            return df.SELL.count()

    with sample_dag:
        for i in range(5):
            # ensure we can create multiple tasks
            f = my_df_func(df=test_table)

    task_ids = [x.task_id for x in sample_dag.tasks]
    assert task_ids == ["foo", "foo__1", "foo__2", "foo__3", "foo__4"]

    test_utils.run_dag(sample_dag)

    assert (
        XCom.get_one(execution_date=DEFAULT_DATE, key=f.key, task_id=f.operator.task_id)
        == 5
    )


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes2.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "schema": SCHEMA,
                "table_name": test_utils.get_table_name("test_stats_check_2"),
            },
        }
    ],
    indirect=True,
)
def test_dataframe_from_sql_basic_op_arg(sample_dag, sql_server, test_table):
    sql_name, hook = sql_server

    @df(conn_id=test_table.conn_id, database=test_table.database)
    def my_df_func(df: pandas.DataFrame):
        if sql_name != "snowflake":
            return df.sell.count()
        else:
            return df.SELL.count()

    with sample_dag:
        res = my_df_func(test_table)
    test_utils.run_dag(sample_dag)

    assert (
        XCom.get_one(
            execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
        )
        == 5
    )


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes2.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "schema": SCHEMA,
                "table_name": test_utils.get_table_name("test_stats_check_2"),
            },
        }
    ],
    indirect=True,
)
def test_dataframe_from_sql_basic_op_arg_and_kwarg(sample_dag, sql_server, test_table):
    sql_name, hook = sql_server

    @df(conn_id=test_table.conn_id, database=test_table.database)
    def my_df_func(df_1: pandas.DataFrame, df_2: pandas.DataFrame):
        if sql_name != "snowflake":
            return df_1.sell.count() + df_2.sell.count()
        else:
            return df_1.SELL.count() + df_2.SELL.count()

    with sample_dag:
        res = my_df_func(test_table, df_2=test_table)
    test_utils.run_dag(sample_dag)

    assert (
        XCom.get_one(
            execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
        )
        == 10
    )
