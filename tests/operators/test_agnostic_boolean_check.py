"""
Unittest module to test Operators.
Requires the unittest, pytest, and requests-mock Python libraries.
"""

import logging
import os
import pathlib
import time
import unittest.mock

import pytest
from airflow.exceptions import BackfillUnfinished
from airflow.models import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone

# Import Operator
import astro.sql as aql
from astro.settings import SCHEMA
from astro.sql.operators.agnostic_boolean_check import (
    AgnosticBooleanCheck,
    Check,
    boolean_check,
)
from astro.sql.table import Table
from tests.operators.utils import get_dag, run_dag


log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


def get_table_name(prefix):
    """get unique table name"""
    return prefix + "_" + str(int(time.time()))


def drop_table_snowflake(
    table_name: str,
    conn_id: str = "snowflake_conn",
    schema: str = os.environ["SNOWFLAKE_SCHEMA"],
    database: str = os.environ["SNOWFLAKE_DATABASE"],
    warehouse: str = os.environ["SNOWFLAKE_WAREHOUSE"],
):
    hook = SnowflakeHook(
        snowflake_conn_id=conn_id,
        schema=schema,
        database=database,
        warehouse=warehouse,
    )
    snowflake_conn = hook.get_conn()
    cursor = snowflake_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    snowflake_conn.commit()
    cursor.close()
    snowflake_conn.close()


class TestBooleanCheckOperator(unittest.TestCase):
    """
    Test Boolean Check Operator.
    """

    cwd = pathlib.Path(__file__).parent

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.table = Table(
            "boolean_check_test",
            conn_id="postgres_conn",
            database="pagila",
            schema="public",
        )
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes_append.csv",
            output_table=cls.table,
        ).operator.execute({"run_id": "foo"})

        cls.snowflake_table = Table(
            conn_id="snowflake_conn",
            table_name=get_table_name("boolean_check_test"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        )
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes_append.csv",
            output_table=cls.snowflake_table,
        ).operator.execute({"run_id": "foo"})

        cls.table_bigquery = Table(
            "boolean_check_test",
            conn_id="bigquery",
            schema=DEFAULT_SCHEMA,
        )
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes_append.csv",
            output_table=cls.table_bigquery,
        ).operator.execute({"run_id": "foo"})

        cls.table_sqlite = Table(
            "boolean_check_test", conn_id="sqlite_conn", schema="sqlite_schema"
        )
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes_append.csv",
            output_table=cls.table_sqlite,
        ).operator.execute({"run_id": "foo"})

    @classmethod
    def tearDownClass(cls):
        drop_table_snowflake(
            table_name=cls.snowflake_table.table_name,
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )

    def clear_run(self):
        self.run = False

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

    def test_happyflow_postgres_success(self):
        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        dag = get_dag()
        with dag:
            table = get_table(self.table)
            aql.boolean_check(
                table=table,
                checks=[Check("test_1", "{{table}}.rooms > 3")],
                max_rows_returned=10,
            )
        run_dag(dag)

    def test_happyflow_postgres_fail(self):
        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        with pytest.raises(BackfillUnfinished):
            dag = get_dag()
            with dag:
                table = get_table(self.table)
                aql.boolean_check(
                    table=table,
                    checks=[
                        Check("test_1", "{{table}}.rooms > 7"),
                        Check("test_2", "{{table}}.beds >= 3"),
                    ],
                    max_rows_returned=10,
                )
            run_dag(dag)

    def test_happyflow_snowflake_success(self):
        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        dag = get_dag()
        with dag:
            table = get_table(self.snowflake_table)
            aql.boolean_check(
                table=table,
                checks=[Check("test_1", " rooms > 3")],
                max_rows_returned=10,
            )
        run_dag(dag)

    def test_happyflow_snowflake_fail(self):
        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        with pytest.raises(BackfillUnfinished):
            dag = get_dag()
            with dag:
                table = get_table(self.snowflake_table)
                aql.boolean_check(
                    table=table,
                    checks=[
                        Check("test_1", " rooms > 7"),
                        Check("test_2", " beds >= 3"),
                    ],
                    max_rows_returned=10,
                )
            run_dag(dag)

    def test_happyflow_bigquery_success(self):
        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        dag = get_dag()
        with dag:
            table = get_table(self.table_bigquery)
            aql.boolean_check(
                table=table,
                checks=[Check("test_1", "rooms > 3")],
                max_rows_returned=10,
            )
        run_dag(dag)

    def test_happyflow_sqlite_success(self):
        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        dag = get_dag()
        with dag:
            table = get_table(self.table_sqlite)
            aql.boolean_check(
                table=table,
                checks=[Check("test_1", "rooms > 3")],
                max_rows_returned=10,
            )
        run_dag(dag)

    def test_happyflow_bigquery_fail(self):
        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        with pytest.raises(BackfillUnfinished):
            dag = get_dag()
            with dag:
                table = get_table(self.table_bigquery)
                aql.boolean_check(
                    table=table,
                    checks=[
                        Check("test_1", "rooms > 7"),
                        Check("test_2", "beds >= 3"),
                    ],
                    max_rows_returned=10,
                )
            run_dag(dag)
