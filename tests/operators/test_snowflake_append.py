"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.operators.test_snowflake_append.TestSnowflakeAppend.test_append

"""

import logging
import os
import pathlib
import unittest.mock

from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

# Import Operator
from astronomer_sql_decorator import sql as aql

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


def drop_table(table_name, snowflake_conn):
    cursor = snowflake_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    snowflake_conn.commit()
    cursor.close()
    snowflake_conn.close()


def get_snowflake_hook():
    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_conn",
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        database=os.environ["SNOWFLAKE_DB"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    )
    return hook


class TestSnowflakeAppend(unittest.TestCase):
    """Test Snowflake Append."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def setUp(self):
        super().setUp()
        self.dag = DAG(
            "test_dag",
            default_args={
                "owner": "airflow",
                "start_date": DEFAULT_DATE,
            },
        )
        self.addCleanup(self.dag.clear)
        self.clear_run()
        self.addCleanup(self.clear_run)
        cwd = pathlib.Path(__file__).parent
        aql.load_file(
            path=str(cwd) + "/../data/homes_main.csv",
            output_table_name="test_append_1",
            output_conn_id="snowflake_conn",
        ).operator.execute(None)
        aql.load_file(
            path=str(cwd) + "/../data/homes_append.csv",
            output_table_name="test_append_2",
            output_conn_id="snowflake_conn",
        ).operator.execute(None)

    def clear_run(self):
        self.run = False

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def wait_for_task_finish(self, dr, task_id):
        import time

        task = dr.get_task_instance(task_id)
        while task.state not in ["success", "failed"]:
            time.sleep(1)
            task = dr.get_task_instance(task_id)

    def run_append_func(
        self, main_table_name, append_table_name, columns, casted_columns
    ):
        MAIN_TABLE_NAME = main_table_name
        APPEND_TABLE_NAME = append_table_name

        with self.dag:
            foo = aql.append(
                conn_id="snowflake_conn",
                append_table=APPEND_TABLE_NAME,
                columns=columns,
                casted_columns=casted_columns,
                main_table=MAIN_TABLE_NAME,
            )
        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        foo.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, foo.task_id)

    def test_append(self):
        MAIN_TABLE_NAME = "TEST_APPEND_1"
        APPEND_TABLE_NAME = "TEST_APPEND_2"
        hook = get_snowflake_hook()

        self.run_append_func(MAIN_TABLE_NAME, APPEND_TABLE_NAME, [], {})
        main_table_count = hook.run(f"SELECT COUNT(*) FROM {MAIN_TABLE_NAME}")
        assert main_table_count[0]["COUNT(*)"] == 6

    def test_append_no_cast(self):
        MAIN_TABLE_NAME = "TEST_APPEND_1"
        APPEND_TABLE_NAME = "TEST_APPEND_2"
        hook = get_snowflake_hook()

        self.run_append_func(MAIN_TABLE_NAME, APPEND_TABLE_NAME, ["BEDS"], {})

        df = hook.get_pandas_df(f"SELECT * FROM {MAIN_TABLE_NAME}")

        assert len(df) == 6
        assert not df["BEDS"].hasnans
        assert df["ROOMS"].hasnans

    def test_append_with_cast(self):
        MAIN_TABLE_NAME = "TEST_APPEND_1"
        APPEND_TABLE_NAME = "TEST_APPEND_2"

        self.run_append_func(MAIN_TABLE_NAME, APPEND_TABLE_NAME, [], {"ACRES": "FLOAT"})

        hook = get_snowflake_hook()

        df = hook.get_pandas_df(f"SELECT * FROM {MAIN_TABLE_NAME}")

        assert len(df) == 6
        assert not df["ACRES"].hasnans
        assert df["BEDS"].hasnans

    def test_append_with_cast_and_no_cast(self):
        MAIN_TABLE_NAME = "TEST_APPEND_1"
        APPEND_TABLE_NAME = "TEST_APPEND_2"
        hook = get_snowflake_hook()

        self.run_append_func(
            MAIN_TABLE_NAME,
            APPEND_TABLE_NAME,
            ["BEDS"],
            {"ACRES": "FLOAT"},
        )

        df = hook.get_pandas_df(f"SELECT * FROM {MAIN_TABLE_NAME}")

        assert len(df) == 6
        assert not df["BEDS"].hasnans
        assert not df["ACRES"].hasnans
        assert df["LIVING"].hasnans


if __name__ == "__main__":
    unittest.main()
