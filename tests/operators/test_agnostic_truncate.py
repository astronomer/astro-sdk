"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

"""

import logging
import math
import pathlib
import unittest.mock

from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

# Import Operator
import astro.sql as aql
from astro.sql.table import Table

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


def drop_table(table_name, postgres_conn):
    cursor = postgres_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    postgres_conn.commit()
    cursor.close()
    postgres_conn.close()


class TestPostgresTruncateOperator(unittest.TestCase):
    """
    Test Postgres Merge Operator.
    """

    cwd = pathlib.Path(__file__).parent

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

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
        aql.load_file(
            path=str(self.cwd) + "/../data/homes_merge_1.csv",
            output_conn_id="postgres_conn",
            output_table_name="truncate_test",
            database="pagila",
        ).operator.execute({"run_id": "foo"})

    def test_truncate(self):
        hook = PostgresHook(schema="pagila", postgres_conn_id="postgres_conn")
        df = hook.get_pandas_df(sql="SELECT * FROM truncate_test")
        assert df.count()[0] == 4
        a = aql.truncate(
            table=Table(
                table_name="truncate_test", database="pagila", conn_id="postgres_conn"
            ),
        )
        a.execute({"run_id": "foo"})
        df = hook.get_pandas_df(sql="SELECT * FROM truncate_test")
        assert df.count()[0] == 0
