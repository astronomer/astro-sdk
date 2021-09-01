"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:
    AIRFLOW_CONN_AWS_DEFAULT=aws://KEY:SECRET@ \
    AIRFLOW_CONN_POSTGRES_CONN=postgres://USER:PASSWORD@HOST:5432/DB_NAME \
    python3 -m unittest tests.operators.test_postgres_operator.TestSampleOperator.test_load_s3_to_db

"""

import logging
import os
import tempfile
import unittest.mock
from unittest import mock

import pandas as pd
import sqlalchemy
from airflow.models import Connection
from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from pandas import DataFrame

# Import Operator
from astronomer_sql_decorator.operators.postgres_decorator import postgres_decorator

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


# Mock the `conn_sample` Airflow connection
@mock.patch.dict(
    "os.environ", AIRFLOW_CONN_CONN_SAMPLE="http://https%3A%2F%2Fwww.httpbin.org%2F"
)
class TestSampleOperator(unittest.TestCase):
    """
    Test Sample Operator.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            postgres_connection = Connection(
                conn_id="postgres_conn",
                conn_type="postgres",
                host="localhost",
                schema="astro",
                port=5432,
                login="postgres",
                password="postgres",
            )
            session.query(DagRun).delete()
            session.query(TI).delete()
            session.query(Connection).delete()
            session.add(postgres_connection)

    def setUp(self):
        super().setUp()
        self.clear_run()
        self.addCleanup(self.clear_run)
        self.dag = DAG(
            "test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE}
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

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        f.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        return f

    def test_dataframe_func(self):
        @postgres_decorator(postgres_conn_id="postgres_conn", to_dataframe=True)
        def print_table(input_df: DataFrame):
            print(input_df.to_string)

        self.create_and_run_task(print_table, ("bar",), {})

    def test_postgres(self):
        @postgres_decorator(postgres_conn_id="postgres_conn", database="pagila")
        def sample_pg(input_table):
            return "SELECT * FROM %(input_table)s WHERE last_name LIKE 'G%%'"

        self.create_and_run_task(sample_pg, (), {"input_table": "actor"})

    def test_load_s3_to_sql_db(self):
        OUTPUT_TABLE_NAME = "table_test_load_s3_to_sql_db"

        @postgres_decorator(
            postgres_conn_id="postgres_conn", database="astro", from_s3=True
        )
        def task_from_s3(s3_path, input_table=None, output_table=None):
            return """SELECT * FROM %(input_table)s LIMIT 8"""

        self.create_and_run_task(
            task_from_s3,
            (),
            {
                "s3_path": "s3://tmp9/homes.csv",
                "input_table": "actor",
                "output_table": OUTPUT_TABLE_NAME,
            },
        )

        # Generate target db hook
        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="astro"
        )

        # Read table from db
        df = pd.read_sql(
            f"SELECT * FROM {OUTPUT_TABLE_NAME}", con=self.hook_target.get_conn()
        )

        # Assert output table structure
        assert len(df) == 8

    def test_load_local_csv_to_sql_db(self):
        OUTPUT_TABLE_NAME = "expected_table_from_csv"
        hook = PostgresHook(postgres_conn_id="postgres_conn", schema="astro")

        @postgres_decorator(
            postgres_conn_id="postgres_conn", database="astro", from_csv=True
        )
        def task_from_local_csv(csv_path, input_table=None, output_table=None):
            return """SELECT "Sell" FROM %(input_table)s LIMIT 3"""

        self.create_and_run_task(
            task_from_local_csv,
            (),
            {
                "csv_path": "../data/homes.csv",
                "input_table": "input_raw_table_from_csv",
                "output_table": OUTPUT_TABLE_NAME,
            },
        )

        # Read table from db
        df = pd.read_sql(f"SELECT * FROM {OUTPUT_TABLE_NAME}", con=hook.get_conn())

        # Assert output table structure
        assert df.to_json() == '{"Sell":{"0":142,"1":175,"2":129}}'

    def test_save_sql_table_to_csv(self):
        @postgres_decorator(
            postgres_conn_id="postgres_conn",
            database="pagila",
            to_csv=True,
        )
        def task_to_local_csv(csv_path, input_table=None):
            return "SELECT * FROM %(input_table)s WHERE last_name LIKE 'G%%'"

        with tempfile.TemporaryDirectory() as tmp:
            self.create_and_run_task(
                task_to_local_csv,
                (),
                {"input_table": "actor", "csv_path": tmp + "/output.csv"},
            )
            with open(tmp + "/output.csv") as file:
                lines = [line for line in file.readlines()]
                assert len(lines) == 13
