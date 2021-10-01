"""
Unittest module to test Agnostic Load File function.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:
    AIRFLOW__SQL_DECORATOR__CONN_AWS_DEFAULT=aws://AKIAZG42HVH6Z3B6ELRB:SgwfrcO2NdKpeKhUG77K%2F6B2HuRJJopbHPV84NbY@ \
    python3 -m unittest tests.operators.test_agnostic_load_file.TestAgnosticLoadFile.test_aql_local_file_to_postgres

"""

import logging
import os
import pathlib
import tempfile
import unittest.mock
from unittest import mock

import pandas as pd
from airflow.models import DAG, Connection, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from pandas import DataFrame

# Import Operator
import astronomer_sql_decorator.sql as aql
from astronomer_sql_decorator.operators.agnostic_load_file import load_file

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


def drop_table_postgres(table_name, postgres_conn):
    cursor = postgres_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    postgres_conn.commit()
    cursor.close()
    postgres_conn.close()


# Mock the `postgres_conn` Airflow connection
@mock.patch.dict(
    "os.environ",
    AIRFLOW_CONN_POSTGRES_CONN="postgres://postgres:postgres@localhost:5432/pagila",
)
class TestAgnosticLoadFile(unittest.TestCase):
    """
    Test agnostic load file.
    """

    cwd = pathlib.Path(__file__).parent

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            postgres_connection = Connection(
                conn_id="postgres_conn",
                conn_type="postgres",
                host="localhost",
                port=5432,
                login="postgres",
                password="postgres",
            )
            session.query(DagRun).delete()
            session.query(TI).delete()
            session.query(Connection).delete()
            session.add(postgres_connection)
            snowflake_connection = Connection(
                conn_id="snowflake_conn",
                conn_type="snowflake",
                host="https://gp21411.us-east-1.snowflakecomputing.com",
                login=os.environ["SNOW_ACCOUNT_NAME"],
                port=443,
                password=os.environ["SNOW_PASSWORD"],
                extra={
                    "account": "gp21411",
                    "region": "us-east-1",
                    "role": "TRANSFORMER",
                },
            )
            session.add(snowflake_connection)

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

    def test_aql_local_file_to_postgres(self):
        OUTPUT_TABLE_NAME = "expected_table_from_csv"

        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )

        # Drop target table
        drop_table_postgres(OUTPUT_TABLE_NAME, self.hook_target.get_conn())

        self.create_and_run_task(
            load_file,
            (),
            {
                "path": str(self.cwd) + "/../data/homes.csv",
                "file_conn_id": "",
                "output_conn_id": "postgres_conn",
                "output_table_name": OUTPUT_TABLE_NAME,
            },
        )

        # Read table from db
        df = pd.read_sql(
            f"SELECT * FROM {OUTPUT_TABLE_NAME}", con=self.hook_target.get_conn()
        )

        assert df.iloc[0].to_dict() == {
            "Sell": 142.0,
            "List": 160.0,
            "Living": 28.0,
            "Rooms": 10.0,
            "Beds": 5.0,
            "Baths": 3.0,
            "Age": 60.0,
            "Acres": 0.28,
            "Taxes": 3167.0,
        }

    def test_aql_local_file_to_postgres_no_output_table_name(self):
        OUTPUT_TABLE_NAME = "test_dag_unique_task_name_1"
        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )

        # Drop target table
        drop_table_postgres(OUTPUT_TABLE_NAME, self.hook_target.get_conn())

        # Run task without specifying `output_table_name`
        out = self.create_and_run_task(
            load_file,
            (),
            {
                "path": str(self.cwd) + "/../data/homes.csv",
                "file_conn_id": "",
                "output_conn_id": "postgres_conn",
                "output_table_name": OUTPUT_TABLE_NAME,
            },
        )

        # Read table from db
        df = pd.read_sql(
            f"SELECT * FROM {OUTPUT_TABLE_NAME}", con=self.hook_target.get_conn()
        )

        assert df.iloc[0].to_dict() == {
            "Sell": 142.0,
            "List": 160.0,
            "Living": 28.0,
            "Rooms": 10.0,
            "Beds": 5.0,
            "Baths": 3.0,
            "Age": 60.0,
            "Acres": 0.28,
            "Taxes": 3167.0,
        }

    def test_aql_s3_file_to_postgres(self):
        OUTPUT_TABLE_NAME = "expected_table_from_s3_csv"

        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )

        # Drop target table
        drop_table_postgres(OUTPUT_TABLE_NAME, self.hook_target.get_conn())

        self.create_and_run_task(
            load_file,
            (),
            {
                "path": "s3://tmp9/homes.csv",
                "file_conn_id": "",
                "output_conn_id": "postgres_conn",
                "output_table_name": OUTPUT_TABLE_NAME,
            },
        )

        # Read table from db
        df = pd.read_sql(
            f"SELECT * FROM {OUTPUT_TABLE_NAME}", con=self.hook_target.get_conn()
        )

        assert df.iloc[0].to_dict()["Sell"] == 142.0

    def test_aql_local_file_to_snowflake(self):
        OUTPUT_TABLE_NAME = "expected_table_from_csv"

        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn",
            schema="SANDBOX_DANIEL",
            database="DWH_LEGACY",
            warehouse="TRANSFORMING_DEV",
        )

        # Drop target table
        hook.run(f"DROP TABLE IF EXISTS {OUTPUT_TABLE_NAME}")
        self.create_and_run_task(
            load_file,
            (),
            {
                "path": str(self.cwd) + "/../data/homes.csv",
                "file_conn_id": "",
                "output_conn_id": "snowflake_conn",
                "output_table_name": OUTPUT_TABLE_NAME,
                "database": "DWH_LEGACY",
                "schema": "SANDBOX_DANIEL",
                "warehouse": "TRANSFORMING_DEV",
            },
        )

        # Read table from db
        df = hook.get_pandas_df(f"SELECT * FROM {OUTPUT_TABLE_NAME}")

        assert df.iloc[0].to_dict() == {
            "Sell": 142.0,
            "List": 160.0,
            "Living": 28.0,
            "Rooms": 10.0,
            "Beds": 5.0,
            "Baths": 3.0,
            "Age": 60.0,
            "Acres": 0.28,
            "Taxes": 3167.0,
        }
