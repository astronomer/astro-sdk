"""
Unittest module to test Agnostic Load File function.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:
    AIRFLOW_CONN_POSTGRES_CONN=postgres://postgres:postgres@localhost:5432/pagila \
    AIRFLOW__SQL_DECORATOR__CONN_AWS_DEFAULT=aws://KEY:SECRET@ \
    python3 -m unittest tests.operators.test_save_file.TestSaveFile.test_save_postgres_table_to_local

"""

import logging
import os
import pathlib
import unittest.mock
from unittest import mock

import boto3
import pandas as pd
from airflow.models import DAG, Connection, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from pandas import DataFrame

# Import Operator
import astronomer_sql_decorator.sql as aql
from astronomer_sql_decorator.operators.agnostic_save_file import save_file

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
    AIRFLOW_CONN_POSTGRES_CONN="postgresql://postgres:postgres@localhost:5432/pagila",
)
class TestSaveFile(unittest.TestCase):
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

    def test_save_snowflake_table_to_local(self):

        OUTPUT_FILE_PATH = str(self.cwd) + "/../data/save_snow_file_out.csv"
        INPUT_TABLE_NAME = "rental"

        # Delete output file prior to run
        try:
            os.remove(OUTPUT_FILE_PATH)
        except OSError:
            pass

        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )

        self.create_and_run_task(
            save_file,
            (),
            {
                "table": INPUT_TABLE_NAME,
                "output_file_path": OUTPUT_FILE_PATH,
                "input_conn_id": "postgres_conn",
                "output_conn_id": None,
                "overwrite": True,
            },
        )

        # Read table from db
        df = pd.read_sql(
            f"SELECT * FROM {INPUT_TABLE_NAME}", con=self.hook_target.get_conn()
        )

        # Read output CSV
        df_file = pd.read_csv(OUTPUT_FILE_PATH)

        assert len(df_file) == 16044
        assert (df["rental_id"] == df_file["rental_id"]).all()

        # Delete output file after run
        os.remove(OUTPUT_FILE_PATH)

    def test_save_postgres_table_to_local(self):

        OUTPUT_FILE_PATH = str(self.cwd) + "/../data/save_file_out.csv"
        INPUT_TABLE_NAME = "rental"

        # Delete output file prior to run
        try:
            os.remove(OUTPUT_FILE_PATH)
        except OSError:
            pass

        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )

        self.create_and_run_task(
            save_file,
            (),
            {
                "table": INPUT_TABLE_NAME,
                "output_file_path": OUTPUT_FILE_PATH,
                "input_conn_id": "postgres_conn",
                "output_conn_id": None,
                "overwrite": True,
            },
        )

        # Read table from db
        df = pd.read_sql(
            f"SELECT * FROM {INPUT_TABLE_NAME}", con=self.hook_target.get_conn()
        )

        # Read output CSV
        df_file = pd.read_csv(OUTPUT_FILE_PATH)

        assert len(df_file) == 16044
        assert (df["rental_id"] == df_file["rental_id"]).all()

        # Delete output file after run
        os.remove(OUTPUT_FILE_PATH)

    def test_save_postgres_table_to_local_file_exists_overwrite_false(self):

        OUTPUT_FILE_PATH = str(self.cwd) + "/../data/save_file_out.csv"
        INPUT_TABLE_NAME = "rental"

        # Create output file prior to run
        open(OUTPUT_FILE_PATH, "a").close()

        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )

        def run_task():
            self.create_and_run_task(
                save_file,
                (),
                {
                    "table": INPUT_TABLE_NAME,
                    "output_file_path": OUTPUT_FILE_PATH,
                    "input_conn_id": "postgres_conn",
                    "output_conn_id": None,
                    "overwrite": False,
                },
            )

        # Assert task throws `FileExistsError` exception.
        self.assertRaises(FileExistsError, run_task)

        # Delete output file after run
        os.remove(OUTPUT_FILE_PATH)

    def test_save_postgres_table_to_s3(self):

        _creds = self._s3fs_creds()

        # Delete object from S3
        s3 = boto3.Session(_creds["key"], _creds["secret"]).resource("s3")
        s3.Object("tmp9", "test_save_postgres_table_to_s3.csv").delete()

        OUTPUT_FILE_PATH = "s3://tmp9/test_save_postgres_table_to_s3.csv"
        INPUT_TABLE_NAME = "rental"

        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )

        self.create_and_run_task(
            save_file,
            (),
            {
                "table": INPUT_TABLE_NAME,
                "output_file_path": OUTPUT_FILE_PATH,
                "input_conn_id": "postgres_conn",
                "output_conn_id": "aws_default",
                "overwrite": True,
            },
        )

        # Read table from db
        df = pd.read_sql(
            f"SELECT * FROM {INPUT_TABLE_NAME}", con=self.hook_target.get_conn()
        )

        # # Read output CSV
        df_file = pd.read_csv(OUTPUT_FILE_PATH, storage_options=self._s3fs_creds())

        assert len(df_file) == 16044
        assert (df["rental_id"] == df_file["rental_id"]).all()

        # Delete object from S3
        s3 = boto3.Session(_creds["key"], _creds["secret"]).resource("s3")
        s3.Object("tmp9", "test_save_postgres_table_to_s3.csv").delete()

    def test_save_postgres_table_to_s3_file_exists_overwrite_false(self):

        OUTPUT_FILE_PATH = "s3://tmp9/test_table_to_s3_file_exists_overwrite_false.csv"
        INPUT_TABLE_NAME = "rental"

        # Create object in S3
        _creds = self._s3fs_creds()
        s3 = boto3.Session(_creds["key"], _creds["secret"]).resource("s3")
        s3.Object("tmp9", OUTPUT_FILE_PATH).put(Body="123")

        def run_task():
            self.create_and_run_task(
                save_file,
                (),
                {
                    "table": INPUT_TABLE_NAME,
                    "output_file_path": OUTPUT_FILE_PATH,
                    "input_conn_id": "postgres_conn",
                    "output_conn_id": "aws_default",
                    "overwrite": False,
                },
            )

        # Assert task throws `FileExistsError` exception.
        self.assertRaises(FileExistsError, run_task)

        # Delete object from S3
        # s3.Object("tmp9", "test_table_to_s3_file_exists_overwrite_false.csv").delete()

    @staticmethod
    def _s3fs_creds():
        # To-do: reuse this method from sql decorator
        """Structure s3fs credentials from Airflow connection.
        s3fs enables pandas to write to s3
        """
        # To-do: clean-up how S3 creds are passed to s3fs
        k, v = (
            os.environ["AIRFLOW__SQL_DECORATOR__CONN_AWS_DEFAULT"]
            .replace("%2F", "/")
            .replace("aws://", "")
            .replace("@", "")
            .split(":")
        )

        return {"key": k, "secret": v}
