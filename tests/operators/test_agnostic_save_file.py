"""
Copyright Astronomer, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
"""
Unittest module to test Agnostic Load File function.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:
    AIRFLOW__ASTRO__CONN_AWS_DEFAULT=aws://KEY:SECRET@
    python3 -m unittest tests.operators.test_save_file.TestSaveFile.test_save_postgres_table_to_local

"""

import logging
import os
import pathlib
import unittest.mock

import boto3
import pandas as pd
from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

# Import Operator
from astro.sql.operators.agnostic_save_file import save_file
from astro.sql.table import Table

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


def drop_table_postgres(table_name, postgres_conn):
    cursor = postgres_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    postgres_conn.commit()
    cursor.close()
    postgres_conn.close()


class TestSaveFile(unittest.TestCase):
    """
    Test agnostic load file.
    """

    cwd = pathlib.Path(__file__).parent

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

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

    def create_and_run_tasks(self, decorator_funcs):
        # To Do: Merge create_and_run_tasks and create_and_run_task into a single fucntion.
        tasks = []
        with self.dag:
            for decorator_func in decorator_funcs:
                tasks.append(
                    decorator_func["func"](
                        *decorator_func["op_args"], **decorator_func["op_kwargs"]
                    )
                )

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        for task in tasks:
            task.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        return tasks

    def test_save_postgres_table_to_local_with_csv_format(self):

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
                "output_file_path": OUTPUT_FILE_PATH,
                "input_table": Table(
                    INPUT_TABLE_NAME, conn_id="postgres_conn", database="pagila"
                ),
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

    def test_save_postgres_table_to_local_with_parquet_format(self):

        OUTPUT_FILE_PATH = str(self.cwd) + "/../data/save_snow_file_out.parquet"
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
                "output_file_path": OUTPUT_FILE_PATH,
                "input_table": Table(
                    INPUT_TABLE_NAME, conn_id="postgres_conn", database="pagila"
                ),
                "output_conn_id": None,
                "overwrite": True,
                "output_file_format": "parquet",
            },
        )

        # Read table from db
        df = pd.read_sql(
            f"SELECT * FROM {INPUT_TABLE_NAME}", con=self.hook_target.get_conn()
        )

        # Read output Parquet
        df_file = pd.read_parquet(OUTPUT_FILE_PATH)

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
                "input_table": Table(
                    INPUT_TABLE_NAME, conn_id="postgres_conn", database="pagila"
                ),
                "output_file_path": OUTPUT_FILE_PATH,
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
                    "input_table": Table(
                        INPUT_TABLE_NAME, conn_id="postgres_conn", database="pagila"
                    ),
                    "output_file_path": OUTPUT_FILE_PATH,
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
                "input_table": Table(
                    INPUT_TABLE_NAME, conn_id="postgres_conn", database="pagila"
                ),
                "output_file_path": OUTPUT_FILE_PATH,
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
                    "input_table": Table(
                        INPUT_TABLE_NAME, conn_id="postgres_conn", database="pagila"
                    ),
                    "output_file_path": OUTPUT_FILE_PATH,
                    "output_conn_id": "aws_default",
                    "overwrite": False,
                },
            )

        # Assert task throws `FileExistsError` exception.
        self.assertRaises(FileExistsError, run_task)

        # Delete object from S3
        # s3.Object("tmp9", "test_table_to_s3_file_exists_overwrite_false.csv").delete()

    def test_unique_task_id_for_same_path(self):
        OUTPUT_FILE_PATH = str(self.cwd) + "/../data/output.csv"
        INPUT_TABLE_NAME = "rental"

        tasks_params = []
        for _ in range(4):
            tasks_params.append(
                {
                    "func": save_file,
                    "op_args": (),
                    "op_kwargs": {
                        "input_table": Table(
                            INPUT_TABLE_NAME, conn_id="postgres_conn", database="pagila"
                        ),
                        "output_file_path": OUTPUT_FILE_PATH,
                        "output_conn_id": None,
                        "overwrite": True,
                    },
                }
            )
        tasks_params[-1]["op_kwargs"]["task_id"] = "task_id"

        tasks = self.create_and_run_tasks(tasks_params)

        assert tasks[0].operator.task_id != tasks[1].operator.task_id
        assert tasks[1].operator.task_id == "save_file_output_csv__1"
        assert tasks[2].operator.task_id == "save_file_output_csv__2"
        assert tasks[3].operator.task_id == "task_id"

        os.remove(OUTPUT_FILE_PATH)

    @staticmethod
    def _s3fs_creds():
        # To-do: reuse this method from sql decorator
        """Structure s3fs credentials from Airflow connection.
        s3fs enables pandas to write to s3
        """
        # To-do: clean-up how S3 creds are passed to s3fs
        k, v = (
            os.environ["AIRFLOW__ASTRO__CONN_AWS_DEFAULT"]
            .replace("%2F", "/")
            .replace("aws://", "")
            .replace("@", "")
            .split(":")
        )

        return {"key": k, "secret": v}
