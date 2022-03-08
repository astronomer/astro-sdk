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
    AWS_ACCESS_KEY_ID=KEY \
    AWS_SECRET_ACCESS_KEY=SECRET \
    python3 -m unittest tests.operators.test_save_file.TestSaveFile.test_save_postgres_table_to_local

"""
import copy
import logging
import os
import pathlib
import tempfile
import unittest.mock
from pathlib import Path

import boto3
import pandas as pd
import pytest
from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

import astro.dataframe as adf
import astro.sql as aql

# Import Operator
from astro.sql.operators.agnostic_save_file import save_file
from astro.sql.table import Table
from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
INPUT_TABLE_NAME = test_utils.get_table_name("save_file_test_table")


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
        test_utils.run_dag(self.dag)
        return tasks

    def test_save_dataframe_to_local(self):
        @adf
        def make_df():
            d = {"col1": [1, 2], "col2": [3, 4]}
            return pd.DataFrame(data=d)

        with self.dag:
            df = make_df()
            aql.save_file(
                input=df, output_file_path="/tmp/saved_df.csv", overwrite=True
            )

        test_utils.run_dag(self.dag)
        df = pd.read_csv("/tmp/saved_df.csv")
        assert df.equals(pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]}))

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
                    "input": Table(
                        INPUT_TABLE_NAME,
                        conn_id="postgres_conn",
                        database="pagila",
                        schema="public",
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

        _creds = TestSaveFile._s3fs_creds()

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
                "input": Table(
                    INPUT_TABLE_NAME,
                    conn_id="postgres_conn",
                    database="pagila",
                    schema="public",
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
        _creds = TestSaveFile._s3fs_creds()
        s3 = boto3.Session(_creds["key"], _creds["secret"]).resource("s3")
        s3.Object("tmp9", OUTPUT_FILE_PATH).put(Body="123")

        def run_task():
            self.create_and_run_task(
                save_file,
                (),
                {
                    "input": Table(
                        INPUT_TABLE_NAME,
                        conn_id="postgres_conn",
                        database="pagila",
                        schema="public",
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
                        "input": Table(
                            INPUT_TABLE_NAME,
                            conn_id="postgres_conn",
                            database="pagila",
                            schema="public",
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

    def test_save_bigquery_table_to_local_file_exists_overwrite_false(self):

        INPUT_TABLE_NAME = "save_file_bigquery_test"
        INPUT_FILE_PATH = str(self.cwd) + "/../data/homes.csv"

        OUTPUT_FILE_PATH = str(self.cwd) + "/../data/save_file_bigquery_test.csv"

        aql.load_file(
            path=INPUT_FILE_PATH,
            output_table=Table(
                INPUT_TABLE_NAME,
                conn_id="bigquery",
                schema=test_utils.DEFAULT_SCHEMA,
            ),
        ).operator.execute({"run_id": "foo"})

        self.create_and_run_task(
            save_file,
            (),
            {
                "input": Table(
                    INPUT_TABLE_NAME,
                    conn_id="bigquery",
                    schema=test_utils.DEFAULT_SCHEMA,
                ),
                "output_file_path": OUTPUT_FILE_PATH,
                "output_conn_id": None,
                "overwrite": True,
            },
        )

        input_df = pd.read_csv(INPUT_FILE_PATH)
        output_df = pd.read_csv(OUTPUT_FILE_PATH)

        assert input_df.shape == output_df.shape

        # Delete output file after run
        os.remove(OUTPUT_FILE_PATH)

    def test_save_sqlite_table_to_local_file_exists_overwrite_false(self):

        INPUT_TABLE_NAME = "save_file_sqlite_test"
        INPUT_FILE_PATH = str(self.cwd) + "/../data/homes.csv"

        OUTPUT_FILE_PATH = str(self.cwd) + f"/../data/{INPUT_TABLE_NAME}.csv"

        aql.load_file(
            path=INPUT_FILE_PATH,
            output_table=Table(
                INPUT_TABLE_NAME,
                conn_id="sqlite_conn",
            ),
        ).operator.execute({"run_id": "foo"})

        self.create_and_run_task(
            save_file,
            (),
            {
                "input": Table(INPUT_TABLE_NAME, conn_id="sqlite_conn"),
                "output_file_path": OUTPUT_FILE_PATH,
                "output_conn_id": None,
                "overwrite": True,
            },
        )

        input_df = pd.read_csv(INPUT_FILE_PATH)
        output_df = pd.read_csv(OUTPUT_FILE_PATH)

        assert input_df.shape == output_df.shape

        # Delete output file after run
        os.remove(OUTPUT_FILE_PATH)

    @staticmethod
    def _s3fs_creds():
        # To-do: reuse this method from sql decorator
        """Structure s3fs credentials from Airflow connection.
        s3fs enables pandas to write to s3
        """
        # To-do: clean-up how S3 creds are passed to s3fs

        return {"key": os.environ["AWS_ACCESS_KEY_ID"], "secret":  os.environ["AWS_SECRET_ACCESS_KEY"]}


@pytest.fixture
def sql_server(request):
    sql_name = request.param
    hook_parameters = test_utils.SQL_SERVER_HOOK_PARAMETERS.get(sql_name)
    hook_class = test_utils.SQL_SERVER_HOOK_CLASS.get(sql_name)
    if hook_parameters is None or hook_class is None:
        raise ValueError(f"Unsupported SQL server {sql_name}")
    hook = hook_class(**hook_parameters)
    schema = hook_parameters.get("schema")
    if schema:
        table = ".".join([schema, INPUT_TABLE_NAME])
    else:
        table = INPUT_TABLE_NAME

    hook.run(f"DROP TABLE IF EXISTS {table}")
    hook.run(f"CREATE TABLE {table} (ID int, Name varchar(255));")
    hook.run(f"INSERT INTO {table} (ID, Name) VALUES (1, 'Someone');")
    hook.run(f"INSERT INTO {table} (ID, Name) VALUES (2, 'Alguém');")
    yield (sql_name, hook)
    hook.run(f"DROP TABLE IF EXISTS {table}")


def load_to_dataframe(filepath, file_type):
    read = {
        "parquet": pd.read_parquet,
        "csv": pd.read_csv,
        "json": pd.read_json,
        "ndjson": pd.read_json,
    }
    read_params = {"ndjson": {"lines": True}}
    mode = {"parquet": "rb"}
    with open(filepath, mode.get(file_type, "r")) as fp:
        return read[file_type](fp, **read_params.get(file_type, {}))


@pytest.mark.parametrize("sql_server", ["snowflake", "postgres"], indirect=True)
@pytest.mark.parametrize("file_type", ["ndjson", "json", "csv", "parquet"])
def test_save_file(sample_dag, sql_server, file_type):
    sql_name, sql_hook = sql_server

    # While hooks expect specific attributes for connection (e.g. `snowflake_conn_id`)
    # the load_file operator expects a generic attribute name (`conn_id`)
    sql_server_params = copy.deepcopy(test_utils.SQL_SERVER_HOOK_PARAMETERS[sql_name])
    conn_id_value = sql_server_params.pop(
        test_utils.SQL_SERVER_CONNECTION_KEY[sql_name]
    )
    sql_server_params["conn_id"] = conn_id_value
    if type(sql_hook) == PostgresHook:
        sql_server_params["schema"] = "public"

    with tempfile.TemporaryDirectory() as tmp_dir:
        filepath = Path(tmp_dir, f"sample.{file_type}")

        task_params = {
            "input": Table(table_name=INPUT_TABLE_NAME, **sql_server_params),
            "output_file_path": str(filepath),
            "output_file_format": file_type,
            "output_conn_id": None,
            "overwrite": False,
        }
        test_utils.create_and_run_task(sample_dag, save_file, (), task_params)
        df = load_to_dataframe(filepath, file_type)
        assert len(df) == 2
        expected = pd.DataFrame(
            [{"id": 1, "name": "Someone"}, {"id": 2, "name": "Alguém"}]
        )
        assert df.rename(columns=str.lower).equals(expected)
