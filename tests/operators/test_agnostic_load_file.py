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
    AIRFLOW__ASTRO__CONN_AWS_DEFAULT=aws://AKIAZG42HVH6Z3B6ELRB:SgwfrcO2NdKpeKhUG77K%2F6B2HuRJJopbHPV84NbY@ \
    python3 -m unittest tests.operators.test_agnostic_load_file.TestAgnosticLoadFile.test_aql_local_file_to_postgres

"""
import copy
import logging
import os
import pathlib
import unittest.mock
import uuid
from unittest import mock

import pandas as pd
import pytest
from airflow import settings
from airflow.exceptions import BackfillUnfinished, DuplicateTaskIdFound
from airflow.models import DAG, Connection, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

from astro.sql.operators.agnostic_load_file import AgnosticLoadFile, load_file
from astro.sql.table import Table, TempTable
from astro.utils.cloud_storage_creds import parse_s3_env_var
from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
OUTPUT_TABLE_NAME = test_utils.get_table_name("load_file_test_table")
OUTPUT_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
CWD = pathlib.Path(__file__).parent


def drop_table_postgres(table_name, postgres_conn):
    cursor = postgres_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    postgres_conn.commit()
    cursor.close()
    postgres_conn.close()


class TestAgnosticLoadFile(unittest.TestCase):
    """
    Test agnostic load file.
    """

    bucket_name = "dag-authoring"
    blob_file_name = "homes.csv"
    storage_client = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.SNOWFLAKE_OUTPUT_TABLE_NAME = test_utils.get_table_name(
            "expected_table_from_csv"
        )

    @classmethod
    def tearDownClass(cls) -> None:
        test_utils.drop_table_snowflake(
            table_name=cls.SNOWFLAKE_OUTPUT_TABLE_NAME,  # type: ignore
            database=os.getenv("SNOWFLAKE_DATABASE"),  # type: ignore
            schema=os.getenv("SNOWFLAKE_SCHEMA"),  # type: ignore
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),  # type: ignore
            conn_id="snowflake_conn",
        )

    def setUp(self):
        super().setUp()
        self.clear_run()
        self.addCleanup(self.clear_run)
        self.dag = DAG(
            "test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE}
        )
        self.init_storage_client()

    def init_storage_client(self):
        self.storage_client = storage.Client()

    def upload_blob(self):
        self.delete_blob()

        with open(str(CWD) + "/../data/" + str(self.blob_file_name)) as f:
            content = f.read()

        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(self.blob_file_name)
        t = blob.upload_from_filename(str(CWD) + "/../data/" + str(self.blob_file_name))
        print("File uploaded.")

    def delete_blob(self):
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(self.blob_file_name)
        try:
            blob.delete()
            print(f"Blob {self.blob_file_name} deleted.")
        except NotFound as e:
            print(f"File {self.blob_file_name} not found.")

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
            data_interval=[DEFAULT_DATE, DEFAULT_DATE],
            state=State.RUNNING,
        )
        f.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        return f

    def create_and_run_tasks(self, decorator_funcs):
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

    def test_path_validation(self):
        test_table = [
            {"input": "S3://mybucket/puppy.jpg", "output": True},
            {
                "input": "https://my-bucket.s3.us-west-2.amazonaws.com/puppy.png",
                "output": True,
            },
            {"input": "/etc/someFile/randomFileName.csv", "output": False},
            {"input": "\x00", "output": False},
            {"input": "a" * 256, "output": False},
        ]

        for test in test_table:
            assert AgnosticLoadFile.validate_path(test["input"]) == test["output"]

    def test_poc_for_need_unique_task_id_for_same_path(self):
        OUTPUT_TABLE_NAME = "expected_table_from_csv_1"

        tasks_params = []
        for _ in range(5):
            tasks_params.append(
                {
                    "func": load_file,
                    "op_args": (),
                    "op_kwargs": {
                        "path": str(CWD) + "/../data/homes.csv",
                        "file_conn_id": "",
                        "task_id": "task_id",
                        "output_table": Table(
                            OUTPUT_TABLE_NAME,
                            database="pagila",
                            conn_id="postgres_conn",
                        ),
                    },
                }
            )
        tasks_params[-1]["op_kwargs"]["task_id"] = "task_id"
        try:
            self.create_and_run_tasks(tasks_params)
            assert False
        except DuplicateTaskIdFound:
            assert True

    def test_unique_task_id_for_same_path(self):
        OUTPUT_TABLE_NAME = "expected_table_from_csv_1"

        tasks_params = []
        for _ in range(4):
            tasks_params.append(
                {
                    "func": load_file,
                    "op_args": (),
                    "op_kwargs": {
                        "path": str(CWD) + "/../data/homes.csv",
                        "file_conn_id": "",
                        "output_table": Table(
                            OUTPUT_TABLE_NAME,
                            database="pagila",
                            conn_id="postgres_conn",
                        ),
                    },
                }
            )
        tasks_params[-1]["op_kwargs"]["task_id"] = "task_id"

        tasks = self.create_and_run_tasks(tasks_params)

        assert tasks[0].operator.task_id != tasks[1].operator.task_id
        assert tasks[1].operator.task_id == "load_file_homes_csv__1"
        assert tasks[2].operator.task_id == "load_file_homes_csv__2"
        assert tasks[3].operator.task_id == "task_id"

    def test_aql_local_file_to_postgres_no_table_name(self):
        OUTPUT_TABLE_NAME = "expected_table_from_csv"

        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )

        # Drop target table
        drop_table_postgres(OUTPUT_TABLE_NAME, self.hook_target.get_conn())

        task = self.create_and_run_task(
            load_file,
            (),
            {
                "path": str(CWD) + "/../data/homes.csv",
                "file_conn_id": "",
                "output_table": TempTable(database="pagila", conn_id="postgres_conn"),
            },
        )

        # Read table from db
        df = pd.read_sql(
            f"SELECT * FROM {test_utils.DEFAULT_SCHEMA}.test_dag_load_file_homes_csv_1",
            con=self.hook_target.get_conn(),
        )

        assert df.iloc[0].to_dict() == {
            "sell": 142.0,
            "list": 160.0,
            "living": 28.0,
            "rooms": 10.0,
            "beds": 5.0,
            "baths": 3.0,
            "age": 60.0,
            "acres": 0.28,
            "taxes": 3167.0,
        }

    def test_aql_local_file_to_bigquery_no_table_name(self):
        OUTPUT_TABLE_NAME = "expected_table_from_csv"
        data_path = str(CWD) + "/../data/homes.csv"
        self.hook_target = BigQueryHook(gcp_conn_id="bigquery", use_legacy_sql=False)

        task = self.create_and_run_task(
            load_file,
            (),
            {
                "path": data_path,
                "file_conn_id": "",
                "output_table": Table(
                    OUTPUT_TABLE_NAME,
                    conn_id="bigquery",
                    schema=test_utils.DEFAULT_SCHEMA,
                ),
            },
        )

        client = bigquery.Client()
        query_job = client.query(
            f"SELECT * FROM astronomer-dag-authoring.{test_utils.DEFAULT_SCHEMA}.{OUTPUT_TABLE_NAME}"
        )
        bigquery_df = query_job.to_dataframe()

        data_df = pd.read_csv(data_path)

        assert bigquery_df.shape == data_df.shape

    def test_aql_overwrite_existing_table(self):
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
                "path": str(CWD) + "/../data/homes.csv",
                "file_conn_id": "",
                "output_table": Table(
                    table_name=OUTPUT_TABLE_NAME,
                    database="pagila",
                    conn_id="postgres_conn",
                ),
            },
        )

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

        self.create_and_run_task(
            load_file,
            (),
            {
                "path": str(CWD) + "/../data/homes.csv",
                "file_conn_id": "",
                "output_table": Table(
                    table_name=OUTPUT_TABLE_NAME,
                    database="pagila",
                    conn_id="postgres_conn",
                ),
            },
        )

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
                "output_table": Table(
                    table_name=OUTPUT_TABLE_NAME,
                    database="pagila",
                    conn_id="postgres_conn",
                ),
            },
        )

        # Read table from db
        df = pd.read_sql(
            f"SELECT * FROM {test_utils.DEFAULT_SCHEMA}.{OUTPUT_TABLE_NAME}",
            con=self.hook_target.get_conn(),
        )

        assert df.iloc[0].to_dict()["Sell"] == 142.0

    def test_aql_s3_file_to_postgres_no_table_name(self):
        OUTPUT_TABLE_NAME = "test_dag_load_file_homes_csv_2"

        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )

        # Drop target table
        drop_table_postgres(
            f"{test_utils.DEFAULT_SCHEMA}.{OUTPUT_TABLE_NAME}",
            self.hook_target.get_conn(),
        )

        self.create_and_run_task(
            load_file,
            (),
            {
                "path": "s3://tmp9/homes.csv",
                "file_conn_id": "",
                "output_table": Table(
                    table_name=OUTPUT_TABLE_NAME,
                    database="pagila",
                    conn_id="postgres_conn",
                ),
            },
        )

        # Read table from db
        df = pd.read_sql(
            f"SELECT * FROM {test_utils.DEFAULT_SCHEMA}.{OUTPUT_TABLE_NAME}",
            con=self.hook_target.get_conn(),
        )

        assert df.iloc[0].to_dict()["Sell"] == 142.0

    def test_aql_s3_file_to_postgres_specify_schema(self):
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
                "output_table": Table(
                    OUTPUT_TABLE_NAME,
                    database="pagila",
                    conn_id="postgres_conn",
                    schema="public",
                ),
            },
        )

        # Read table from db
        df = pd.read_sql(
            f"SELECT * FROM {OUTPUT_TABLE_NAME}", con=self.hook_target.get_conn()
        )

        assert df.iloc[0].to_dict()["Sell"] == 142.0

    def test_aql_gcs_file_to_postgres(self):
        # To Do: add service account creds
        self.upload_blob()
        OUTPUT_TABLE_NAME = "expected_table_from_gcs_csv"

        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )

        # Drop target table
        drop_table_postgres(OUTPUT_TABLE_NAME, self.hook_target.get_conn())

        self.create_and_run_task(
            load_file,
            (),
            {
                "path": "gs://dag-authoring/homes.csv",
                "file_conn_id": "",
                "output_table": Table(
                    OUTPUT_TABLE_NAME,
                    database="pagila",
                    conn_id="postgres_conn",
                    schema="public",
                ),
            },
        )

        # Read table from db
        df = pd.read_sql(
            f"SELECT * FROM {OUTPUT_TABLE_NAME}", con=self.hook_target.get_conn()
        )
        assert df.iloc[0].to_dict()["sell"] == 142.0


@mock.patch.dict(
    os.environ,
    {
        "AIRFLOW__ASTRO__CONN_AWS_DEFAULT": "abcd:%40%23%24%25%40%24%23ASDH%40Ksd23%25SD546@"
    },
)
def test_aws_decode():
    from astro.utils.cloud_storage_creds import parse_s3_env_var

    k, v = parse_s3_env_var()
    assert v == "@#$%@$#ASDH@Ksd23%SD546"


@pytest.fixture
def sql_server(request):
    sql_name = request.param
    hook_parameters = test_utils.SQL_SERVER_HOOK_PARAMETERS.get(sql_name)
    hook_class = test_utils.SQL_SERVER_HOOK_CLASS.get(sql_name)
    if hook_parameters is None or hook_class is None:
        raise ValueError(f"Unsupported SQL server {sql_name}")
    hook = hook_class(**hook_parameters)
    schema = hook_parameters.get("schema", test_utils.DEFAULT_SCHEMA)
    if not isinstance(hook, BigQueryHook):
        hook.run(f"DROP TABLE IF EXISTS {schema}.{OUTPUT_TABLE_NAME}")
    yield (sql_name, hook)
    if not isinstance(hook, BigQueryHook):
        hook.run(f"DROP TABLE IF EXISTS {schema}.{OUTPUT_TABLE_NAME}")


@pytest.mark.parametrize("sql_server", ["sqlite"], indirect=True)
def test_load_file_templated_filename(sample_dag, sql_server):
    database_name, sql_hook = sql_server

    sql_server_params = copy.deepcopy(
        test_utils.SQL_SERVER_HOOK_PARAMETERS[database_name]
    )
    conn_id_value = sql_server_params.pop(
        test_utils.SQL_SERVER_CONNECTION_KEY[database_name]
    )
    sql_server_params["conn_id"] = conn_id_value

    task_params = {
        "path": str(CWD) + "/../data/{{ var.value.foo }}/example.csv",
        "file_conn_id": "",
        "output_table": Table(
            table_name=OUTPUT_TABLE_NAME + "_{{ var.value.foo }}", **sql_server_params
        ),
    }

    test_utils.create_and_run_task(sample_dag, load_file, (), task_params)
    df = sql_hook.get_pandas_df(
        f"SELECT * FROM {OUTPUT_TABLE_NAME}_templated_file_name"
    )
    assert len(df) == 3


@pytest.fixture
def remote_file(request):
    provider = request.param
    if provider == "google":
        conn_id = "test_google"
        conn_type = "google_cloud_platform"
        extra = {
            "extra__google_cloud_platform__key_path": os.getenv(
                "GOOGLE_APPLICATION_CREDENTIALS"
            )
        }
    elif provider == "amazon":
        key, secret = parse_s3_env_var()
        conn_id = "test_amazon"
        conn_type = "S3"
        extra = {"aws_access_key_id": key, "aws_secret_access_key": secret}
    else:
        raise ValueError(f"File location {request.param} not supported")

    new_connection = Connection(conn_id=conn_id, conn_type=conn_type, extra=extra)
    session = settings.Session()
    if not (
        session.query(Connection)
        .filter(Connection.conn_id == new_connection.conn_id)
        .first()
    ):
        session.add(new_connection)
        session.commit()

    object_prefix = f"test/{uuid.uuid4()}.csv"
    filename = pathlib.Path(CWD.parent, "data/sample.csv")
    if provider == "google":
        bucket_name = os.getenv("GOOGLE_BUCKET", "dag-authoring")
        hook = GCSHook(gcp_conn_id=conn_id)
        hook.upload(bucket_name, object_prefix, filename)
        object_path = f"gs://{bucket_name}/{object_prefix}"
    else:
        bucket_name = os.getenv("AWS_BUCKET", "tmp9")
        hook = S3Hook(aws_conn_id=conn_id)
        hook.load_file(filename, object_prefix, bucket_name)
        object_path = f"s3://{bucket_name}/{object_prefix}"

    yield conn_id, object_path

    if provider == "google":
        if hook.exists(bucket_name, object_prefix):
            hook.delete(bucket_name, object_prefix)
    else:
        if hook.check_for_prefix(object_prefix, delimiter="/", bucket_name=bucket_name):
            hook.delete_objects(bucket_name, object_prefix)

    session.delete(new_connection)
    session.flush()


@pytest.mark.integration
@pytest.mark.parametrize("sql_server", ["sqlite"], indirect=True)
@pytest.mark.parametrize("remote_file", ["google", "amazon"], indirect=True)
def test_load_file_using_file_connection(sample_dag, remote_file, sql_server):
    database_name, sql_hook = sql_server
    file_conn_id, file_uri = remote_file

    sql_server_params = test_utils.get_default_parameters(database_name)

    task_params = {
        "path": file_uri,
        "file_conn_id": file_conn_id,
        "output_table": Table(table_name=OUTPUT_TABLE_NAME, **sql_server_params),
    }

    test_utils.create_and_run_task(sample_dag, load_file, (), task_params)
    df = sql_hook.get_pandas_df(f"SELECT * FROM {OUTPUT_TABLE_NAME}")
    assert len(df) == 3


def test_load_file_using_file_connection_fails_inexistent_conn(caplog, sample_dag):
    database_name = "postgres"
    file_conn_id = "fake_conn"
    file_uri = "s3://fake-bucket/fake-object"

    sql_server_params = test_utils.get_default_parameters(database_name)

    task_params = {
        "path": file_uri,
        "file_conn_id": file_conn_id,
        "output_table": Table(table_name=OUTPUT_TABLE_NAME, **sql_server_params),
    }
    with pytest.raises(BackfillUnfinished) as exec_info:
        test_utils.create_and_run_task(sample_dag, load_file, (), task_params)
    expected_error = "Failed to execute task: The conn_id `fake_conn` isn't defined."
    assert expected_error in caplog.text


def create_task_parameters(database_name, file_type):
    sql_server_params = test_utils.get_default_parameters(database_name)

    task_params = {
        "path": str(pathlib.Path(CWD.parent, f"data/sample.{file_type}")),
        "file_conn_id": "",
        "output_table": Table(table_name=OUTPUT_TABLE_NAME, **sql_server_params),
    }
    return task_params


@pytest.mark.parametrize(
    "sql_server", ["snowflake", "postgres", "bigquery", "sqlite"], indirect=True
)
@pytest.mark.parametrize("file_type", ["parquet", "ndjson", "json", "csv"])
def test_load_file(sample_dag, sql_server, file_type):
    database_name, sql_hook = sql_server

    task_params = create_task_parameters(database_name, file_type)
    test_utils.create_and_run_task(sample_dag, load_file, (), task_params)

    qualified_name = task_params["output_table"].qualified_name()
    df = sql_hook.get_pandas_df(f"SELECT * FROM {qualified_name}")

    assert len(df) == 3
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    assert df.rename(columns=str.lower).equals(expected)


@pytest.mark.parametrize(
    "sql_server", ["bigquery", "postgres", "snowflake"], indirect=True
)
def test_load_file_chunks(sample_dag, sql_server):
    file_type = "csv"
    database_name, sql_hook = sql_server

    chunk_function = {
        "bigquery": "pandas.DataFrame.to_gbq",
        "postgres": "pandas.DataFrame.to_sql",
        "snowflake": "snowflake.connector.pandas_tools.write_pandas",
    }[database_name]

    chunk_size_argument = {
        "bigquery": "chunksize",
        "postgres": "chunksize",
        "snowflake": "chunk_size",
    }[database_name]

    with mock.patch(chunk_function) as mock_chunk_function:
        task_params = create_task_parameters(database_name, file_type)
        test_utils.create_and_run_task(sample_dag, load_file, (), task_params)

    assert mock_chunk_function.call_args_list[0].kwargs[chunk_size_argument] == 1000000
