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

import os
from typing import Union
from urllib.parse import urlparse

import boto3
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, DagRun, TaskInstance
from google.cloud.storage import Client
from smart_open import open

from astro.sql.table import Table, TempTable, create_table_name
from astro.utils.load_dataframe import move_dataframe_to_sql
from astro.utils.schema_util import get_schema
from astro.utils.task_id_helper import get_task_id


class AgnosticLoadFile(BaseOperator):
    """Load S3/local table to postgres/snowflake database.

    :param path: File path.
    :type path: str
    :param output_table_name: Name of table to create.
    :type output_table_name: str
    :param file_conn_id: Airflow connection id of input file (optional)
    :type file_conn_id: str
    :param output_conn_id: Database connection id.
    :type output_conn_id: str
    """

    def __init__(
        self,
        path,
        output_table: Union[TempTable, Table],
        file_conn_id="",
        chunksize=None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.output_table: Union[TempTable, Table] = output_table
        self.path = path
        self.chunksize = chunksize
        self.file_conn_id = file_conn_id
        self.kwargs = kwargs
        self.output_table = output_table

    def execute(self, context):
        """Loads csv/parquet table from local/S3/GCS with Pandas.

        Infers SQL database type based on connection then loads table to db.
        """

        # Read file with Pandas load method based on `file_type` (S3 or local).
        df = self._load_dataframe(self.path)

        # Retrieve conn type
        conn = BaseHook.get_connection(self.output_table.conn_id)
        if type(self.output_table) == TempTable:
            self.output_table = self.output_table.to_table(
                create_table_name(context=context), get_schema()
            )
        else:
            self.output_table.schema = self.output_table.schema or get_schema()
        move_dataframe_to_sql(
            output_table_name=self.output_table.table_name,
            conn_id=self.output_table.conn_id,
            database=self.output_table.database,
            warehouse=self.output_table.warehouse,
            schema=self.output_table.schema,
            df=df,
            conn_type=conn.conn_type,
            user=conn.login,
        )
        self.log.info(f"returning table {self.output_table}")
        return self.output_table

    @staticmethod
    def validate_path(path):
        """Validate a URL or local file path"""
        try:
            result = urlparse(path)
            return all([result.scheme, result.netloc]) or os.path.isfile(path)
        except:
            return False

    def _load_dataframe(self, path):
        """Read file with Pandas.

        Select method based on `file_type` (S3 or local).
        """

        if not AgnosticLoadFile.validate_path(path):
            raise ValueError("Invalid path: {}".format(path))

        file_type = path.split(".")[-1]
        transport_params = {
            "s3": self._s3fs_creds,
            "gs": self._gcs_client,
            "": lambda: None,
        }[urlparse(path).scheme]()
        deserialiser = {
            "parquet": pd.read_parquet,
            "csv": pd.read_csv,
            "json": pd.read_json,
            "ndjson": pd.read_json,
        }
        deserialiser_params = {"ndjson": {"lines": True}}
        with open(path, transport_params=transport_params) as stream:
            return deserialiser[file_type](
                stream, **deserialiser_params.get(file_type, {})
            )

    def _s3fs_creds(self):
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
        session = boto3.Session(
            aws_access_key_id=k,
            aws_secret_access_key=v,
        )
        return dict(client=session.client("s3"))

    def _gcs_client(self):
        """
        get GCS credentials for storage.
        """
        client = Client()
        return dict(client=client)


def load_file(
    path,
    output_table=None,
    file_conn_id=None,
    task_id=None,
    **kwargs,
):
    """Convert AgnosticLoadFile into a function.

    Returns an XComArg object.

    :param path: File path.
    :type path: str
    :param output_table: Table to create
    :type output_table: Table
    :param file_conn_id: Airflow connection id of input file (optional)
    :type file_conn_id: str
    :param task_id: task id, optional.
    :type task_id: str
    """

    task_id = task_id if task_id is not None else get_task_id("load_file", path)

    return AgnosticLoadFile(
        task_id=task_id,
        path=path,
        output_table=output_table,
        file_conn_id=file_conn_id,
        **kwargs,
    ).output
