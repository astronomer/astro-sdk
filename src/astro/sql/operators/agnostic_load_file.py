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

import glob
import os
from typing import Union
from urllib.parse import urlparse, urlunparse

import pandas as pd
import smart_open
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

from astro.constants import DEFAULT_CHUNK_SIZE
from astro.sql.table import Table, TempTable, create_table_name
from astro.utils.cloud_storage_creds import gcs_client, s3fs_creds
from astro.utils.dependencies import gcs, s3
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

    template_fields = (
        "output_table",
        "file_conn_id",
        "path",
    )

    def __init__(
        self,
        path,
        output_table: Union[TempTable, Table],
        file_conn_id="",
        chunksize=DEFAULT_CHUNK_SIZE,
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
        # Retrieve conn type
        conn = BaseHook.get_connection(self.output_table.conn_id)
        if type(self.output_table) == TempTable:
            self.output_table = self.output_table.to_table(
                create_table_name(context=context), get_schema()
            )
        else:
            self.output_table.schema = self.output_table.schema or get_schema()
        if not self.output_table.table_name:
            self.output_table.table_name = create_table_name(context=context)

        paths = self.get_paths(self.path, self.file_conn_id)
        if_table_exist = "replace"
        for path in paths:
            # Read file with Pandas load method based on `file_type` (S3 or local).
            df = self._load_dataframe(path)

            move_dataframe_to_sql(
                output_table_name=self.output_table.table_name,
                conn_id=self.output_table.conn_id,
                database=self.output_table.database,
                warehouse=self.output_table.warehouse,
                schema=self.output_table.schema,
                df=df,
                conn_type=conn.conn_type,
                user=conn.login,
                chunksize=self.chunksize,
                if_exists=if_table_exist,
            )
            if_table_exist = "append"

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
            raise ValueError(f"Invalid path: {path}")

        file_type = path.split(".")[-1]
        file_scheme = urlparse(path).scheme

        default_params_getter = lambda conn_id: None
        transport_params = {"s3": s3fs_creds, "gs": gcs_client}.get(
            file_scheme, default_params_getter
        )(conn_id=self.file_conn_id)

        deserialiser = {
            "parquet": pd.read_parquet,
            "csv": pd.read_csv,
            "json": pd.read_json,
            "ndjson": pd.read_json,
        }
        mode = {"parquet": "rb"}
        deserialiser_params = {"ndjson": {"lines": True}}
        with smart_open.open(
            path, mode=mode.get(file_type, "r"), transport_params=transport_params
        ) as stream:
            return deserialiser[file_type](
                stream, **deserialiser_params.get(file_type, {})
            )

    def get_paths(self, path, file_conn_id):
        url = urlparse(path)
        file_location = url.scheme
        return {
            "s3": self.get_paths_from_s3,
            "gs": self.get_paths_from_gcs,
            "": self.get_paths_from_filesystem,
        }[file_location](url, file_conn_id)

    def get_paths_from_s3(self, url, file_conn_id=None):
        bucket = url.netloc
        prefix = url.path
        hook = s3.S3Hook(aws_conn_id=file_conn_id) if file_conn_id else s3.S3Hook()
        return [
            urlunparse((url.scheme, url.netloc, keys, "", "", ""))
            for keys in hook.list_keys(bucket_name=bucket, prefix=prefix[1:])
        ]

    def get_paths_from_gcs(self, url, file_conn_id=None):
        bucket = url.netloc
        prefix = url.path
        hook = gcs.GCSHook(gcp_conn_id=file_conn_id) if file_conn_id else gcs.GCSHook()
        return [
            urlunparse((url.scheme, url.netloc, keys, "", "", ""))
            for keys in hook.list(bucket_name=bucket, prefix=prefix[1:])
        ]

    def get_paths_from_filesystem(self, url, file_conn_id):
        return glob.glob(url.path)


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

    # Note - using path for task id is causing issues as it's a pattern and
    # contain chars like - ?, * etc. Which are not acceptable as task id.
    task_id = task_id if task_id is not None else get_task_id("load_file", "")

    return AgnosticLoadFile(
        task_id=task_id,
        path=path,
        output_table=output_table,
        file_conn_id=file_conn_id,
        **kwargs,
    ).output
