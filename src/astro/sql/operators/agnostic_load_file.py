import json
import glob
import os
from typing import Union
from urllib.parse import urlparse, urlunparse

import pandas as pd
import smart_open
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

from astro.constants import DEFAULT_CHUNK_SIZE
from astro.settings import SCHEMA
from astro.sql.table import Table, TempTable, create_table_name
from astro.utils.cloud_storage_creds import gcs_client, s3fs_creds
from astro.utils.dependencies import gcs, s3
from astro.utils.load_dataframe import move_dataframe_to_sql
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
        if_exists="replace",
        flatten_ndjson={},
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.output_table: Union[TempTable, Table] = output_table
        self.path = path
        self.chunksize = chunksize
        self.file_conn_id = file_conn_id
        self.kwargs = kwargs
        self.output_table = output_table
        self.if_exists = if_exists
        self.flatten_ndjson = flatten_ndjson

    def execute(self, context):
        """Loads csv/parquet table from local/S3/GCS with Pandas.

        Infers SQL database type based on connection then loads table to db.
        """

        if self.file_conn_id:
            BaseHook.get_connection(self.file_conn_id)

        # Retrieve conn type
        conn = BaseHook.get_connection(self.output_table.conn_id)
        if type(self.output_table) == TempTable:
            self.output_table = self.output_table.to_table(
                create_table_name(context=context), SCHEMA
            )
        else:
            self.output_table.schema = self.output_table.schema or SCHEMA
        if not self.output_table.table_name:
            self.output_table.table_name = create_table_name(context=context)

        if_exists = self.if_exists
        paths = self.get_paths(self.path, self.file_conn_id)
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
                if_exists=if_exists,
            )
            if_exists = "append"
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
            if len(self.flatten_ndjson) > 0 and file_type == "ndjson":
                df = None
                while True:
                    rows = stream.readlines(DEFAULT_CHUNK_SIZE)
                    if len(rows) == 0:
                        break
                    if df is None:
                        df = pd.DataFrame(self.process_ndjson(rows))
                    else:
                        df = df.append(pd.DataFrame(self.process_ndjson(rows)))
                return df
            else:
                return deserialiser[file_type](
                    stream, **deserialiser_params.get(file_type, {})
                )

    def process_ndjson(self, rows):
        results = {}
        for row in rows:

            row = json.loads(row)
            if len(row) == 0 or len(row.keys()) != 8:
                continue

            for k in row:
                if not results.get(k):
                    results[k] = []

                if k in self.flatten_ndjson:
                    v = self.flatten_ndjson[k]

                    if isinstance(v, str):
                        results[k].append(self.ndjson_get_values(v, row))
                    elif self.isLambda(v):
                        results[k].append(v(row))
                else:
                    results[k].append(row[k])
        return results

    def ndjson_get_values(self, path, data):
        steps = path.split(".")
        for step in steps:
            data = data.get(step, None)
            if data is None:
                break
        return data

    def isLambda(self, val):
        return callable(val) and val.__name__ == "<lambda>"

    def get_paths(self, path, file_conn_id):
        url = urlparse(path)
        file_location = url.scheme
        return {
            "s3": self.get_paths_from_s3_or_gcs,
            "gs": self.get_paths_from_s3_or_gcs,
            "http": lambda url, file_conn_id: [urlunparse(list(url))],
            "https": lambda url, file_conn_id: [urlunparse(list(url))],
            "": self.get_paths_from_filesystem,
        }[file_location](url, file_conn_id)

    def get_prefix_list(self, scheme, conn_id, bucket_name, prefix):
        if scheme == "s3":
            hook = s3.S3Hook(aws_conn_id=conn_id) if conn_id else s3.S3Hook()
            return hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        elif scheme == "gs":
            hook = gcs.GCSHook(gcp_conn_id=conn_id) if conn_id else gcs.GCSHook()
            return hook.list(bucket_name=bucket_name, prefix=prefix)

    def get_paths_from_s3_or_gcs(self, url, file_conn_id=None):
        bucket = url.netloc
        prefix = url.path
        list_keys = self.get_prefix_list(
            url.scheme, file_conn_id, bucket_name=bucket, prefix=prefix[1:]
        )
        return [
            urlunparse((url.scheme, url.netloc, keys, "", "", "")) for keys in list_keys
        ]

    def get_paths_from_filesystem(self, url, file_conn_id):
        return glob.glob(url.path)


def load_file(
    path,
    output_table=None,
    file_conn_id=None,
    task_id=None,
    if_exists="replace",
    flatten_ndjson={},
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
        if_exists=if_exists,
        flatten_ndjson=flatten_ndjson,
        **kwargs,
    ).output
