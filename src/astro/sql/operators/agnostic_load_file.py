import io
import json
import glob
from typing import Union
from urllib.parse import urlparse, urlunparse

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

from astro.constants import DEFAULT_CHUNK_SIZE
from astro.sql.table import Table, TempTable, create_table_name
from astro.utils import get_hook
from astro.utils.dependencies import gcs, s3
from astro.utils.file import get_filetype
from astro.utils.load import load_dataframe_into_sql_table, load_file_into_dataframe
from astro.utils.path import get_paths, get_transport_params, validate_path
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
        normalize_config=None,
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
        self.normalize_config = normalize_config or {}

    def execute(self, context):
        """
        Load an existing dataset from a supported file into a SQL table.
        """
        if self.file_conn_id:
            BaseHook.get_connection(self.file_conn_id)

        hook = get_hook(
            conn_id=self.output_table.conn_id,
            database=self.output_table.database,
            schema=self.output_table.schema,
            warehouse=self.output_table.warehouse,
        )
        paths = get_paths(self.path, self.file_conn_id)
        transport_params = get_transport_params(paths[0], self.file_conn_id)
        return self.load_using_pandas(context, paths, hook, transport_params)

    def load_using_pandas(self, context, paths, hook, transport_params):
        """Loads csv/parquet table from local/S3/GCS with Pandas.

        Infers SQL database type based on connection then loads table to db.
        """
        self._configure_output_table(context)
        self.log.info(f"Loading {self.path} into {self.output_table}...")
        if_exists = self.if_exists
        for path in paths:
            pandas_dataframe = self._load_file_into_dataframe(path, transport_params)
            load_dataframe_into_sql_table(
                pandas_dataframe,
                self.output_table,
                hook,
                self.chunksize,
                if_exists=if_exists,
            )
            if_exists = "append"

        self.log.info(f"Completed loading the data into {self.output_table}.")

        return self.output_table

    def _configure_output_table(self, context):
        # TODO: Move this function to the SQLDecorator, so it can be reused across operators
        if isinstance(self.output_table, TempTable):
            self.output_table = self.output_table.to_table(
                create_table_name(context=context)
            )
        if not self.output_table.table_name:
            self.output_table.table_name = create_table_name(context=context)

    def _load_file_into_dataframe(self, filepath, transport_params):
        """Read file with Pandas.

        Select method based on `file_type` (S3 or local).
        """

        validate_path(filepath)
        filetype = get_filetype(filepath)
        return load_file_into_dataframe(filepath, filetype, transport_params)

    def check_ndjson_config_delimiter(self, conn_type, normalize_config):
        if conn_type in ["bigquery", "snowflake"]:
            meta_prefix = normalize_config.get("meta_prefix")
            if meta_prefix and meta_prefix == ".":
                normalize_config["meta_prefix"] == "__"

            meta_prefix = normalize_config.get("record_prefix")
            if meta_prefix and meta_prefix == ".":
                normalize_config["record_prefix"] == "__"

        return normalize_config

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
    normalize_config=None,
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
    :param normalize_config: config dict for pandas json_normalize method https://pandas.pydata.org/docs/reference/api/pandas.json_normalize.html
    :type normalize_config: dict
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
        normalize_config=normalize_config,
        **kwargs,
    ).output
