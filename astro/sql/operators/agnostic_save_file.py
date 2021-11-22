import os
from typing import Optional

import boto3
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, DagRun, TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from astro.sql.operators.temp_hooks import TempPostgresHook, TempSnowflakeHook


class SaveFile(BaseOperator):
    """Write SQL table to csv/parquet on local/S3/GCS.

    :param output_file_path: Path and name of table to create.
    :type output_file_path: str
    :param table: Input table name.
    :type table: str
    :param input_conn_id: Database connection id.
    :type input_conn_id: str
    :param output_conn_id: File system connection id (if S3 or GCS).
    :type output_conn_id: str
    :param overwrite: Overwrite file if exists. Default False.
    :type overwrite: bool
    """

    def __init__(
        self,
        table="",
        output_file_path="",
        input_conn_id="",
        output_conn_id=None,
        overwrite=None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table = table
        self.output_file_path = output_file_path
        self.input_conn_id = input_conn_id
        self.output_conn_id = output_conn_id
        self.overwrite = overwrite
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.kwargs = kwargs

    def execute(self, context):
        """Write SQL table to csv/parquet on local/S3/GCS.

        Infers SQL database type based on connection.
        """

        # Infer db type from `input_conn_id`.
        conn_type = BaseHook.get_connection(self.input_conn_id).conn_type

        # Select database Hook based on `conn` type
        input_hook = {
            "postgres": TempPostgresHook(
                postgres_conn_id=self.input_conn_id, schema=self.database
            ),
            "snowflake": TempSnowflakeHook(
                snowflake_conn_id=self.output_conn_id,
                database=self.database,
                schema=self.schema,
                warehouse=self.warehouse,
            ),
        }.get(conn_type, None)

        eng = input_hook.get_sqlalchemy_engine()
        # Load table from SQL db.
        df = pd.read_sql(
            f"SELECT * FROM {self.table}", con=input_hook.get_sqlalchemy_engine()
        )

        # Write file if overwrite == True or if file doesn't exist.
        if self.overwrite == True or not self.file_exists(
            self.output_file_path, self.output_conn_id
        ):
            self.agnostic_write_file(df, self.output_file_path, self.output_conn_id)
        else:
            raise FileExistsError

    def file_exists(self, output_file_path, output_conn_id=None):
        if "s3://" in output_file_path:

            bucket_name, object_path = output_file_path.replace("s3://", "").split(
                "/", 1
            )

            # Check if object exists in S3.
            _creds = self._s3fs_creds()
            s3 = boto3.Session(_creds["key"], _creds["secret"]).resource("s3")

            # Return True if file in S3, else False.
            try:
                s3.Object(bucket_name, object_path).load()
                return True
            except:
                return False

        else:
            # Return True if file in local fs, else False.
            return os.path.isfile(output_file_path)

    def agnostic_write_file(self, df, output_file_path, output_conn_id=None):
        storage_options = self._s3fs_creds() if "s3://" in output_file_path else None
        if "s3://" in output_file_path:
            df.to_csv(output_file_path, storage_options=storage_options)
        else:
            df.to_csv(output_file_path)

    def _load_dataframe(self, path):
        """Read file with Pandas.

        Select method based on `file_type` (S3 or local).
        """
        file_type = path.split(".")[-1]
        storage_options = self._s3fs_creds() if "s3://" in path else None
        return {"parquet": pd.read_parquet, "csv": pd.read_csv}[file_type](
            path, storage_options=storage_options
        )

    def _s3fs_creds(self):
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

    @staticmethod
    def create_table_name(context):
        ti: TaskInstance = context["ti"]
        dag_run: DagRun = ti.get_dagrun()
        return f"{dag_run.dag_id}_{ti.task_id}_{dag_run.id}"


def save_file(
    output_file_path,
    table=None,
    input_conn_id=None,
    output_conn_id=None,
    overwrite=False,
    database="",
    schema="",
    warehouse="",
    **kwargs,
):
    """Convert SaveFile into a function. Returns XComArg.

    Returns an XComArg object.

    :param output_file_path: Path and name of table to create.
    :type output_file_path: str
    :param table: Input table name.
    :type table: str
    :param input_conn_id: Database connection id.
    :type input_conn_id: str
    :param output_conn_id: File system connection id (if S3 or GCS).
    :type output_conn_id: str
    :param overwrite: Overwrite file if exists. Default False.
    :type overwrite: bool
    """
    task_id = "save_file_" + output_file_path.rsplit("/", 1)[-1].replace(".", "_")

    return SaveFile(
        task_id=task_id,
        output_file_path=output_file_path,
        table=table,
        input_conn_id=input_conn_id,
        output_conn_id=output_conn_id,
        overwrite=overwrite,
        database=database,
        schema=schema,
        warehouse=warehouse,
    ).output
