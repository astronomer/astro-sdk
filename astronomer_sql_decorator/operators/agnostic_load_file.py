import os
from typing import Optional

import pandas as pd
import sqlalchemy
from airflow.hooks.base import BaseHook
from airflow.hooks.dbapi import DbApiHook
from airflow.models import BaseOperator, DagRun, TaskInstance
from airflow.models.xcom_arg import XComArg
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import pd_writer


class TempSnowflakeHook(SnowflakeHook):
    """
    Temporary class to get around a bug in the snowflakehook when creating URIs
    """

    def get_uri(self) -> str:
        """Override DbApiHook get_uri method for get_sqlalchemy_engine()"""
        conn_config = self._get_conn_params()
        uri = (
            "snowflake://{user}:{password}@{account}.{region}/{database}/{schema}"
            "?warehouse={warehouse}&role={role}&authenticator={authenticator}"
        )
        return uri.format(**conn_config)


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
        path="",
        output_table_name=None,
        file_conn_id="",
        output_conn_id="",
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.file_conn_id = file_conn_id
        self.output_conn_id = output_conn_id
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.kwargs = kwargs
        self.output_table_name = output_table_name

    def execute(self, context):
        """Loads csv/parquet table from local/S3/GCS with Pandas.

        Infers SQL database type based on connection then loads table to db.
        """

        # Read file with Pandas load method based on `file_type` (S3 or local).
        df = self._load_dataframe(self.path)

        # Retrieve conn type
        conn_type = BaseHook.get_connection(self.output_conn_id).conn_type

        # Select database Hook based on `conn` type
        hook = {
            "postgres": PostgresHook(postgres_conn_id=self.output_conn_id),
            "snowflake": TempSnowflakeHook(
                snowflake_conn_id=self.output_conn_id,
                database=self.database,
                schema=self.schema,
                warehouse=self.warehouse,
            ),
        }.get(conn_type, None)

        if self.database:
            hook.database = self.database

        # Autogenerate table name
        if not self.output_table_name:
            self.output_table_name = self.create_table_name(context)

        # Write df to target db
        # Note: the `method` argument changes when writing to Snowflake
        write_method = None
        if conn_type == "snowflake":
            meth = pd_writer
        df.to_sql(
            self.output_table_name,
            con=hook.get_sqlalchemy_engine(),
            schema=None,
            if_exists="replace",
            method=write_method,
            index=False,
        )

        return self.output_table_name

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
        """Generate output table name."""
        ti: TaskInstance = context["ti"]
        dag_run: DagRun = ti.get_dagrun()
        return f"{dag_run.dag_id}_{ti.task_id}_{dag_run.id}"


def load_file(
    path,
    output_table_name,
    file_conn_id=None,
    output_conn_id=None,
    database=None,
    schema=None,
    warehouse=None,
    **kwargs,
):
    """Convert AgnosticLoadFile into a function.

    Returns an XComArg object.

    :param path: File path.
    :type path: str
    :param output_table_name: Name of table to create.
    :type output_table_name: str
    :param file_conn_id: Airflow connection id of input file (optional)
    :type file_conn_id: str
    :param output_conn_id: Database connection id.
    :type output_conn_id: str
    """

    return AgnosticLoadFile(
        task_id=output_table_name,
        path=path,
        output_table_name=output_table_name,
        file_conn_id=file_conn_id,
        output_conn_id=output_conn_id,
        database=database,
        schema=schema,
        warehouse=warehouse,
        **kwargs,
    ).output
