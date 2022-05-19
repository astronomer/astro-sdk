from typing import Any, Callable, Dict, Iterator, Optional, Union
from urllib.parse import urlparse

import pandas as pd
import smart_open
from airflow.models import BaseOperator
from airflow.models.xcom_arg import XComArg
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from astro.constants import Database
from astro.sql.table import Table
from astro.sqlite_utils import create_sqlalchemy_engine_with_sqlite
from astro.utils.cloud_storage_creds import gcs_client, s3fs_creds
from astro.utils.database import get_database_from_conn_id
from astro.utils.dependencies import BigQueryHook, PostgresHook, SnowflakeHook
from astro.utils.task_id_helper import get_task_id


class SaveFile(BaseOperator):
    """Write SQL table to csv/parquet on local/S3/GCS.

    :param input_data: Table to convert to file
    :param output_file_path: Path and name of table to create.
    :param output_conn_id: File system connection id (if S3 or GCS).
    :param overwrite: Overwrite file if exists. Default False.
    :param output_file_format: file formats, valid values csv/parquet. Default: 'csv'.
    """

    template_fields = (
        "input_data",
        "output_file_path",
        "output_conn_id",
        "output_file_format",
    )

    def __init__(
        self,
        input_data: Union[Table, pd.DataFrame],
        output_file_path: str = "",
        output_conn_id: Optional[str] = None,
        output_file_format: str = "csv",
        overwrite: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.output_file_path = output_file_path
        self.input_data = input_data
        self.output_conn_id = output_conn_id
        self.overwrite = overwrite
        self.output_file_format = output_file_format
        self.kwargs = kwargs

    def execute(self, context: dict) -> None:
        """Write SQL table to csv/parquet on local/S3/GCS.

        Infers SQL database type based on connection.
        """
        # Infer db type from `input_conn_id`.
        if type(self.input_data) is Table:
            df = self.convert_sql_table_to_dataframe()
        elif type(self.input_data) is pd.DataFrame:
            df = self.input_data
        else:
            raise ValueError(
                "Expected input_table to be Table or dataframe. Got %s",
                type(self.input_data),
            )
        # Write file if overwrite == True or if file doesn't exist.
        if self.overwrite or not self.file_exists(self.output_file_path):
            self.agnostic_write_file(df, self.output_file_path)
        else:
            raise FileExistsError(f"{self.output_file_path} file already exists.")

    def file_exists(self, output_file_path: str) -> bool:
        def null_scheme(_: Any = None) -> dict:
            """dummy function to get dummy creds"""
            return {}

        transport_dict: Dict[str, Callable[[Optional[str]], dict]] = {
            "s3": s3fs_creds,
            "gs": gcs_client,
            "": null_scheme,
        }
        transport_params = transport_dict[urlparse(output_file_path).scheme](None)

        try:
            with smart_open.open(
                output_file_path, mode="r", transport_params=transport_params
            ):
                return True
        except OSError:
            return False

    def convert_sql_table_to_dataframe(
        self,
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        input_table = self.input_data
        database = get_database_from_conn_id(input_table.conn_id)

        # Select database Hook based on `conn` type
        hook_kwargs = {
            Database.POSTGRES: {
                "postgres_conn_id": input_table.conn_id,
                "schema": input_table.database,
            },
            Database.SNOWFLAKE: {
                "snowflake_conn_id": input_table.conn_id,
                "database": input_table.database,
                "schema": input_table.schema,
                "warehouse": input_table.warehouse,
            },
            Database.BIGQUERY: {
                "use_legacy_sql": False,
                "gcp_conn_id": input_table.conn_id,
            },
            Database.SQLITE: {"sqlite_conn_id": input_table.conn_id},
        }

        hook_class = {
            Database.POSTGRES: PostgresHook,
            Database.SNOWFLAKE: SnowflakeHook,
            Database.BIGQUERY: BigQueryHook,
            Database.SQLITE: SqliteHook,
        }

        try:
            input_hook = hook_class[database](**hook_kwargs[database])
        except KeyError:
            raise ValueError(
                f"The conn_id {input_table.conn_id} is of unsupported type {database}. "
                f"Support types: {list(hook_class.keys())}"
            )

        if database == Database.SQLITE:
            con_engine = create_sqlalchemy_engine_with_sqlite(input_hook)
        else:
            con_engine = input_hook.get_sqlalchemy_engine()

        return pd.read_sql(
            f"SELECT * FROM {input_table.qualified_name()}",
            con=con_engine,
        )

    def agnostic_write_file(self, df: pd.DataFrame, output_file_path: str) -> None:
        """Write dataframe to csv/parquet files formats

        Select output file format based on param output_file_format to class.
        """

        def null_scheme(_: Any = None) -> dict:
            """dummy function to get dummy creds"""
            return {}

        transport_dict: Dict[str, Callable[[Optional[str]], dict]] = {
            "s3": s3fs_creds,
            "gs": gcs_client,
            "": null_scheme,
        }
        transport_params = transport_dict[urlparse(output_file_path).scheme](None)

        serialiser = {
            "parquet": df.to_parquet,
            "csv": df.to_csv,
            "json": df.to_json,
            "ndjson": df.to_json,
        }
        serialiser_params = {
            "csv": {"index": False},
            "json": {"orient": "records"},
            "ndjson": {"orient": "records", "lines": True},
        }
        with smart_open.open(
            output_file_path, mode="wb", transport_params=transport_params
        ) as stream:
            serialiser[self.output_file_format](
                stream, **serialiser_params.get(self.output_file_format, {})
            )


def save_file(
    input_data: Union[Table, pd.DataFrame],
    output_file_path: str = "",
    output_conn_id: Optional[str] = None,
    overwrite: bool = False,
    output_file_format: str = "csv",
    task_id: Optional[str] = None,
    **kwargs,
) -> XComArg:
    """Convert SaveFile into a function. Returns XComArg.

    Returns an XComArg object.

    :param output_file_path: Path and name of table to create.
    :param input_data: Input table / dataframe.
    :param output_conn_id: File system connection id (if S3 or GCS).
    :param overwrite: Overwrite file if exists. Default False.
    :param output_file_format: file formats, valid values csv/parquet. Default: 'csv'.
    :param task_id: task id, optional.
    """

    task_id = (
        task_id if task_id is not None else get_task_id("save_file", output_file_path)
    )

    return SaveFile(
        task_id=task_id,
        output_file_path=output_file_path,
        input_data=input_data,
        output_conn_id=output_conn_id,
        overwrite=overwrite,
        output_file_format=output_file_format,
    ).output
