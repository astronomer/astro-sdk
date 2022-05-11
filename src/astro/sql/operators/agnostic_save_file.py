from typing import Optional, Union

import pandas as pd
from airflow.models import BaseOperator
from airflow.models.xcom_arg import XComArg

from astro.constants import ExportExistsStrategy
from astro.databases import create_database
from astro.files import File
from astro.sql.table import Table
from astro.utils.task_id_helper import get_task_id


class SaveFile(BaseOperator):
    """Write SQL table to csv/parquet on local/S3/GCS.

    :param input_data: Table to convert to file
    :param output_file_path: Path and name of table to create.
    :param output_conn_id: File system connection id (if S3 or GCS).
    :param overwrite: Overwrite file if exists. Default False.
    :param output_file_format: file formats, valid values csv/parquet. Default: 'csv'.
    """

    template_fields = ("input_data", "output_file")

    def __init__(
        self,
        input_data: Union[Table, pd.DataFrame],
        output_file: File,
        output_conn_id: Optional[str] = None,
        if_exists: ExportExistsStrategy = "exception",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.output_file = output_file
        self.input_data = input_data
        self.output_conn_id = output_conn_id
        self.if_exists = if_exists
        self.kwargs = kwargs

    def execute(self, context: dict) -> None:
        """Write SQL table to csv/parquet on local/S3/GCS.

        Infers SQL database type based on connection.
        """
        # Infer db type from `input_conn_id`.
        if type(self.input_data) is Table:
            database = create_database(self.input_data.conn_id)
            df = database.export_table_to_pandas_dataframe(self.input_data)
        elif type(self.input_data) is pd.DataFrame:
            df = self.input_data
        else:
            raise ValueError(
                "Expected input_table to be Table or dataframe. Got %s",
                type(self.input_data),
            )
        # Write file if overwrite == True or if file doesn't exist.
        if self.if_exists == "replace" or not self.output_file.exists():
            self.output_file.create_from_dataframe(df)
        else:
            raise FileExistsError(f"{self.output_file.path} file already exists.")


def save_file(
    input_data: Union[Table, pd.DataFrame],
    output_file: File,
    if_exists: ExportExistsStrategy = "exception",
    task_id: Optional[str] = None,
    **kwargs,
) -> XComArg:
    """Convert SaveFile into a function. Returns XComArg.

    Returns an XComArg object.

    :param output_file: Path and conn_id
    :param input_data: Input table / dataframe
    :param if_exists: Overwrite file if exists. Default "exception"
    :param task_id: task id, optional
    """

    task_id = (
        task_id if task_id is not None else get_task_id("save_file", output_file.path)
    )

    return SaveFile(
        task_id=task_id,
        output_file=output_file,
        input_data=input_data,
        if_exists=if_exists,
    ).output
