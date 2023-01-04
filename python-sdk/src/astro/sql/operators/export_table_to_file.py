from __future__ import annotations

import warnings
from typing import Any

import pandas as pd
from airflow.models.xcom_arg import XComArg

from astro.constants import ExportExistsStrategy
from astro.files import File
from astro.sql.operators.export_to_file import ExportToFileOperator, export_to_file
from astro.table import BaseTable


class ExportTableToFileOperator(ExportToFileOperator):
    """Write SQL table to csv/parquet on local/S3/GCS.
    :param input_data: Table to convert to file
    :param output_file: File object containing the path to the file and connection id.
    :param if_exists: Overwrite file if exists. Default False.
    """

    template_fields = ("input_data", "output_file")

    def __init__(
        self,
        input_data: BaseTable | pd.DataFrame,
        output_file: File,
        if_exists: ExportExistsStrategy = "exception",
        **kwargs,
    ) -> None:
        super().__init__(input_data=input_data, output_file=output_file, if_exists=if_exists, **kwargs)
        warnings.warn(
            """This class is deprecated.
            Please use `astro.sql.operators.export_to_file.ExportToFileOperator`.
            And, will be removed in astro-sdk-python>=1.5.0.""",
            DeprecationWarning,
            stacklevel=2,
        )


def export_table_to_file(
    input_data: BaseTable | pd.DataFrame,
    output_file: File,
    if_exists: ExportExistsStrategy = "exception",
    task_id: str | None = None,
    **kwargs: Any,
) -> XComArg:
    """Convert ExportTableToFileOperator into a function. Returns XComArg.

    Returns an XComArg object of type File which matches the output_file parameter.

    This will allow users to perform further actions with the exported file.

    e.g.:

    .. code-block:: python

      with sample_dag:
          table = aql.load_file(input_file=File(path=data_path), output_table=test_table)
          exported_file = aql.export_file(
              input_data=table,
              output_file=File(path="/tmp/saved_df.csv"),
              if_exists="replace",
          )
          res_df = aql.load_file(input_file=exported_file)


    :param output_file: Path and conn_id
    :param input_data: Input table / dataframe
    :param if_exists: Overwrite file if exists. Default "exception"
    :param task_id: task id, optional
    """

    warnings.warn(
        """This decorator is deprecated.
        Please use `astro.sql.operators.export_to_file.export_to_file`.
        And, will be removed in astro-sdk-python>=1.5.0.""",
        DeprecationWarning,
        stacklevel=2,
    )

    return export_to_file(
        input_data=input_data, output_file=output_file, if_exists=if_exists, task_id=task_id, **kwargs
    )
