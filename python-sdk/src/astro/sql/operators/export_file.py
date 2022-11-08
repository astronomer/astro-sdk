from __future__ import annotations

from typing import Any

import pandas as pd
from airflow.decorators.base import get_unique_task_id
from airflow.models.xcom_arg import XComArg
from openlineage.client.facet import (
    BaseFacet,
    DataQualityMetricsInputDatasetFacet,
    DataSourceDatasetFacet,
    OutputStatisticsOutputDatasetFacet,
    SchemaDatasetFacet,
    SchemaField,
)
from openlineage.client.run import Dataset as OpenlineageDataset

from astro.airflow.datasets import kwargs_with_datasets
from astro.constants import ExportExistsStrategy
from astro.databases import create_database
from astro.files import File
from astro.lineage.extractor import OpenLineageFacets
from astro.lineage.facets import ExportFileFacet
from astro.sql.operators.base_operator import AstroSQLBaseOperator
from astro.table import BaseTable, Table
from astro.utils.typing_compat import Context


class ExportFileOperator(AstroSQLBaseOperator):
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
        self.output_file = output_file
        self.input_data = input_data
        self.if_exists = if_exists
        self.kwargs = kwargs
        datasets = {"output_datasets": self.output_file}
        if isinstance(input_data, Table):
            datasets["input_datasets"] = input_data
        super().__init__(**kwargs_with_datasets(kwargs=kwargs, **datasets))

    def execute(self, context: Context) -> File:  # skipcq PYL-W0613
        """Write SQL table to csv/parquet on local/S3/GCS.

        Infers SQL database type based on connection.
        """
        # Infer db type from `input_conn_id`.
        if isinstance(self.input_data, BaseTable):
            database = create_database(self.input_data.conn_id, table=self.input_data)
            self.input_data = database.populate_table_metadata(self.input_data)
            df = database.export_table_to_pandas_dataframe(self.input_data)
        elif isinstance(self.input_data, pd.DataFrame):
            df = self.input_data
        else:
            raise ValueError(f"Expected input_table to be Table or dataframe. Got {type(self.input_data)}")
        # Write file if overwrite == True or if file doesn't exist.
        if self.if_exists == "replace" or not self.output_file.exists():
            self.output_file.create_from_dataframe(df, store_as_dataframe=False)
            return self.output_file
        else:
            raise FileExistsError(f"{self.output_file.path} file already exists.")

    def get_openlineage_facets(self, task_instance) -> OpenLineageFacets:  # skipcq: PYL-W0613
        """
        Collect the input, output, job and run facets for export file operator
        """
        input_dataset: list[OpenlineageDataset] = []
        if isinstance(self.input_data, BaseTable) and self.input_data.openlineage_emit_temp_table_event():
            input_uri = (
                f"{self.input_data.openlineage_dataset_namespace()}"
                f"://{self.input_data.openlineage_dataset_name()}"
            )
            input_dataset = [
                OpenlineageDataset(
                    namespace=self.input_data.openlineage_dataset_namespace(),
                    name=self.input_data.openlineage_dataset_name(),
                    facets={
                        "dataSource": DataSourceDatasetFacet(
                            name=self.input_data.openlineage_dataset_name(), uri=input_uri
                        ),
                        "schema": SchemaDatasetFacet(
                            fields=[
                                SchemaField(
                                    name=self.input_data.metadata.schema,
                                    type=self.input_data.metadata.database,
                                )
                            ]
                        ),
                        "dataQualityMetrics": DataQualityMetricsInputDatasetFacet(
                            rowCount=self.input_data.row_count, columnMetrics={}
                        ),
                    },
                )
            ]
        output_uri = (
            f"{self.output_file.openlineage_dataset_namespace}"
            f"://{self.output_file.openlineage_dataset_name}"
        )
        output_dataset = [
            OpenlineageDataset(
                namespace=self.output_file.openlineage_dataset_namespace,
                name=self.output_file.openlineage_dataset_name,
                facets={
                    "output_file_facet": ExportFileFacet(
                        filepath=self.output_file.path,
                        file_size=self.output_file.size,
                        file_type=self.output_file.type.name,
                        if_exists=self.if_exists,
                    ),
                    "dataSource": DataSourceDatasetFacet(name=self.output_file.path, uri=output_uri),
                    "outputStatistics": OutputStatisticsOutputDatasetFacet(
                        rowCount=self.input_data.row_count
                        if isinstance(self.input_data, BaseTable)
                        else len(self.input_data),
                        size=self.output_file.size,
                    ),
                },
            )
        ]

        run_facets: dict[str, BaseFacet] = {}
        job_facets: dict[str, BaseFacet] = {}

        return OpenLineageFacets(
            inputs=input_dataset, outputs=output_dataset, run_facets=run_facets, job_facets=job_facets
        )


def export_file(
    input_data: BaseTable | pd.DataFrame,
    output_file: File,
    if_exists: ExportExistsStrategy = "exception",
    task_id: str | None = None,
    **kwargs: Any,
) -> XComArg:
    """Convert ExportFileOperator into a function. Returns XComArg.

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

    task_id = task_id or get_unique_task_id("export_file")

    return ExportFileOperator(
        task_id=task_id,
        output_file=output_file,
        input_data=input_data,
        if_exists=if_exists,
        **kwargs,
    ).output
