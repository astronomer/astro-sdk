from __future__ import annotations

from typing import Any

from airflow.decorators.base import get_unique_task_id
from airflow.models.xcom_arg import XComArg
from openlineage.client.facet import (
    BaseFacet,
    DataQualityMetricsInputDatasetFacet,
    DataSourceDatasetFacet,
    OutputStatisticsOutputDatasetFacet,
    SchemaDatasetFacet,
    SchemaField,
    SqlJobFacet,
)
from openlineage.client.run import Dataset as OpenlineageDataset

from astro.airflow.datasets import kwargs_with_datasets
from astro.databases import create_database
from astro.lineage.extractor import OpenLineageFacets
from astro.lineage.facets import TableDatasetFacet
from astro.sql.operators.base_operator import AstroSQLBaseOperator
from astro.table import BaseTable
from astro.utils.typing_compat import Context


class AppendOperator(AstroSQLBaseOperator):
    """
    Append the source table rows into a destination table.

    :param source_table: Contains the rows to be appended to the target_table (templated)
    :param target_table: Contains the destination table in which the rows will be appended (templated)
    :param columns: List/Tuple of columns if name of source and target tables are same.
        If the column names in source and target tables are different pass a dictionary
        of source_table columns names to target_table columns names.
        Examples: ``["sell", "list"]`` or ``{"s_sell": "t_sell", "s_list": "t_list"}``
    """

    template_fields = ("source_table", "target_table")

    def __init__(
        self,
        source_table: BaseTable,
        target_table: BaseTable,
        columns: list[str] | tuple[str] | dict[str, str] | None = None,
        task_id: str = "",
        **kwargs: Any,
    ) -> None:
        self.source_table = source_table
        self.target_table = target_table
        if isinstance(columns, (list, tuple)):
            columns = dict(zip(columns, columns))
        if columns and not isinstance(columns, dict):
            raise ValueError(
                f"columns is not a valid type. Valid types: [tuple, list, dict], Passed: {type(columns)}"
            )
        self.columns = columns or {}
        task_id = task_id or get_unique_task_id("append_table")
        super().__init__(
            task_id=task_id,
            **kwargs_with_datasets(kwargs=kwargs, input_datasets=source_table, output_datasets=target_table),
        )

    def execute(self, context: Context) -> BaseTable:  # skipcq: PYL-W0613
        db = create_database(self.target_table.conn_id, table=self.source_table)
        self.source_table = db.populate_table_metadata(self.source_table)
        self.target_table = db.populate_table_metadata(self.target_table)
        db.append_table(
            source_table=self.source_table,
            target_table=self.target_table,
            source_to_target_columns_map=self.columns,
        )
        context["ti"].xcom_push(key="append_query", value=str(db.sql))
        return self.target_table

    def get_openlineage_facets(self, task_instance) -> OpenLineageFacets:
        """
        Collect the input, output, job and run facets for append operator
        """
        append_query = task_instance.xcom_pull(task_ids=task_instance.task_id, key="append_query")
        source_table_rows = self.source_table.row_count
        input_dataset: list[OpenlineageDataset] = [OpenlineageDataset(namespace=None, name=None, facets={})]
        output_dataset: list[OpenlineageDataset] = [OpenlineageDataset(namespace=None, name=None, facets={})]
        if self.source_table.openlineage_emit_temp_table_event():
            input_uri = (
                f"{self.source_table.openlineage_dataset_namespace()}"
                f"://{self.source_table.openlineage_dataset_name()}"
            )
            input_dataset = [
                OpenlineageDataset(
                    namespace=self.source_table.openlineage_dataset_namespace(),
                    name=self.source_table.openlineage_dataset_name(),
                    facets={
                        "input_table_facet": TableDatasetFacet(
                            table_name=self.source_table.name,
                            source_table_rows=source_table_rows,
                            columns=self.columns,
                            metadata=self.source_table.metadata,
                        ),
                        "schema": SchemaDatasetFacet(
                            fields=[
                                SchemaField(
                                    name=self.source_table.metadata.schema,
                                    type=self.source_table.metadata.database,
                                )
                            ]
                        ),
                        "dataSource": DataSourceDatasetFacet(name=self.source_table.name, uri=input_uri),
                        "dataQualityMetrics": DataQualityMetricsInputDatasetFacet(
                            rowCount=self.source_table.row_count, columnMetrics={}
                        ),
                    },
                )
            ]

        if self.source_table.openlineage_emit_temp_table_event():
            output_uri = (
                f"{self.target_table.openlineage_dataset_namespace()}"
                f"://{self.target_table.openlineage_dataset_name()}"
            )
            output_dataset = [
                OpenlineageDataset(
                    namespace=self.target_table.openlineage_dataset_namespace(),
                    name=self.target_table.openlineage_dataset_name(),
                    facets={
                        "output_table_facet": TableDatasetFacet(
                            table_name=self.target_table.name,
                            columns=self.columns,
                            source_table_rows=source_table_rows,
                            metadata=self.target_table.metadata,
                        ),
                        "outputStatistics": OutputStatisticsOutputDatasetFacet(
                            rowCount=self.target_table.row_count
                        ),
                        "dataSource": DataSourceDatasetFacet(name=self.target_table.name, uri=output_uri),
                        "dataQualityMetrics": DataQualityMetricsInputDatasetFacet(
                            rowCount=self.target_table.row_count, columnMetrics={}
                        ),
                    },
                )
            ]

        run_facets: dict[str, BaseFacet] = {}
        job_facets: dict[str, BaseFacet] = {"sql": SqlJobFacet(query=str(append_query))}

        return OpenLineageFacets(
            inputs=input_dataset, outputs=output_dataset, run_facets=run_facets, job_facets=job_facets
        )


def append(
    *,
    source_table: BaseTable,
    target_table: BaseTable,
    columns: list[str] | tuple[str] | dict[str, str] | None = None,
    **kwargs: Any,
) -> XComArg:
    """
    Append the source table rows into a destination table.

    :param source_table: Contains the rows to be appended to the target_table (templated)
    :param target_table: Contains the destination table in which the rows will be appended (templated)
    :param columns: List/Tuple of columns if name of source and target tables are same.
        If the column names in source and target tables are different pass a dictionary
        of source_table columns names to target_table columns names.
        Examples: ``["sell", "list"]`` or ``{"s_sell": "t_sell", "s_list": "t_list"}``
    :param kwargs: Any keyword arguments supported by the BaseOperator is supported (e.g ``queue``, ``owner``)
    """
    return AppendOperator(
        target_table=target_table,
        source_table=source_table,
        columns=columns,
        **kwargs,
    ).output
