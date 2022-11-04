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
from astro.constants import MergeConflictStrategy
from astro.databases import create_database
from astro.lineage.extractor import OpenLineageFacets
from astro.lineage.facets import SourceTableMergeDatasetFacet, TargetTableMergeDatasetFacet
from astro.sql.operators.base_operator import AstroSQLBaseOperator
from astro.table import BaseTable
from astro.utils.typing_compat import Context


class MergeOperator(AstroSQLBaseOperator):
    """
    Merge the source table rows into a destination table.

    :param source_table: Contains the rows to be merged to the target_table (templated)
    :param target_table: Contains the destination table in which the rows will be merged (templated)
    :param columns: List/Tuple of columns if name of source and target tables are same.
        If the column names in source and target tables are different pass a dictionary
        of source_table columns names to target_table columns names.
        Examples: ``["sell", "list"]`` or ``{"s_sell": "t_sell", "s_list": "t_list"}``
    :param target_conflict_columns: List of cols where we expect to have a conflict while combining
    :param if_conflicts: The strategy to be applied if there are conflicts.
    """

    template_fields = ("target_table", "source_table")

    def __init__(
        self,
        *,
        target_table: BaseTable,
        source_table: BaseTable,
        columns: list[str] | tuple[str] | dict[str, str],
        if_conflicts: MergeConflictStrategy,
        target_conflict_columns: list[str],
        task_id: str = "",
        **kwargs: Any,
    ):
        self.target_table = target_table
        self.source_table = source_table
        self.target_conflict_columns = target_conflict_columns
        if isinstance(columns, (list, tuple)):
            columns = dict(zip(columns, columns))
        if columns and not isinstance(columns, dict):
            raise ValueError(
                f"columns is not a valid type. Valid types: [tuple, list, dict], Passed: {type(columns)}"
            )
        self.columns = columns or {}
        self.if_conflicts = if_conflicts
        task_id = task_id or get_unique_task_id("merge")
        super().__init__(
            task_id=task_id,
            **kwargs_with_datasets(
                kwargs=kwargs,
                input_datasets=source_table,
                output_datasets=target_table,
            ),
        )

    def execute(self, context: Context) -> BaseTable:
        db = create_database(self.target_table.conn_id, table=self.source_table)
        self.source_table = db.populate_table_metadata(self.source_table)
        self.target_table = db.populate_table_metadata(self.target_table)

        db.merge_table(
            source_table=self.source_table,
            target_table=self.target_table,
            if_conflicts=self.if_conflicts,
            target_conflict_columns=self.target_conflict_columns,
            source_to_target_columns_map=self.columns,
        )
        context["ti"].xcom_push(key="merge_query", value=str(db.sql))
        return self.target_table

    def get_openlineage_facets(self, task_instance) -> OpenLineageFacets:
        """
        Collect the input, output, job and run facets for merge operator
        """
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
                        "input_table_facet": SourceTableMergeDatasetFacet(
                            table_name=self.source_table.name,
                            if_conflicts=self.if_conflicts,
                            source_table_rows=self.source_table.row_count,
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

        if self.target_table.openlineage_emit_temp_table_event():
            output_uri = (
                f"{self.target_table.openlineage_dataset_namespace()}"
                f"://{self.target_table.openlineage_dataset_name()}"
            )
            output_dataset = [
                OpenlineageDataset(
                    namespace=self.target_table.openlineage_dataset_namespace(),
                    name=self.target_table.openlineage_dataset_name(),
                    facets={
                        "output_table_facet": TargetTableMergeDatasetFacet(
                            table_name=self.target_table.name,
                            target_conflict_columns=self.target_conflict_columns,
                            columns=self.columns,
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

        merge_query = task_instance.xcom_pull(task_ids=task_instance.task_id, key="merge_query")
        job_facets: dict[str, BaseFacet] = {"sql": SqlJobFacet(query=str(merge_query))}

        return OpenLineageFacets(
            inputs=input_dataset, outputs=output_dataset, run_facets=run_facets, job_facets=job_facets
        )


def merge(
    *,
    target_table: BaseTable,
    source_table: BaseTable,
    columns: list[str] | tuple[str] | dict[str, str],
    target_conflict_columns: list[str],
    if_conflicts: MergeConflictStrategy,
    **kwargs: Any,
) -> XComArg:
    """
    Merge the source table rows into a destination table.

    :param source_table: Contains the rows to be merged to the target_table (templated)
    :param target_table: Contains the destination table in which the rows will be merged (templated)
    :param columns: List/Tuple of columns if name of source and target tables are same.
        If the column names in source and target tables are different pass a dictionary
        of source_table columns names to target_table columns names.
        Examples: ``["sell", "list"]`` or ``{"s_sell": "t_sell", "s_list": "t_list"}``
    :param target_conflict_columns: List of cols where we expect to have a conflict while combining
    :param if_conflicts: The strategy to be applied if there are conflicts.
    :param kwargs: Any keyword arguments supported by the BaseOperator is supported (e.g ``queue``, ``owner``)
    """

    return MergeOperator(
        target_table=target_table,
        source_table=source_table,
        columns=columns,
        target_conflict_columns=target_conflict_columns,
        if_conflicts=if_conflicts,
        **kwargs,
    ).output
