from __future__ import annotations

from typing import Any

from airflow.decorators.base import get_unique_task_id
from airflow.models.baseoperator import BaseOperator
from airflow.models.xcom_arg import XComArg

from astro.constants import MergeConflictStrategy
from astro.databases import create_database
from astro.sql.table import Table


class MergeOperator(BaseOperator):
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
        target_table: Table,
        source_table: Table,
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

        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context: dict) -> Table:
        db = create_database(self.target_table.conn_id)
        self.source_table = db.populate_table_metadata(self.source_table)
        self.target_table = db.populate_table_metadata(self.target_table)

        db.merge_table(
            source_table=self.source_table,
            target_table=self.target_table,
            if_conflicts=self.if_conflicts,
            target_conflict_columns=self.target_conflict_columns,
            source_to_target_columns_map=self.columns,
        )
        return self.target_table


def merge(
    *,
    target_table: Table,
    source_table: Table,
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
