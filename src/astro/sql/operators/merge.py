from typing import Dict, List

from airflow.models.baseoperator import BaseOperator

from astro.constants import MergeConflictStrategy
from astro.databases import create_database
from astro.sql.table import Table


class MergeOperator(BaseOperator):
    """
    Merge the source table rows into a destination table.

    :param source_table: Contains the rows to be merged to the target_table (templated)
    :param target_table: Contains the destination table in which the rows will be merged (templated)
    :param source_to_target_columns_map: Dict of target_table columns names to source_table columns names
    :param target_conflict_columns: List of cols where we expect to have a conflict while combining
    :param if_conflicts: The strategy to be applied if there are conflicts.
    """

    template_fields = ("target_table", "source_table")

    def __init__(
        self,
        *,
        target_table: Table,
        source_table: Table,
        source_to_target_columns_map: Dict[str, str],
        if_conflicts: MergeConflictStrategy,
        target_conflict_columns: List[str],
        task_id: str = "",
        **kwargs,
    ):
        self.target_table = target_table
        self.source_table = source_table
        self.target_conflict_columns = target_conflict_columns
        self.source_to_target_columns_map = source_to_target_columns_map
        self.if_conflicts = if_conflicts
        task_id = task_id or "merge"

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
            source_to_target_columns_map=self.source_to_target_columns_map,
        )
        return self.target_table
