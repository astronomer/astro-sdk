from typing import List

from airflow.decorators.base import get_unique_task_id
from airflow.models.baseoperator import BaseOperator

from astro.databases import create_database
from astro.sql.table import Table


class CleanupOperator(BaseOperator):
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
        tables_to_cleanup: List[Table],
        task_id: str = "",
        **kwargs,
    ):
        self.tables_to_cleanup = tables_to_cleanup
        task_id = task_id or get_unique_task_id("_cleanup")

        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context: dict):
        for table in self.tables_to_cleanup:
            if not isinstance(table, Table):
                continue
            db = create_database(table.conn_id)
            db.drop_table(table)
