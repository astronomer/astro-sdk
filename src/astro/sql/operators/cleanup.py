from typing import List

from airflow.decorators.base import get_unique_task_id
from airflow.models.baseoperator import BaseOperator

from astro.databases import create_database
from astro.sql.table import Table


class CleanupOperator(BaseOperator):
    """
    Clean up temporary tables at the end of a DAG run
    :param tables_to_cleanup: List of tbles to drop at the end of the DAG run
    :param task_id: Optional custom task id
    """

    template_fields = ("tables_to_cleanup",)

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
            if not isinstance(table, Table) or not table.temp:
                continue
            db = create_database(table.conn_id)
            db.drop_table(table)
