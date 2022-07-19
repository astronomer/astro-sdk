from typing import Dict

from airflow.decorators.base import get_unique_task_id
from airflow.models import BaseOperator

from astro.sql.table import Table


class TruncateOperator(BaseOperator):
    """Airflow Operator for truncating SQL tables."""

    def __init__(
        self,
        table: Table,
        task_id: str = "",
        **kwargs,
    ):
        task_id = task_id or get_unique_task_id("truncate")
        super().__init__(
            task_id=task_id,
            **kwargs,
        )
        self.table = table

    def execute(self, context: Dict) -> None:  # skipcq: PYL-W0613
        """Method run when the Airflow runner calls the operator."""
        # database = create_database(self.table.conn_id)
        # self.table = database.populate_table_metadata(self.table)
        # database.drop_table(self.table)
        pass
