from typing import Dict

from airflow.decorators.base import get_unique_task_id
from airflow.models import BaseOperator

from astro.databases import create_database
from astro.sql.table import Table


class TruncateOperator(BaseOperator):
    """Airflow Operator for truncating SQL tables."""

    def __init__(
        self,
        table: Table,
        task_id: str = "",
        **kwargs,
    ):
        self.table = table
        task_id = task_id or get_unique_task_id(table.name + "_truncate")
        super().__init__(
            task_id=task_id,
            **kwargs,
        )

    def execute(self, context: Dict) -> None:  # skipcq: PYL-W0613
        """Method run when the Airflow runner calls the operator."""
        database = create_database(self.table.conn_id)
        self.table = database.populate_table_metadata(self.table)
        database.drop_table(self.table)
