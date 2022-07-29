from typing import Dict

from airflow.decorators.base import get_unique_task_id

from astro.databases import create_database
from astro.sql.operators.upstream_tasks import UpstreamTaskHandler
from astro.sql.table import Table


class DropTableOperator(UpstreamTaskHandler):
    """Airflow Operator for dropping SQL tables."""

    template_fields = ("table",)

    def __init__(
        self,
        table: Table,
        task_id: str = "",
        **kwargs,
    ):
        self.table = table
        task_id = task_id or get_unique_task_id("_drop")
        super().__init__(
            task_id=task_id,
            **kwargs,
        )

    def execute(self, context: Dict) -> Table:  # skipcq: PYL-W0613
        """Method run when the Airflow runner calls the operator."""
        database = create_database(self.table.conn_id)
        self.table = database.populate_table_metadata(self.table)
        database.drop_table(self.table)
        return self.table
