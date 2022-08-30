from __future__ import annotations

from typing import Any

from airflow.decorators.base import get_unique_task_id
from airflow.models import BaseOperator
from airflow.models.xcom_arg import XComArg

from astro.databases import create_database
from astro.sql.table import Table


class DropTableOperator(BaseOperator):
    """Airflow Operator for dropping SQL tables."""

    template_fields = ("table",)

    def __init__(
        self,
        table: Table,
        task_id: str = "",
        **kwargs,
    ):
        self.table = table
        task_id = task_id or get_unique_task_id("drop")
        super().__init__(
            task_id=task_id,
            **kwargs,
        )

    def execute(self, context: dict) -> Table:  # skipcq: PYL-W0613
        """Method run when the Airflow runner calls the operator."""
        database = create_database(self.table.conn_id)
        self.table = database.populate_table_metadata(self.table)
        database.drop_table(self.table)
        return self.table


def drop_table(
    table: Table,
    **kwargs: Any,
) -> XComArg:
    """
    Drops a table.

    :param table: Table to be dropped
    :param kwargs: Any keyword arguments supported by the BaseOperator is supported (e.g ``queue``, ``owner``)
    """

    return DropTableOperator(table=table, **kwargs).output
