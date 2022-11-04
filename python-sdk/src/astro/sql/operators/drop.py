from __future__ import annotations

from typing import Any

from airflow.decorators.base import get_unique_task_id
from airflow.models.xcom_arg import XComArg

from astro.databases import create_database
from astro.sql.operators.base_operator import AstroSQLBaseOperator
from astro.table import BaseTable
from astro.utils.typing_compat import Context


class DropTableOperator(AstroSQLBaseOperator):
    """Airflow Operator for dropping SQL tables."""

    template_fields = ("table",)

    def __init__(
        self,
        table: BaseTable,
        task_id: str = "",
        **kwargs,
    ):
        self.table = table
        task_id = task_id or get_unique_task_id("drop")
        super().__init__(
            task_id=task_id,
            **kwargs,
        )

    def execute(self, context: Context) -> BaseTable:  # skipcq: PYL-W0613
        """Method run when the Airflow runner calls the operator."""
        database = create_database(self.table.conn_id, table=self.table)
        self.table = database.populate_table_metadata(self.table)
        database.drop_table(self.table)
        return self.table


def drop_table(
    table: BaseTable,
    **kwargs: Any,
) -> XComArg:
    """
    Drops a table.

    :param table: Table to be dropped
    :param kwargs: Any keyword arguments supported by the BaseOperator is supported (e.g ``queue``, ``owner``)
    """

    return DropTableOperator(table=table, **kwargs).output
