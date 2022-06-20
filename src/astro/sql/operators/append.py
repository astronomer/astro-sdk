from typing import Any, Dict, List, Optional, Tuple, Union

from airflow.models.baseoperator import BaseOperator

from astro.databases import create_database
from astro.sql.table import Table
from astro.utils.task_id_helper import get_unique_task_id

APPEND_COLUMN_TYPE = Optional[Union[List[str], Tuple[str], Dict[str, str]]]


class AppendOperator(BaseOperator):
    """
    Append the source table rows into a destination table.

    :param source_table: Contains the rows to be appended to the target_table (templated)
    :param target_table: Contains the destination table in which the rows will be appended (templated)
    :param columns: List/Tuple of columns if name of source and target tables are same.
        If the column names in source and target tables are different pass a dictionary
        of source_table columns names to target_table columns names.
        Examples: ``["sell", "list"]`` or ``{"s_sell": "t_sell", "s_list": "t_list"}``
    """

    template_fields = ("source_table", "target_table")

    def __init__(
        self,
        source_table: Table,
        target_table: Table,
        columns: APPEND_COLUMN_TYPE = None,
        task_id: str = "",
        **kwargs: Any,
    ) -> None:
        self.source_table = source_table
        self.target_table = target_table
        if isinstance(columns, (list, tuple)):
            columns = dict(zip(columns, columns))
        if columns and not isinstance(columns, dict):
            raise ValueError(
                f"columns is not a valid type. Valid types: [tuple, list, dict], Passed: {type(columns)}"
            )
        self.columns = columns or {}
        task_id = task_id or get_unique_task_id("append_table")

        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context: dict) -> Table:  # skipcq: PYL-W0613
        db = create_database(self.target_table.conn_id)
        self.source_table = db.populate_table_metadata(self.source_table)
        self.target_table = db.populate_table_metadata(self.target_table)
        db.append_table(
            source_table=self.source_table,
            target_table=self.target_table,
            source_to_target_columns_map=self.columns,
        )
        return self.target_table
