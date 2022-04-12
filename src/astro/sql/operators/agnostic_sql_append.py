import importlib
from typing import List, Optional, Union

from sqlalchemy import MetaData, cast, column, insert, select
from sqlalchemy.sql.elements import Cast, ColumnClause
from sqlalchemy.sql.schema import Table as SqlaTable

from astro.constants import Database
from astro.sql.operators.sql_decorator import SqlDecoratedOperator
from astro.sql.table import Table, TempTable
from astro.utils.database import get_database_name
from astro.utils.schema_util import (
    get_error_string_for_multiple_dbs,
    tables_from_same_db,
)
from astro.utils.table_handler import TableHandler
from astro.utils.task_id_helper import get_unique_task_id


class SqlAppendOperator(SqlDecoratedOperator, TableHandler):
    template_fields = ("main_table", "append_table")

    def __init__(
        self,
        append_table: Union[Table, TempTable],
        main_table: Union[Table, TempTable],
        columns: Optional[List[str]] = None,
        casted_columns: Optional[dict] = None,
        **kwargs,
    ) -> None:
        if columns is None:
            columns = []
        if casted_columns is None:
            casted_columns = {}
        self.append_table = append_table
        self.main_table = main_table
        self.sql = ""

        self.columns = columns
        self.casted_columns = casted_columns
        task_id = get_unique_task_id("append_table")

        def null_function():
            pass

        super().__init__(
            raw_sql=True,
            parameters={},
            task_id=kwargs.get("task_id") or task_id,
            op_args=(),
            python_callable=null_function,
            handler=lambda x: None,
            **kwargs,
        )

    def execute(self, context: dict) -> Union[Table, TempTable]:
        if not tables_from_same_db([self.append_table, self.main_table]):
            raise ValueError(
                get_error_string_for_multiple_dbs([self.append_table, self.main_table])
            )

        self.main_table.conn_id = self.main_table.conn_id or self.append_table.conn_id
        self.conn_id = self.main_table.conn_id or self.append_table.conn_id
        self.database = self.main_table.database or self.append_table.database
        self.warehouse = self.main_table.warehouse or self.append_table.warehouse
        self.schema = self.main_table.schema or self.append_table.schema
        self.sql = self.append(
            main_table=self.main_table,
            append_table=self.append_table,
            columns=self.columns,
            casted_columns=self.casted_columns,
        )
        super().execute(context)
        return self.main_table

    def append(
        self,
        main_table: Union[Table, TempTable],
        columns: List[str],
        casted_columns: dict,
        append_table: Union[Table, TempTable],
    ):
        engine = self.get_sql_alchemy_engine()
        if self.schema and get_database_name(engine) != Database.SQLITE:
            metadata = MetaData(schema=self.schema)
        else:
            metadata = MetaData()
        # TO Do - fix bigquery and postgres reflection table issue.
        main_table_sqla = SqlaTable(
            main_table.table_name, metadata, autoload_with=engine
        )
        append_table_sqla = SqlaTable(
            append_table.table_name, metadata, autoload_with=engine
        )

        column_names: List[Union[ColumnClause, Cast]] = [column(c) for c in columns]
        sqlalchemy = importlib.import_module("sqlalchemy")
        casted_fields = [
            cast(column(k), getattr(sqlalchemy, v)) for k, v in casted_columns.items()
        ]
        main_columns = [k for k, v in casted_columns.items()]
        main_columns.extend(list(columns))

        if len(column_names) + len(casted_fields) == 0:
            column_names = [column(c) for c in append_table_sqla.c.keys()]
            main_columns = column_names

        column_names.extend(casted_fields)
        sel = select(column_names).select_from(append_table_sqla)
        return insert(main_table_sqla).from_select(main_columns, sel)
