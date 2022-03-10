"""
Copyright Astronomer, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import importlib
from typing import Dict, List

from sqlalchemy import MetaData, cast, column, insert, select
from sqlalchemy.sql.schema import Table as SqlaTable

from astro.sql.operators.sql_decorator import SqlDecoratedOperator
from astro.sql.table import Table
from astro.utils.schema_util import (
    get_error_string_for_multiple_dbs,
    get_table_name,
    tables_from_same_db,
)
from astro.utils.table_handler import TableHandler
from astro.utils.task_id_helper import get_unique_task_id


class SqlAppendOperator(SqlDecoratedOperator, TableHandler):
    template_fields = ("main_table", "append_table")

    def __init__(
        self,
        append_table: Table,
        main_table: Table,
        columns: List[str] = [],
        casted_columns: dict = {},
        **kwargs,
    ):
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

    def execute(self, context: Dict):
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
            conn_id=self.main_table.conn_id,
        )
        super().execute(context)
        return self.main_table

    def append(
        self, main_table: Table, columns, casted_columns, append_table: Table, conn_id
    ):
        engine = self.get_sql_alchemy_engine()
        if self.conn_type in ["postgres", "postgresql"]:
            metadata = MetaData(schema=self.schema)
        else:
            metadata = MetaData()
        # TO Do - fix bigquery and postgres reflection table issue.
        main_table_sqla = SqlaTable(
            get_table_name(main_table), metadata, autoload_with=engine
        )
        append_table_sqla = SqlaTable(
            get_table_name(append_table), metadata, autoload_with=engine
        )

        column_names = [column(c) for c in columns]
        sqlalchemy = importlib.import_module("sqlalchemy")
        casted_fields = [
            cast(column(k), getattr(sqlalchemy, v)) for k, v in casted_columns.items()
        ]
        main_columns = [k for k, v in casted_columns.items()]
        main_columns.extend([c for c in columns])

        if len(column_names) + len(casted_fields) == 0:
            column_names = [column(c) for c in append_table_sqla.c.keys()]
            main_columns = column_names

        column_names.extend(casted_fields)
        sel = select(column_names).select_from(append_table_sqla)
        return insert(main_table_sqla).from_select(main_columns, sel)
