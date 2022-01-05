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

from typing import Dict, List

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from astro.sql.operators.sql_decorator import SqlDecoratoratedOperator
from astro.sql.table import Table
from astro.utils.postgres_append import postgres_append_func
from astro.utils.snowflake_append import snowflake_append_func
from astro.utils.task_id_helper import get_unique_task_id


class SqlAppendOperator(SqlDecoratoratedOperator):
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
            database=main_table.database,
            schema=main_table.schema,
            warehouse=main_table.warehouse,
            conn_id=main_table.conn_id,
            op_args=(),
            python_callable=null_function,
            **kwargs,
        )

    def execute(self, context: Dict):
        conn_type = BaseHook.get_connection(self.conn_id).conn_type  # type: ignore
        if conn_type == "postgres":
            self.sql = postgres_append_func(
                main_table=self.main_table,
                append_table=self.append_table,
                columns=self.columns,
                casted_columns=self.casted_columns,
                conn_id=self.conn_id,
            )
        elif conn_type == "snowflake":
            self.sql, self.parameters = snowflake_append_func(
                main_table=self.main_table,
                append_table=self.append_table,
                columns=self.columns,
                casted_columns=self.casted_columns,
                snowflake_conn_id=self.conn_id,
            )
        else:
            raise AirflowException(f"Please specify a postgres or snowflake conn id.")

        super().execute(context)
