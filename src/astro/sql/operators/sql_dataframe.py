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
import inspect
from typing import Dict, Optional

import pandas as pd
from airflow.decorators.base import DecoratedOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from astro.sql.table import Table, TempTable, create_table_name
from astro.utils.load_dataframe import move_dataframe_to_sql
from astro.utils.schema_util import get_schema


class SqlDataframeOperator(DecoratedOperator):
    def __init__(
        self,
        conn_id: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        **kwargs,
    ):
        """
        Converts a SQL table into a dataframe. Users can then give a python function that takes a dataframe as
        one of its inputs and run that python function. Once that function has completed, the result is accessible
        via the Taskflow API.

        :param conn_id: Connection to the DB that you will pull the table from
        :param database: Database for input table
        :param schema:  schema for input table
        :param warehouse: (Snowflake) Which warehouse to use for the input table
        :param kwargs:
        """
        self.conn_id = conn_id
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.kwargs = kwargs or {}
        self.op_kwargs: Dict = self.kwargs.get("op_kwargs") or {}
        if self.op_kwargs.get("output_table"):
            self.output_table: Optional[Table] = self.op_kwargs.pop("output_table")
        else:
            self.output_table = None
        self.op_args = self.kwargs.get("op_args")  # type: ignore

        super().__init__(
            **kwargs,
        )

    def handle_op_args(self):
        full_spec = inspect.getfullargspec(self.python_callable)
        op_args = list(self.op_args)
        ret_args = []
        for arg in op_args:
            current_arg = full_spec.args.pop(0)
            if (
                full_spec.annotations[current_arg] == pd.DataFrame
                and type(arg) == Table
            ):
                ret_args.append(self._get_dataframe(arg))
            else:
                ret_args.append(arg)
        self.op_args = tuple(ret_args)

    def handle_op_kwargs(self):
        param_types = inspect.signature(self.python_callable).parameters
        self.op_kwargs = {
            k: self._get_dataframe(v)
            if param_types.get(k).annotation == pd.DataFrame and type(v) == Table
            else v
            for k, v in self.op_kwargs.items()
        }

    def execute(self, context: Dict):
        self.handle_op_args()
        self.handle_op_kwargs()

        ret = self.python_callable(*self.op_args, **self.op_kwargs)
        if self.output_table:
            if type(self.output_table) == TempTable:
                self.output_table = self.output_table.to_table(
                    table_name=create_table_name(context=context), schema=get_schema()
                )
            self.output_table.schema = self.output_table.schema or get_schema()
            conn = BaseHook.get_connection(self.output_table.conn_id)
            move_dataframe_to_sql(
                output_table_name=self.output_table.table_name,
                conn_id=self.output_table.conn_id,
                database=self.output_table.database,
                warehouse=self.output_table.warehouse,
                schema=self.output_table.schema,
                df=ret,
                conn_type=conn.conn_type,
                user=conn.login,
            )
            return self.output_table
        else:
            return ret

    def get_snow_hook(self, table: Table) -> SnowflakeHook:
        """
        Create and return SnowflakeHook.
        :return: a SnowflakeHook instance.
        :rtype: SnowflakeHook
        """
        return SnowflakeHook(
            snowflake_conn_id=table.conn_id,
            warehouse=table.warehouse,
            database=table.database,
            role=None,
            schema=table.schema,
            authenticator=None,
            session_parameters=None,
        )

    def _get_dataframe(self, table: Table):
        conn_type = BaseHook.get_connection(table.conn_id).conn_type
        self.log.info(f"Getting dataframe for {table}")
        if conn_type == "postgres":
            from psycopg2 import sql

            self.hook = PostgresHook(
                postgres_conn_id=table.conn_id, schema=table.database
            )
            query = (
                sql.SQL("SELECT * FROM {input_table}")
                .format(input_table=sql.Identifier(table.table_name))
                .as_string(self.hook.get_conn())
            )
            return self.hook.get_pandas_df(query)

        elif conn_type == "snowflake":
            hook = self.get_snow_hook(table)
            return hook.get_pandas_df(
                "SELECT * FROM IDENTIFIER(%(input_table)s)",
                parameters={"input_table": table.table_name},
            )
