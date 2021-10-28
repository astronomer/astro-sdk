import inspect
from builtins import NotImplementedError
from typing import Callable, Dict, Iterable, Mapping, Optional, Union

import pandas as pd
from airflow.decorators.base import DecoratedOperator, task_decorator_factory
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import DagRun, TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.db import provide_session


class SqlToDataframeOperator(DecoratedOperator):
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
        self.op_kwargs = self.kwargs.get("op_kwargs")
        self.op_args = self.kwargs.get("op_args")

        super().__init__(
            **kwargs,
        )

    def handle_op_args(self):
        full_spec = inspect.getfullargspec(self.python_callable)
        op_args = list(self.op_args)
        ret_args = []
        for arg in op_args:
            current_arg = full_spec.args.pop(0)
            if full_spec.annotations[current_arg] == pd.DataFrame and type(arg) == str:
                ret_args.append(self._get_dataframe(arg))
            else:
                ret_args.append(arg)
        self.op_args = tuple(ret_args)

    def handle_op_kwargs(self):
        param_types = inspect.signature(self.python_callable).parameters
        self.op_kwargs = {
            k: self._get_dataframe(v)
            if param_types.get(k).annotation == pd.DataFrame and type(v) == str
            else v
            for k, v in self.op_kwargs.items()
        }

    def execute(self, context: Dict):
        self.conn_type = BaseHook.get_connection(self.conn_id).conn_type
        self.handle_op_args()
        self.handle_op_kwargs()

        return self.python_callable(*self.op_args, **self.op_kwargs)

    def get_snow_hook(self) -> SnowflakeHook:
        """
        Create and return SnowflakeHook.
        :return: a SnowflakeHook instance.
        :rtype: SnowflakeHook
        """
        return SnowflakeHook(
            snowflake_conn_id=self.conn_id,
            warehouse=self.warehouse,
            database=self.database,
            role=None,
            schema=self.schema,
            authenticator=None,
            session_parameters=None,
        )

    def _get_dataframe(self, param):
        if self.conn_type == "postgres":
            from psycopg2 import sql

            self.hook = PostgresHook(
                postgres_conn_id=self.conn_id, schema=self.database
            )
            query = (
                sql.SQL("SELECT * FROM {input_table}")
                .format(input_table=sql.Identifier(param))
                .as_string(self.hook.get_conn())
            )
            return self.hook.get_pandas_df(query)

        elif self.conn_type == "snowflake":
            hook = self.get_snow_hook()
            return hook.get_pandas_df(
                "SELECT * FROM IDENTIFIER(%(input_table)s)",
                parameters={"input_table": param},
            )
