from builtins import NotImplementedError
from typing import Callable, Dict, Iterable, Mapping, Optional, Union

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

        if self.op_kwargs.get("df", None):  # type: ignore
            self.table_name = self.op_kwargs.pop("df")  # type: ignore
        else:
            op_arg_list = list(self.op_args)  # type: ignore
            if len(op_arg_list) < 1:
                raise AirflowException(
                    "Please either supply a df kwarg or one arg so we can render your dataframe"
                )
            self.op_args = tuple(op_arg_list[1:])
            self.table_name = op_arg_list[0]

        super().__init__(
            **kwargs,
        )

    def execute(self, context: Dict):
        self.conn_type = BaseHook.get_connection(self.conn_id).conn_type

        self.op_kwargs["df"] = self._get_dataframe()  # type: ignore

        return self.python_callable(**self.op_kwargs)

        # To-do: Type check `sql_stuff`

        # If we return two things, assume the second thing is the params

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

    def _get_dataframe(self):
        if self.conn_type == "postgres":
            from psycopg2 import sql

            self.hook = PostgresHook(
                postgres_conn_id=self.conn_id, schema=self.database
            )
            query = (
                sql.SQL("SELECT * FROM {input_table}")
                .format(input_table=sql.Identifier(self.table_name))
                .as_string(self.hook.get_conn())
            )
            return self.hook.get_pandas_df(query)

        elif self.conn_type == "snowflake":
            hook = self.get_snow_hook()
            return hook.get_pandas_df(
                "SELECT * FROM IDENTIFIER(%(input_table)s)",
                parameters={"input_table": self.table_name},
            )
