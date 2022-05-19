import inspect
from typing import Dict, Optional

import pandas as pd
from airflow.decorators.base import DecoratedOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from astro.constants import Database
from astro.settings import SCHEMA
from astro.sql.table import Table, TempTable, create_table_name
from astro.sqlite_utils import create_sqlalchemy_engine_with_sqlite
from astro.utils import get_hook
from astro.utils.database import get_database_from_conn_id
from astro.utils.dependencies import (
    BigQueryHook,
    PostgresHook,
    SnowflakeHook,
    postgres_sql,
)
from astro.utils.load import load_dataframe_into_sql_table
from astro.utils.table_handler import TableHandler


class SqlDataframeOperator(DecoratedOperator, TableHandler):
    def __init__(
        self,
        conn_id: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        identifiers_as_lower: Optional[bool] = True,
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
        self.role = role
        self.parameters = None
        self.kwargs = kwargs or {}
        self.op_kwargs: Dict = self.kwargs.get("op_kwargs") or {}
        if self.op_kwargs.get("output_table"):
            self.output_table: Optional[Table] = self.op_kwargs.pop("output_table")
        else:
            self.output_table = None
        self.op_args = self.kwargs.get("op_args")  # type: ignore
        self.identifiers_as_lower = identifiers_as_lower

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
        self._set_variables_from_first_table()
        self.handle_op_args()
        self.handle_op_kwargs()

        pandas_dataframe = self.python_callable(*self.op_args, **self.op_kwargs)
        if self.output_table:
            self.populate_output_table()
            if type(self.output_table) == TempTable:
                self.output_table = self.output_table.to_table(
                    table_name=create_table_name(context=context), schema=SCHEMA
                )
            self.output_table.schema = self.output_table.schema or SCHEMA
            hook = get_hook(
                conn_id=self.output_table.conn_id,
                database=self.output_table.database,
                schema=self.output_table.schema,
                warehouse=self.output_table.warehouse,
            )
            load_dataframe_into_sql_table(pandas_dataframe, self.output_table, hook)
            return self.output_table
        else:
            return pandas_dataframe

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
            role=self.role,
            schema=table.schema,
            authenticator=None,
            session_parameters=None,
        )

    def _get_dataframe(self, table: Table):
        database = get_database_from_conn_id(table.conn_id)
        self.log.info(f"Getting dataframe for {table}")
        if database in (Database.POSTGRES, Database.POSTGRESQL):
            self.hook = PostgresHook(
                postgres_conn_id=table.conn_id, schema=table.database
            )
            schema = table.schema or SCHEMA
            query = (
                postgres_sql.SQL("SELECT * FROM {schema}.{input_table}")
                .format(
                    schema=postgres_sql.Identifier(schema),
                    input_table=postgres_sql.Identifier(table.table_name),
                )
                .as_string(self.hook.get_conn())
            )
            df = self.hook.get_pandas_df(query)
        elif database == Database.SNOWFLAKE:
            hook = self.get_snow_hook(table)
            df = hook.get_pandas_df(
                "SELECT * FROM IDENTIFIER(%(input_table)s)",
                parameters={"input_table": table.table_name},
            )
        elif database == Database.SQLITE:
            hook = SqliteHook(sqlite_conn_id=table.conn_id, database=table.database)
            engine = create_sqlalchemy_engine_with_sqlite(hook)
            df = pd.read_sql_table(table.table_name, engine)
        elif database == Database.BIGQUERY:
            hook = BigQueryHook(gcp_conn_id=table.conn_id)
            engine = hook.get_sqlalchemy_engine()
            df = pd.read_sql_table(table.qualified_name(), engine)

        if self.identifiers_as_lower:
            df.columns = [col_label.lower() for col_label in df.columns]
        return df
