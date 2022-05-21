import inspect
from typing import Callable, Dict, Optional, Tuple

import pandas as pd
from airflow.decorators.base import DecoratedOperator

from astro.databases import create_database
from astro.settings import SCHEMA
from astro.sql.table import Table
from astro.utils import get_hook
from astro.utils.dependencies import SnowflakeHook
from astro.utils.load import load_dataframe_into_sql_table
from astro.utils.table_handler import TableHandler


def _get_dataframe(table: Table, identifiers_as_lower: bool = False) -> pd.DataFrame:
    """
    Exports records from a SQL table and converts it into a pandas dataframe
    """
    database = create_database(table.conn_id)
    df = database.export_table_to_pandas_dataframe(source_table=table)
    if identifiers_as_lower:
        df.columns = [col_label.lower() for col_label in df.columns]
    return df


def load_op_arg_table_into_dataframe(
    op_args: Tuple, python_callable: Callable
) -> Tuple:
    """For dataframe based functions, takes any Table objects from the op_args
    and converts them into local dataframes that can be handled in the python context"""
    full_spec = inspect.getfullargspec(python_callable)
    op_args_list = list(op_args)
    ret_args = []
    for arg in op_args_list:
        current_arg = full_spec.args.pop(0)
        if full_spec.annotations[current_arg] == pd.DataFrame and type(arg) is Table:
            ret_args.append(_get_dataframe(arg))
        else:
            ret_args.append(arg)
    return tuple(ret_args)


def load_op_kwarg_table_into_dataframe(
    op_kwargs: Dict, python_callable: Callable
) -> Dict:
    """For dataframe based functions, takes any Table objects from the op_kwargs
    and converts them into local dataframes that can be handled in the python context"""
    param_types = inspect.signature(python_callable).parameters
    return {
        k: _get_dataframe(v)
        if param_types.get(k).annotation is pd.DataFrame and type(v) is Table  # type: ignore
        else v
        for k, v in op_kwargs.items()
    }


class SqlDataframeOperator(DecoratedOperator, TableHandler):
    def __init__(
        self,
        conn_id: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
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
                ret_args.append(
                    _get_dataframe(arg, identifiers_as_lower=self.identifiers_as_lower)
                )
            else:
                ret_args.append(arg)
        self.op_args = tuple(ret_args)

    def handle_op_kwargs(self):
        param_types = inspect.signature(self.python_callable).parameters
        self.op_kwargs = {
            k: _get_dataframe(v, identifiers_as_lower=self.identifiers_as_lower)
            if param_types.get(k).annotation == pd.DataFrame and type(v) == Table
            else v
            for k, v in self.op_kwargs.items()
        }

    def execute(self, context: Dict):  # skipcq: PYL-W0613
        self._set_variables_from_first_table()
        self.handle_op_args()
        self.handle_op_kwargs()

        pandas_dataframe = self.python_callable(*self.op_args, **self.op_kwargs)
        if self.output_table:
            self.populate_output_table()
            self.output_table.metadata.schema = (  # type: ignore
                self.output_table.metadata.schema or SCHEMA
            )
            hook = get_hook(
                conn_id=self.output_table.conn_id,
                database=self.output_table.metadata.database,
                schema=self.output_table.metadata.schema,
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
            database=table.metadata.database,
            schema=table.metadata.schema,
            authenticator=None,
            session_parameters=None,
        )
