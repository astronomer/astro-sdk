import inspect
from typing import Callable, Dict, Optional

import pandas as pd
from airflow.decorators.base import DecoratedOperator, task_decorator_factory

from astro.utils.table_handler import TableHandler
from astronew.constants import SCHEMA
from astronew.table import Table, TempTable


class SqlDataframeOperator(DecoratedOperator, TableHandler):
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

        super().__init__(**kwargs)

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
                db = arg.get_database()
                ret_args.append(db.get_pandas_dataframe())
            else:
                ret_args.append(arg)
        self.op_args = tuple(ret_args)

    def handle_op_kwargs(self):
        param_types = inspect.signature(self.python_callable).parameters
        self.op_kwargs = {
            k: v.get_database().get_pandas_dataframe()
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
            output_db = self.output_table.get_database()
            if type(self.output_table) == TempTable:
                table_name = output_db.generate_table_name(context=context)
                self.output_table = self.output_table.to_table(
                    table_name=table_name, schema=SCHEMA
                )
            output_db.load_pandas_dataframe(pandas_dataframe=pandas_dataframe)
            return self.output_table
        else:
            return pandas_dataframe


def dataframe(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "",
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
    task_id: Optional[str] = None,
    identifiers_as_lower: Optional[bool] = True,
):
    """
    This function allows a user to run python functions in Airflow but with the huge benefit that SQL files
    will automatically be turned into dataframes and resulting dataframes can automatically used in astro.sql functions
    """
    param_map = {
        "conn_id": conn_id,
        "database": database,
        "schema": schema,
        "warehouse": warehouse,
        "identifiers_as_lower": identifiers_as_lower,
    }
    if task_id:
        param_map["task_id"] = task_id
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=SqlDataframeOperator,  # type: ignore
        **param_map,
    )
