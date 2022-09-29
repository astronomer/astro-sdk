from __future__ import annotations

import inspect
from typing import Any, Callable

import pandas as pd
from airflow.decorators.base import DecoratedOperator
from astro.airflow.datasets import kwargs_with_datasets

try:
    from airflow.decorators.base import TaskDecorator, task_decorator_factory
except ImportError:
    from airflow.decorators.base import task_decorator_factory
    from airflow.decorators import _TaskDecorator as TaskDecorator

from astro.constants import ColumnCapitalization
from astro.databases import create_database
from astro.files import File
from astro.sql.operators.base_operator import AstroSQLBaseOperator
from astro.sql.table import BaseTable, Table
from astro.utils.dataframe import convert_columns_names_capitalization, convert_to_file
from astro.utils.serializer import deserialize, serialize
from astro.utils.table import find_first_table
from astro.utils.typing_compat import Context


def _get_dataframe(
        table: BaseTable, columns_names_capitalization: ColumnCapitalization = "lower"
) -> pd.DataFrame:
    """
    Exports records from a SQL table and converts it into a pandas dataframe
    """
    database = create_database(table.conn_id)
    df = database.export_table_to_pandas_dataframe(source_table=table)
    df = convert_columns_names_capitalization(
        df=df, columns_names_capitalization=columns_names_capitalization
    )

    return df


def load_op_arg_table_into_dataframe(
        op_args: tuple,
        python_callable: Callable,
        columns_names_capitalization: ColumnCapitalization,
) -> tuple:
    """For dataframe based functions, takes any Table objects from the op_args
    and converts them into local dataframes that can be handled in the python context"""
    full_spec = inspect.getfullargspec(python_callable)
    op_args_list = list(op_args)
    ret_args = []
    # We check if the type annotation is of type dataframe to determine that the user actually WANTS
    # this table to be converted into a dataframe, rather that passed in as a table
    for arg in op_args_list:
        current_arg = full_spec.args.pop(0)
        if full_spec.annotations.get(current_arg) == pd.DataFrame and isinstance(
                arg, BaseTable
        ):
            ret_args.append(
                _get_dataframe(
                    arg, columns_names_capitalization=columns_names_capitalization
                )
            )
        elif isinstance(
                arg, File
        ) and (full_spec.annotations.get(current_arg) == pd.DataFrame or arg.is_dataframe):
            ret_args.append(arg.export_to_dataframe())
        else:
            ret_args.append(arg)
    return tuple(ret_args)


def load_op_kwarg_table_into_dataframe(
        op_kwargs: dict,
        python_callable: Callable,
        columns_names_capitalization: ColumnCapitalization,
) -> dict:
    """For dataframe based functions, takes any Table objects from the op_kwargs
    and converts them into local dataframes that can be handled in the python context"""
    param_types = inspect.signature(python_callable).parameters
    # We check if the type annotation is of type dataframe to determine that the user actually WANTS
    # this table to be converted into a dataframe, rather that passed in as a table
    out_dict = {}
    for k, v in op_kwargs.items():
        if param_types.get(k).annotation is pd.DataFrame and isinstance(v, BaseTable):  # type: ignore
            out_dict[k] = _get_dataframe(
                v, columns_names_capitalization=columns_names_capitalization
            )
        elif param_types.get(k).annotation is pd.DataFrame and isinstance(v, File):  # type: ignore
            out_dict[k] = v.export_to_dataframe()
        else:
            out_dict[k] = v
    return out_dict


class DataframeOperator(AstroSQLBaseOperator, DecoratedOperator):
    """
    Converts a SQL table into a dataframe. Users can then give a python function that takes a dataframe as
    one of its inputs and run that python function. Once that function has completed, the result is accessible
    via the Taskflow API.

    :param conn_id: Connection to the DB that you will pull the table from
    :param database: Database for input table
    :param schema:  schema for input table
    :param warehouse: (Snowflake) Which warehouse to use for the input table
    :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
        in the resulting dataframe
    :param kwargs: Any keyword arguments supported by the BaseOperator is supported (e.g ``queue``, ``owner``)
    :return: If ``raw_sql`` is true, we return the result of the handler function, otherwise we will return the
        generated output_table.
    """

    def __init__(
            self,
            conn_id: str | None = None,
            database: str | None = None,
            schema: str | None = None,
            columns_names_capitalization: ColumnCapitalization = "lower",
            **kwargs,
    ):
        self.conn_id: str = conn_id or ""
        self.database = database
        self.schema = schema
        self.parameters = None
        self.kwargs = kwargs or {}
        self.op_kwargs: dict = self.kwargs.get("op_kwargs") or {}
        if self.op_kwargs.get("output_table"):
            self.output_table: BaseTable | None = self.op_kwargs.pop("output_table")
        else:
            self.output_table = None
        self.op_args = self.kwargs.get("op_args", ())  # type: ignore
        self.columns_names_capitalization = columns_names_capitalization

        # We purposely do NOT render upstream_tasks otherwise we could have a case where a user
        # has 10 dataframes as upstream tasks and it crashes the worker
        upstream_tasks = self.op_kwargs.pop("upstream_tasks", [])
        super().__init__(
            upstream_tasks=upstream_tasks,
            **kwargs_with_datasets(kwargs=kwargs, output_datasets=self.output_table),
        )

    def execute(self, context: Context) -> Table | pd.DataFrame | list:
        self.op_args = [deserialize(i) for i in self.op_args]
        self.op_kwargs = {k: deserialize(v) for k, v in self.op_kwargs.items()}
        first_table = find_first_table(
            op_args=self.op_args,  # type: ignore
            op_kwargs=self.op_kwargs,
            python_callable=self.python_callable,
            parameters=self.parameters or {},  # type: ignore
        )
        if first_table:
            self.conn_id = self.conn_id or first_table.conn_id  # type: ignore
            self.database = self.database or first_table.metadata.database  # type: ignore
            self.schema = self.schema or first_table.metadata.schema  # type: ignore
        self.op_args = load_op_arg_table_into_dataframe(
            self.op_args, self.python_callable, self.columns_names_capitalization
        )
        self.op_kwargs = load_op_kwarg_table_into_dataframe(
            self.op_kwargs, self.python_callable, self.columns_names_capitalization
        )

        function_output = self.python_callable(*self.op_args, **self.op_kwargs)
        pandas_dataframe = convert_columns_names_capitalization(
            df=function_output,
            columns_names_capitalization=self.columns_names_capitalization,
        )
        if self.output_table:
            self.output_table.conn_id = self.output_table.conn_id or self.conn_id
            db = create_database(self.output_table.conn_id)
            self.output_table = db.populate_table_metadata(self.output_table)
            db.load_pandas_dataframe_to_table(
                source_dataframe=pandas_dataframe,
                target_table=self.output_table,
                if_exists="replace",
            )
            return serialize(self.output_table)
        else:
            if isinstance(function_output, pd.DataFrame):
                return serialize(convert_to_file(pandas_dataframe))
            elif isinstance(function_output, list):
                return [
                    serialize(convert_to_file(obj))
                    if isinstance(obj, pd.DataFrame)
                    else serialize(obj)
                    for obj in function_output
                ]
            else:
                return serialize(function_output)


def dataframe(
        python_callable: Callable | None = None,
        multiple_outputs: bool | None = None,
        conn_id: str = "",
        database: str | None = None,
        schema: str | None = None,
        columns_names_capitalization: ColumnCapitalization = "lower",
        **kwargs: Any,
) -> TaskDecorator:
    """
    This decorator will allow users to write python functions while treating SQL tables as dataframes

    This decorator allows a user to run python functions in Airflow but with the huge benefit that SQL tables
    will automatically be turned into dataframes and resulting dataframes can automatically used in astro.sql functions

    :param python_callable: This parameter is filled in automatically when you use the dataframe function
        as a decorator. This is where the python function gets passed to the wrapping function
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys. Defaults to False.
    :param conn_id: Connection ID for the database you want to connect to. If you do
        not pass in a value for this object
        we can infer the connection ID from the first table passed into the python_callable function.
        (required if there are no table arguments)
    :param database: Database within the SQL instance you want to access. If left blank we will
        default to the table.metatadata.database in the first Table passed
        to the function (required if there are no table arguments)
    :param schema: Schema within the SQL instance you want to access. If left blank we will
        default to the table.metatadata.schema in the first Table passed to the
        function (required if there are no table arguments)
    :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
        in the resulting dataframe
    :param kwargs: Any keyword arguments supported by the BaseOperator is supported (e.g ``queue``, ``owner``)
    """
    kwargs.update(
        {
            "conn_id": conn_id,
            "database": database,
            "schema": schema,
            "columns_names_capitalization": columns_names_capitalization,
        }
    )
    decorated_function = task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=DataframeOperator,  # type: ignore
        **kwargs,
    )
    return decorated_function
