import inspect
from typing import Callable, Dict, Optional, Tuple, Union

import pandas as pd
from airflow.decorators.base import DecoratedOperator

from astro.databases import create_database
from astro.sql.table import Table
from astro.utils.table import find_first_table


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
    op_args: Tuple, python_callable: Callable, identifiers_as_lower: bool
) -> Tuple:
    """For dataframe based functions, takes any Table objects from the op_args
    and converts them into local dataframes that can be handled in the python context"""
    full_spec = inspect.getfullargspec(python_callable)
    op_args_list = list(op_args)
    ret_args = []
    # We check if the type annotation is of type dataframe to determine that the user actually WANTS
    # this table to be converted into a dataframe, rather that passed in as a table
    for arg in op_args_list:
        current_arg = full_spec.args.pop(0)
        if full_spec.annotations[current_arg] == pd.DataFrame and isinstance(
            arg, Table
        ):
            ret_args.append(
                _get_dataframe(arg, identifiers_as_lower=identifiers_as_lower)
            )
        else:
            ret_args.append(arg)
    return tuple(ret_args)


def load_op_kwarg_table_into_dataframe(
    op_kwargs: Dict, python_callable: Callable, identifiers_as_lower
) -> Dict:
    """For dataframe based functions, takes any Table objects from the op_kwargs
    and converts them into local dataframes that can be handled in the python context"""
    param_types = inspect.signature(python_callable).parameters
    # We check if the type annotation is of type dataframe to determine that the user actually WANTS
    # this table to be converted into a dataframe, rather that passed in as a table
    return {
        k: _get_dataframe(v, identifiers_as_lower=identifiers_as_lower)
        if param_types.get(k).annotation is pd.DataFrame and isinstance(v, Table)  # type: ignore
        else v
        for k, v in op_kwargs.items()
    }


class DataframeOperator(DecoratedOperator):
    def __init__(
        self,
        conn_id: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        identifiers_as_lower: bool = True,
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
        :param identifiers_as_lower: determines whether to force all columns to lowercase in the resulting dataframe
        :param kwargs:

        :return: If ``raw_sql`` is true, we return the result of the handler function, otherwise we will return the
        generated output_table.
        """
        self.conn_id: str = conn_id or ""
        self.database = database
        self.schema = schema
        self.parameters = None
        self.kwargs = kwargs or {}
        self.op_kwargs: Dict = self.kwargs.get("op_kwargs") or {}
        if self.op_kwargs.get("output_table"):
            self.output_table: Optional[Table] = self.op_kwargs.pop("output_table")
        else:
            self.output_table = None
        self.op_args = self.kwargs.get("op_args", ())  # type: ignore
        self.identifiers_as_lower = identifiers_as_lower

        super().__init__(
            **kwargs,
        )

    def execute(self, context: Dict) -> Union[Table, pd.DataFrame]:
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
            self.op_args, self.python_callable, self.identifiers_as_lower
        )
        self.op_kwargs = load_op_kwarg_table_into_dataframe(
            self.op_kwargs, self.python_callable, self.identifiers_as_lower
        )

        pandas_dataframe = self.python_callable(*self.op_args, **self.op_kwargs)
        if self.output_table:
            self.output_table.conn_id = self.output_table.conn_id or self.conn_id
            db = create_database(self.output_table.conn_id)
            self.output_table = db.populate_table_metadata(self.output_table)
            db.load_pandas_dataframe_to_table(
                source_dataframe=pandas_dataframe,
                target_table=self.output_table,
                if_exists="replace",
            )
            return self.output_table
        else:
            return pandas_dataframe
