import inspect
from abc import ABC
from typing import Callable, Dict, Optional, Tuple

import pandas as pd

from astro.databases import create_database
from astro.databases.base import BaseDatabase
from astro.sql.table import Table


def load_op_arg_dataframes_into_sql(conn_id, op_args, target_table):
    """Identifies dataframes in op_args and loads them to the table"""
    final_args = []
    database = create_database(conn_id=conn_id)
    for arg in op_args:
        if isinstance(arg, pd.DataFrame):
            database.load_pandas_dataframe_to_table(
                source_dataframe=arg, target_table=target_table
            )
            final_args.append(target_table)
        elif isinstance(arg, Table):
            arg = database.populate_table_metadata(arg)
            final_args.append(arg)
        else:
            final_args.append(arg)
        return tuple(final_args)


def load_op_kwarg_dataframes_into_sql(conn_id, op_kwargs, target_table):
    """Identifies dataframes in op_kwargs and loads them to the table"""
    final_kwargs = {}
    database = create_database(conn_id=conn_id)
    for key, value in op_kwargs.items():
        if isinstance(value, pd.DataFrame):
            df_table = target_table.create_new_table()
            database.load_pandas_dataframe_to_table(
                source_dataframe=value, target_table=df_table
            )
            final_kwargs[key] = df_table
        elif isinstance(value, Table):
            value = database.populate_table_metadata(value)
            final_kwargs[key] = value
        else:
            final_kwargs[key] = value
    return final_kwargs


class DataframeFunctionHandler(ABC):
    """Contains functions for converting to dataframe or converting from dataframe"""

    database_impl: BaseDatabase
    output_table: Optional[Table]
    op_args: Tuple
    op_kwargs: Dict
    python_callable: Callable
    identifiers_as_lower: bool = False
    conn_id: str = ""

    def load_op_arg_table_into_dataframe(self):
        """For dataframe based functions, takes any Table objects from the op_args
        and converts them into local dataframes that can be handled in the python context"""
        full_spec = inspect.getfullargspec(self.python_callable)
        op_args = list(self.op_args)
        ret_args = []
        for arg in op_args:
            current_arg = full_spec.args.pop(0)
            if (
                full_spec.annotations[current_arg] == pd.DataFrame
                and type(arg) is Table
            ):
                ret_args.append(self._get_dataframe(arg))
            else:
                ret_args.append(arg)
        self.op_args = tuple(ret_args)

    def load_op_kwarg_table_into_dataframe(self):
        """For dataframe based functions, takes any Table objects from the op_kwargs
        and converts them into local dataframes that can be handled in the python context"""
        param_types = inspect.signature(self.python_callable).parameters
        self.op_kwargs = {
            k: self._get_dataframe(v)
            if param_types.get(k).annotation is pd.DataFrame and type(v) is Table
            else v
            for k, v in self.op_kwargs.items()
        }

    def _get_dataframe(self, table: Table):
        """
        grabs a SQL table and converts it into a dataframe
        :param table:
        :return:
        """
        database = create_database(self.conn_id)
        df = database.export_table_to_pandas_dataframe(source_table=table)
        if self.identifiers_as_lower:
            df.columns = [col_label.lower() for col_label in df.columns]
        return df
