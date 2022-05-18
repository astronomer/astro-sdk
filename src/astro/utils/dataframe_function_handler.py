import inspect
from abc import ABC
from typing import Callable, Dict, Optional, Tuple

import pandas as pd

from astro.databases import create_database
from astro.databases.base import BaseDatabase
from astro.sql.tables import Table


class DataframeFunctionHandler(ABC):
    database_impl: BaseDatabase
    output_table: Optional[Table]
    op_args: Tuple
    op_kwargs: Dict
    python_callable: Callable
    identifiers_as_lower: bool = False
    conn_id: str = ""

    def load_op_arg_dataframes_into_sql(self):
        """Identifies dataframes in op_args and loads them to the table"""
        final_args = []
        for arg in self.op_args:
            if isinstance(arg, pd.DataFrame):
                self.database_impl.load_pandas_dataframe_to_table(
                    source_dataframe=arg, target_table=self.output_table
                )
                final_args.append(self.output_table)
            else:
                final_args.append(arg)
            self.op_args = tuple(final_args)

    def load_op_kwarg_dataframes_into_sql(self):
        """Identifies dataframes in op_kwargs and loads them to the table"""
        final_kwargs = {}
        for key, value in self.op_kwargs.items():
            if isinstance(value, pd.DataFrame):
                self.database_impl.load_pandas_dataframe_to_table(
                    source_dataframe=value, target_table=self.output_table
                )
                final_kwargs[key] = self.output_table
            else:
                final_kwargs[key] = value
        self.op_kwargs = final_kwargs

    def load_op_arg_table_into_dataframe(self):
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
        param_types = inspect.signature(self.python_callable).parameters
        self.op_kwargs = {
            k: self._get_dataframe(v)
            if param_types.get(k).annotation is pd.DataFrame and type(v) is Table
            else v
            for k, v in self.op_kwargs.items()
        }

    def _get_dataframe(self, table: Table):
        database = create_database(self.conn_id)
        df = database.export_table_to_pandas_dataframe(source_table=table)
        if self.identifiers_as_lower:
            df.columns = [col_label.lower() for col_label in df.columns]
        return df
