import inspect
from typing import Callable, Dict, Optional, Tuple, Union

import pandas as pd
from airflow.configuration import conf
from airflow.decorators.base import DecoratedOperator

from astro.constants import ColumnCapitalization
from astro.databases import create_database
from astro.exceptions import IllegalLoadToDatabaseException
from astro.sql.table import Table
from astro.utils.dataframe import convert_columns_names_capitalization
from astro.utils.table import find_first_table


def _get_dataframe(
    table: Table, columns_names_capitalization: ColumnCapitalization = "lower"
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
    op_args: Tuple,
    python_callable: Callable,
    columns_names_capitalization: ColumnCapitalization,
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
                _get_dataframe(
                    arg, columns_names_capitalization=columns_names_capitalization
                )
            )
        else:
            ret_args.append(arg)
    return tuple(ret_args)


def load_op_kwarg_table_into_dataframe(
    op_kwargs: Dict,
    python_callable: Callable,
    columns_names_capitalization: ColumnCapitalization,
) -> Dict:
    """For dataframe based functions, takes any Table objects from the op_kwargs
    and converts them into local dataframes that can be handled in the python context"""
    param_types = inspect.signature(python_callable).parameters
    # We check if the type annotation is of type dataframe to determine that the user actually WANTS
    # this table to be converted into a dataframe, rather that passed in as a table
    return {
        k: _get_dataframe(v, columns_names_capitalization=columns_names_capitalization)
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
        columns_names_capitalization: ColumnCapitalization = "lower",
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
        :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
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
        self.columns_names_capitalization = columns_names_capitalization

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
            self.op_args, self.python_callable, self.columns_names_capitalization
        )
        self.op_kwargs = load_op_kwarg_table_into_dataframe(
            self.op_kwargs, self.python_callable, self.columns_names_capitalization
        )

        pandas_dataframe = self.python_callable(*self.op_args, **self.op_kwargs)
        pandas_dataframe = convert_columns_names_capitalization(
            df=pandas_dataframe,
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
            return self.output_table
        else:
            if conf.get(
                "core", "xcom_backend"
            ) == "airflow.models.xcom.BaseXCom" and not conf.getboolean(
                "astro_sdk", "dataframe_allow_unsafe_storage"
            ):
                raise IllegalLoadToDatabaseException()
            return pandas_dataframe
