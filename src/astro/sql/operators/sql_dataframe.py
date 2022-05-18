from typing import Dict

from airflow.decorators.base import DecoratedOperator

from astro.databases import create_database
from astro.settings import SCHEMA
from astro.sql.table import Table
from astro.utils.dataframe_function_handler import DataframeFunctionHandler
from astro.utils.table_handler import TableHandler


class SqlDataframeOperator(DataframeFunctionHandler, DecoratedOperator, TableHandler):
    """Helper class for `astro.df` decorator"""

    def __init__(
        self,
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
        :param kwargs:
        """
        self.parameters: Dict = {}
        self.kwargs = kwargs or {}
        self.write_to_table = False
        self.op_kwargs: Dict = self.kwargs.get("op_kwargs") or {}
        if self.op_kwargs.get("output_table"):
            self.output_table: Table = self.op_kwargs.pop("output_table")
            self.write_to_table = True
        self.op_args = self.kwargs.get("op_args")  # type: ignore
        self.identifiers_as_lower = identifiers_as_lower

        super().__init__(
            **kwargs,
        )

    def execute(self, context: Dict):  # skipcq
        self._set_variables_from_first_table()
        self.load_op_arg_table_into_dataframe()
        self.load_op_kwarg_table_into_dataframe()

        pandas_dataframe = self.python_callable(*self.op_args, **self.op_kwargs)
        if self.write_to_table:
            self.output_table.metadata.schema = (
                self.output_table.metadata.schema or SCHEMA
            )
            database = create_database(self.output_table.conn_id)
            database.load_pandas_dataframe_to_table(
                source_dataframe=pandas_dataframe, target_table=self.output_table
            )
            return self.output_table
        return pandas_dataframe
