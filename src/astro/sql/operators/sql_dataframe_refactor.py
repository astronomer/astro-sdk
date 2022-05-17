from typing import Dict, Optional

from airflow.decorators.base import DecoratedOperator

from astro.databases import create_database
from astro.settings import SCHEMA
from astro.sql.table import Table as OldTable
from astro.sql.table import TempTable
from astro.sql.tables import Metadata
from astro.sql.tables import Table as NewTable
from astro.utils.dataframe_function_handler import DataframeFunctionHandler
from astro.utils.table_handler_new import TableHandler


class SqlDataframeOperator(DataframeFunctionHandler, DecoratedOperator, TableHandler):
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
        self.parameters = {}
        self.kwargs = kwargs or {}
        self.op_kwargs: Dict = self.kwargs.get("op_kwargs") or {}
        if self.op_kwargs.get("output_table"):
            self.output_table: Optional[NewTable] = self.op_kwargs.pop("output_table")
        else:
            self.output_table = None
        self.op_args = self.kwargs.get("op_args")  # type: ignore
        self.identifiers_as_lower = identifiers_as_lower

        super().__init__(
            **kwargs,
        )

    @staticmethod
    def convert_old_table_to_new(table):
        """
        This function is only temporary until other functions use the new table format.

        Converts a TempTable or a Table object into the new Table format.
        :param table:
        :return:
        """
        if isinstance(table, TempTable):
            table = table.to_table(None)
        if isinstance(table, OldTable):
            table = NewTable(
                conn_id=table.conn_id,
                name=table.table_name,
                metadata=Metadata(
                    schema=table.schema,
                    warehouse=table.warehouse,
                    database=table.database,
                ),
            )
        return table

    def handle_conversions(self):
        """
        This is a temporary holdover until all other functions use the new table format.
        Converts old tables to new tables for op_args and op_kwargs.
        :return:
        """
        self.op_args = tuple(
            self.convert_old_table_to_new(t) if isinstance(t, OldTable) else t
            for t in self.op_args  # type: ignore
        )  # type: ignore
        self.op_kwargs = {
            k: self.convert_old_table_to_new(t) if isinstance(t, OldTable) else t
            for k, t in self.op_kwargs.items()
        }

    def execute(self, context: Dict):  # skipcq
        self.handle_conversions()
        self._set_variables_from_first_table()
        self.load_op_arg_table_into_dataframe()
        self.load_op_kwarg_table_into_dataframe()

        pandas_dataframe = self.python_callable(*self.op_args, **self.op_kwargs)
        if self.output_table:
            self.output_table = self.convert_old_table_to_new(self.output_table)
            self.populate_output_table()
            self.conn_id = self.conn_id or self.output_table.conn_id
            self.output_table.metadata.schema = (
                self.output_table.metadata.schema or SCHEMA
            )
            database = create_database(self.conn_id)
            database.load_pandas_dataframe_to_table(
                source_dataframe=pandas_dataframe, target_table=self.output_table
            )
            return self.output_table
        return pandas_dataframe
