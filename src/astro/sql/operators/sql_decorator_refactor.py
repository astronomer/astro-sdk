import inspect
from typing import Dict, Optional

import pandas as pd
from airflow.decorators.base import DecoratedOperator
from sqlalchemy.sql.functions import Function

from astro.databases import create_database
from astro.sql.table import Table as OldTable
from astro.sql.tables import Metadata, Table
from astro.utils.sql_refactor import SQL
from astro.utils.table_handler_new import TableHandler


class SqlDecoratedOperator(SQL, DecoratedOperator, TableHandler):
    def __init__(
        self,
        conn_id: Optional[str] = None,
        autocommit: bool = False,
        parameters: Optional[dict] = None,
        handler: Optional[Function] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        raw_sql=False,
        sql="",
        **kwargs,
    ):
        self.kwargs = kwargs or {}
        self.op_kwargs: Dict = self.kwargs.get("op_kwargs") or {}
        if self.op_kwargs.get("output_table"):
            self.output_table: Optional[Table] = self.op_kwargs.pop("output_table")
        else:
            self.output_table = None

        if self.op_kwargs.get("handler"):
            self.handler = self.op_kwargs.pop("handler")

        super().__init__(
            sql=sql,
            autocommit=autocommit,
            handler=handler,
            parameters=parameters,
            conn_id=self.op_kwargs.pop("conn_id", conn_id),
            database=self.op_kwargs.pop("database", database),
            schema=self.op_kwargs.pop("schema", schema),
            warehouse=self.op_kwargs.pop("warehouse", warehouse),
            role=self.op_kwargs.pop("role", role),
            raw_sql=raw_sql,
            **kwargs,
        )

    def convert_old_table_to_new(self, table):
        return Table(
            conn_id=table.conn_id,
            name=table.table_name,
            metadata=Metadata(
                schema=table.schema, warehouse=table.warehouse, database=table.database
            ),
        )

    def execute(self, context: Dict):
        self.handle_conversions()

        self._set_variables_from_first_table()
        self.database_impl = create_database(self.conn_id)
        self.load_op_arg_dataframes_into_sql()
        self.load_op_kwarg_dataframes_into_sql()
        self.read_sql_from_function()
        self.handle_params(context)
        self.template(context)
        self.handle_schema()
        self.create_output_table(self.output_table_name)
        if self.raw_sql:
            result = self.database_impl.run_sql(
                sql_statement=self.sql, parameters=self.parameters
            )
            if self.handler:
                return self.handler(result)
        else:
            self.database_impl.create_table_from_select_statement(
                statement=self.sql,
                target_table=self.output_table,
                parameters=self.parameters,
            )
            return self.output_table

    def handle_conversions(self):
        """
        This is a temporary holdover until all other functions use the new table format
        :return:
        """
        self.op_args = [
            self.convert_old_table_to_new(t) if isinstance(t, OldTable) else t
            for t in self.op_args  # type: ignore
        ]  # type: ignore
        self.op_kwargs = {
            k: self.convert_old_table_to_new(t) if isinstance(t, OldTable) else t
            for k, t in self.op_kwargs.items()
        }

    def create_output_table(self, output_table_name):
        if self.output_table:
            self.populate_output_table()
            self.log.info("Returning table %s", self.output_table)
            return self.output_table
        else:
            self.output_table = Table(
                name=output_table_name,
            )
            self.populate_output_table()
            self.log.info("Returning table %s", self.output_table)
            return self.output_table

    def read_sql_from_function(self):
        """
        This function runs the provided python function and stores the resulting
        SQL query in the `sql` attribute. We can also store parameters if the user
        provides a dictionary.
        :return:
        """
        if self.sql == "":
            sql_stuff = self.python_callable(*self.op_args, **self.op_kwargs)
            # If we return two things, assume the second thing is the params
            if len(sql_stuff) == 2:
                self.sql, self.parameters = sql_stuff
            else:
                self.sql = sql_stuff
                self.parameters = {}
        elif self.sql[-4:] == ".sql":
            with open(self.sql) as file:
                self.sql = file.read().replace("\n", " ")

    # TODO: Is this function doing too many things?
    def handle_params(self, context):
        """
        This function does the following:

        1. Pull values from the function op_args and op_kwargs and places them into
        parameters for SQLAlchemy to parse
        2. Renders each parameter given the current task context
        :param context:
        :return:
        """
        if self.op_kwargs:
            self.parameters.update(self.op_kwargs)  # type: ignore
        if self.op_args:
            params = list(inspect.signature(self.python_callable).parameters.keys())
            for i, arg in enumerate(self.op_args):
                self.parameters[params[i]] = arg  # type: ignore
        if context:
            self.parameters = {
                k: self.render_template(v, context) for k, v in self.parameters.items()  # type: ignore
            }
        # TODO why are there two of this line?
        self.parameters.update(self.op_kwargs)  # type: ignore

    def load_op_arg_dataframes_into_sql(self):
        final_args = []
        for i, arg in enumerate(self.op_args):
            if type(arg) == pd.DataFrame:
                output_table = Table(
                    conn_id=self.conn_id,
                    metadata=Metadata(
                        database=self.database,
                        schema=self.schema,
                        warehouse=self.warehouse,
                    ),
                )
                self.database_impl.load_pandas_dataframe_to_table(
                    source_dataframe=arg, target_table=output_table
                )
                final_args.append(output_table)
            else:
                final_args.append(arg)
            self.op_args = tuple(final_args)

    def load_op_kwarg_dataframes_into_sql(self):
        final_kwargs = {}
        for key, value in self.op_kwargs.items():
            if type(value) == pd.DataFrame:
                output_table = Table(
                    conn_id=self.conn_id,
                    metadata=Metadata(
                        database=self.database,
                        schema=self.schema,
                        warehouse=self.warehouse,
                    ),
                )
                self.database_impl.load_pandas_dataframe_to_table(
                    source_dataframe=value, target_table=output_table
                )
                final_kwargs[key] = output_table
            else:
                final_kwargs[key] = value
        self.op_kwargs = final_kwargs
