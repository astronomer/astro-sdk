import inspect
from typing import Optional

import pandas

from astro.settings import SCHEMA
from astro.sql.table import Table


class TableHandler:
    def _set_variables_from_first_table(self):
        """
        When we create our SQL operation, we run with the assumption that the first table given is the "main table".
        This means that a user doesn't need to define default conn_id, database, etc. in the function unless they want
        to create default values.
        """
        first_table: Optional[Table] = None
        if self.op_args:
            table_index = [
                x for x, t in enumerate(self.op_args) if isinstance(t, Table)
            ]
            conn_id_set = {x.conn_id for x in self.op_args if isinstance(x, Table)}
            # Check to see if all tables belong to same conn_id. Otherwise, we this can go wrong for cases
            # 1. When we have tables from different DBs.
            # 2. When we have tables from different conn_id, since they can be configured with different
            # database/schema etc.
            if table_index and len(conn_id_set) == 1:
                first_table = self.op_args[table_index[0]]

        if not first_table and self.op_kwargs and self.python_callable:
            table_kwargs = [
                x
                for x in inspect.signature(self.python_callable).parameters.values()
                if (
                    x.annotation == Table
                    and isinstance(self.op_kwargs[x.name], Table)
                    or x.annotation == pandas.DataFrame
                    and isinstance(self.op_kwargs[x.name], Table)
                )
            ]
            conn_id_set = {
                self.op_kwargs[x.name].conn_id
                for x in inspect.signature(self.python_callable).parameters.values()
                if (
                    x.annotation == Table
                    and isinstance(self.op_kwargs[x.name], Table)
                    or x.annotation == pandas.DataFrame
                    and isinstance(self.op_kwargs[x.name], Table)
                )
            }
            if table_kwargs and len(conn_id_set) == 1:
                first_table = self.op_kwargs[table_kwargs[0].name]

        # If there is no first table via op_ags or kwargs, we check the parameters
        if not first_table and self.parameters:
            if self.parameters:
                param_tables = [
                    t for t in self.parameters.values() if isinstance(t, Table)
                ]
                conn_id_set = {
                    t.conn_id for t in self.parameters.values() if isinstance(t, Table)
                }
                if param_tables and len(conn_id_set) == 1:
                    first_table = param_tables[0]

        if first_table:
            self.conn_id = first_table.conn_id or self.conn_id
            self.database = first_table.metadata.database or self.database
            self.schema = first_table.metadata.schema or self.schema

    def populate_output_table(self):
        self.output_table.conn_id = self.output_table.conn_id or self.conn_id
        self.output_table.metadata.database = (
            self.output_table.metadata.database or self.database
        )
        self.output_table.metadata.schema = self.output_table.metadata.schema or SCHEMA
