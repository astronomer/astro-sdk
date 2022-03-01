"""
Copyright Astronomer, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import inspect
from typing import Optional

import pandas

from astro.sql.table import Table
from astro.utils.schema_util import get_schema


class TableHandler:
    def _set_variables_from_first_table(self):
        """
        When we create our SQL operation, we run with the assumption that the first table given is the "main table".
        This means that a user doesn't need to define default conn_id, database, etc. in the function unless they want
        to create default values.
        """
        first_table: Optional[Table] = None
        if self.op_args:
            table_index = [x for x, t in enumerate(self.op_args) if type(t) == Table]
            if table_index:
                first_table = self.op_args[table_index[0]]
        elif not first_table:
            table_kwargs = [
                x
                for x in inspect.signature(self.python_callable).parameters.values()
                if (
                    x.annotation == Table
                    and type(self.op_kwargs[x.name]) == Table
                    or x.annotation == pandas.DataFrame
                    and type(self.op_kwargs[x.name]) == Table
                )
            ]
            if table_kwargs:
                first_table = self.op_kwargs[table_kwargs[0].name]

        # If there is no first table via op_ags or kwargs, we check the parameters
        elif not first_table:
            if self.parameters:
                param_tables = [t for t in self.parameters.values() if type(t) == Table]
                if param_tables:
                    first_table = param_tables[0]

        if first_table:
            self.conn_id = first_table.conn_id or self.conn_id
            self.database = first_table.database or self.database
            self.schema = first_table.schema or self.schema
            self.warehouse = first_table.warehouse or self.warehouse
            self.role = first_table.role or self.role

    def populate_output_table(self):
        self.output_table.conn_id = self.output_table.conn_id or self.conn_id
        self.output_table.database = self.output_table.database or self.database
        self.output_table.warehouse = self.output_table.warehouse or self.warehouse
        self.output_table.schema = self.output_table.schema or get_schema()
