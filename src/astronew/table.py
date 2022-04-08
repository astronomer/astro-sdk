import inspect
import random
import string
from typing import Optional

import pandas
from airflow.hooks.base import BaseHook

from astronew.constants import SCHEMA, UNIQUE_TABLE_NAME_LENGTH


class Table:
    template_fields = (
        "table_name",
        "conn_id",
        "database",
        "schema",
        "warehouse",
        "role",
    )

    def __init__(
        self,
        table_name="",
        conn_id=None,
        database=None,
        schema=None,
        warehouse=None,
        role=None,
    ):
        self.table_name = table_name
        self.conn_id = conn_id
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.role = role
        self._conn_type = None

    @property
    def conn_type(self):
        if self._conn_type:
            return self._conn_type
        self._conn_type = BaseHook.get_connection(self.conn_id).conn_type
        return self._conn_type

    def __str__(self):
        return (
            f"Table(table_name={self.table_name}, database={self.database}, "
            f"schema={self.schema}, conn_id={self.conn_id}, warehouse={self.warehouse}, role={self.role})"
        )

    def get_database(self):
        from .databases import get_db_from_table

        return get_db_from_table(self)


class TempTable(Table):
    def __init__(self, conn_id=None, database=None, schema=None, warehouse="", role=""):
        self.table_name = ""
        super().__init__(
            table_name=self.table_name,
            conn_id=conn_id,
            database=database,
            warehouse=warehouse,
            role=role,
            schema=schema,
        )

    def to_table(self, table_name: str, schema: str = None) -> Table:
        self.table_name = table_name
        self.schema = schema or self.schema or SCHEMA

        return Table(
            table_name=table_name,
            conn_id=self.conn_id,
            database=self.database,
            warehouse=self.warehouse,
            role=self.role,
            schema=self.schema,
        )


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
        self.output_table.schema = self.output_table.schema or SCHEMA


def create_unique_table_name(length: int = UNIQUE_TABLE_NAME_LENGTH) -> str:
    """
    Create a unique table name of the requested size, which is compatible with all supported databases.

    :return: Unique table name
    :rtype: str
    """
    unique_id = random.choice(string.ascii_lowercase) + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(length - 1)
    )
    return unique_id
