import inspect
from abc import ABC
from typing import Callable, Optional, Tuple

from astro.sql.tables import Metadata, Table


class TableHandler(ABC):
    """This class contains any functions involving modifying or reading from a Table object"""

    op_args: Tuple
    op_kwargs: dict
    python_callable: Callable
    parameters: dict
    output_table: Optional[Table]
    warehouse: str = ""
    schema: str = ""
    database: str = ""
    conn_id: str = ""
    role: str = ""

    def _set_variables_from_first_table(self):
        """
        When we create our SQL operation, we run with the assumption that the first table given is the "main table".
        This means that a user doesn't need to define default conn_id, database, etc. in the function unless they want
        to create default values.
        """
        first_table: Optional[Table] = None
        if self.op_args:
            args_of_table_type = [arg for arg in self.op_args if isinstance(arg, Table)]

            # Check to see if all tables belong to same conn_id. Otherwise, we this can go wrong for cases
            # 1. When we have tables from different DBs.
            # 2. When we have tables from different conn_id, since they can be configured with different
            # database/schema etc.
            if (
                len(args_of_table_type) == 1
                or len({arg.conn_id for arg in args_of_table_type}) == 1
            ):
                first_table = args_of_table_type[0]

        if not first_table and self.op_kwargs and self.python_callable:
            kwargs_of_table_type = [
                self.op_kwargs[kwarg.name]
                for kwarg in inspect.signature(self.python_callable).parameters.values()
                if isinstance(self.op_kwargs[kwarg.name], Table)
            ]
            if (
                len(kwargs_of_table_type) == 1
                or len({kwarg.conn_id for kwarg in kwargs_of_table_type}) == 1
            ):
                first_table = kwargs_of_table_type[0]

        # If there is no first table via op_ags or kwargs, we check the parameters
        if not first_table and self.parameters:
            params_of_table_type = [
                param for param in self.parameters.values() if isinstance(param, Table)
            ]
            if (
                len(params_of_table_type) == 1
                or len({param.conn_id for param in params_of_table_type}) == 1
            ):
                first_table = params_of_table_type[0]

        if first_table:
            self.conn_id = first_table.conn_id or self.conn_id
            self.database = first_table.metadata.database or self.database
            self.schema = first_table.metadata.schema or self.schema
            self.warehouse = first_table.metadata.warehouse or self.warehouse
            self.role = first_table.metadata.role or self.role

    def populate_output_table(self):
        """
        When returning an output_table, we want to fill in as much metadata as possible to ensure that the next
        task can pick up the same table. In this function we ensure that the output_table has all of the same
        metadata we used to create the table in the first place.
        :return:
        """
        old_meta: Metadata = self.output_table.metadata
        meta = Metadata(
            database=old_meta.database or self.database,
            warehouse=old_meta.warehouse or self.warehouse,
            schema=old_meta.schema or self.schema,
        )
        self.output_table.conn_id = self.output_table.conn_id or self.conn_id
        self.output_table.metadata = meta
