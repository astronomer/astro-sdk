from typing import Dict, Optional

from airflow.exceptions import AirflowException
from airflow.operators.dummy import BaseOperator
from sqlalchemy.sql.functions import Function

from astro.databases.base import BaseDatabase
from astro.settings import SCHEMA
from astro.sql.tables import Metadata, Table


class SQLHandler(BaseOperator):
    output_table = None
    database_impl: BaseDatabase = None  # type: ignore

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
        raw_sql: bool = True,
        sql="",
        **kwargs,
    ):
        self.autocommit = autocommit
        self.parameters: dict = parameters or {}
        self.handler = handler
        self.kwargs = kwargs or {}
        self.sql = sql
        self.conn_id = conn_id
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.role = role
        self.raw_sql = raw_sql
        self._output_table_name = None
        self._full_output_table_name = None
        super().__init__(**kwargs)

    def handle_schema(self):
        # Create DEFAULT SCHEMA if not created.
        self.database_impl.create_schema_if_needed(SCHEMA)
        if not self.raw_sql:
            # Create a table name for the temp table
            self.database_impl.create_schema_if_needed(self.schema)
            self.database_impl.drop_table(
                Table(
                    name=self.output_table_name,
                    conn_id=self.conn_id,
                    metadata=Metadata(
                        schema=self.schema,
                        database=self.database,
                    ),
                )
            )
        else:
            # If there's no SQL to run we raise an error
            if self.sql == "" or not self.sql:
                return AirflowException("There's no SQL to run")

    @property
    def output_table_name(self):
        if self._output_table_name:
            return self._output_table_name
        if not self.output_table:
            self.output_table = Table()
        self._output_table_name = self.output_table.name
        return self._output_table_name

    def template(self, context: Dict):

        # update table name in context based on database
        context = self.database_impl.add_templates_to_context(self.parameters, context)

        # render templating in sql query
        if context:
            self.sql = self.render_template(self.sql, context)
