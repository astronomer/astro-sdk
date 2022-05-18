from typing import Dict, Optional

from airflow.exceptions import AirflowException
from airflow.operators.dummy import BaseOperator
from sqlalchemy.sql.functions import Function

from astro.databases.base import BaseDatabase
from astro.settings import SCHEMA
from astro.sql.table import Metadata, Table


class SQLHandler(BaseOperator):
    """Contains helper functions for creating and running SQL queries"""

    output_table: Optional[Table] = None
    database_impl: BaseDatabase = None  # type: ignore

    def __init__(
        self,
        conn_id: str = "",
        autocommit: bool = False,
        parameters: Optional[dict] = None,
        handler: Optional[Function] = None,
        database: str = "",
        schema: str = "",
        warehouse: str = "",
        role: str = "",
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
        """
        In this function we ensure that both the temporary schema and the explicitly stated schema exist.
        We also ensure that if there is a table conflict, the conflicting table is dropped so the new data
        can be added.
        :return:
        """
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
                raise AirflowException("There's no SQL to run")

    @property
    def output_table_name(self):
        if self._output_table_name:
            return self._output_table_name
        if not self.output_table:
            self.output_table = Table()
        self._output_table_name = self.output_table.name
        return self._output_table_name

    def template(self, context: Dict):
        """
        This function handles all jinja templating to ensure that the SQL statement is ready for
        processing by SQLAlchemy. We use the database object here as different databases will have
        different templating rules.
        :param context:
        :return:
        """

        # update table name in context based on database
        context = self.database_impl.add_templates_to_context(self.parameters, context)

        # render templating in sql query
        if context:
            self.sql = self.render_template(self.sql, context)
