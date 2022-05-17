"""Postgres database implementation."""
import sqlalchemy
from airflow.providers.postgres.hooks.postgres import PostgresHook

from astro.databases.base import BaseDatabase
from astro.sql.tables import Metadata, Table

DEFAULT_CONN_ID = PostgresHook.default_conn_name


class PostgresDatabase(BaseDatabase):
    """
    Handle interactions with Postgres databases. If this class is successful, we should not have any Postgres-specific
    logic in other parts of our code-base.
    """

    def __init__(self, conn_id: str = DEFAULT_CONN_ID):
        super().__init__(conn_id)

    @property
    def hook(self):
        """Retrieve Airflow hook to interface with the Postgres database."""
        return PostgresHook(postgres_conn_id=self.conn_id)

    @property
    def default_metadata(self) -> Metadata:
        schema = self.hook.schema
        return Metadata(schema=schema)

    def schema_exists(self, schema):
        created_schemas = self.hook.run(
            "SELECT schema_name FROM information_schema.schemata;",
            handler=lambda x: [y[0] for y in x.fetchall()],
        )
        return schema.upper() in [c.upper() for c in created_schemas]

    def table_exists(self, table: Table) -> bool:
        """
        Check if a table exists in the database.

        :param table: Details of the table we want to check that exists
        """
        inspector = sqlalchemy.inspect(self.sqlalchemy_engine)
        return bool(
            inspector.dialect.has_table(
                self.connection, table.name, table.metadata.schema
            )
        )
