"""Postgres database implementation."""
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

    def get_table_qualified_name(self, table: Table):
        """
        Return table qualified name for Postgres.

        :param table: The table we want to retrieve the qualified name for.
        """
        if table.metadata is not None and table.metadata.schema is not None:
            qualified_name = f"{table.metadata.schema}.{table.name}"
        else:
            qualified_name = table.name
        return qualified_name

    @property
    def default_metadata(self) -> Metadata:
        schema = self.hook.schema
        return Metadata(schema=schema)
