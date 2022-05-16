from airflow.providers.postgres.hooks.postgres import PostgresHook

from astro.databases.base import BaseDatabase

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

    # def get_table_qualified_name(self, table: Table) -> str:
    #     """
    #     Return table qualified name. This is Database-specific.
    #     For instance, in Sqlite this is the table name. In Snowflake, however, it is the database, schema and table
    #
    #     :param table: The table we want to retrieve the qualified name for.
    #     """
    #     return str(table.name)

    # def get_sqla_table_object(self, table: Table) -> SqlaTable:
    #     """
    #     Return SQLAlchemy table object using reflections
    #
    #     :param table: The table we want to retrieve the qualified name for.
    #     """
    #     metadata_params: Dict[str, str] = {}
    #     metadata = MetaData(**metadata_params)
    #     return SqlaTable(
    #         self.get_table_qualified_name(table), metadata, autoload_with=self.sqlalchemy_engine
    #     )
