"""Snowflake database implementation."""
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pandas.io.sql import SQLDatabase

from astro.constants import DEFAULT_CHUNK_SIZE, LoadExistStrategy
from astro.databases.base import BaseDatabase
from astro.sql.tables import Metadata, Table
from astro.utils.dependencies import pandas_tools

DEFAULT_CONN_ID = SnowflakeHook.default_conn_name


class SnowflakeDatabase(BaseDatabase):
    """
    Handle interactions with snowflake databases. If this class is successful, we should not have any snowflake-specific
    logic in other parts of our code-base.
    """

    def __init__(self, conn_id: str = DEFAULT_CONN_ID):
        super().__init__(conn_id)

    @property
    def hook(self):
        """Retrieve Airflow hook to interface with the snowflake database."""
        return SnowflakeHook(snowflake_conn_id=self.conn_id)

    def get_table_qualified_name(self, table: Table):
        """
        Return table qualified name for Snowflake.

        :param table: The table we want to retrieve the qualified name for.
        """
        if table.metadata is not None and table.metadata.schema is not None:
            qualified_name = f"{table.metadata.schema}.{table.name}"
        else:
            qualified_name = table.name
        return qualified_name

    @property
    def default_metadata(self) -> Metadata:
        connection = self.hook.get_conn()
        return Metadata(
            host=connection.account,
            schema=connection.schema,
            warehouse=connection.warehouse,
            database=connection.database,
            account=connection.account,
            role=connection.role,
            region=connection.region,
        )

    def load_pandas_dataframe_to_table(
        self,
        source_dataframe: pd.DataFrame,
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        """
        Create a table with the dataframe's contents.
        If the table already exists, append or replace the content, depending on the value of `if_exists`.

        :param source_dataframe: Local or remote filepath
        :param target_table: Table in which the file will be loaded
        :param if_exists: Strategy to be used in case the target table already exists.
        :param chunk_size: Specify the number of rows in each batch to be written at a time.
        """
        db = SQLDatabase(engine=self.sqlalchemy_engine)
        # Make columns uppercase to prevent weird errors in snowflake
        source_dataframe.columns = source_dataframe.columns.str.upper()
        schema = None
        if target_table.metadata:
            schema = getattr(target_table.metadata, "schema", None)

        db.prep_table(
            source_dataframe,
            target_table.name,
            schema=schema,
            if_exists=if_exists,
            index=False,
        )
        pandas_tools.write_pandas(
            self.hook.get_conn(),
            source_dataframe,
            target_table.name,
            chunk_size=chunk_size,
            quote_identifiers=False,
        )
