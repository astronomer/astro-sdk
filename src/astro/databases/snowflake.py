"""Snowflake database implementation."""
from typing import Dict

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

    def add_templates_to_context(self, parameters: Dict, context: Dict) -> Dict:
        """
        When running functions through the `aql.transform` and `aql.render` functions, we need to add
        the parameters given to the SQL statement to the Airflow context dictionary. This is how we can
        then use jinja to render those parameters into the SQL function when users use the {{}} syntax
        (e.g. "SELECT * FROM {{input_table}}").

        With this system we should handle Table objects differently from other variables. Since we will later
        pass the parameter dictionary into SQLAlchemy, the safest (From a security standpoint) default is to use
        a `:variable` syntax. This syntax will ensure that SQLAlchemy treats the value as an unsafe template. With
        Table objects, however, we have to give a raw value or the query will not work. Because of this we recommend
        looking into the documentation of your database and seeing what best practices exist (e.g. Identifier wrappers
        in snowflake).

        :param parameters: A Dict of SQL key-value parameters
        :param context: Airflow Context dictionary
        :return: Dictionary with values with Table type replaced with the table name
        """
        for k, v in parameters.items():
            if isinstance(v, Table):
                context[k] = "IDENTIFIER(:" + k + ")"
            else:
                context[k] = ":" + k
        return context

    def process_sql_parameters(self, parameters: Dict) -> Dict:
        """
        Used in conjunction with add_templates_to_context to pass the name of the table
        """
        return {
            k: (self.get_table_qualified_name(v) if isinstance(v, Table) else v)
            for k, v in parameters.items()
        }

    def schema_exists(self, schema):
        created_schemas = [
            x["SCHEMA_NAME"]
            for x in self.hook.run(
                "SELECT SCHEMA_NAME from information_schema.schemata;"
            )
        ]
        return schema.upper() in [c.upper() for c in created_schemas]
