from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

import pandas as pd
import sqlalchemy
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from sqlalchemy.sql import ClauseElement

from astro.constants import DEFAULT_CHUNK_SIZE, LoadExistStrategy
from astro.databases.base import BaseDatabase
from astro.table import BaseTable, Metadata

DEFAULT_CONN_ID = MsSqlHook.default_conn_name
if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.engine.cursor import CursorResult


class MssqlDatabase(BaseDatabase):

    _create_schema_statement: str = """
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{0}')
    BEGIN
        EXEC( 'CREATE SCHEMA {0}' );
    END
    """
    _truncate_table_statement: str = "TRUNCATE TABLE {0}"

    # CTAS IS NOT SUPPORTED IN SQL SERVER. HENCE USE SELECT * INTO
    # SELECT * INTO FROM A SUBQUERY WILL WORK IN SQL SERVER ONLY WHEN AN ALIAS IS ADDED
    _create_table_statement: str = """
    IF (NOT EXISTS (SELECT *
                 FROM INFORMATION_SCHEMA.TABLES
                 WHERE TABLE_SCHEMA = '{0}'
                 AND   TABLE_NAME    = '{1}'
                 AND   TABLE_CATALOG = '{2}'))
    BEGIN
        SELECT * INTO  {2}.{0}.{1} FROM ({3}) as astro;
    END
    """

    def __init__(self, conn_id: str = DEFAULT_CONN_ID, table: BaseTable | None = None):
        super().__init__(conn_id)
        self.table = table

    @property
    def hook(self) -> MsSqlHook:
        """Retrieve Airflow hook to interface with the mssql database."""

        kwargs = {}
        _hook = MsSqlHook(mssql_conn_id=self.conn_id)
        if self.table and self.table.metadata:
            if _hook.schema is None and self.table.metadata.database:
                # kwargs.update({"schema": self.table.metadata.schema})
                kwargs.update({"schema": self.table.metadata.database})
        return MsSqlHook(mssql_conn_id=self.conn_id, **kwargs)

    @property
    def sql_type(self) -> str:
        return "mssql"

    def table_exists(self, table: BaseTable) -> bool:
        """
        Check if a table exists in the database

        :param table: Details of the table we want to check that exists
        """
        table_qualified_name = self.get_table_qualified_name(table)
        inspector = sqlalchemy.inspect(self.sqlalchemy_engine)

        schema = None
        if table and table.metadata:
            schema = table.metadata.schema
        elif self.hook.schema:
            schema = self.hook.schema
        return bool(inspector.dialect.has_table(self.connection, table_qualified_name, schema=schema))

    @staticmethod
    def get_table_qualified_name(table: BaseTable, fully_qualified: bool = False) -> str:
        """
        Return the table qualified name.

        :param table: The table we want to retrieve the qualified name for.
        """
        if fully_qualified:
            qualified_name_lists = [
                table.metadata.database,
                table.metadata.schema,
                table.name,
            ]
            qualified_name = ".".join(name for name in qualified_name_lists if name)
            return qualified_name
        else:
            return str(table.name)

    @property
    def default_metadata(self) -> Metadata:
        """Fill in default metadata values for table objects addressing mssql databases.

        Currently, Schema is not being fetched from airflow connection for Mssql because, in Mssql,
        databases and schema are different concepts

        The MssqlHook only exposes schema:
        :external+airflow-postgres:py:class:`airflow.providers.mssql.hooks.mssql.MssqlHook`

        However, implementation-wise, it seems that if the MssqlHook receives a schema during
        initialization, but it uses it as a database in the connection to Mssql:
        """
        # TODO: Change airflow MssqlHook to fetch database and schema separately
        database = self.hook.get_connection(self.conn_id).schema
        return Metadata(database=database, schema=self.DEFAULT_SCHEMA)  # type: ignore

    def schema_exists(self, schema) -> bool:
        """
        Checks if a schema exists in the database

        :param schema: DB Schema - a namespace that contains named objects like (tables, functions, etc)
        """
        schema_result = self.hook.run(
            "SELECT schema_name FROM information_schema.schemata WHERE lower(schema_name) = lower(%(schema_name)s);",
            parameters={"schema_name": schema.lower()},
            handler=lambda x: [y[0] for y in x.fetchall()],
        )
        return len(schema_result) > 0

    def generate_query_with_fully_qualified_table_names(self, sql):
        from sql_metadata import Parser

        try:
            candidate_tables = Parser(sql).tables
        except ValueError:
            candidate_tables = []
        try:
            query_type = Parser(sql).query_type.value
        except ValueError:
            query_type = ""
        final_candidate_tables = [tbl for tbl in candidate_tables if tbl.count(".") == 0]
        dict_candidate_tables = {
            tbl: self.default_metadata.database + "." + self.default_metadata.schema + "." + tbl
            for tbl in final_candidate_tables
        }
        for tbl in final_candidate_tables:
            sql = sql.replace(tbl, dict_candidate_tables[tbl])
        return sql, query_type

    def run_sql(
        self,
        sql: str | ClauseElement = "",
        parameters: dict | None = None,
        autocommit: bool = False,
        **kwargs,
    ) -> CursorResult:
        """
        Return the results to running a SQL statement.

        Whenever possible, this method should be implemented using Airflow Hooks,
        since this will simplify the integration with Async operators.

        :param sql: Contains SQL query to be run against database
        :param parameters: Optional parameters to be used to render the query
        """
        if parameters is None:
            parameters = {}

        if "sql_statement" in kwargs:  # pragma: no cover
            warnings.warn(
                "`sql_statement` is deprecated and will be removed in future release"
                "Please use  `sql` param instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            sql = kwargs.get("sql_statement")  # type: ignore

        # We need to set autocommit=True for DDL and False for all other queries
        if isinstance(sql, str):
            sql, query_type = self.generate_query_with_fully_qualified_table_names(sql)

            if self.connection.engine.name == "mssql" and autocommit is True:
                self.connection.execute(
                    sqlalchemy.text(sql).execution_options(autocommit=autocommit), parameters
                )
            else:
                result = self.connection.execute(
                    sqlalchemy.text(sql).execution_options(autocommit=autocommit), parameters
                )
                print("RESULT$$$$$$$$$$$$$", result)
                return result

    def create_schema_if_needed(self, schema: str | None) -> None:
        """
        This function checks if the expected schema exists in the database. If the schema does not exist,
        it will attempt to create it.

        :param schema: DB Schema - a namespace that contains named objects like (tables, functions, etc)
        """
        # We check if the schema exists first because snowflake will fail on a create schema query even if it
        # doesn't actually create a schema.
        if schema and not self.schema_exists(schema):
            statement = self._create_schema_statement.format(schema)
            self.run_sql(statement, autocommit=True)

    def place_holder(self, values):
        return "({})".format(", ".join("?" * len(values)))

    def create_table_from_select_statement(
        self,
        statement: str,
        target_table: BaseTable,
        parameters: dict | None = None,
    ) -> None:
        """
        Export the result rows of a query statement into another table.

        :param statement: SQL query statement
        :param target_table: Destination table where results will be recorded.
        :param parameters: (Optional) parameters to be used to render the SQL query
        """

        statement = self.generate_query_with_fully_qualified_table_names(statement)[0]

        statement = self._create_table_statement.format(
            target_table.metadata.schema,
            self.get_table_qualified_name(target_table),
            target_table.metadata.database,
            statement,
        )

        self.run_sql(statement, parameters, autocommit=True)

    # Require skipcq because method overriding we need param chunk_size
    def load_pandas_dataframe_to_table(
        self,
        source_dataframe: pd.DataFrame,
        target_table: BaseTable,
        if_exists: LoadExistStrategy = "replace",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:  # skipcq PYL-W0613
        """
        Create a table with the dataframe's contents.
        If the table already exists, append or replace the content, depending on the value of `if_exists`.

        :param source_dataframe: Local or remote filepath
        :param target_table: Table in which the file will be loaded
        :param if_exists: Strategy to be used in case the target table already exists.
        :param chunk_size: Specify the number of rows in each batch to be written at a time.
        """
        self._assert_not_empty_df(source_dataframe)

        self.create_schema_if_needed(target_table.metadata.schema)
        if not self.table_exists(table=target_table) or if_exists == "replace":
            self.create_table(table=target_table, dataframe=source_dataframe)

        table_name = self.get_table_qualified_name(target_table, fully_qualified=True)
        mssql_conn = self.hook.get_conn()
        mssql_conn.bulk_copy(table_name, list(source_dataframe.itertuples(index=False, name=None)))
        mssql_conn.commit()
