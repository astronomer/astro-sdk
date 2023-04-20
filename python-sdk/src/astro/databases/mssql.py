from __future__ import annotations

import warnings
from typing import Any, Callable

import pandas as pd
import sqlalchemy
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from pymssql._pymssql import ProgrammingError
from sqlalchemy.sql import ClauseElement

from astro.constants import DEFAULT_CHUNK_SIZE, LoadExistStrategy, MergeConflictStrategy
from astro.databases.base import BaseDatabase
from astro.options import LoadOptions
from astro.query_modifier import QueryModifier
from astro.settings import MSSQL_SCHEMA
from astro.table import BaseTable, Metadata
from astro.utils.compat.functools import cached_property

DEFAULT_CONN_ID = MsSqlHook.default_conn_name


class MssqlDatabase(BaseDatabase):
    DEFAULT_SCHEMA = MSSQL_SCHEMA

    _create_schema_statement: str = """
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{0}')
    BEGIN
        EXEC( 'CREATE SCHEMA {0}' );
    END
    """
    _truncate_table_statement: str = "TRUNCATE TABLE {0}"

    # CTAS is not supported in sql server. Hence, use select * into
    # select * into from a subquery will work in sql server only when an alias is added
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

    # alter , select * into, drop table are considered as DDL(they change the system catalog).
    # sqlalchemy based on mssql dialect considers autocommit as False by default which causes these
    # statements to not execute

    _queries_requiring_autocommit: list = ["alter", "select * into"]

    def __init__(
        self,
        conn_id: str = DEFAULT_CONN_ID,
        table: BaseTable | None = None,
        load_options: LoadOptions | None = None,
    ):
        super().__init__(conn_id)
        self.table = table
        self.load_options = load_options

    @cached_property
    def hook(self) -> MsSqlHook:
        """Retrieve Airflow hook to interface with the mssql database."""
        kwargs = {}
        _hook = MsSqlHook(mssql_conn_id=self.conn_id)
        if (_hook.schema is None) and (self.table and self.table.metadata and self.table.metadata.database):
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
        inspector = sqlalchemy.inspect(self.sqlalchemy_engine)

        schema = None
        if table and table.metadata:
            schema = table.metadata.schema
        return bool(inspector.dialect.has_table(self.connection, table.name, schema=schema))

    @staticmethod
    def get_table_qualified_name(table: BaseTable) -> str:
        """
        Return the table qualified name.

        :param table: The table we want to retrieve the qualified name for.
        """
        qualified_name_lists = [
            table.metadata.database,
            table.metadata.schema,
            table.name,
        ]
        qualified_name = ".".join(name for name in qualified_name_lists if name)
        return qualified_name

    @property
    def default_metadata(self) -> Metadata:
        """Fill in default metadata values for table objects addressing mssql databases.
        Currently, Schema is not being fetched from airflow connection for Mssql because, in Mssql,
        databases and schema are different concepts
        The MssqlHook only exposes schema
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
        try:
            schema_result = self.hook.run(
                "SELECT schema_name FROM information_schema.schemata WHERE "
                "lower(schema_name) = lower(%(schema_name)s);",
                parameters={"schema_name": schema.lower()},
                handler=lambda x: [y[0] for y in x.fetchall()],
            )
            return len(schema_result) > 0
        except ProgrammingError:
            return False

    def is_autocommit_required(self, sql) -> bool:
        """
        Checks if autocommit is required for a query

        :param sql: sql query which needs to be checked for autocommit setting

        """
        return any(query in sql.lower() for query in self._queries_requiring_autocommit)

    def run_sql(
        self,
        sql: str | ClauseElement = "",
        parameters: dict | None = None,
        handler: Callable | None = None,
        query_modifier: QueryModifier = QueryModifier(),
        **kwargs,
    ) -> Any:
        """
        Return the results to running a SQL statement.
        Whenever possible, this method should be implemented using Airflow Hooks,
        since this will simplify the integration with Async operators.

        :param sql: Contains SQL query to be run against database
        :param parameters: Optional parameters to be used to render the query
        :param handler: function that takes in a cursor as an argument.
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

        sql = query_modifier.merge_pre_and_post_queries(sql)
        autocommit = kwargs.get("autocommit", False)

        # We need to set autocommit=True for specific queries
        if isinstance(sql, str):
            if (autocommit is True) or self.is_autocommit_required(sql):
                self.connection.execute(sqlalchemy.text(sql).execution_options(autocommit=True), parameters)
            else:
                result = self.connection.execute(
                    sqlalchemy.text(sql).execution_options(autocommit=autocommit), parameters
                )
        else:
            # this is used for append
            result = self.connection.execute(sql, parameters)
        if handler:
            return handler(result)
        return None

    def create_schema_if_needed(self, schema: str | None) -> None:
        """
        This function checks if the expected schema exists in the database. If the schema does not exist,
        it will attempt to create it.

        :param schema: DB Schema - a namespace that contains named objects like (tables, functions, etc)
        """
        if schema and not self.schema_exists(schema):
            statement = self._create_schema_statement.format(schema)
            self.run_sql(statement, autocommit=True)

    def create_table_from_select_statement(
        self,
        statement: str,
        target_table: BaseTable,
        parameters: dict | None = None,
        query_modifier: QueryModifier = QueryModifier(),
    ) -> None:
        """
        Export the result rows of a query statement into another table.

        :param statement: SQL query statement
        :param target_table: Destination table where results will be recorded.
        :param parameters: (Optional) parameters to be used to render the SQL query
        """

        statement = self._create_table_statement.format(
            target_table.metadata.schema,
            target_table.name,
            target_table.metadata.database,
            statement,
        )

        self.run_sql(statement, parameters, autocommit=True)

    def drop_table(self, table: BaseTable) -> None:
        """
        Delete a SQL table, if it exists.

        :param table: The table to be deleted.
        """
        statement = self._drop_table_statement.format(self.get_table_qualified_name(table))
        self.run_sql(statement, autocommit=True)

    def fetch_all_rows(self, table: BaseTable, row_limit: int = -1) -> Any:
        """
        Fetches all rows for a table and returns as a list. This is needed because some
        databases have different cursors that require different methods to fetch rows

        :param row_limit: Limit the number of rows returned, by default return all rows.
        :param table: The table metadata needed to fetch the rows
        :return: a list of rows
        """
        statement = f"SELECT * FROM {self.get_table_qualified_name(table)}"  # skipcq: BAN-B608
        if row_limit > -1:
            statement = f"SELECT TOP {row_limit} * FROM {self.get_table_qualified_name(table)}"
        response: list = self.run_sql(statement, handler=lambda x: x.fetchall())
        return response

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

        table_name = self.get_table_qualified_name(target_table)
        mssql_conn = self.hook.get_conn()
        source_dataframe = source_dataframe.astype(object).where(pd.notnull(source_dataframe), None)
        mssql_conn.bulk_copy(table_name, list(source_dataframe.itertuples(index=False, name=None)))
        mssql_conn.commit()

    def merge_table(
        self,
        source_table: BaseTable,
        target_table: BaseTable,
        source_to_target_columns_map: dict[str, str],
        target_conflict_columns: list[str],
        if_conflicts: MergeConflictStrategy = "exception",
    ) -> None:
        """
        Merge the source table rows into a destination table.
        The argument `if_conflicts` allows the user to define how to handle conflicts.

        :param source_table: Contains the rows to be merged to the target_table
        :param target_table: Contains the destination table in which the rows will be merged
        :param source_to_target_columns_map: Dict of target_table columns names to source_table columns names
        :param target_conflict_columns: List of cols where we expect to have a conflict while combining
        :param if_conflicts: The strategy to be applied if there are conflicts.
        """

        statement, params = self._build_merge_sql(
            source_table=source_table,
            target_table=target_table,
            source_to_target_columns_map=source_to_target_columns_map,
            target_conflict_columns=target_conflict_columns,
            if_conflicts=if_conflicts,
        )
        # sql server generates nvarchar strings when passing parameter values to the query
        # Hence replace the params here instead of letting sql server doing it

        for k, v in params.items():
            statement = statement.replace(k, v)
        self.run_sql(sql=statement, parameters={}, autocommit=True)

    @staticmethod
    def get_merge_initialization_query(parameters: tuple) -> str:
        """
        Handles database-specific logic to handle constraints, keeping
        it agnostic to database.
        """
        sql = "RETURN"
        return sql

    def _build_merge_sql(
        self,
        source_table: BaseTable,
        target_table: BaseTable,
        source_to_target_columns_map: dict[str, str],
        target_conflict_columns: list[str],
        if_conflicts: MergeConflictStrategy = "exception",
    ):
        """Build the SQL statement for Merge operation"""
        source_table_name = source_table.name
        target_table_name = target_table.name

        source_cols = source_to_target_columns_map.keys()
        target_cols = source_to_target_columns_map.values()

        target_identifier_enclosure = ""
        source_identifier_enclosure = ""

        (
            source_table_identifier,
            source_table_param,
        ) = self.get_sqlalchemy_template_table_identifier_and_parameter(source_table, "source_table")

        (
            target_table_identifier,
            target_table_param,
        ) = self.get_sqlalchemy_template_table_identifier_and_parameter(target_table, "target_table")

        statement = (
            f"merge {target_table_identifier} as target using {source_table_identifier} as source "
            "on {merge_clauses}"
        )

        merge_target_dict = {
            f"merge_clause_target_{i}": f"target.{x}" for i, x in enumerate(target_conflict_columns)
        }
        merge_source_dict = {
            f"merge_clause_source_{i}": f"source.{x}" for i, x in enumerate(target_conflict_columns)
        }

        statement = statement.replace(
            "{merge_clauses}",
            " AND ".join(
                f"{wrap_identifier(k)}={wrap_identifier(v)}"
                for k, v in zip(merge_target_dict.keys(), merge_source_dict.keys())
            ),
        )

        values_to_check = [target_table_name, source_table_name]
        values_to_check.extend(source_cols)
        values_to_check.extend(target_cols)
        if if_conflicts == "update":
            statement += " when matched then UPDATE SET {merge_vals}"
            merge_statement = ",".join(
                [
                    f"target.{target_identifier_enclosure}{t}{target_identifier_enclosure}="
                    f"source.{source_identifier_enclosure}{s}{source_identifier_enclosure}"
                    for s, t in source_to_target_columns_map.items()
                ]
            )
            statement = statement.replace("{merge_vals}", merge_statement)
        statement += " when not matched by target then insert({target_columns}) values ({append_columns}); "
        statement = statement.replace(
            "{target_columns}",
            ",".join(f"{target_identifier_enclosure}{t}{target_identifier_enclosure}" for t in target_cols),
        )
        statement = statement.replace(
            "{append_columns}",
            ",".join(
                f"source.{source_identifier_enclosure}{s}{source_identifier_enclosure}" for s in source_cols
            ),
        )

        params = {
            **merge_target_dict,
            **merge_source_dict,
            "source_table": source_table_param,
            "target_table": target_table_param,
        }
        return statement, params

    def openlineage_dataset_name(self, table: BaseTable) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: database.schema.table.name
        """
        database = self.hook.get_connection(self.conn_id).schema
        schema = "dbo"
        if table.metadata and table.metadata.schema:
            schema = table.metadata.schema
        return f"{database}.{schema}.{table.name}"

    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: mssql://localhost:3306
        """
        conn = self.hook.get_connection(self.conn_id)
        return f"{self.sql_type}://{conn.host}:{conn.port}"

    def openlineage_dataset_uri(self, table: BaseTable) -> str:
        """
        Returns the open lineage dataset uri as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        return f"{self.openlineage_dataset_namespace()}/{self.openlineage_dataset_name(table=table)}"


def wrap_identifier(inp: str) -> str:
    return f"{inp}"
