from __future__ import annotations

import pandas as pd
import sqlalchemy
from airflow.exceptions import AirflowException
from airflow.providers.mysql.hooks.mysql import MySqlHook
from MySQLdb import OperationalError

from astro.constants import DEFAULT_CHUNK_SIZE, LoadExistStrategy, MergeConflictStrategy
from astro.databases.base import BaseDatabase
from astro.options import LoadOptions
from astro.settings import MYSQL_SCHEMA
from astro.table import BaseTable, Metadata
from astro.utils.compat.functools import cached_property

DEFAULT_CONN_ID = MySqlHook.default_conn_name


class MysqlDatabase(BaseDatabase):
    DEFAULT_SCHEMA = MYSQL_SCHEMA

    _create_schema_statement: str = (
        "CREATE SCHEMA IF NOT EXISTS {} "
        "DEFAULT CHARACTER SET = 'utf8' DEFAULT COLLATE = "
        "'utf8_unicode_ci'"
    )

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
    def hook(self) -> MySqlHook:
        """Retrieve Airflow hook to interface with the mysql database."""
        conn = MySqlHook(mysql_conn_id=self.conn_id).get_connection(self.conn_id)
        kwargs = {}
        if conn.schema is None:
            if (
                self.table
                and self.table.metadata
                and self.table.metadata.database
                and self.table.metadata.schema
            ):
                raise AirflowException(
                    "You have provided both database and schema in Metadata."
                    "Enter only schema while connecting to MySQL!"
                )
            if (
                self.table
                and self.table.metadata
                and self.table.metadata.database
                and self.table.metadata.schema is None
            ):
                raise AirflowException(
                    "You have provided database in Metadata.Enter only schema while connecting to MySQL!"
                )
            if self.table and self.table.metadata and self.table.metadata.schema:
                kwargs.update({"schema": self.table.metadata.schema})
            else:
                kwargs.update({"schema": self.DEFAULT_SCHEMA})
        return MySqlHook(mysql_conn_id=self.conn_id, **kwargs)

    def populate_table_metadata(self, table: BaseTable) -> BaseTable:
        """
        Given a table, check if the table has metadata.
        If the metadata is missing, and the database has metadata, assign it to the table.
        If the table schema was not defined by the end, retrieve the user-defined schema.
        This method performs the changes in-place and also returns the table.
        For mysql - schema is synonymous with database.

        :param table: Table to potentially have their metadata changed
        :return table: Return the modified table
        """
        if table.metadata and table.metadata.is_empty() and self.default_metadata:
            table.metadata = self.default_metadata
        return table

    @property
    def default_metadata(self) -> Metadata:
        """schema and database are synonymous in MySQL"""
        schema_from_conn = self.hook.get_connection(self.conn_id).schema
        database = schema_from_conn if schema_from_conn else self.DEFAULT_SCHEMA
        return Metadata(database=database, schema=database)  # type: ignore

    def load_pandas_dataframe_to_table(
        self,
        source_dataframe: pd.DataFrame,
        target_table: BaseTable,
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
        self._assert_not_empty_df(source_dataframe)

        source_dataframe.to_sql(
            self.get_table_qualified_name(target_table),
            con=self.sqlalchemy_engine,
            schema=target_table.metadata.schema,
            if_exists=if_exists,
            chunksize=chunk_size,
            method="multi",
            index=False,
        )

    @property
    def sql_type(self) -> str:
        return "mysql"

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
        except OperationalError:
            return False

    @staticmethod
    def get_table_qualified_name(table: BaseTable) -> str:
        """
        Return the table qualified name.

        :param table: The table we want to retrieve the qualified name for.
        """
        return table.name

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
        statement = (
            "insert into {target_table}"
            "({target_columns})  "
            "select {source_columns} "
            "from {source_table}  "
            "on duplicate key update {merge_vals};"
        )

        source_column_names = list(source_to_target_columns_map.keys())
        target_column_names = list(source_to_target_columns_map.values())

        target_identifier_enclosure = ""
        source_identifier_enclosure = ""

        join_conditions = ",".join(
            [
                f"{target_table.name}.{target_identifier_enclosure}{t}{target_identifier_enclosure}="
                f"{source_table.name}.{source_identifier_enclosure}{s}{source_identifier_enclosure}"
                for s, t in source_to_target_columns_map.items()
            ]
        )
        statement = statement.replace("{merge_vals}", join_conditions)

        statement = statement.format(
            target_columns=",".join(target_column_names),
            target_table=target_table.name,
            source_columns=",".join(source_column_names),
            source_table=source_table.name,
        )

        self.run_sql(sql=statement)

    def openlineage_dataset_name(self, table: BaseTable) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: database.schema.table.name
        """
        schema = self.hook.get_connection(self.conn_id).schema
        if table.metadata and table.metadata.schema:
            schema = table.metadata.schema
        return f"{schema}.{table.name}"

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
