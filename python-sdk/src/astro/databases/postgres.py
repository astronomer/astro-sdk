"""Postgres database implementation."""
from __future__ import annotations

import io
from contextlib import closing

import pandas as pd
import sqlalchemy
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql as postgres_sql

from astro.constants import DEFAULT_CHUNK_SIZE, LoadExistStrategy, MergeConflictStrategy
from astro.databases.base import BaseDatabase
from astro.files import File
from astro.settings import POSTGRES_SCHEMA
from astro.table import BaseTable, Metadata

DEFAULT_CONN_ID = PostgresHook.default_conn_name


class PostgresDatabase(BaseDatabase):
    """
    Handle interactions with Postgres databases. If this class is successful, we should not have any Postgres-specific
    logic in other parts of our code-base.
    """

    DEFAULT_SCHEMA = POSTGRES_SCHEMA
    illegal_column_name_chars: list[str] = ["."]
    illegal_column_name_chars_replacement: list[str] = ["_"]

    def __init__(self, conn_id: str = DEFAULT_CONN_ID, table: BaseTable | None = None):
        super().__init__(conn_id)
        self.table = table

    @property
    def sql_type(self) -> str:
        return "postgresql"

    @property
    def hook(self) -> PostgresHook:
        """Retrieve Airflow hook to interface with the Postgres database."""
        conn = PostgresHook(postgres_conn_id=self.conn_id).get_connection(self.conn_id)
        kwargs = {}
        if (conn.schema is None) and (self.table and self.table.metadata and self.table.metadata.schema):
            kwargs.update({"database": self.table.metadata.schema})
        return PostgresHook(postgres_conn_id=self.conn_id, **kwargs)

    @property
    def default_metadata(self) -> Metadata:
        """Fill in default metadata values for table objects addressing Postgres databases.

        Currently, Schema is not being fetched from airflow connection for Postgres because, in Postgres,
        databases and schema are different concepts: https://www.postgresql.org/docs/current/ddl-schemas.html

        The PostgresHook only exposes schema:
        :external+airflow-postgres:py:class:`airflow.providers.postgres.hooks.postgres.PostgresHook`

        However, implementation-wise, it seems that if the PostgresHook receives a schema during
        initialization, but it uses it as a database in the connection to Postgres:
        https://github.com/apache/airflow/blob/main/airflow/providers/postgres/hooks/postgres.py#L96
        """
        # TODO: Change airflow PostgresHook to fetch database and schema separately
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

        self.create_schema_if_needed(target_table.metadata.schema)
        if not self.table_exists(table=target_table) or if_exists == "replace":
            self.create_table(table=target_table, dataframe=source_dataframe)

        output_buffer = io.StringIO()
        source_dataframe.to_csv(output_buffer, sep=",", header=True, index=False)
        output_buffer.seek(0)
        table_name = self.get_table_qualified_name(target_table)
        postgres_conn = self.hook.get_conn()
        with closing(postgres_conn) as conn, closing(conn.cursor()) as cur:
            cur.copy_expert(
                f"COPY {table_name} FROM STDIN DELIMITER ',' CSV HEADER;",
                output_buffer,
            )
            conn.commit()

    @staticmethod
    def get_table_qualified_name(table: BaseTable) -> str:  # skipcq: PYL-R0201
        """
        Return table qualified name. This is Database-specific.
        For instance, in Sqlite this is the table name. In Snowflake, however, it is the database, schema and table

        :param table: The table we want to retrieve the qualified name for.
        """
        # Initially this method belonged to the Table class.
        # However, in order to have an agnostic table class implementation,
        # we are keeping all methods which vary depending on the database within the Database class.
        if table.metadata and table.metadata.schema:
            qualified_name = f"{table.metadata.schema.lower()}.{table.name}"  # type: ignore
        else:
            qualified_name = table.name
        return qualified_name

    def table_exists(self, table: BaseTable) -> bool:
        """
        Check if a table exists in the database

        :param table: Details of the table we want to check that exists
        """
        _schema = table.metadata.schema
        # when creating schemas they are created in a lowercase even when we have a schema in uppercase.
        # while checking for schema there in no lowercase applied in 'has_table()' which leads to table not found.
        # Added 'schema.lower()' to make sure we search for schema in lowercase to match the creation lowercase.
        schema = _schema.lower() if _schema else _schema

        inspector = sqlalchemy.inspect(self.sqlalchemy_engine)
        return bool(inspector.dialect.has_table(self.connection, table.name, schema))

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

        def identifier_args(table: BaseTable):
            schema = table.metadata.schema
            return (schema, table.name) if schema else (table.name,)

        statement = (
            "INSERT INTO {target_table} ({target_columns}) SELECT {source_columns} FROM {source_table}"
        )

        source_columns = list(source_to_target_columns_map.keys())
        target_columns = list(source_to_target_columns_map.values())

        if if_conflicts == "ignore":
            statement += " ON CONFLICT ({target_conflict_columns}) DO NOTHING"
        elif if_conflicts == "update":
            statement += " ON CONFLICT ({target_conflict_columns}) DO UPDATE SET {update_statements}"

        source_column_names = [postgres_sql.Identifier(col) for col in source_columns]
        target_column_names = [postgres_sql.Identifier(col) for col in target_columns]
        update_statements = [
            postgres_sql.SQL("{col_name}=EXCLUDED.{col_name}").format(col_name=col_name)
            for col_name in target_column_names
        ]

        query = postgres_sql.SQL(statement).format(
            target_columns=postgres_sql.SQL(",").join(target_column_names),
            target_table=postgres_sql.Identifier(*identifier_args(target_table)),
            source_columns=postgres_sql.SQL(",").join(source_column_names),
            source_table=postgres_sql.Identifier(*identifier_args(source_table)),
            update_statements=postgres_sql.SQL(",").join(update_statements),
            target_conflict_columns=postgres_sql.SQL(",").join(
                [postgres_sql.Identifier(x) for x in target_conflict_columns]
            ),
        )

        sql = query.as_string(self.hook.get_conn())
        self.run_sql(sql=sql)

    @staticmethod
    def get_dataframe_from_file(file: File):
        """
        Get pandas dataframe file

        :param file: File path and conn_id for object stores
        """
        return file.export_to_dataframe_via_byte_stream()

    def openlineage_dataset_name(self, table: BaseTable) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: schema_name.table_name
        """
        schema = self.hook.get_connection(self.conn_id).schema or "public"
        return f"{schema}.{table.name}"

    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: postgresql://localhost:5432
        """
        conn = self.hook.get_connection(self.conn_id)
        return f"{self.sql_type}://{conn.host}:{conn.port}"
