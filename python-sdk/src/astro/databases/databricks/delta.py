from __future__ import annotations

import tempfile
import uuid
import warnings
from textwrap import dedent
from typing import Any, Callable

import pandas as pd
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from databricks.sql.client import Cursor
from databricks_cli.sdk.api_client import ApiClient
from sqlalchemy.engine.base import Engine as SqlAlchemyEngine
from sqlalchemy.sql import ClauseElement

from astro.constants import DEFAULT_CHUNK_SIZE, ColumnCapitalization, LoadExistStrategy, MergeConflictStrategy
from astro.databases.base import BaseDatabase
from astro.databases.databricks.load_file.load_file_job import load_file_to_delta
from astro.databases.databricks.load_options import DeltaLoadOptions
from astro.dataframes.pandas import PandasDataframe
from astro.files import File
from astro.options import LoadOptions
from astro.query_modifier import QueryModifier
from astro.settings import ASSUME_SCHEMA_EXISTS
from astro.table import BaseTable, Metadata


class DeltaDatabase(BaseDatabase):
    LOAD_OPTIONS_CLASS_NAME = ("DeltaLoadOptions",)
    _create_table_statement: str = "CREATE TABLE IF NOT EXISTS {} USING DELTA AS {} "

    def __init__(self, conn_id: str, table: BaseTable | None = None, load_options: LoadOptions | None = None):
        super().__init__(conn_id)
        self.table = table
        self.load_options = load_options or DeltaLoadOptions.get_default_delta_options()

    def populate_table_metadata(self, table: BaseTable) -> BaseTable:
        # TODO: Do we need default configurations for a delta table?
        """
        Given a table, populates the "metadata" field with what we would consider as "defaults"
        These defaults are determined based on environment variables and the connection settings.

        :param table: table to be populated
        :return:
        """
        table.conn_id = table.conn_id or self.conn_id
        if not table.metadata or table.metadata.is_empty():
            if self.table:
                table.metadata = self.table.metadata
            else:
                table.metadata = self.default_metadata
        return table

    @property
    def api_client(self) -> ApiClient:
        """
        Returns the databricks API client. Used for interacting with databricks services like
        DBFS, Jobs, etc.

        :return: A databricks ApiClient
        """
        conn = DatabricksHook(
            databricks_conn_id=self.conn_id
        ).get_conn()  # add this because DatabricksSqlHook does not expose password
        api_client = ApiClient(host=conn.host, token=conn.password)
        return api_client

    @property
    def sql_type(self):
        return "delta"

    @property
    def hook(self) -> DatabricksSqlHook:
        """
        Return the hook for the relevant databricks conn_id

        :return: a DatabricksSqlHook with metadata
        """
        return DatabricksSqlHook(databricks_conn_id=self.conn_id)

    @property
    def sqlalchemy_engine(self) -> SqlAlchemyEngine:
        raise NotImplementedError("We are not using sqlalchemy for databricks")

    @property
    def default_metadata(self) -> Metadata:
        return Metadata()

    def create_table_using_native_schema_autodetection(self, table: BaseTable, file: File) -> None:
        # TODO Do we need to implement this function? It seems like databricks will handle schemas for us
        raise NotImplementedError("Not implemented yet.")

    def schema_exists(self, schema: str) -> bool:
        # Schemas do not need to be created for delta, so we can assume this is true
        return True

    def create_schema_if_applicable(
        self, schema: str | None, assume_exists: bool = ASSUME_SCHEMA_EXISTS
    ) -> None:  # skipcq: PYL-W0613
        # Schemas do not need to be created for delta, so we don't need to do anything here
        return None

    def fetch_all_rows(self, table: BaseTable, row_limit: int = -1) -> list:
        """
        Fetches all rows for a table and returns as a list. This is needed because some
        databases have different cursors that require different methods to fetch rows

        :param table: The table metadata needed to fetch the rows
        :param row_limit: Limit the number of rows returned, by default return all rows.
        :return: a list of rows
        """
        statement = f"SELECT * FROM {self.get_table_qualified_name(table)};"
        if row_limit > -1:
            statement = statement + f" LIMIT {row_limit}"
        return self.run_sql(statement, handler=lambda x: x.fetchall())  # type: ignore

    def load_file_to_table(
        self,
        input_file: File,
        output_table: BaseTable,
        normalize_config: dict | None = None,
        if_exists: LoadExistStrategy = "replace",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        use_native_support: bool = True,
        native_support_kwargs: dict | None = None,
        columns_names_capitalization: ColumnCapitalization = "original",
        enable_native_fallback: bool | None = None,
        assume_schema_exists: bool = ASSUME_SCHEMA_EXISTS,
        databricks_job_name: str = "",
        **kwargs,
    ):
        """
        Load content of multiple files in output_table.
        Multiple files are sourced from the file path, which can also be path pattern.

        :param databricks_job_name: Create a consistent job name so that we don't litter the databricks job screen.
            This should be <dag_id>_<task_id>
        :param input_file: File path and conn_id for object stores
        :param output_table: Table to create
        :param if_exists: Overwrite file if exists
        :param chunk_size: Specify the number of records in each batch to be written at a time
        :param use_native_support: Use native support for data transfer if available on the destination
        :param normalize_config: pandas json_normalize params config
        :param native_support_kwargs: kwargs to be used by method involved in native support flow
        :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
        :param enable_native_fallback: Use enable_native_fallback=True to fall back to default transfer
        :param assume_schema_exists: If True, skips check to see if output_table schema exists
        """
        load_file_to_delta(
            input_file=input_file,
            delta_table=output_table,
            databricks_job_name=databricks_job_name,
            delta_load_options=self.load_options,  # type: ignore
            if_exists=if_exists,
        )

    def openlineage_dataset_name(self, table: BaseTable) -> str:
        return ""

    def openlineage_dataset_namespace(self) -> str:
        return ""

    def openlineage_dataset_uri(self, table: BaseTable) -> str:
        return ""

    def create_table_from_select_statement(
        self,
        statement: str,
        target_table: BaseTable,
        parameters: dict | None = None,
        query_modifier=QueryModifier(),
    ) -> None:
        """
        Create a Delta table from a SQL SELECT statement.

        :param statement: Statement that will return a table
        :param target_table: The table which the result of the SQL statement will be placed
        :param parameters: Parameters to pass to databricks
        :return: None
        """
        statement = self._create_table_statement.format(
            self.get_table_qualified_name(target_table), statement
        )
        statement = query_modifier.merge_pre_and_post_queries(statement)
        self.run_sql(sql=statement, parameters=parameters)

    def parameterize_variable(self, variable: str) -> str:
        """
        Parameterize a variable in a way that the Databricks SQL API can recognize.
        :param variable: Variable to parameterize
        :return: The number of rows in the table
        """
        return "%(" + variable + ")s"

    def row_count(self, table: BaseTable):
        result = self.run_sql(
            f"SELECT COUNT(*) FROM {self.get_table_qualified_name(table)}",  # skipcq: BAN-B608
            handler=lambda x: x.fetchone(),
        )
        return result.asDict()["count(1)"]

    def run_sql(
        self,
        sql: str | ClauseElement = "",
        parameters: dict | None = None,
        handler: Callable | None = None,
        query_modifier: QueryModifier = QueryModifier(),
        **kwargs,
    ) -> Any:
        """
        Run SQL against a delta table using spark SQL.

        :param query_modifier:
        :param sql: SQL Query to run on delta table
        :param parameters: parameters to pass to delta
        :param handler: function that takes in a databricks cursor as an argument.
        :param kwargs:
        :return: None if there is no handler, otherwise return result of handler function
        """
        hook = DatabricksSqlHook(
            databricks_conn_id=self.conn_id,
        )
        sql = query_modifier.merge_pre_and_post_queries(sql)
        return hook.run(sql, parameters=parameters, handler=handler)

    def table_exists(self, table: BaseTable) -> bool:
        """
        Queries databricks to check if a table exists.

        Since the databricks SQL API returns an exception if the table does not exist
        we look out for the relevant exception.
        :param table: Table that may or may not exist
        :return: True if the table exists, false if it does not
        """
        from databricks.sql.exc import ServerOperationError

        try:
            self.hook.run(
                f"DESCRIBE TABLE {table.name}", handler=lambda cur: cur.fetchall_arrow().to_pandas()
            )
            return True
        except ServerOperationError as s:
            if "TABLE_OR_VIEW_NOT_FOUND" in s.message:
                return False
            else:
                raise s

    def create_table_using_columns(self, table: BaseTable) -> None:
        """
        Create a SQL table using the table columns provided by the user.

        :param table: The table to be created.
        """
        if not table.columns:
            raise ValueError("To use this method, table.columns must be defined")
        columns = "\n\t".join([f"{c.name} {c.type}," for c in table.columns])
        self.run_sql(
            dedent(
                f"""
            CREATE TABLE {table.name}
            (
                {columns[:-1]}
            );
            """
            )
        )

    def append_table(
        self,
        source_table: BaseTable,
        target_table: BaseTable,
        source_to_target_columns_map: dict[str, str],
    ) -> None:
        """
        Append the source table rows into a destination table.

        :param source_table: Contains the rows to be appended to the target_table
        :param target_table: Contains the destination table in which the rows will be appended
        :param source_to_target_columns_map: Dict of source_table columns names to target_table columns names
        """

        if source_to_target_columns_map:
            warnings.warn(
                'Warning: Databricks does not support "partial" inserts. '
                "You will need to cast all columns if you wish to use this feature"
            )
            append_query = (
                f"INSERT INTO `{self.get_table_qualified_name(target_table)}` "
                f"({','.join(source_to_target_columns_map.keys())}) SELECT "
                f"{','.join(source_to_target_columns_map.values())} "
                f"FROM `{self.get_table_qualified_name(source_table)}`"
            )
        else:
            append_query = (
                f"INSERT INTO `{self.get_table_qualified_name(target_table)}` "
                f"SELECT * FROM `{self.get_table_qualified_name(source_table)}`"
            )

        self.run_sql(append_query)

    def export_table_to_pandas_dataframe(
        self, source_table: BaseTable, select_kwargs: dict | None = None
    ) -> pd.DataFrame:
        """
        Converts a delta table into a pandas dataframe that can be processed locally.

        Please note that this is a local pandas dataframe and not a spark dataframe. Be careful
        of the size of dataframe you return.
        :param source_table: Delta table to convert to dataframe
        :param select_kwargs: Unused in this function
        :return:
        """

        def convert_delta_table_to_df(cur: Cursor) -> pd.DataFrame:
            df = cur.fetchall_arrow().to_pandas()
            return df

        df = self.hook.run(f"SELECT * FROM {source_table.name}", handler=convert_delta_table_to_df)
        return PandasDataframe.from_pandas_df(df)

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
        from astro.constants import FileType

        with tempfile.TemporaryDirectory() as t:
            # We have to give each dataframe a unique name because delta does not want to upload the same
            # file multiple times.
            file = File(path=t + f"/dataframe_{uuid.uuid4()}.parquet", filetype=FileType.PARQUET)
            file.create_from_dataframe(source_dataframe)
            self.load_file_to_table(input_file=file, output_table=target_table, if_exists=if_exists)

    @staticmethod
    def get_merge_initialization_query(parameters: tuple) -> str:
        """
        Handles database-specific logic to handle constraints
        for Delta. setting constraints is not required for merging tables in Delta so we only return "1"
        """
        return "SELECT 1"

    def merge_table(
        self,
        source_table: BaseTable,
        target_table: BaseTable,
        source_to_target_columns_map: dict[str, str],
        target_conflict_columns: list[str],
        if_conflicts: MergeConflictStrategy = "exception",
    ):
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
        self.run_sql(sql=statement, parameters=params)

    def _build_merge_sql(
        self,
        source_table: BaseTable,
        target_table: BaseTable,
        source_to_target_columns_map: dict[str, str],
        target_conflict_columns: list[str],
        if_conflicts: MergeConflictStrategy = "exception",
    ):
        """Build the SQL statement for Merge operation"""
        # TODO: Simplify this function
        source_table_name = source_table.name
        target_table_name = target_table.name

        source_cols = source_to_target_columns_map.keys()
        target_cols = source_to_target_columns_map.values()

        (
            source_table_identifier,
            source_table_param,
        ) = self.get_sqlalchemy_template_table_identifier_and_parameter(source_table, "source_table")

        (
            target_table_identifier,
            target_table_param,
        ) = self.get_sqlalchemy_template_table_identifier_and_parameter(target_table, "target_table")
        merge_target_dict = {
            f"`target_table`.`{x}`": f"`target_table`.`{x}`" for i, x in enumerate(target_conflict_columns)
        }
        merge_source_dict = {
            f"`source_table`.`{x}`": f"`source_table`.`{x}`" for i, x in enumerate(target_conflict_columns)
        }
        merge_clauses = " AND ".join(
            f"{k}={v}" for k, v in zip(merge_target_dict.keys(), merge_source_dict.keys())
        )
        statement = (
            f"merge into {target_table_identifier} as `target_table` "
            f"using {source_table_identifier} as `source_table`"
            f" on {merge_clauses}"
        )

        values_to_check = [target_table_name, source_table_name]
        values_to_check.extend(source_cols)
        values_to_check.extend(target_cols)
        if if_conflicts == "update":
            merge_statement = ",".join(
                [f"target_table.{t} = source_table.{s}" for s, t in source_to_target_columns_map.items()]
            )
            statement += f" when matched then UPDATE SET {merge_statement}"
        target_columns = ",".join(f"target_table.{t}" for t in target_cols)
        append_columns = ",".join(f"source_table.{s}" for s in source_cols)
        statement += f" when not matched then insert({target_columns}) values ({append_columns})"

        params = {
            **merge_target_dict,
            **merge_source_dict,
            "source_table": source_table_param,
            "target_table": target_table_param,
        }
        return statement, params
