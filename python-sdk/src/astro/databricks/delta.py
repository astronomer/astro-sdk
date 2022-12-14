from __future__ import annotations

from textwrap import dedent

import pandas
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from databricks.sql.client import Cursor
from databricks_cli.sdk.api_client import ApiClient
from sqlalchemy.engine.base import Engine as SqlAlchemyEngine
from sqlalchemy.sql import ClauseElement

from astro.constants import DEFAULT_CHUNK_SIZE, ColumnCapitalization, LoadExistStrategy, MergeConflictStrategy
from astro.databases.base import BaseDatabase
from astro.databricks.load_file.load_file_job import load_file_to_delta
from astro.databricks.load_options import DeltaLoadOptions
from astro.files import File
from astro.options import LoadOptions
from astro.table import BaseTable, Metadata


class DeltaDatabase(BaseDatabase):
    _create_table_statement: str = "CREATE TABLE IF NOT EXISTS {} USING DELTA AS {} "

    def __init__(self, conn_id: str, table: BaseTable | None = None):
        super().__init__(conn_id)
        self.table = table

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

    def merge_table(
        self,
        source_table: BaseTable,
        target_table: BaseTable,
        source_to_target_columns_map: dict[str, str],
        target_conflict_columns: list[str],
        if_conflicts: MergeConflictStrategy = "exception",
    ) -> None:
        raise NotImplementedError("We do not yet support merge for databricks")

    def schema_exists(self, schema: str) -> bool:
        # Schemas do not need to be created for delta, so we can assume this is true
        return True

    def create_schema_if_needed(
        self, schema: str | None, location: str | None = None  # skipcq: PYL-W0613
    ) -> None:
        # Schemas do not need to be created for delta, so we don't need to do anything here
        return None

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
        load_options: LoadOptions = DeltaLoadOptions.get_default_delta_options(),
        databricks_job_name: str = "",
        **kwargs,
    ):
        """
        Load content of multiple files in output_table.
        Multiple files are sourced from the file path, which can also be path pattern.

        :param load_options: options for passing into COPY INTO. currently we support
            ``format_options`` and ``copy_options``
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

        """
        if not isinstance(load_options, DeltaLoadOptions):
            raise ValueError("Please use a DeltaLoadOption for the load_options parameter")
        load_file_to_delta(
            input_file=input_file,
            delta_table=output_table,
            databricks_job_name=databricks_job_name,
            delta_load_options=load_options or DeltaLoadOptions.get_default_delta_options(),
        )

    def openlineage_dataset_name(self, table: BaseTable) -> str:
        return ""

    def openlineage_dataset_namespace(self) -> str:
        return ""

    def create_table_from_select_statement(
        self,
        statement: str,
        target_table: BaseTable,
        parameters: dict | None = None,
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
            f"SELECT COUNT(*) FROM {self.get_table_qualified_name(table)}",
            handler=lambda x: x.fetchone(),
        )  # skipcq: BAN-B608
        return result.asDict()["count(1)"]

    def run_sql(
        self,
        sql: str | ClauseElement = "",
        parameters: dict | None = None,
        handler=None,
        **kwargs,
    ):
        """
        Run SQL against a delta table using spark SQL.

        :param sql: SQL Query to run on delta table
        :param parameters: parameters to pass to delta
        :param handler: function that takes in a databricks cursor as an argument.
        :param kwargs:
        :return: None if there is no handler, otherwise return result of handler function
        """
        hook = DatabricksSqlHook(
            databricks_conn_id=self.conn_id,
        )
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
            if "Table or view not found" in s.message:
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

    def export_table_to_pandas_dataframe(
        self, source_table: BaseTable, select_kwargs: dict | None = None
    ) -> pandas.DataFrame:
        """
        Converts a delta table into a pandas dataframe that can be processed locally.

        Please note that this is a local pandas dataframe and not a spark dataframe. Be careful
        of the size of dataframe you return.
        :param source_table: Delta table to convert to dataframe
        :param select_kwargs: Unused in this function
        :return:
        """

        def convert_delta_table_to_df(cur: Cursor) -> pandas.DataFrame:
            df = cur.fetchall_arrow().to_pandas()
            return df

        return self.hook.run(f"SELECT * FROM {source_table.name}", handler=convert_delta_table_to_df)
