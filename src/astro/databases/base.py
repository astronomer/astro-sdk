from abc import ABC
from typing import Dict, Optional

import pandas as pd
import sqlalchemy
from airflow.hooks.base import BaseHook

from astro.constants import (
    DEFAULT_CHUNK_SIZE,
    AppendConflictStrategy,
    ExportExistsStrategy,
    LoadExistStrategy,
)
from astro.exceptions import NonExistentTableException
from astro.files import File
from astro.settings import SCHEMA
from astro.sql.table import Metadata, Table


class BaseDatabase(ABC):
    """
    Base class to represent all the Database interactions.

    The goal is to be able to support new databases by adding
    a new module to the `astro/databases` directory, without the need of
    changing other modules and classes.

    The exception is if the Airflow connection type does not match the new
    Database module name. In that case, we should update the dictionary
    `CUSTOM_CONN_TYPE_TO_MODULE_PATH` available at `astro/databases/__init__.py`.
    """

    _create_schema_statement: str = "CREATE SCHEMA IF NOT EXISTS {}"
    _drop_table_statement: str = "DROP TABLE IF EXISTS {}"
    _create_table_statement: str = "CREATE TABLE IF NOT EXISTS {} AS {}"

    def __init__(self, conn_id: str):
        self.conn_id = conn_id

    @property
    def hook(self) -> BaseHook:
        """Return an instance of the database-specific Airflow hook."""
        raise NotImplementedError

    @property
    def connection(self) -> sqlalchemy.engine.base.Connection:
        """Return a Sqlalchemy connection object for the given database."""
        return self.sqlalchemy_engine.connect()

    @property
    def sqlalchemy_engine(self) -> sqlalchemy.engine.base.Engine:
        """Return Sqlalchemy engine."""
        return self.hook.get_sqlalchemy_engine()

    def get_sqlalchemy_engine(self, table: Table):
        return self.hook.get_sqlalchemy_engine()

    def get_connection(self, table) -> sqlalchemy.engine.base.Connection:
        """Return a Sqlalchemy connection object for the given database."""
        eng = self.get_sqlalchemy_engine(table)
        return eng.connect()

    def run_sql(self, sql_statement: str, parameters: Optional[dict] = None):
        """
        Return the results to running a SQL statement.

        Whenever possible, this method should be implemented using Airflow Hooks,
        since this will simplify the integration with Async operators.

        :param sql_statement: Contains SQL query to be run against database
        :param parameters: Optional parameters to be used to render the query
        """
        if parameters is None:
            parameters = {}

        if isinstance(sql_statement, str):
            result = self.connection.execute(sqlalchemy.text(sql_statement), parameters)
        else:
            result = self.connection.execute(sql_statement, parameters)
        return result

    def table_exists(self, table: Table) -> bool:
        """
        Check if a table exists in the database.

        :param table: Details of the table we want to check that exists
        """
        table_qualified_name = self.get_table_qualified_name(table)
        inspector = sqlalchemy.inspect(self.sqlalchemy_engine)
        return bool(
            inspector.dialect.has_table(
                self.get_connection(table), table_qualified_name
            )
        )

    # ---------------------------------------------------------
    # Table metadata
    # ---------------------------------------------------------
    def get_table_qualified_name(self, table: Table) -> str:  # skipcq: PYL-R0201
        """
        Return table qualified name. This is Database-specific.
        For instance, in Sqlite this is the table name. In Snowflake, however, it is the database, schema and table

        :param table: The table we want to retrieve the qualified name for.
        """
        # Initially this method belonged to the Table class.
        # However, in order to have an agnostic table class implementation,
        # we are keeping all methods which vary depending on the database within the Database class.
        if table.metadata and table.metadata.schema:
            qualified_name = f"{table.metadata.schema}.{table.name}"
        else:
            qualified_name = table.name
        return qualified_name

    @property
    def default_metadata(self) -> Metadata:
        raise NotImplementedError

    def populate_table_metadata(self, table: Table):
        # default = self.default_metadata
        # table_meta = table.metadata
        # table.metadata = Metadata(
        #     schema=table_meta.schema or default.schema,
        #     role=table_meta.role or default.role,
        #     warehouse=table_meta.warehouse or default.warehouse,
        #     database=table_meta.database or default.database,
        #     account=table_meta.account or default.account,
        #     region=table_meta.region or default.region
        # )

        if table.metadata and table.metadata.is_empty() and self.default_metadata:
            table.metadata = self.default_metadata
        if not table.metadata.schema:
            table.metadata.schema = SCHEMA
        return table

    # ---------------------------------------------------------
    # Table creation & deletion methods
    # ---------------------------------------------------------
    def create_table(self, table: Table) -> None:
        """
        Create a SQL table. For this method to work, the table instance must contain columns.

        :param table: The table to be created.
        """
        metadata = table.sqlalchemy_metadata
        sqlalchemy_table = sqlalchemy.Table(table.name, metadata, *table.columns)
        metadata.create_all(self.sqlalchemy_engine, tables=[sqlalchemy_table])

    def create_table_from_select_statement(
        self,
        statement: str,
        target_table: Table,
        parameters: Optional[dict] = None,
    ) -> None:
        """
        Export the result rows of a query statement into another table.

        :param statement: SQL query statement
        :param target_table: Destination table where results will be recorded.
        :param parameters: (Optional) parameters to be used to render the SQL query
        """
        statement = self._create_table_statement.format(
            self.get_table_qualified_name(target_table), statement
        )
        self.run_sql(statement, parameters)

    def drop_table(self, table: Table) -> None:
        """
        Delete a SQL table, if it exists.

        :param table: The table to be deleted.
        """
        statement = self._drop_table_statement.format(
            self.get_table_qualified_name(table)
        )
        self.run_sql(statement)

    # ---------------------------------------------------------
    # Table load methods
    # ---------------------------------------------------------
    def load_file_to_table(
        self,
        source_file: File,
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        """
        Load the content of the source file to the target database table.
        If the table already exists, append or replace the content, depending on the value of `if_exists`.

        :param source_file: Local or remote filepath (e.g. a File("/tmp/sample_data.csv"))
        :param target_table: Table in which the file will be loaded
        :param if_exists: Strategy to be used in case the target table already exists
        :param chunk_size: Specify the number of rows in each batch to be written at a time.
        """
        dataframe = source_file.export_to_dataframe()
        self.load_pandas_dataframe_to_table(
            dataframe,
            target_table,
            if_exists,
            chunk_size,
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
        source_dataframe.to_sql(
            self.get_table_qualified_name(target_table),
            con=self.sqlalchemy_engine,
            if_exists=if_exists,
            chunksize=chunk_size,
            method="multi",
            index=False,
        )

    def append_table(
        self,
        source_table: Table,
        target_table: Table,
        if_conflicts: AppendConflictStrategy = "exception",
    ):
        """
        Append the source table rows into a destination table.
        The argument `if_conflicts` allows the user to define how to handle conflicts.

        :param source_table: Contains the rows to be appended to the target_table
        :param target_table: Contains the destination table in which the rows will be appended
        :param if_conflicts: The strategy to be applied if there are conflicts.
        """
        raise NotImplementedError

    # ---------------------------------------------------------
    # Extract methods
    # ---------------------------------------------------------
    def export_table_to_pandas_dataframe(self, source_table: Table) -> pd.DataFrame:
        """
        Copy the content of a table to an in-memory Pandas dataframe.

        :param source_table: An existing table in the database
        """
        table_qualified_name = self.get_table_qualified_name(source_table)
        if self.table_exists(source_table):
            return pd.read_sql(
                # We are avoiding SQL injection by confirming the table exists before this statement
                f"SELECT * FROM {table_qualified_name}",  # skipcq BAN-B608
                con=self.get_sqlalchemy_engine(table=source_table),
            )
        raise NonExistentTableException(
            "The table %s does not exist" % table_qualified_name
        )

    def export_table_to_file(
        self,
        source_table: Table,
        target_file: File,
        if_exists: ExportExistsStrategy = "exception",
    ) -> None:
        """
        Copy the content of a table to a target file of supported type, in a supported location.

        :param source_table: An existing table in the database
        :param target_file: The path to the file to which we aim to dump the content of the database.
        """
        if if_exists == "exception" and target_file.exists():
            raise FileExistsError(f"The file {target_file} already exists.")

        df = self.export_table_to_pandas_dataframe(source_table)
        target_file.create_from_dataframe(df)

    # ---------------------------------------------------------
    # Schema Management
    # ---------------------------------------------------------

    def create_schema_if_needed(self, schema):
        """
        This function checks if the expected schema exists in the database. If the schema does not exist,
        it will attempt to create it.

        :param schema: DB Schema - a namespace that contains named objects like (tables, functions, etc)
        """
        if not schema:
            return
        if not self.schema_exists(schema):
            statement = self._create_schema_statement.format(schema)
            self.run_sql(statement)

    def schema_exists(self, schema):
        """
        Checks if a schema exists in the database

        :param schema: DB Schema - a namespace that contains named objects like (tables, functions, etc)
        """
        raise NotImplementedError

    # ---------------------------------------------------------
    # Context & Template Rendering methods (Transformations)
    # ---------------------------------------------------------

    def add_templates_to_context(
        self, parameters: Dict, context: Dict
    ) -> Dict:  # skipcq
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
                context[k] = self.get_table_qualified_name(v)
            else:
                context[k] = ":" + k
        return context

    def process_sql_parameters(self, parameters: Dict):
        """
        Used for DB-specific processing on parameters. It is used in Snowflake in conjunction with
        add_templates_to_context to pass the name of the table
        """
        return parameters
