from __future__ import annotations

from tempfile import NamedTemporaryFile
from typing import Sequence

import attr
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector import pandas_tools

from universal_transfer_operator.constants import DEFAULT_CHUNK_SIZE, ColumnCapitalization, LoadExistStrategy
from universal_transfer_operator.data_providers.database.base import DatabaseDataProvider, FileStream
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Metadata, Table
from universal_transfer_operator.settings import LOAD_TABLE_AUTODETECT_ROWS_COUNT, SNOWFLAKE_SCHEMA
from universal_transfer_operator.universal_transfer_operator import TransferParameters


class SnowflakeDataProvider(DatabaseDataProvider):
    """SnowflakeDataProvider represent all the DataProviders interactions with Snowflake Databases."""

    DEFAULT_SCHEMA = SNOWFLAKE_SCHEMA

    def __init__(
        self,
        dataset: Table,
        transfer_mode,
        transfer_params: TransferParameters = attr.field(
            factory=TransferParameters,
            converter=lambda val: TransferParameters(**val) if isinstance(val, dict) else val,
        ),
    ):
        self.dataset = dataset
        self.transfer_params = transfer_params
        self.transfer_mode = transfer_mode
        self.transfer_mapping = {}
        self.LOAD_DATA_NATIVELY_FROM_SOURCE: dict = {}
        super().__init__(
            dataset=self.dataset, transfer_mode=self.transfer_mode, transfer_params=self.transfer_params
        )

    def __repr__(self):
        return f'{self.__class__.__name__}(conn_id="{self.dataset.conn_id})'

    @property
    def sql_type(self) -> str:
        return "snowflake"

    @property
    def hook(self) -> SnowflakeHook:
        """Retrieve Airflow hook to interface with the Snowflake database."""
        kwargs = {}
        _hook = SnowflakeHook(snowflake_conn_id=self.dataset.conn_id)
        if self.dataset and self.dataset.metadata:
            if _hook.database is None and self.dataset.metadata.database:
                kwargs.update({"database": self.dataset.metadata.database})
            if _hook.schema is None and self.dataset.metadata.schema:
                kwargs.update({"schema": self.dataset.metadata.schema})
        return SnowflakeHook(snowflake_conn_id=self.dataset.conn_id, **kwargs)

    @property
    def default_metadata(self) -> Metadata:
        """
        Fill in default metadata values for table objects addressing snowflake databases
        """
        connection = self.hook.get_conn()
        return Metadata(  # type: ignore
            schema=connection.schema,
            database=connection.database,
        )

    def read(self):
        """ ""Read the dataset and write to local reference location"""
        with NamedTemporaryFile(delete=True) as tmp_file:
            df = self.export_table_to_pandas_dataframe()
            df.to_parquet(tmp_file.name)
            return tmp_file.file

    def write(self, source_ref: FileStream):
        """
        Write the data from local reference location to the dataset

        :param source_ref: Stream of data to be loaded into snowflake table.
        """
        return self.load_file_to_table(
            input_file=source_ref.actual_file,
            output_table=self.dataset,
        )

    # ---------------------------------------------------------
    # Table metadata
    # ---------------------------------------------------------
    @staticmethod
    def get_table_qualified_name(table: Table) -> str:  # skipcq: PYL-R0201
        """
        Return table qualified name. In Snowflake, it is the database, schema and table

        :param table: The table we want to retrieve the qualified name for.
        """
        qualified_name_lists = [
            table.metadata.database,
            table.metadata.schema,
            table.name,
        ]
        qualified_name = ".".join(name for name in qualified_name_lists if name)
        return qualified_name

    def schema_exists(self, schema: str) -> bool:
        """
        Checks if a schema exists in the database

        :param schema: DB Schema - a namespace that contains named objects like (tables, functions, etc)
        """

        # Below code is added due to breaking change in apache-airflow-providers-snowflake==3.2.0,
        # we need to pass handler param to get the rows. But in version apache-airflow-providers-snowflake==3.1.0
        # if we pass the handler provider raises an exception AttributeError 'sfid'.
        try:
            schemas = self.hook.run(
                "SELECT SCHEMA_NAME from information_schema.schemata WHERE LOWER(SCHEMA_NAME) = %(schema_name)s;",
                parameters={"schema_name": schema.lower()},
                handler=lambda cur: cur.fetchall(),
            )
        except AttributeError:
            schemas = self.hook.run(
                "SELECT SCHEMA_NAME from information_schema.schemata WHERE LOWER(SCHEMA_NAME) = %(schema_name)s;",
                parameters={"schema_name": schema.lower()},
            )
        try:
            # Handle case for apache-airflow-providers-snowflake<4.0.1
            created_schemas = [x["SCHEMA_NAME"] for x in schemas]
        except TypeError:
            # Handle case for apache-airflow-providers-snowflake>=4.0.1
            created_schemas = [x[0] for x in schemas]
        return len(created_schemas) == 1

    def create_table_using_schema_autodetection(
        self,
        table: Table,
        file: File | None = None,
        dataframe: pd.DataFrame | None = None,
        columns_names_capitalization: ColumnCapitalization = "original",
    ) -> None:  # skipcq PYL-W0613
        """
        Create a SQL table, automatically inferring the schema using the given file.
        Overriding default behaviour and not using the `prep_table` since it doesn't allow the adding quotes.

        :param table: The table to be created.
        :param file: File used to infer the new table columns.
        :param dataframe: Dataframe used to infer the new table columns if there is no file
        :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
        """
        if file is None:
            if dataframe is None:
                raise ValueError(
                    "File or Dataframe is required for creating table using schema autodetection"
                )
            source_dataframe = dataframe
        else:
            source_dataframe = file.export_to_dataframe(nrows=LOAD_TABLE_AUTODETECT_ROWS_COUNT)

        # We are changing the case of table name to ease out on the requirements to add quotes in raw queries.
        # ToDO - Currently, we cannot to append using load_file to a table name which is having name in lower case.
        pandas_tools.write_pandas(
            conn=self.hook.get_conn(),
            df=source_dataframe,
            table_name=table.name.upper(),
            schema=table.metadata.schema,
            database=table.metadata.database,
            chunk_size=DEFAULT_CHUNK_SIZE,
            quote_identifiers=self.use_quotes(source_dataframe),
            auto_create_table=True,
        )
        # We are truncating since we only expect table to be created with required schema.
        # Since this method is used by both native and pandas path we cannot skip this step.
        self.truncate_table(table)

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
        self._assert_not_empty_df(source_dataframe)

        auto_create_table = False
        if not self.table_exists(target_table):
            auto_create_table = True
        elif if_exists == "replace":
            self.create_table(target_table, dataframe=source_dataframe)

        # We are changing the case of table name to ease out on the requirements to add quotes in raw queries.
        # ToDO - Currently, we cannot to append using load_file to a table name which is having name in lower case.
        pandas_tools.write_pandas(
            conn=self.hook.get_conn(),
            df=source_dataframe,
            table_name=target_table.name.upper(),
            schema=target_table.metadata.schema,
            database=target_table.metadata.database,
            chunk_size=chunk_size,
            quote_identifiers=self.use_quotes(source_dataframe),
            auto_create_table=auto_create_table,
        )

    def truncate_table(self, table):
        """Truncate table"""
        self.run_sql(f"TRUNCATE {self.get_table_qualified_name(table)}")

    @classmethod
    def use_quotes(cls, cols: Sequence[str]) -> bool:
        """
        With snowflake identifier we have two cases,

        1. When Upper/Mixed case col names are used
            We are required to preserver the text casing of the col names. By adding the quotes around identifier.
        2. When lower case col names are used
            We can use them as is

        This is done to be in sync with Snowflake SQLAlchemy dialect.
        https://docs.snowflake.com/en/user-guide/sqlalchemy.html#object-name-case-handling

        Snowflake stores all case-insensitive object names in uppercase text. In contrast, SQLAlchemy considers all
        lowercase object names to be case-insensitive. Snowflake SQLAlchemy converts the object name case during
        schema-level communication (i.e. during table and index reflection). If you use uppercase object names,
        SQLAlchemy assumes they are case-sensitive and encloses the names with quotes. This behavior will cause
        mismatches against data dictionary data received from Snowflake, so unless identifier names have been truly
        created as case sensitive using quotes (e.g. "TestDb"), all lowercase names should be used on the SQLAlchemy
        side.

        :param cols: list of columns
        """
        return any(col for col in cols if not col.islower() and not col.isupper())

    @property
    def openlineage_dataset_name(self) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: db_name.schema_name.table_name
        """
        conn = self.hook.get_connection(self.dataset.conn_id)
        conn_extra = conn.extra_dejson
        schema = conn_extra.get("schema") or conn.schema
        db = conn_extra.get("database")
        return f"{db}.{schema}.{self.dataset.name}"

    @property
    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: snowflake://ACCOUNT
        """
        account = self.hook.get_connection(self.dataset.conn_id).extra_dejson.get("account")
        return f"{self.sql_type}://{account}"

    @property
    def openlineage_dataset_uri(self) -> str:
        """
        Returns the open lineage dataset uri as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        return f"{self.openlineage_dataset_namespace()}{self.openlineage_dataset_name()}"
