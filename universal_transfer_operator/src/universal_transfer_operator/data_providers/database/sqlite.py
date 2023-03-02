from __future__ import annotations

import socket

import attr
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlalchemy import MetaData as SqlaMetaData, create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.schema import Table as SqlaTable

from universal_transfer_operator.data_providers.database.base import DatabaseDataProvider, FileStream
from universal_transfer_operator.datasets.table import Metadata, Table
from universal_transfer_operator.universal_transfer_operator import TransferParameters


class SqliteDataProvider(DatabaseDataProvider):
    """SqliteDataProvider represent all the DataProviders interactions with Sqlite Databases."""

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
        return "sqlite"

    @property
    def hook(self) -> SqliteHook:
        """Retrieve Airflow hook to interface with the Sqlite database."""
        return SqliteHook(sqlite_conn_id=self.dataset.conn_id)

    @property
    def sqlalchemy_engine(self) -> Engine:
        """Return SQAlchemy engine."""
        # Airflow uses sqlite3 library and not SqlAlchemy for SqliteHook
        # and it only uses the hostname directly.
        airflow_conn = self.hook.get_connection(self.dataset.conn_id)
        return create_engine(f"sqlite:///{airflow_conn.host}")

    @property
    def default_metadata(self) -> Metadata:
        """Since Sqlite does not use Metadata, we return an empty Metadata instances."""
        return Metadata()

    def read(self):
        """ ""Read the dataset and write to local reference location"""
        raise NotImplementedError

    def write(self, source_ref: FileStream):
        """Write the data from local reference location to the dataset"""
        return self.load_file_to_table(
            input_file=source_ref.actual_file,
            output_table=self.dataset,
        )

    # ---------------------------------------------------------
    # Table metadata
    # ---------------------------------------------------------
    @staticmethod
    def get_table_qualified_name(table: Table) -> str:
        """
        Return the table qualified name.

        :param table: The table we want to retrieve the qualified name for.
        """
        return str(table.name)

    def populate_table_metadata(self, table: Table) -> Table:
        """
        Since SQLite does not have a concept of databases or schemas, we just return the table as is,
        without any modifications.
        """
        table.conn_id = table.conn_id or self.dataset.conn_id
        return table

    def create_schema_if_needed(self, schema: str | None) -> None:
        """
        Since SQLite does not have schemas, we do not need to set a schema here.
        """

    def schema_exists(self, schema: str) -> bool:  # skipcq PYL-W0613,PYL-R0201
        """
        Check if a schema exists. We return false for sqlite since sqlite does not have schemas
        """
        return False

    def get_sqla_table(self, table: Table) -> SqlaTable:
        """
        Return SQLAlchemy table instance

        :param table: Astro Table to be converted to SQLAlchemy table instance
        """
        return SqlaTable(table.name, SqlaMetaData(), autoload_with=self.sqlalchemy_engine)

    @property
    def openlineage_dataset_name(self) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: /tmp/local.db.table_name
        """
        conn = self.hook.get_connection(self.dataset.conn_id)
        return f"{conn.host}.{self.dataset.name}"

    @property
    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: file://127.0.0.1:22
        """
        conn = self.hook.get_connection(self.dataset.conn_id)
        port = conn.port or 22
        return f"file://{socket.gethostbyname(socket.gethostname())}:{port}"

    @property
    def openlineage_dataset_uri(self) -> str:
        """
        Returns the open lineage dataset uri as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        return f"{self.openlineage_dataset_namespace()}{self.openlineage_dataset_name()}"
