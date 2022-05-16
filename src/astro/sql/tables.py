import random
import string

from dataclasses import dataclass, fields
from typing import Any, List, Optional, Union

import pandas as pd
from sqlalchemy import Column, MetaData

from astro.constants import DEFAULT_CHUNK_SIZE, LoadExistStrategy

MAX_TABLE_NAME_LENGTH = 63


@dataclass
class Metadata:
    """
    Contains additional information to access a SQL Table, which is very likely optional and, in some cases, may
    be database-specific.
    """

    # e.g.: Postgres, Snowflake:
    schema: Union[str, None] = None
    # e.g.: Snowflake:
    account: Union[str, None] = None
    database: Union[str, None] = None
    host: Union[str, None] = None
    region: Union[str, None] = None
    role: Union[str, None] = None
    warehouse: Union[str, None] = None

    def is_empty(self):
        """Check if all the fields are None."""
        values = [getattr(self, field.name) for field in fields(self)]
        return values.count(None) == len(values)


@dataclass
class Table:
    """
    Withholds the information necessary to access a SQL Table.
    It is agnostic to the database type.
    """

    # TODO: discuss alternative names to this class, since it contains metadata as opposed to be the
    # SQL table itself
    # Some ideas: TableRef, TableMetadata, TableData, TableDataset
    conn_id: str = ""
    name: str = ""
    metadata: Optional[Metadata] = None
    columns: Optional[List[Column]] = None
    temp: bool = False

    def __init__(
        self,
        name="",
        conn_id=None,
        database=None,
        schema=None,
        warehouse=None,
        role=None,
        metadata=None,
        columns=None,
    ):
        self.db = None
        self.name = name
        self.conn_id = conn_id

        self.metadata = metadata
        if self.metadata is None:
            self.metadata = Metadata()
            self.metadata.database = database
            self.metadata.schema = schema
            self.metadata.warehouse = warehouse
            self.metadata.role = role

        self.columns = columns
        if self.columns is None:
            self.columns = []

        if not name:
            self.name = self._create_unique_table_name()
            self.temp = True

    def _create_unique_table_name(self) -> str:
        """
        If a table is instantiated without a name, create a unique table for it.
        This new name should be compatible with all supported databases.
        """
        schema_length = len((self.metadata and self.metadata.schema) or "") + 1
        unique_id = random.choice(string.ascii_lowercase) + "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in range(MAX_TABLE_NAME_LENGTH - schema_length)
        )
        return unique_id

    @property
    def sqlalchemy_metadata(self) -> MetaData:
        """Return Sqlalchemy metadata for the given table."""
        if self.metadata and self.metadata.schema:
            alchemy_metadata = MetaData(schema=self.metadata.schema)
        else:
            alchemy_metadata = MetaData()
        return alchemy_metadata

    def __set_db(self):
        if not self.db and self.conn_id:
            from astro.databases import create_database

            self.db = create_database(self.conn_id)

    @property
    def qualified_name(self) -> Optional[str]:
        """Return table qualified name. This is Database-specific."""
        self.__set_db()
        return str(self.db.get_table_qualified_name(self))

    def load_pandas_dataframe_to_table(
        self,
        source_dataframe: pd.DataFrame,
        target_table: Any,  # To Do - Fix me!
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
        self.__set_db()
        self.db.load_pandas_dataframe_to_table(
            source_dataframe=source_dataframe,
            target_table=target_table,
            if_exists=if_exists,
            chunk_size=chunk_size,
        )

    @property
    def schema(self) -> Optional[str]:
        return getattr(self.metadata, "schema", None)

    @schema.setter
    def schema(self, val: str):
        if self.metadata is None:
            self.metadata = Metadata()

        self.metadata.schema = val

    @property
    def database(self) -> Optional[str]:
        return getattr(self.metadata, "database", None)

    @database.setter
    def database(self, val: str):
        if self.metadata is None:
            self.metadata = Metadata()

        self.metadata.database = val

    @property
    def warehouse(self) -> Optional[str]:
        return getattr(self.metadata, "warehouse", None)

    @warehouse.setter
    def warehouse(self, val: str):
        if self.metadata is None:
            self.metadata = Metadata()

        self.metadata.warehouse = val

    @property
    def role(self) -> Optional[str]:
        return getattr(self.metadata, "role", None)

    @role.setter
    def role(self, val: str):
        if self.metadata is None:
            self.metadata = Metadata()

        self.metadata.role = val

    @property
    def table_name(self) -> str:
        # To Do -- replace all the instance of table.table_name with table.name
        return str(self.name)

    @table_name.setter
    def table_name(self, val: str):
        self.name = val

    def drop(self):
        if self.db:
            self.db.drop_table(self)
