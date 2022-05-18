import random
import string
from dataclasses import dataclass, field, fields
from typing import List, Union

from sqlalchemy import Column
from sqlalchemy import MetaData as SqlAlcMetaData

MAX_TABLE_NAME_LENGTH = 62


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
    metadata: Metadata = Metadata()
    columns: List[Column] = field(default_factory=list)
    temp: bool = False

    def __post_init__(self):
        if not self.name:
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
    def sqlalchemy_metadata(self) -> SqlAlcMetaData:
        """Return Sqlalchemy metadata for the given table."""
        if self.metadata and self.metadata.schema:
            alchemy_metadata = SqlAlcMetaData(schema=self.metadata.schema)
        else:
            alchemy_metadata = SqlAlcMetaData()
        return alchemy_metadata
