from __future__ import annotations

import random
import string

from attr import define, field, fields_dict
from sqlalchemy import Column, MetaData

MAX_TABLE_NAME_LENGTH = 62
TEMP_PREFIX = "_tmp_"


@define
class Metadata:
    """
    Contains additional information to access a SQL Table, which is very likely optional and, in some cases, may
    be database-specific.
    """

    # This property is used by several databases, including: Postgres, Snowflake and BigQuery ("namespace")
    schema: str | None = None
    database: str | None = None

    def is_empty(self) -> bool:
        """Check if all the fields are None."""
        li = [
            getattr(self, field_name) is None
            for field_name in fields_dict(self.__class__)
        ]
        return all(li)


@define
class Table:
    """
    Withholds the information necessary to access a SQL Table.
    It is agnostic to the database type.
    If no name is given, it auto-generates a name for the Table and considers it temporary.

    Temporary tables are prefixed with the prefix TEMP_PREFIX.
    """

    template_fields = ("name",)

    # TODO: discuss alternative names to this class, since it contains metadata as opposed to be the
    # SQL table itself
    # Some ideas: TableRef, TableMetadata, TableData, TableDataset
    conn_id: str = field(default="")
    name: str = field(default="")
    metadata: Metadata = field(factory=Metadata)
    columns: list[Column] = field(factory=list)
    temp: bool = field(default=False)

    def __attrs_post_init__(self) -> None:
        if not self.name or self.name.startswith("_tmp"):
            self.temp = True
            self.name = self._create_unique_table_name(TEMP_PREFIX)

    def _create_unique_table_name(self, prefix: str = "") -> str:
        """
        If a table is instantiated without a name, create a unique table for it.
        This new name should be compatible with all supported databases.
        """
        schema_length = len((self.metadata and self.metadata.schema) or "") + 1
        prefix_length = len(prefix)

        unique_id = random.choice(string.ascii_lowercase) + "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in range(MAX_TABLE_NAME_LENGTH - schema_length - prefix_length)
        )
        if prefix:
            unique_id = f"{prefix}{unique_id}"

        return unique_id

    def create_similar_table(self) -> Table:
        """
        Create a new table with a unique name but with the same metadata.
        """
        return Table(
            name=self._create_unique_table_name(),
            conn_id=self.conn_id,
            metadata=self.metadata,
        )

    @property
    def sqlalchemy_metadata(self) -> MetaData:
        """Return the Sqlalchemy metadata for the given table."""
        if self.metadata and self.metadata.schema:
            alchemy_metadata = MetaData(schema=self.metadata.schema)
        else:
            alchemy_metadata = MetaData()
        return alchemy_metadata
