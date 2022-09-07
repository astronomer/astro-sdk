from __future__ import annotations

import random
import string

from attrs import define, field, fields_dict
from sqlalchemy import Column, MetaData

MAX_TABLE_NAME_LENGTH = 62
TEMP_PREFIX = "_tmp_"


@define
class Metadata:
    """
    Contains additional information to access a SQL Table, which is very likely optional and, in some cases, may
    be database-specific.

    :param schema: A schema name
    :param database: A database name
    """

    # This property is used by several databases, including: Postgres, Snowflake and BigQuery ("namespace")
    schema: str | None = None
    database: str | None = None

    def is_empty(self) -> bool:
        """Check if all the fields are None."""
        return all(
            getattr(self, field_name) is None
            for field_name in fields_dict(self.__class__)
        )


@define
class Table:
    """
    Withholds the information necessary to access a SQL Table.
    It is agnostic to the database type.
    If no name is given, it auto-generates a name for the Table and considers it temporary.

    Temporary tables are prefixed with the prefix TEMP_PREFIX.

    :param conn_id: The Airflow connection id. This will be used to identify the right database type at the runtime
    :param name: The name of the database table. If name not provided then it would create a temporary name
    :param metadata: A metadata object which will have database or schema name
    :param columns: Table columns names.
        If columns values provided then it would create table having provided columns names
        otherwise it autodetect columns name from dataset
    """

    template_fields = ("name",)

    # TODO: discuss alternative names to this class, since it contains metadata as opposed to be the
    # SQL table itself
    # Some ideas: TableRef, TableMetadata, TableData, TableDataset
    conn_id: str = field(default="")
    _name: str = field(default="")
    # Setting converter allows passing a dictionary to metadata arg
    metadata: Metadata = field(
        factory=Metadata,
        converter=lambda val: Metadata(**val) if isinstance(val, dict) else val,
    )
    columns: list[Column] = field(factory=list)
    temp: bool = field(default=False)

    def __attrs_post_init__(self) -> None:
        if not self._name or self._name.startswith("_tmp"):
            self.temp = True

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
        return Table(  # type: ignore
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

    @property
    def name(self) -> str:
        """
        Return either the user-defined name or auto-generate one.
        :sphinx-autoapi-skip:
        """
        if self.temp and not self._name:
            self._name = self._create_unique_table_name(TEMP_PREFIX)
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """
        Set the table name. Once this happens, the table is no longer considered temporary.
        """
        if not isinstance(value, property) and value != self._name:
            self._name = value
            self.temp = False
