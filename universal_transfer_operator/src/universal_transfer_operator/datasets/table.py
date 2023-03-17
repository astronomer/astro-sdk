from __future__ import annotations

import random
import string
from typing import Any, cast

from attr import define, field, fields_dict
from sqlalchemy import Column, MetaData

from universal_transfer_operator.datasets.base import Dataset

MAX_TABLE_NAME_LENGTH = 62
TEMP_PREFIX = "_tmp"


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
        return all(getattr(self, field_name) is None for field_name in fields_dict(self.__class__))


@define
class Table(Dataset):
    """
    Repersents all Table datasets.
    Intended to be used within library.

    :param name: The name of the database table. If name not provided then it would create a temporary name
    :param conn_id: The Airflow connection id. This will be used to identify the right database type at the runtime
    :param metadata: A metadata object which will have database or schema name
    :param columns: columns which define the database table schema.
    """

    name: str = field(default="")
    conn_id: str = field(default="")
    # Setting converter allows passing a dictionary to metadata arg
    metadata: Metadata = field(
        factory=Metadata,
        converter=lambda val: Metadata(**val) if isinstance(val, dict) else val,
    )
    columns: list[Column] = field(factory=list)
    uri: str = field(init=False)
    extra: dict = field(init=True, factory=dict)

    def exists(self):
        """Check if the table exists or not"""
        raise NotImplementedError

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

    def __attrs_post_init__(self) -> None:
        if not self.name:
            self.name = self._create_unique_table_name(TEMP_PREFIX + "_")

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
    def row_count(self) -> Any:
        """
        Return the row count of table.
        """
        from universal_transfer_operator.data_providers import create_dataprovider
        from universal_transfer_operator.data_providers.database.base import DatabaseDataProvider

        database_provider = cast(DatabaseDataProvider, create_dataprovider(dataset=self))
        return database_provider.row_count(self)

    @property
    def sql_type(self) -> Any:
        from universal_transfer_operator.data_providers import create_dataprovider
        from universal_transfer_operator.data_providers.database.base import DatabaseDataProvider

        if self.conn_id:
            database_provider = cast(DatabaseDataProvider, create_dataprovider(dataset=self))
            return database_provider.sql_type

    def to_json(self):
        return {
            "class": "Table",
            "name": self.name,
            "metadata": {
                "schema": self.metadata.schema,
                "database": self.metadata.database,
            },
            "temp": self.temp,
            "conn_id": self.conn_id,
        }

    @classmethod
    def from_json(cls, obj: dict):
        return Table(
            name=obj["name"],
            metadata=Metadata(**obj["metadata"]),
            conn_id=obj["conn_id"],
        )

    def openlineage_dataset_name(self) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        from universal_transfer_operator.data_providers import create_dataprovider
        from universal_transfer_operator.data_providers.database.base import DatabaseDataProvider

        database_provider = cast(DatabaseDataProvider, create_dataprovider(dataset=self))
        return database_provider.openlineage_dataset_name

    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        from universal_transfer_operator.data_providers import create_dataprovider
        from universal_transfer_operator.data_providers.database.base import DatabaseDataProvider

        database_provider = cast(DatabaseDataProvider, create_dataprovider(dataset=self))
        return database_provider.openlineage_dataset_namespace

    def openlineage_dataset_uri(self) -> str:
        """
        Returns the open lineage dataset uri as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        from universal_transfer_operator.data_providers import create_dataprovider
        from universal_transfer_operator.data_providers.database.base import DatabaseDataProvider

        database_provider = cast(DatabaseDataProvider, create_dataprovider(dataset=self))
        return database_provider.openlineage_dataset_uri

    @uri.default
    def _path_to_dataset_uri(self) -> str:
        """Build a URI to be passed to Dataset obj introduced in Airflow 2.4"""
        from urllib.parse import urlencode, urlparse

        path = f"astro://{self.conn_id}@"
        db_extra = {"table": self.name}
        if self.metadata.schema:
            db_extra["schema"] = self.metadata.schema
        if self.metadata.database:
            db_extra["database"] = self.metadata.database
        parsed_url = urlparse(url=path)
        new_parsed_url = parsed_url._replace(query=urlencode(db_extra))
        return new_parsed_url.geturl()
