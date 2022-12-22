from __future__ import annotations

from urllib.parse import urlparse

from attr import define, field, fields_dict
from sqlalchemy import Column
from transfers.datasets.base import UniversalDataset


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
class Table(UniversalDataset):
    """
    Repersents all Table datasets.
    Intended to be used within library.

    :param path: Path to a database
    :param conn_id: Airflow connection ID
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

    @property
    def sql_type(self):
        raise NotImplementedError

    def exists(self):
        """Check if the table exists or not"""
        raise NotImplementedError

    def __str__(self) -> str:
        return self.path

    def __hash__(self) -> int:
        return hash((self.path, self.conn_id))

    def dataset_scheme(self):
        """
        Return the scheme based on path
        """
        parsed = urlparse(self.path)
        return parsed.scheme

    def dataset_namespace(self):
        """
        The namespace of a dataset can be combined to form a URI (scheme:[//authority]path)

        Namespace = scheme:[//authority] (the dataset)
        """
        parsed = urlparse(self.path)
        namespace = f"{self.dataset_scheme()}://{parsed.netloc}"
        return namespace

    def dataset_name(self):
        """
        The name of a dataset can be combined to form a URI (scheme:[//authority]path)

        Name = path (the datasets)
        """
        parsed = urlparse(self.path)
        return parsed.path if self.path else self.name
