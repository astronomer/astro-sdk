import random
import string
from dataclasses import dataclass, field, fields
from typing import List, Optional

from airflow.models import DagRun, TaskInstance
from sqlalchemy import Column, MetaData

from astro.constants import UNIQUE_TABLE_NAME_LENGTH

MAX_TABLE_NAME_LENGTH = 62
TEMP_PREFIX = "_tmp_"


@dataclass
class Metadata:
    """
    Contains additional information to access a SQL Table, which is very likely optional and, in some cases, may
    be database-specific.
    """

    # This property is used by several databases, including: Postgres, Snowflake and BigQuery ("namespace")
    schema: Optional[str] = None
    database: Optional[str] = None

    def is_empty(self):
        """Check if all the fields are None."""
        values = [getattr(self, field.name) for field in fields(self)]
        return values.count(None) == len(values)


@dataclass
class Table:
    """
    Withholds the information necessary to access a SQL Table.
    It is agnostic to the database type.
    If no name is given, it auto-generates a name for the Table and considers it temporary.

    Temporary tables are prefixed with the prefix TEMP_PREFIX.
    """

    # TODO: discuss alternative names to this class, since it contains metadata as opposed to be the
    # SQL table itself
    # Some ideas: TableRef, TableMetadata, TableData, TableDataset
    conn_id: str = ""
    name: str = ""
    _name: str = field(init=False, repr=False, default="")
    metadata: Metadata = field(default_factory=Metadata)
    columns: List[Column] = field(default_factory=list)
    temp: bool = False

    def __post_init__(self):
        if not self._name:
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

    def create_similar_table(self) -> "Table":
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

    @property  # type: ignore
    def name(self) -> str:
        """
        Return either the user-defined name or auto-generate one.
        """
        if self.temp and not self._name:
            self._name = self._create_unique_table_name(TEMP_PREFIX)
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """
        Set the table name. Once this happens, the table is no longer considered temporary.
        """
        if not isinstance(value, property):
            self._name = value
            self.temp = False


# TODO: deprecate by the end of the refactoring
def create_table_name(context) -> str:
    ti: TaskInstance = context["ti"]
    dag_run: DagRun = ti.get_dagrun()
    table_name = f"{dag_run.dag_id}_{ti.task_id}_{dag_run.id}".replace(
        "-", "_"
    ).replace(".", "__")[:MAX_TABLE_NAME_LENGTH]
    if not table_name.isidentifier():
        table_name = f'"{table_name}"'
    return table_name


# TODO: deprecate by the end of the refactoring
def create_unique_table_name(length: int = UNIQUE_TABLE_NAME_LENGTH) -> str:
    """
    Create a unique table name of the requested size, which is compatible with all supported databases.

    :return: Unique table name
    :rtype: str
    """
    unique_id = random.choice(string.ascii_lowercase) + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(length - 1)
    )
    return unique_id
