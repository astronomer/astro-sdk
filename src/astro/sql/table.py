import random
import string
from typing import Optional

from airflow.hooks.base import BaseHook
from airflow.models import DagRun, TaskInstance

from astro.constants import UNIQUE_TABLE_NAME_LENGTH, Database
from astro.settings import SCHEMA
from astro.utils import get_hook
from astro.utils.database import get_database_name

MAX_TABLE_NAME_SIZE = 63


class Table:
    template_fields = (
        "table_name",
        "conn_id",
        "database",
        "schema",
        "warehouse",
        "role",
    )

    def __init__(
        self,
        table_name="",
        conn_id=None,
        database=None,
        schema=None,
        warehouse=None,
        role=None,
    ):
        self.table_name = table_name
        self.conn_id = conn_id
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.role = role
        self._conn_type = None

    @property
    def conn_type(self):
        if self._conn_type:
            return self._conn_type
        self._conn_type = BaseHook.get_connection(self.conn_id).conn_type
        return self._conn_type

    def identifier_args(self):
        return (self.schema, self.table_name) if self.schema else (self.table_name,)

    def qualified_name(self):
        if self.conn_type == "sqlite":
            return self.table_name
        else:
            return (
                self.schema + "." + self.table_name if self.schema else self.table_name
            )

    def __str__(self):
        return (
            f"Table(table_name={self.table_name}, database={self.database}, "
            f"schema={self.schema}, conn_id={self.conn_id}, warehouse={self.warehouse}, role={self.role})"
        )

    def drop(self):
        hook = get_hook(
            conn_id=self.conn_id,
            database=self.database,
            schema=self.schema,
            warehouse=self.warehouse,
        )

        query = f"DROP TABLE IF EXISTS {self.table_name};"
        database_name = get_database_name(hook)
        if database_name == Database.BIGQUERY:
            query = f"DROP TABLE IF EXISTS {self.schema}.{self.table_name};"
        hook.run(query)


class TempTable(Table):
    def __init__(self, conn_id=None, database=None, schema=None, warehouse="", role=""):
        self.table_name = ""
        super().__init__(
            table_name=self.table_name,
            conn_id=conn_id,
            database=database,
            warehouse=warehouse,
            role=role,
            schema=schema,
        )

    def to_table(self, table_name: str, schema: Optional[str] = None) -> Table:
        self.table_name = table_name
        self.schema = schema or self.schema or SCHEMA

        return Table(
            table_name=table_name,
            conn_id=self.conn_id,
            database=self.database,
            warehouse=self.warehouse,
            role=self.role,
            schema=self.schema,
        )


def create_table_name(context) -> str:
    ti: TaskInstance = context["ti"]
    dag_run: DagRun = ti.get_dagrun()
    table_name = f"{dag_run.dag_id}_{ti.task_id}_{dag_run.id}".replace(
        "-", "_"
    ).replace(".", "__")[:MAX_TABLE_NAME_SIZE]
    if not table_name.isidentifier():
        table_name = f'"{table_name}"'
    return table_name


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
