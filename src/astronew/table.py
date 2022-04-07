from airflow.hooks.base import BaseHook

from astro.settings import SCHEMA


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

    def __str__(self):
        return (
            f"Table(table_name={self.table_name}, database={self.database}, "
            f"schema={self.schema}, conn_id={self.conn_id}, warehouse={self.warehouse}, role={self.role})"
        )


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

    def to_table(self, table_name: str, schema: str = None) -> Table:
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
