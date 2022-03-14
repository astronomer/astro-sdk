from airflow.hooks.base import BaseHook
from airflow.models import DagRun, TaskInstance

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
        return f"Table(table_name={self.table_name}, database={self.database}, schema={self.schema}, conn_id={self.conn_id}, warehouse={self.warehouse})"


class TempTable(Table):
    def __init__(self, conn_id=None, database=None, warehouse=""):
        self.table_name = ""
        super().__init__(
            table_name=self.table_name,
            conn_id=conn_id,
            database=database,
            warehouse=warehouse,
        )

    def to_table(self, table_name: str, schema: str) -> Table:
        self.table_name = table_name
        self.schema = schema
        return Table(
            table_name=table_name,
            conn_id=self.conn_id,
            database=self.database,
            warehouse=self.warehouse,
            schema=schema,
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
