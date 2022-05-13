from typing import Dict

from airflow.decorators.base import get_unique_task_id
from sqlalchemy import MetaData
from sqlalchemy.sql.schema import Table as SqlaTable

from astro.constants import Database
from astro.sql.operators.sql_decorator import SqlDecoratedOperator
from astro.sql.tables import Table
from astro.utils.database import create_database_from_conn_id


class SqlTruncateOperator(SqlDecoratedOperator):
    def __init__(
        self,
        table: Table,
        **kwargs,
    ):
        self.sql = ""
        self.table = table

        task_id = get_unique_task_id(table.name + "_truncate")

        def null_function(table: Table):
            pass

        super().__init__(
            raw_sql=True,
            parameters={},
            task_id=task_id,
            op_args=(),
            op_kwargs={"table": table},
            python_callable=null_function,
            conn_id=self.table.conn_id,
            database=getattr(self.table.metadata, "database", None),
            schema=getattr(self.table.metadata, "schema", None),
            warehouse=getattr(self.table.metadata, "warehouse", None),
            **kwargs,
        )

    def execute(self, context: Dict):
        database = create_database_from_conn_id(self.table.conn_id)
        if getattr(self.table.metadata, "schema", None) and database == Database.SQLITE:
            metadata = MetaData()
        else:
            metadata = MetaData(schema=getattr(self.table.metadata, "schema", None))

        engine = self.get_sql_alchemy_engine()
        table_sqla = SqlaTable(self.table.name, metadata, autoload_with=engine)
        self.sql = table_sqla.delete()
        super().execute(context)
