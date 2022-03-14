from typing import Dict

from airflow.decorators.base import get_unique_task_id
from sqlalchemy import MetaData
from sqlalchemy.sql.schema import Table as SqlaTable

from astro.sql.operators.sql_decorator import SqlDecoratedOperator
from astro.sql.table import Table


class SqlTruncateOperator(SqlDecoratedOperator):
    def __init__(
        self,
        table: Table,
        **kwargs,
    ):
        self.sql = ""
        self.table = table

        task_id = get_unique_task_id(table.table_name + "_truncate")

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
            database=self.table.database,
            schema=self.table.schema,
            warehouse=self.table.warehouse,
            **kwargs,
        )

    def execute(self, context: Dict):
        engine = self.get_sql_alchemy_engine()
        metadata = MetaData()
        table_sqla = SqlaTable(self.table.table_name, metadata, autoload_with=engine)
        self.sql = table_sqla.delete()

        super().execute(context)
