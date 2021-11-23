from typing import Dict

from astro.sql.operators.sql_decorator import SqlDecoratoratedOperator


class SqlTruncateOperator(SqlDecoratoratedOperator):
    def __init__(
        self,
        database: str,
        conn_id: str = "",
        table_name: str = "",
        **kwargs,
    ):
        self.conn_id = conn_id
        self.sql = ""
        self.table_name = table_name

        task_id = table_name + "_truncate"

        def null_function():
            pass

        super().__init__(
            raw_sql=True,
            parameters={},
            conn_id=conn_id,
            task_id=task_id,
            op_args=(),
            python_callable=null_function,
            database=database,
            **kwargs,
        )

    def execute(self, context: Dict):
        from astro.sql.types import Table

        self.sql = "TRUNCATE TABLE {table_name};"

        def table_func(table_name: Table):
            pass

        self.python_callable = table_func
        self.parameters = {"table_name": self.table_name}

        super().execute(context)
