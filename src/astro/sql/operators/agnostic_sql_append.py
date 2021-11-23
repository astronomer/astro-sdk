from typing import Dict, List

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from astro.sql.operators.sql_decorator import SqlDecoratoratedOperator
from astro.utils.postgres_append import postgres_append_func
from astro.utils.snowflake_append import snowflake_append_func


class SqlAppendOperator(SqlDecoratoratedOperator):
    def __init__(
        self,
        database: str,
        append_table: str,
        main_table: str,
        conn_id: str = "",
        columns: List[str] = [],
        casted_columns: dict = {},
        **kwargs,
    ):
        self.append_table = append_table
        self.main_table = main_table
        self.conn_id = conn_id
        self.sql = ""

        self.columns = columns
        self.casted_columns = casted_columns
        task_id = main_table + "_" + append_table + "_" + "append"

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
        conn_type = BaseHook.get_connection(self.conn_id).conn_type
        if conn_type == "postgres":
            self.sql = postgres_append_func(
                main_table=self.main_table,
                append_table=self.append_table,
                columns=self.columns,
                casted_columns=self.casted_columns,
                conn_id=self.conn_id,
            )
        elif conn_type == "snowflake":
            self.sql, self.parameters = snowflake_append_func(
                main_table=self.main_table,
                append_table=self.append_table,
                columns=self.columns,
                casted_columns=self.casted_columns,
                snowflake_conn_id=self.conn_id,
            )
        else:
            raise AirflowException(f"Please specify a postgres or snowflake conn id.")

        super().execute(context)
