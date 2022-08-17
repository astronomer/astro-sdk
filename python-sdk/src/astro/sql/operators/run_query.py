from typing import Any, Callable, Optional

from airflow.decorators.base import get_unique_task_id
from airflow.models import BaseOperator
from airflow.utils.context import Context

from astro.databases import create_database


class RunQueryOperator(BaseOperator):
    """
    Run a SQL statement and return the result of execution

    :param sql_statement: sql query to execute.
        If the sql query will return huge number of row then it can overload the XCOM.
        also, If you are using output of this method to expand a task using dynamic task map then
        it can create lots of parallel task. So it is advisible to limit your sql query statement.
    :param conn_id: Airflow connection id. This connection id will be used to identify the database client
        and connect with it at runtime
    :param handler: A Python callable handler to run on sqlalchemy legacy cursor result
    """

    template_fields = ("sql_statement", "conn_id")

    def __init__(
        self,
        sql_statement: str,
        conn_id: str,
        handler: Optional[Callable] = None,
        **kwargs
    ):
        kwargs["task_id"] = kwargs.get("task_id") or get_unique_task_id(
            "get_value_list", dag=kwargs.get("dag"), task_group=kwargs.get("task_group")
        )
        self.sql_statement = sql_statement
        self.conn_id = conn_id
        self.handler = handler
        super().__init__(**kwargs)

    def execute(self, context: Context) -> Any:  # skipcq: PYL-W0613
        db = create_database(self.conn_id)
        result_set = db.run_sql(self.sql_statement)
        # Note: This might push lots of value in Xcom depend on the SQL statement
        if self.handler:
            return self.handler(result_set)
        else:
            return result_set.fetchall()
