from typing import Callable, Iterable, Mapping, Optional, Union

from airflow.decorators.base import DecoratedOperator, task_decorator_factory

from astronomer_sql_decorator.operators.sql_to_dataframe import SqlToDataframeOperator


def from_sql(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "",
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
):
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=SqlToDataframeOperator,
        **{
            "conn_id": conn_id,
            "database": database,
            "schema": schema,
            "warehouse": warehouse,
        }
    )
