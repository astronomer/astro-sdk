from typing import Callable, Optional

from airflow.decorators.base import task_decorator_factory

from astro.sql.operators.sql_dataframe import SqlDataframeOperator


def dataframe(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "",
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
):
    """
    This function allows a user to run python functions in Airflow but with the huge benefit that SQL files
    will automatically be turned into dataframes and resulting dataframes can automatically used in astro.sql functions
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=SqlDataframeOperator,
        **{
            "conn_id": conn_id,
            "database": database,
            "schema": schema,
            "warehouse": warehouse,
        }
    )
