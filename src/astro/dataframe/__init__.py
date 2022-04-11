from typing import Any, Callable, Optional, TypeVar, Union

from airflow.decorators.base import task_decorator_factory

from astro.sql.operators.sql_dataframe import SqlDataframeOperator

T = TypeVar("T", bound=Callable)


def dataframe(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "",
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
    task_id: Optional[str] = None,
    identifiers_as_lower: Optional[bool] = True,
) -> Union[Callable[[T], T], Any]:
    """
    This function allows a user to run python functions in Airflow but with the huge benefit that SQL files
    will automatically be turned into dataframes and resulting dataframes can automatically used in astro.sql functions
    """
    param_map = {
        "conn_id": conn_id,
        "database": database,
        "schema": schema,
        "warehouse": warehouse,
        "identifiers_as_lower": identifiers_as_lower,
    }
    if task_id:
        param_map["task_id"] = task_id
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=SqlDataframeOperator,  # type: ignore
        **param_map,
    )
