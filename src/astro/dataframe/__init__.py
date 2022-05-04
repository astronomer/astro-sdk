from typing import Any, Callable, Dict, Optional

import pandas as pd
from airflow.decorators.base import task_decorator_factory

from astro.sql.operators.sql_dataframe import SqlDataframeOperator


def dataframe(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    task_id: Optional[str] = None,
    identifiers_as_lower: Optional[bool] = True,
    **kwargs
) -> Callable[..., pd.DataFrame]:
    """
    This function allows a user to run python functions in Airflow but with the huge benefit that SQL files
    will automatically be turned into dataframes and resulting dataframes can automatically used in astro.sql functions
    """
    param_map: Dict[str, Any] = {
        "identifiers_as_lower": identifiers_as_lower,
    }
    if task_id:
        param_map["task_id"] = task_id
    df_class = SqlDataframeOperator
    if kwargs.get("_experimental", False):
        from astro.sql.operators.sql_dataframe_refactor import (
            SqlDataframeOperator as DFNew,
        )

        df_class = DFNew
        kwargs.pop("_experimental")
    decorated_function: Callable[..., pd.DataFrame] = task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=df_class,  # type: ignore
        **param_map,
    )
    return decorated_function
