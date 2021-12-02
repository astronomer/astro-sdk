from typing import Callable, Optional

from astro.dataframe import dataframe


def train(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "",
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
):
    """
    For now this will have the same functionality as dataframe, but eventually we will add features
    that will optimize this experience for training models
    """
    return dataframe(
        python_callable,
        multiple_outputs,
        conn_id,
        database,
        schema,
        warehouse,
    )


def predict(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "",
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
):
    """
    For now this will have the same functionality as dataframe, but eventually we will add features
    that will optimize this experience for deploying inference
    """
    return dataframe(
        python_callable,
        multiple_outputs,
        conn_id,
        database,
        schema,
        warehouse,
    )
