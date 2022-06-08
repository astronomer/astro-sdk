from typing import Callable, Dict, Iterable, List, Mapping, Optional, Union

import pandas as pd

try:
    from airflow.decorators.base import TaskDecorator, task_decorator_factory
except ImportError:
    from airflow.decorators.base import task_decorator_factory
    from airflow.decorators import _TaskDecorator as TaskDecorator

from astro.constants import MergeConflictStrategy
from astro.sql.operators.append import AppendOperator
from astro.sql.operators.dataframe import DataframeOperator
from astro.sql.operators.export_file import export_file  # noqa: F401
from astro.sql.operators.load_file import load_file  # noqa: F401
from astro.sql.operators.merge import MergeOperator
from astro.sql.operators.raw_sql import RawSQLOperator
from astro.sql.operators.transform import TransformOperator  # noqa: F401
from astro.sql.operators.truncate import TruncateOperator
from astro.sql.table import Table


def transform(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "",
    parameters: Optional[Union[Mapping, Iterable]] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    handler: Optional[Callable] = None,
    **kwargs,
) -> TaskDecorator:

    kwargs.update(
        {
            "conn_id": conn_id,
            "parameters": parameters,
            "database": database,
            "schema": schema,
            "handler": handler,
        }
    )
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=TransformOperator,
        **kwargs,
    )


def run_raw_sql(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "",
    parameters: Optional[Union[Mapping, Iterable]] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    handler: Optional[Callable] = None,
    **kwargs,
) -> TaskDecorator:

    kwargs.update(
        {
            "conn_id": conn_id,
            "parameters": parameters,
            "database": database,
            "schema": schema,
            "handler": handler,
        }
    )
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=RawSQLOperator,
        **kwargs,
    )


def append(
    append_table: Table,
    main_table: Table,
    columns: Optional[List[str]] = None,
    casted_columns: Optional[dict] = None,
    **kwargs,
):
    if columns is None:
        columns = []
    if casted_columns is None:
        casted_columns = {}
    return AppendOperator(
        main_table=main_table,
        append_table=append_table,
        columns=columns,
        casted_columns=casted_columns,
        **kwargs,
    ).output


def merge(
    *,
    target_table: Table,
    source_table: Table,
    source_to_target_columns_map: Dict[str, str],
    target_conflict_columns: List[str],
    if_conflicts: MergeConflictStrategy,
    **kwargs,
):
    """
    Merge the source table rows into a destination table.

    :param source_table: Contains the rows to be merged to the target_table (templated)
    :param target_table: Contains the destination table in which the rows will be merged (templated)
    :param source_to_target_columns_map: Dict of target_table columns names to source_table columns names
    :param target_conflict_columns: List of cols where we expect to have a conflict while combining
    :param if_conflicts: The strategy to be applied if there are conflicts.
    """

    return MergeOperator(
        target_table=target_table,
        source_table=source_table,
        source_to_target_columns_map=source_to_target_columns_map,
        target_conflict_columns=target_conflict_columns,
        if_conflicts=if_conflicts,
    ).output


def truncate(
    table: Table,
    **kwargs,
) -> TruncateOperator:
    """`
    :param table: Table to be truncated
    :param kwargs:
    """

    return TruncateOperator(table=table, **kwargs)


def dataframe(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "",
    database: Optional[str] = None,
    schema: Optional[str] = None,
    task_id: Optional[str] = None,
    identifiers_as_lower: Optional[bool] = True,
) -> Callable[..., pd.DataFrame]:
    """
    This function allows a user to run python functions in Airflow but with the huge benefit that SQL files
    will automatically be turned into dataframes and resulting dataframes can automatically used in astro.sql functions
    """
    param_map = {
        "conn_id": conn_id,
        "database": database,
        "schema": schema,
        "identifiers_as_lower": identifiers_as_lower,
    }
    if task_id:
        param_map["task_id"] = task_id
    decorated_function: Callable[..., pd.DataFrame] = task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=DataframeOperator,  # type: ignore
        **param_map,
    )
    return decorated_function
