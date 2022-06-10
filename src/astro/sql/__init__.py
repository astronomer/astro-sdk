from typing import Any, Callable, Iterable, List, Mapping, Optional, Union

import pandas as pd

try:
    from airflow.decorators.base import TaskDecorator, task_decorator_factory
except ImportError:
    from airflow.decorators.base import task_decorator_factory
    from airflow.decorators import _TaskDecorator as TaskDecorator

from astro.constants import MergeConflictStrategy
from astro.sql.operators.append import APPEND_COLUMN_TYPE, AppendOperator
from astro.sql.operators.dataframe import DataframeOperator
from astro.sql.operators.export_file import export_file  # noqa: F401
from astro.sql.operators.load_file import load_file  # noqa: F401
from astro.sql.operators.merge import MERGE_COLUMN_TYPE, MergeOperator
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
    **kwargs: Any,
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
    **kwargs: Any,
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
    *,
    source_table: Table,
    target_table: Table,
    columns: APPEND_COLUMN_TYPE = None,
    **kwargs: Any,
):
    """
    Append the source table rows into a destination table.

    :param source_table: Contains the rows to be appended to the target_table (templated)
    :param target_table: Contains the destination table in which the rows will be appended (templated)
    :param columns: List/Tuple of columns if name of source and target tables are same.
        If the column names in source and target tables are different pass a dictionary
        of source_table columns names to target_table columns names.
        Examples: ``["sell", "list"]`` or ``{"s_sell": "t_sell", "s_list": "t_list"}``
    """
    return AppendOperator(
        target_table=target_table,
        source_table=source_table,
        columns=columns,
        **kwargs,
    ).output


def merge(
    *,
    target_table: Table,
    source_table: Table,
    columns: MERGE_COLUMN_TYPE,
    target_conflict_columns: List[str],
    if_conflicts: MergeConflictStrategy,
    **kwargs: Any,
):
    """
    Merge the source table rows into a destination table.

    :param source_table: Contains the rows to be merged to the target_table (templated)
    :param target_table: Contains the destination table in which the rows will be merged (templated)
    :param columns: List/Tuple of columns if name of source and target tables are same.
        If the column names in source and target tables are different pass a dictionary
        of source_table columns names to target_table columns names.
        Examples: ``["sell", "list"]`` or ``{"s_sell": "t_sell", "s_list": "t_list"}``
    :param target_conflict_columns: List of cols where we expect to have a conflict while combining
    :param if_conflicts: The strategy to be applied if there are conflicts.
    """

    return MergeOperator(
        target_table=target_table,
        source_table=source_table,
        columns=columns,
        target_conflict_columns=target_conflict_columns,
        if_conflicts=if_conflicts,
        **kwargs,
    ).output


def truncate(
    table: Table,
    **kwargs: Any,
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
