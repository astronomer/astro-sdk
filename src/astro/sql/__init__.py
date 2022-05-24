from typing import Callable, Iterable, List, Mapping, Optional, Union

import pandas as pd
from airflow.decorators.base import task_decorator_factory

from astro.sql.operators.append import AppendOperator
from astro.sql.operators.dataframe import DataframeOperator
from astro.sql.operators.export_file import export_file  # noqa: F401
from astro.sql.operators.load_file import load_file  # noqa: F401
from astro.sql.operators.merge import MergeOperator
from astro.sql.operators.transform import transform_decorator  # noqa: F401
from astro.sql.operators.truncate import TruncateOperator
from astro.sql.table import Table


def transform(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "",
    parameters: Optional[Union[Mapping, Iterable]] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
):
    return transform_decorator(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        conn_id=conn_id,
        parameters=parameters,
        database=database,
        schema=schema,
    )


def run_raw_sql(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "",
    parameters: Optional[Union[Mapping, Iterable]] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    handler: Optional[Callable] = None,
):
    return transform_decorator(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        conn_id=conn_id,
        parameters=parameters,
        database=database,
        schema=schema,
        handler=handler,
        raw_sql=True,
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
    target_table: Table,
    merge_table: Table,
    merge_keys: Union[List, dict],
    target_columns: List[str],
    merge_columns: List[str],
    conflict_strategy: str,
    **kwargs,
):
    """`
    Merge two sql tables

    :param target_table: The primary table that we are merging into
    :param merge_table: The table that will be inserted
    :param merge_keys: A key dictionary of what fields we want to compare when determining conflicts.
        ``{"foo": "bar"}`` would be equivalent to ``main_table.foo=merge_table.bar``
    :param target_columns: The columns that will be merged into (order matters and needs to be
    same length as merge_columns
    :param merge_columns:
    :param conn_id: connection ID for SQL instance
    :param conflict_strategy: Do we ignore new values on conflict or overwrite? Two strategies are "ignore" and "update"
    :param database:
    :param schema: Snowflake, specific. Specify Snowflake schema
    :param kwargs:
    :return: None
    :rtype: None
    """

    return MergeOperator(
        target_table=target_table,
        merge_table=merge_table,
        merge_keys=merge_keys,
        target_columns=target_columns,
        merge_columns=merge_columns,
        conflict_strategy=conflict_strategy,
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
