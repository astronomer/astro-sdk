from typing import Callable, Iterable, List, Mapping, Optional, Union

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from astronomer_sql_decorator.operators.agnostic_load_file import load_file
from astronomer_sql_decorator.operators.agnostic_save_file import save_file
from astronomer_sql_decorator.operators.agnostic_sql_append import SqlAppendOperator
from astronomer_sql_decorator.operators.agnostic_sql_merge import SqlMergeOperator
from astronomer_sql_decorator.operators.sql_decorator import transform_decorator


def transform(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "",
    autocommit: bool = False,
    parameters: Optional[Union[Mapping, Iterable]] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
):
    return transform_decorator(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        conn_id=conn_id,
        autocommit=autocommit,
        parameters=parameters,
        database=database,
        schema=schema,
        warehouse=warehouse,
    )


def run_raw_sql(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "",
    autocommit: bool = False,
    parameters: Optional[Union[Mapping, Iterable]] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
):
    return transform_decorator(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        conn_id=conn_id,
        autocommit=autocommit,
        parameters=parameters,
        database=database,
        schema=schema,
        warehouse=warehouse,
        raw_sql=True,
    )


def append(
    database: str,
    append_table: str,
    main_table: str,
    conn_id: str = "",
    columns: List[str] = [],
    casted_columns: dict = {},
    **kwargs,
):
    return SqlAppendOperator(
        main_table=main_table,
        append_table=append_table,
        conn_id=conn_id,
        columns=columns,
        casted_columns=casted_columns,
        database=database,
        **kwargs,
    )


def merge(
    target_table: str,
    merge_table: str,
    merge_keys: Union[List, dict],
    target_columns: List[str],
    merge_columns: List[str],
    conn_id: str,
    conflict_strategy: str,
    database: str = "",
    schema: str = "",
    warehouse: str = "",
    **kwargs,
):
    """`
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
    :return:
    """

    return SqlMergeOperator(
        target_table=target_table,
        merge_table=merge_table,
        merge_keys=merge_keys,
        target_columns=target_columns,
        merge_columns=merge_columns,
        conflict_strategy=conflict_strategy,
        conn_id=conn_id,
        database=database,
        schema=schema,
        warehouse=warehouse,
    )
