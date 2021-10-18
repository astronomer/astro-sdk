from typing import Callable, Iterable, List, Mapping, Optional, Union

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from astronomer_sql_decorator.operators.agnostic_load_file import load_file
from astronomer_sql_decorator.operators.agnostic_save_file import save_file
from astronomer_sql_decorator.operators.agnostic_sql_append import SqlAppendOperator
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
