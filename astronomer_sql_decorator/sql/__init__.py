from typing import Callable, Iterable, List, Mapping, Optional, Union

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from astronomer_sql_decorator.operators.agnostic_load_file import load_file
from astronomer_sql_decorator.operators.postgres_decorator import (
    postgres_append_func,
    postgres_decorator,
)
from astronomer_sql_decorator.operators.snowflake_decorator import snowflake_decorator


def transform(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    postgres_conn_id: Optional[str] = None,
    snowflake_conn_id: Optional[str] = None,
    autocommit: bool = False,
    parameters: Optional[Union[Mapping, Iterable]] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
):
    if postgres_conn_id:
        return postgres_decorator(
            python_callable=python_callable,
            multiple_outputs=multiple_outputs,
            postgres_conn_id=postgres_conn_id,
            autocommit=autocommit,
            parameters=parameters,
            database=database,
        )
    elif snowflake_conn_id:
        return snowflake_decorator(
            python_callable=python_callable,
            multiple_outputs=multiple_outputs,
            snowflake_conn_id=snowflake_conn_id,
            autocommit=autocommit,
            parameters=parameters,
            database=database,
            schema=schema,
            warehouse=warehouse,
        )
    else:
        raise AirflowException(
            f"Please enter a postgres_conn_id or a snowflake_conn_id"
        )


def run_raw_sql(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    postgres_conn_id: Optional[str] = None,
    snowflake_conn_id: Optional[str] = None,
    autocommit: bool = False,
    parameters: Optional[Union[Mapping, Iterable]] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
):
    if postgres_conn_id:
        return postgres_decorator(
            python_callable=python_callable,
            multiple_outputs=multiple_outputs,
            postgres_conn_id=postgres_conn_id,
            autocommit=autocommit,
            parameters=parameters,
            database=database,
            raw_sql=True,
        )
    elif snowflake_conn_id:
        return snowflake_decorator(
            python_callable=python_callable,
            multiple_outputs=multiple_outputs,
            snowflake_conn_id=snowflake_conn_id,
            autocommit=autocommit,
            parameters=parameters,
            database=database,
            schema=schema,
            warehouse=warehouse,
            raw_sql=True,
        )
    else:
        raise AirflowException(
            f"Please enter a postgres_conn_id or a snowflake_conn_id"
        )


def append(
    database: str,
    append_table: str,
    main_table: str,
    postgres_conn_id: Optional[str] = None,
    snowflake_conn_id: Optional[str] = None,
    columns: List[str] = [],
    casted_columns: dict = {},
    **kwargs,
):
    @run_raw_sql(
        postgres_conn_id=postgres_conn_id,
        snowflake_conn_id=snowflake_conn_id,
        database=database,
        **kwargs,
    )
    def append_func(main_table, append_table):
        f = None
        if postgres_conn_id:
            f = postgres_append_func

        if not f:
            raise AirflowException(f"please give a postgres conn id")
        return f(
            main_table=main_table,
            append_table=append_table,
            columns=columns,
            casted_columns=casted_columns,
            postgres_conn_id=postgres_conn_id,
        )

    return append_func(main_table=main_table, append_table=append_table)
