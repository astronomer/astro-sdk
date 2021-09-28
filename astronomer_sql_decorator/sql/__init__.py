from typing import Callable, Iterable, List, Mapping, Optional, Union

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from astronomer_sql_decorator.operators.postgres_decorator import (
    postgres_append_func,
    postgres_decorator,
)
from astronomer_sql_decorator.operators.snowflake_decorator import snowflake_decorator


def transform(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "postgres_default",
    autocommit: bool = False,
    parameters: Optional[Union[Mapping, Iterable]] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
    from_s3: bool = False,
    from_csv: bool = False,
    to_s3: bool = False,
    to_csv: bool = False,
):
    conn_type = BaseHook.get_connection(conn_id).conn_type
    if conn_type == "postgres":
        return postgres_decorator(
            python_callable=python_callable,
            multiple_outputs=multiple_outputs,
            postgres_conn_id=conn_id,
            autocommit=autocommit,
            parameters=parameters,
            database=database,
            from_s3=from_s3,
            from_csv=from_csv,
            to_s3=to_s3,
            to_csv=to_csv,
        )
    elif conn_type == "snowflake":
        return snowflake_decorator(
            python_callable=python_callable,
            multiple_outputs=multiple_outputs,
            snowflake_conn_id=conn_id,
            autocommit=autocommit,
            parameters=parameters,
            database=database,
            schema=schema,
            warehouse=warehouse,
        )
    else:
        raise AirflowException(f"Connection type {conn_type} is not supported")


def run_raw_sql(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "postgres_default",
    autocommit: bool = False,
    parameters: Optional[Union[Mapping, Iterable]] = None,
    database: Optional[str] = None,
):
    conn_type = BaseHook.get_connection(conn_id).conn_type
    if conn_type == "postgres":
        return postgres_decorator(
            python_callable=python_callable,
            multiple_outputs=multiple_outputs,
            postgres_conn_id=conn_id,
            autocommit=autocommit,
            parameters=parameters,
            database=database,
            raw_sql=True,
        )
    else:
        raise AirflowException(f"Connection type {conn_type} is not supported")


def append(
    conn_id: str,
    database: str,
    append_table: str,
    main_table: str,
    columns: List[str] = [],
    casted_columns: dict = {},
    **kwargs,
):
    @run_raw_sql(conn_id=conn_id, database=database, **kwargs)
    def append_func(main_table, append_table):
        conn_type = BaseHook.get_connection(conn_id).conn_type

        f = {"postgres": postgres_append_func}.get(conn_type, None)

        if not f:
            raise AirflowException(f"conn_type {conn_type} not supported")
        return f(
            main_table=main_table,
            append_table=append_table,
            columns=columns,
            casted_columns=casted_columns,
            conn_id=conn_id,
        )

    return append_func(main_table=main_table, append_table=append_table)
