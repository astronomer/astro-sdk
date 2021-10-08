from typing import Callable, Iterable, List, Mapping, Optional, Union

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from astronomer_sql_decorator.operators.agnostic_load_file import load_file
from astronomer_sql_decorator.operators.agnostic_save_file import save_file
from astronomer_sql_decorator.operators.sql_decorator import transform_decorator
from astronomer_sql_decorator.utils.postgres_append import postgres_append_func
from astronomer_sql_decorator.utils.snowflake_append import snowflake_append_func


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
    @run_raw_sql(
        conn_id=conn_id,
        database=database,
        **kwargs,
    )
    def append_func(main_table, append_table):
        conn_type = BaseHook.get_connection(conn_id).conn_type
        if conn_type == "postgres":
            return postgres_append_func(
                main_table=main_table,
                append_table=append_table,
                columns=columns,
                casted_columns=casted_columns,
                conn_id=conn_id,
            )
        elif conn_type == "snowflake":
            return snowflake_append_func(
                main_table=main_table,
                append_table=append_table,
                columns=columns,
                casted_columns=casted_columns,
                snowflake_conn_id=conn_id,
            )
        else:
            raise AirflowException(f"please give a postgres conn id")

    return append_func(main_table=main_table, append_table=append_table)
