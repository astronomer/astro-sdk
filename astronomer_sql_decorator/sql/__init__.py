from typing import Callable, Iterable, Mapping, Optional, Union

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from astronomer_sql_decorator.operators.postgres_decorator import postgres_decorator


def transform(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "postgres_default",
    autocommit: bool = False,
    parameters: Optional[Union[Mapping, Iterable]] = None,
    database: Optional[str] = None,
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
