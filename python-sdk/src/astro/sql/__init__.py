from airflow.configuration import conf
from airflow.decorators.base import get_unique_task_id
from airflow.models.xcom_arg import XComArg

from astro.sql.operators.append import AppendOperator, append
from astro.sql.operators.cleanup import CleanupOperator, cleanup
from astro.sql.operators.dataframe import DataframeOperator, dataframe
from astro.sql.operators.drop import DropTableOperator, drop_table
from astro.sql.operators.export_file import ExportFileOperator, export_file
from astro.sql.operators.load_file import LoadFileOperator, load_file
from astro.sql.operators.merge import MergeOperator, merge
from astro.sql.operators.raw_sql import RawSQLOperator, run_raw_sql
from astro.sql.operators.transform import TransformOperator, transform, transform_file
from astro.sql.table import Metadata, Table


def get_value_list(sql_statement: str, conn_id: str, **kwargs) -> XComArg:
    """
    Execute a sql statement and return the result

    :param sql_statement: sql query to execute.
        If the sql query will return huge number of row then it can overload the XCOM.
        also, If you are using output of this method to expand a task using dynamic task map then
        it can create lots of parallel task. So it is advisible to limit your sql query statement.
    :param conn_id: Airflow connection id. This connection id will be used to identify the database client
        and connect with it at runtime
    """
    handler = kwargs.get("handler") or (lambda result_set: result_set.fetchall())
    max_map_length = conf.get(section="core", key="max_map_length")
    op_kwargs = {
        "handler": handler,
        "response_limit": max_map_length,
    }
    task_id = kwargs.get("task_id") or get_unique_task_id(
        "get_value_list", dag=kwargs.get("dag"), task_group=kwargs.get("task_group")
    )
    kwargs.update({"task_id": task_id})
    return RawSQLOperator(
        sql=sql_statement,
        conn_id=conn_id,
        op_kwargs=op_kwargs,
        python_callable=(lambda *args: None),
        **kwargs
    ).output
