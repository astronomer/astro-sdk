from typing import Any, Callable, Iterable, List, Mapping, Optional, Union

import pandas as pd

try:
    from airflow.decorators.base import TaskDecorator, task_decorator_factory
except ImportError:
    from airflow.decorators.base import task_decorator_factory
    from airflow.decorators import _TaskDecorator as TaskDecorator

from astro.constants import MergeConflictStrategy
from astro.sql.operators.append import APPEND_COLUMN_TYPE, AppendOperator
from astro.sql.operators.cleanup import CleanupOperator
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
    """
    Given a python function that returns a SQL statement and (optional) tables, execute the SQL statement and output
    the result into a SQL table.

    Use this function as a decorator like so:

      @transform
      def my_sql_statement(table1: Table, table2: Table) -> Table:
          return "SELECT * FROM {{table1}} JOIN {{table2}}"

    In this example, by identifying the parameters as `Table` objects, astro knows to automatically convert those
    objects into tables (if they are, for example, a dataframe). Any type besides table will lead astro to assume
    you do not want the parameter converted.

    :param python_callable:
    :param multiple_outputs:
    :param conn_id:
    :param parameters:
    :param database:
    :param schema:
    :param handler:
    :param kwargs:
    :return: Transform functions return a `Table` object that can be passed to future tasks. This table will be
    either an auto-generated temporary table, or will overwrite a table given in the `output_table` parameter.
    """

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
    """
    Given a python function that returns a SQL statement and (optional) tables, execute the SQL statement and output
    the result into a SQL table.

    Use this function as a decorator like so:

      @transform
      def my_sql_statement(table1: Table) -> Table:
          return "DROP TABLE {{table1}}"

    In this example, by identifying parameters as `Table` objects, astro knows to automatically convert those
    objects into tables (if they are, for example, a dataframe). Any type besides table will lead astro to assume
    you do not want the parameter converted.

    Please note that the `run_raw_sql function will not create a temporary table. It will either return the result of a
    provided `handler` function or it will not return anything at all.


    :param python_callable:
    :param multiple_outputs:
    :param conn_id:
    :param parameters:
    :param database:
    :param schema:
    :param handler:
    :param kwargs:
    :return:
    """

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


def cleanup(tables_to_cleanup: Optional[List[Table]] = None, **kwargs):
    """
    Clean up temporary tables once either the DAG or upstream tasks are done

    The cleanup operator allows for two possible scenarios: Either a user wants to clean up a specific set of tables
    during the DAG run, or the user wants to ensure that all temporary tables are deleted once the DAG run is finished.
    The idea here is to ensure that even if a user doesn't have access to a "temp" schema, that astro does not leave
    hanging tables once execution is done.

    :param tables_to_cleanup: A list of tables to cleanup, defaults to waiting for all upstream tasks to finish
    :param kwargs:
    :return:
    """
    return CleanupOperator(tables_to_cleanup=tables_to_cleanup, **kwargs)


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
    """
    Truncate a table.

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
    This decorator will allow users to write python functions while treating SQL tables as dataframes

    This decorator allows a user to run python functions in Airflow but with the huge benefit that SQL tables
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
