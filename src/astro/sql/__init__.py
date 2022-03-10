"""
Copyright Astronomer, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from typing import Callable, Iterable, List, Mapping, Optional, Union

from astro.sql.operators.agnostic_aggregate_check import aggregate_check
from astro.sql.operators.agnostic_boolean_check import boolean_check
from astro.sql.operators.agnostic_load_file import load_file
from astro.sql.operators.agnostic_save_file import save_file
from astro.sql.operators.agnostic_sql_append import SqlAppendOperator
from astro.sql.operators.agnostic_sql_merge import SqlMergeOperator
from astro.sql.operators.agnostic_sql_truncate import SqlTruncateOperator
from astro.sql.operators.agnostic_stats_check import OutlierCheck, stats_check
from astro.sql.operators.sql_decorator import SqlDecoratedOperator, transform_decorator
from astro.sql.parsers.sql_directory_parser import render
from astro.sql.table import Table
from astro.utils.task_id_helper import get_task_id


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
    handler: Callable = None,
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
        handler=handler,
        raw_sql=True,
    )


def append(
    append_table: Table,
    main_table: Table,
    columns: List[str] = [],
    casted_columns: dict = {},
    **kwargs,
):
    return SqlAppendOperator(
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
    ).output


def truncate(
    table: Table,
    **kwargs,
):
    """`
    :param table: The table that we will truncate
    :param database:
    :param schema: Snowflake, specific. Specify Snowflake schema
    :param kwargs:
    :return:
    """

    return SqlTruncateOperator(table=table)
