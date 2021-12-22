from distutils import log as logger
from os import name, stat
from typing import Dict, List

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql
from sqlalchemy.sql.expression import table
from sqlalchemy.sql.functions import Function
from sqlalchemy.sql.schema import Table

from astro.sql.operators.sql_decorator import SqlDecoratoratedOperator
from astro.sql.table import Table
from astro.sql.util.snowflake_merge_func import is_valid_snow_identifier


class OutlierCheck:
    def __init__(
        self,
        name: str,
        main_table_column: str,
        compare_table_column: str,
        accepted_std_div: int,
        threshold: float,
    ) -> None:
        self.name = name
        self.main_column = main_table_column
        self.compare_table_column = compare_table_column
        self.accepted_std_div = accepted_std_div
        self.threshold = threshold


class AgnosticStatsCheck(SqlDecoratoratedOperator):
    def __init__(
        self,
        checks: List[OutlierCheck],
        main_table: Table,
        compare_table: Table,
        max_rows_returned: int,
        **kwargs
    ):
        """
        :param table: table name
        :type table: str
        :param checks: check class object, which represent boolean expression
        :type checks: Check
        :param max_rows_returned: number of row returned if the check fails.
        :type max_rows_returned: int
        """

        self.main_table = main_table
        self.compare_table = compare_table
        self.max_rows_returned = max_rows_returned
        self.conn_id = main_table.conn_id
        self.checks = checks
        self.database = main_table.database

        task_id = main_table.table_name + "_" + "stats_check"

        def null_function():
            pass

        super().__init__(
            raw_sql=True,
            parameters={},
            conn_id=self.conn_id,
            database=self.database,
            schema=main_table.schema,
            warehouse=main_table.warehouse,
            task_id=task_id,
            op_args=(),
            python_callable=null_function,
            **kwargs,
        )

    def execute(self, context: Dict):
        pass


def stats_check(
    main_table: Table,
    compare_table: Table,
    checks: List[OutlierCheck] = [],
    max_rows_returned: int = 100,
):
    """
    :param main_table: table name
    :type main_table: str
    :param compare_table: table name
    :type compare_table: str
    :param checks: check class object, which represent boolean expression
    :type checks: Check
    :param max_rows_returned: number of row returned if the check fails.
    :type max_rows_returned: int
    """

    return AgnosticStatsCheck(
        main_table=table,
        compare_table=compare_table,
        checks=checks,
        max_rows_returned=max_rows_returned,
    )
