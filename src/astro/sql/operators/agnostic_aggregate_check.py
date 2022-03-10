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

from numbers import Number
from typing import Dict, Optional

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from sqlalchemy import text
from sqlalchemy.sql.schema import Table

from astro import sql
from astro.sql.operators.sql_decorator import SqlDecoratedOperator
from astro.utils.task_id_helper import get_unique_task_id


class AgnosticAggregateCheck(SqlDecoratedOperator):
    template_fields = ("table",)

    def __init__(
        self,
        table: Table,
        check: str,
        greater_than: float = None,
        less_than: float = None,
        equal_to: float = None,
        **kwargs
    ):
        """Validate that a table has expected aggregation value.
        Range specified by greater_than and/or less_than is inclusive - [greater_than, less_than] or
        equal_to can be used to check a value.

        :param table: table name
        :type table: str
        :param check: SQL statement
        :type check: str
        :param greater_than: min expected value
        :type greater_than: Number
        :param less_than: max expected value
        :type less_than: Number
        :param equal_to: expected value
        :type equal_to: Number
        :param conn_id: connection id
        :type conn_id: str
        :param database: database name
        :type database: str
        """
        self.table = table
        self.check = check
        self.greater_than = greater_than
        self.less_than = less_than
        self.equal_to = equal_to

        if less_than is None and greater_than is None and equal_to is None:
            raise ValueError(
                "Please provide one or more of these options: less_than, greater_than, equal_to"
            )

        if (
            less_than is not None
            and greater_than is not None
            and less_than < greater_than
        ):
            raise ValueError(
                "less_than should be greater than or equal to greater_than."
            )

        task_id = get_unique_task_id("aggregate_check")

        def null_function():
            pass

        def handler_func(result):
            return result.fetchone()[0]

        super().__init__(
            raw_sql=True,
            parameters={},
            task_id=task_id,
            op_args=(),
            handler=handler_func,
            python_callable=null_function,
            **kwargs,
        )

    def execute(self, context: Dict):
        self.conn_id = self.table.conn_id
        self.database = self.table.database
        self.sql = self.check
        self.parameters = {"table": self.table}
        query_result = super().execute(context)
        if not isinstance(query_result, int) and not isinstance(query_result, float):
            raise ValueError(
                "The aggregate check query should only return a numeric value."
            )

        if self.equal_to is not None and self.equal_to != query_result:
            raise ValueError(
                "Check Failed: query result value {} not equal to {}.".format(
                    query_result, self.equal_to
                )
            )
        elif (
            self.less_than is not None
            and self.greater_than is not None
            and (self.greater_than > query_result or query_result > self.less_than)
        ):
            raise ValueError(
                "Check Failed: query result value {} not in range from {} to {}.".format(
                    query_result, self.greater_than, self.less_than
                )
            )
        elif self.less_than is not None and self.less_than < query_result:
            raise ValueError(
                "Check Failed: query result value {} not less than {}.".format(
                    query_result, self.less_than
                )
            )
        elif self.greater_than is not None and self.greater_than > query_result:
            raise ValueError(
                "Check Failed: query result value {} not greater than {}.".format(
                    query_result, self.greater_than
                )
            )
        return self.table


def aggregate_check(
    table: Table,
    check: str,
    greater_than: float = None,
    less_than: float = None,
    equal_to: float = None,
):
    """
    :param table: table name
    :type table: str
    :param check: SQL statement
    :type check: str
    :param greater_than: min expected rows
    :type greater_than: int
    :param less_than: max expected rows
    :type less_than: int
    :param conn_id: connection id,
    :type conn_id: str
    :param database: database name,
    :type database: str
    """
    return AgnosticAggregateCheck(
        table=table,
        check=check,
        greater_than=greater_than,
        less_than=less_than,
        equal_to=equal_to,
    )
