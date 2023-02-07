from typing import Any, Dict, Optional

from airflow.decorators.base import get_unique_task_id
from airflow.models.xcom_arg import XComArg
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.common.sql.operators.sql import SQLTableCheckOperator

from astro.databases import create_database
from astro.table import BaseTable
from astro.utils.compat.typing import Context


class SQLCheckOperator(SQLTableCheckOperator):
    """
    Performs one or more of the checks provided in the checks dictionary.
    Checks should be written to return a boolean result.

    :param dataset: the table to run checks on
    :param checks: the dictionary of checks, e.g.:

    .. code-block:: python

        {
            "row_count_check": {"check_statement": "COUNT(*) = 1000"},
            "column_sum_check": {"check_statement": "col_a + col_b < col_c"},
        }


    :param partition_clause: a partial SQL statement that is added to a WHERE clause in the query built by
        the operator that creates partition_clauses for the checks to run on, e.g.

    .. code-block:: python

        "date = '1970-01-01'"
    """

    template_fields = ("partition_clause", "dataset")

    def __init__(
        self,
        *,
        dataset: BaseTable,
        checks: Dict[str, Dict[str, Any]],
        partition_clause: Optional[str] = None,
        task_id: Optional[str] = None,
        **kwargs,
    ):
        self.dataset = dataset
        super().__init__(
            table="place_holder_table_name",
            checks=checks,
            partition_clause=partition_clause,
            task_id=task_id or get_unique_task_id("check_table"),
        )

    def execute(self, context: "Context"):
        db = create_database(self.dataset.conn_id)
        self.table = db.get_table_qualified_name(self.dataset)
        self.conn_id = self.dataset.conn_id
        # apache-airflow-providers-common-sql == 1.2.0 which is compatible with airflow 2.2.5 implements the self.sql
        # differently compared to apache-airflow-providers-common-sql == 1.3.3
        try:
            self.sql = f"SELECT check_name, check_result FROM ({self._generate_sql_query()}) AS check_table"
        except AttributeError:
            self.sql = f"SELECT * FROM {self.table};"
        super().execute(context)

    def get_db_hook(self) -> DbApiHook:
        """
        Get the database hook for the connection.

        :return: the database hook object.
        """
        db = create_database(conn_id=self.conn_id)
        if db.sql_type == "bigquery":
            return db.hook
        return super().get_db_hook()


def check_table(
    dataset: BaseTable,
    checks: Dict[str, Dict[str, Any]],
    partition_clause: Optional[str] = None,
    task_id: Optional[str] = None,
    **kwargs,
) -> XComArg:
    """
    Performs one or more of the checks provided in the checks dictionary.
    Checks should be written to return a boolean result.

    :param dataset: the table to run checks on
    :param checks: the dictionary of checks, e.g.:

    .. code-block:: python

        {
            "row_count_check": {"check_statement": "COUNT(*) = 1000"},
            "column_sum_check": {"check_statement": "col_a + col_b < col_c"},
        }


    :param partition_clause: a partial SQL statement that is added to a WHERE clause in the query built by
        the operator that creates partition_clauses for the checks to run on, e.g.

    .. code-block:: python

        "date = '1970-01-01'"
    """
    return SQLCheckOperator(
        dataset=dataset,
        checks=checks,
        partition_clause=partition_clause,
        kwargs=kwargs,
        task_id=task_id,
    ).output
