from typing import Any, Dict, Optional, Union

import pandas
from airflow import AirflowException
from airflow.decorators.base import get_unique_task_id
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator

from astro.databases import create_database
from astro.table import BaseTable
from astro.utils.compat.typing import Context


class ColumnCheckOperator(SQLColumnCheckOperator):
    """
    Performs one or more of the templated checks in the column_checks dictionary.
    Checks are performed on a per-column basis specified by the column_mapping.
    Each check can take one or more of the following options:
    - equal_to: an exact value to equal, cannot be used with other comparison options
    - greater_than: value that result should be strictly greater than
    - less_than: value that results should be strictly less than
    - geq_to: value that results should be greater than or equal to
    - leq_to: value that results should be less than or equal to
    - tolerance: the percentage that the result may be off from the expected value

    :param dataset: the table or dataframe to run checks on
    :param column_mapping: the dictionary of columns and their associated checks, e.g.

    .. code-block:: python

        {
            "col_name": {
                "null_check": {
                    "equal_to": 0,
                },
                "min": {
                    "greater_than": 5,
                    "leq_to": 10,
                    "tolerance": 0.2,
                },
                "max": {"less_than": 1000, "geq_to": 10, "tolerance": 0.01},
            }
        }
    """

    def __init__(
        self,
        dataset: Union[BaseTable, pandas.DataFrame],
        column_mapping: Dict[str, Dict[str, Any]],
        partition_clause: Optional[str] = None,
        task_id: Optional[str] = None,
        **kwargs,
    ):
        for checks in column_mapping.values():
            for check, check_values in checks.items():
                self._column_mapping_validation(check, check_values)

        self.dataset = dataset
        self.column_mapping = column_mapping
        self.partition_clause = partition_clause
        self.kwargs = kwargs
        self.df = None

        dataset_qualified_name = ""
        dataset_conn_id = ""

        if isinstance(dataset, BaseTable):
            db = create_database(conn_id=self.dataset.conn_id)  # type: ignore
            self.conn_id = self.dataset.conn_id
            dataset_qualified_name = db.get_table_qualified_name(table=self.dataset)
            dataset_conn_id = dataset.conn_id

        super().__init__(
            table=dataset_qualified_name,
            column_mapping=self.column_mapping,
            partition_clause=self.partition_clause,
            conn_id=dataset_conn_id,
            task_id=task_id if task_id is not None else get_unique_task_id("check_column"),
        )

    def get_db_hook(self) -> DbApiHook:
        """
        Get the database hook for the connection.

        :return: the database hook object.
        """
        db = create_database(conn_id=self.conn_id)
        if db.sql_type == "bigquery":
            return db.hook
        return super().get_db_hook()

    def execute(self, context: "Context"):
        if isinstance(self.dataset, BaseTable):
            return super().execute(context=context)
        elif isinstance(self.dataset, pandas.DataFrame):
            self.df = self.dataset
        else:
            raise ValueError("dataset can only be of type pandas.dataframe | Table object")

        self.process_checks()

    def get_check_result(self, check_name: str, column_name: str):
        """
        Get the check method results post validating the dataframe
        """
        if self.df is not None and column_name in self.df.columns:
            column_checks = {
                "null_check": lambda column: column.isna().sum(),
                "distinct_check": lambda column: len(column.unique()),
                "unique_check": lambda column: len(column) - len(column.unique()),
                "min": lambda column: column.min(),
                "max": lambda column: column.max(),
            }
            return column_checks[check_name](column=self.df[column_name])
        if self.df is None:
            raise ValueError("Dataframe is None")
        if column_name not in self.df.columns:
            raise ValueError(f"Dataframe doesn't have column {column_name}")

    def process_checks(self):
        """
        Process all the checks and print the result or raise an exception in the event of failed checks
        """
        failed_tests = []
        passed_tests = []

        # Iterating over columns
        for column in self.column_mapping:
            checks = self.column_mapping[column]

            # Iterating over checks
            for check_key, check_val in checks.items():
                tolerance = check_val.get("tolerance")
                result = self.get_check_result(check_key, column_name=column)
                check_val["result"] = result
                check_val["success"] = self._get_match(check_val, result, tolerance)
                failed_tests.extend(_get_failed_checks(checks, column))
                passed_tests.extend(_get_success_checks(checks, column))

        if len(failed_tests) > 0:
            raise AirflowException(f"The following tests have failed: \n{''.join(failed_tests)}")
        if len(passed_tests) > 0:
            print(f"The following tests have passed: \n{''.join(passed_tests)}")


def _get_failed_checks(checks, col=None):
    return [
        f"{get_checks_string(checks, col)} {check_values}\n"
        for check, check_values in checks.items()
        if not check_values["success"]
    ]


def _get_success_checks(checks, col=None):
    return [
        f"{get_checks_string(checks, col)} {check_values}\n"
        for check, check_values in checks.items()
        if check_values["success"]
    ]


def get_checks_string(check, col):
    if col:
        return f"Column: {col}\nCheck: {check},\nCheck Values:"
    return f"\tCheck: {check},\n\tCheck Values:"


def check_column(
    dataset: Union[BaseTable, pandas.DataFrame],
    column_mapping: Dict[str, Dict[str, Any]],
    partition_clause: Optional[str] = None,
    task_id: Optional[str] = None,
    **kwargs,
) -> ColumnCheckOperator:
    """
    Performs one or more of the templated checks in the column_checks dictionary.
    Checks are performed on a per-column basis specified by the column_mapping.
    Each check can take one or more of the following options:
    - equal_to: an exact value to equal, cannot be used with other comparison options
    - greater_than: value that result should be strictly greater than
    - less_than: value that results should be strictly less than
    - geq_to: value that results should be greater than or equal to
    - leq_to: value that results should be less than or equal to
    - tolerance: the percentage that the result may be off from the expected value

    :param dataset: dataframe or BaseTable that has to be validated
    :param column_mapping: the dictionary of columns and their associated checks, e.g.

    .. code-block:: python

        {
            "col_name": {
                "null_check": {
                    "equal_to": 0,
                },
                "min": {
                    "greater_than": 5,
                    "leq_to": 10,
                    "tolerance": 0.2,
                },
                "max": {"less_than": 1000, "geq_to": 10, "tolerance": 0.01},
            }
        }
    """
    return ColumnCheckOperator(
        dataset=dataset,
        column_mapping=column_mapping,
        partition_clause=partition_clause,
        kwargs=kwargs,
        task_id=task_id,
    )
