from typing import Any, Dict, Optional, Union

import pandas
from airflow.models.xcom_arg import XComArg
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator

from astro.databases import create_database
from astro.files import File
from astro.table import BaseTable
from astro.utils.typing_compat import Context


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

    :param table: the table to run checks on
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
        dataset: Union[BaseTable, pandas.DataFrame, File],
        column_mapping: Dict[str, Dict[str, Any]],
        partition_clause: Optional[str] = None,
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
        if type(dataset) == BaseTable:
            db = create_database(conn_id=self.dataset.conn_id)  # type: ignore
            super().__init__(
                table=db.get_table_qualified_name(table=self.dataset),
                column_mapping=self.column_mapping,
                partition_clause=self.partition_clause,
                conn_id=dataset.conn_id,
                database=dataset.metadata.database,
            )

    def execute(self, context: "Context"):
        if type(self.dataset) == BaseTable:
            return super().execute(context=context)
        # elif type(self.dataset) == File:
        #     self.df = self.dataset.export_to_dataframe()
        elif type(self.dataset) == pandas.DataFrame:
            self.df = self.dataset
        else:
            raise ValueError("dataset can only be of type File | pandas.dataframe | Table object")

        self.process_checks()

    def process_checks(self):
        column_checks = {
            "null_check": self.col_null_check,
            "distinct_check": self.col_distinct_check,
            "unique_check": self.col_unique_check,
            "min": self.col_max,
            "max": self.col_min,
        }
        failed_tests = []
        for column in self.column_mapping:
            checks = self.column_mapping[column]
            for check in checks:
                tolerance = self.column_mapping[column][check].get("tolerance")
                result = column_checks[check](column_name=column)
                self.column_mapping[column][check]["result"] = result
                self.column_mapping[column][check]["success"] = self._get_match(
                    self.column_mapping[column][check], result, tolerance
                )
                failed_tests.extend(_get_failed_checks(self.column_mapping[column], column))
        if failed_tests:
            pass
            # raise AirflowException(
            #     f"Test failed.\nResults:\n{records!s}\n"
            #     "The following tests have failed:"
            #     f"\n{''.join(failed_tests)}"
            # )

    def col_null_check(self, column_name: str) -> list:
        if self.df is not None and column_name in self.df.columns:
            return self.df[column_name].isnull().values.any()
        return []

    def col_distinct_check(self, column_name: str) -> list:
        if self.df is not None and column_name in self.df.columns:
            return self.df[column_name].unique()
        return []

    def col_unique_check(self, column_name: str) -> Optional[bool]:
        if self.df is not None and column_name in self.df.columns:
            return len(self.df[column_name].unique()) == 1
        return None

    def col_max(self, column_name: str) -> Optional[float]:
        if self.df is not None and column_name in self.df.columns:
            return self.df[column_name].max()
        return None

    def col_min(self, column_name: str) -> Optional[float]:
        if self.df is not None and column_name in self.df.columns:
            return self.df[column_name].min()
        return None


def _get_failed_checks(checks, col=None):
    if col:
        return [
            f"Column: {col}\nCheck: {check},\nCheck Values: {check_values}\n"
            for check, check_values in checks.items()
            if not check_values["success"]
        ]
    return [
        f"\tCheck: {check},\n\tCheck Values: {check_values}\n"
        for check, check_values in checks.items()
        if not check_values["success"]
    ]


def column_check(
    dataset: Union[BaseTable, pandas.DataFrame, File],
    column_mapping: Dict[str, Dict[str, Any]],
    partition_clause: Optional[str] = None,
    **kwargs,
) -> XComArg:
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

    :param table: the table to run checks on
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
    ).output
