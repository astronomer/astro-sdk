import pathlib

import pandas as pd
import pytest
from airflow import AirflowException

from astro import sql as aql
from tests.sql.operators import utils as test_utils

CWD = pathlib.Path(__file__).parent

df = pd.DataFrame(
    data={
        "name": ["Dwight Schrute", "Michael Scott", "Jim Halpert"],
        "age": [30, None, None],
        "city": [None, "LA", "California City"],
        "emp_id": [10, 1, 35],
    }
)


def test_column_check_operator_with_null_checks(sample_dag):
    """
    Test column_check_operator for null_check case
    """
    with sample_dag:
        aql.check_column(
            dataset=df,
            column_mapping={
                "name": {"null_check": {"geq_to": 0, "leq_to": 1}},
                "city": {
                    "null_check": {
                        "equal_to": 1,
                    },
                },
                "age": {
                    "null_check": {
                        "equal_to": 1,
                        "tolerance": 1,  # Tolerance is + and - the value provided. Acceptable values is 0 to 2.
                    },
                },
            },
        )
    test_utils.run_dag(sample_dag)


def test_failure_of_column_check_operator_with_null_checks__equal_to(sample_dag):
    """
    Test that failure column_check_operator for null_check
    """
    with sample_dag, pytest.raises(AirflowException) as e:
        aql.check_column(
            dataset=df,
            column_mapping={
                "city": {
                    "null_check": {
                        "equal_to": 0,
                    },
                },
            },
        )
        test_utils.run_dag(sample_dag)
    assert "Check Values: {'equal_to': 0, 'result': 1, 'success': False}" in str(e.value)


def test_failure_of_column_check_operator_with_null_checks__geq_to_and_leq_to(sample_dag):
    """
    Test that failure column_check_operator for null_check with geq_to and leq_to
    """
    with sample_dag, pytest.raises(AirflowException) as e:
        aql.ColumnCheckOperator(
            dataset=df,
            column_mapping={"name": {"null_check": {"geq_to": 1, "leq_to": 2}}},
        )
        test_utils.run_dag(sample_dag)
    assert "Check Values: {'geq_to': 1, 'leq_to': 2, 'result': 0, 'success': False}" in str(e.value)


def test_failure_of_column_check_operator_with_null_checks__equal_to_with_tolerance(sample_dag):
    """
    Test that failure column_check_operator for null_check with equal_to and tolerance
    """
    with sample_dag, pytest.raises(AirflowException) as e:
        aql.ColumnCheckOperator(
            dataset=df,
            column_mapping={
                "age": {
                    "null_check": {
                        "equal_to": 0,
                        "tolerance": 1,  # Tolerance is + and - the value provided. Acceptable values is 0 to 0.
                    },
                }
            },
        )
        test_utils.run_dag(sample_dag)
    assert "Check Values: {'equal_to': 0, 'tolerance': 1, 'result': 2, 'success': False}" in str(e.value)


def test_column_check_operator_with_distinct_checks(sample_dag):
    """
    Test column_check_operator for distinct_check case
    """
    with sample_dag:
        aql.ColumnCheckOperator(
            dataset=df,
            column_mapping={
                "name": {
                    "distinct_check": {
                        "equal_to": 3,
                    }
                },
                "city": {
                    "distinct_check": {"geq_to": 2, "leq_to": 3},  # Nulls are treated as values
                },
                "age": {
                    "distinct_check": {
                        "equal_to": 1,
                        "tolerance": 1,  # Tolerance is + and - the value provided. Acceptable values is 0 to 2.
                    },
                },
            },
        )
    test_utils.run_dag(sample_dag)


def test_failure_of_column_check_operator_with_distinct_checks__equal_to(sample_dag):
    """
    Test that failure column_check_operator for distinct_check
    """
    with sample_dag, pytest.raises(AirflowException) as e:
        aql.ColumnCheckOperator(
            dataset=df,
            column_mapping={
                "city": {
                    "distinct_check": {
                        "equal_to": 0,
                    },
                },
            },
        )
        test_utils.run_dag(sample_dag)
    assert "Check Values: {'equal_to': 0, 'result': 3, 'success': False}" in str(e.value)


def test_failure_of_column_check_operator_with_distinct_checks__geq_to_and_leq_to(sample_dag):
    """
    Test that failure column_check_operator for distinct_check with geq_to and leq_to
    """
    with sample_dag, pytest.raises(AirflowException) as e:
        aql.ColumnCheckOperator(
            dataset=df,
            column_mapping={"name": {"distinct_check": {"geq_to": 1, "leq_to": 2}}},
        )
        test_utils.run_dag(sample_dag)
    assert "Check Values: {'geq_to': 1, 'leq_to': 2, 'result': 3, 'success': False}" in str(e.value)


def test_failure_of_column_check_operator_with_distinct_check__equal_to_with_tolerance(sample_dag):
    """
    Test that failure column_check_operator for distinct_check with equal_to and tolerance
    """
    with sample_dag, pytest.raises(AirflowException) as e:
        aql.ColumnCheckOperator(
            dataset=df,
            column_mapping={
                "age": {
                    "distinct_check": {
                        "equal_to": 0,
                        "tolerance": 1,  # Tolerance is + and - the value provided. Acceptable values is 0 to 0.
                    },
                }
            },
        )
        test_utils.run_dag(sample_dag)
    assert "Check Values: {'equal_to': 0, 'tolerance': 1, 'result': 2, 'success': False}" in str(e.value)


def test_column_check_operator_with_unique_check(sample_dag):
    """
    Test column_check_operator for unique_check case
    """
    with sample_dag:
        aql.ColumnCheckOperator(
            dataset=df,
            column_mapping={
                "name": {
                    "unique_check": {
                        "equal_to": 0,
                    }
                },
                "city": {
                    "unique_check": {"geq_to": 0, "leq_to": 1},  # Nulls are treated as values
                },
                "age": {
                    "unique_check": {
                        "equal_to": 1,
                        "tolerance": 1,  # Tolerance is + and - the value provided. Acceptable values is 0 to 2.
                    },
                },
            },
        )
        test_utils.run_dag(sample_dag)


def test_column_check_operator_with_max_min_check(sample_dag):
    """
    Test column_check_operator for max_min_check
    """
    with sample_dag:
        aql.ColumnCheckOperator(
            dataset=df,
            column_mapping={
                "emp_id": {
                    "min": {
                        "geq_to": 1,
                    }
                },
                "age": {
                    "max": {
                        "leq_to": 100,
                    },
                },
            },
        )
    test_utils.run_dag(sample_dag)


def test_failure_of_column_check_operator_with_max_check(sample_dag):
    """
    Test that failure column_check_operator for max_check
    """
    with sample_dag, pytest.raises(AirflowException) as e:
        aql.ColumnCheckOperator(
            dataset=df,
            column_mapping={
                "age": {
                    "max": {
                        "leq_to": 20,
                    },
                }
            },
        )
        test_utils.run_dag(sample_dag)
    assert "Check Values: {'leq_to': 20, 'result': 30.0, 'success': False}" in str(e.value)


def test_failure_of_column_check_operator_with_min_check(sample_dag):
    """
    Test that failure column_check_operator for min_check
    """
    with sample_dag, pytest.raises(AirflowException) as e:
        aql.ColumnCheckOperator(
            dataset=df,
            column_mapping={
                "age": {
                    "min": {
                        "geq_to": 50,
                    },
                }
            },
        )
        test_utils.run_dag(sample_dag)
    assert "Check Values: {'geq_to': 50, 'result': 30.0, 'success': False}" in str(e.value)
